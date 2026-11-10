"""Every collection x every index kind, driven by random mutation, persistence and reload (Task 4.4).

The maintainer's requirement: all collections are indexed and queries can always use
the available indexes.  ``tests/contracts/index_contract.py`` (check f) attaches an
index to every collection at fixed points; this module is the stateful layer on top.
For each combination of

* collection kind (every entry of ``COLLECTION_FACTORIES``),
* index kind (``EqualityIndex``, ``RangeIndex``),
* storage (``MemoryStorage``, ``FileStorage`` in a temporary directory), and
* key kind (the element itself, the record path ``a``, the dotted path ``a.b`` over
  records where ``a`` or ``b`` is sometimes missing, ``None`` or of a foreign type),

a Hypothesis state machine applies random mutations, adds, removes and re-adds the
index on non-empty collections, saves and reloads through the storage (for
``FileStorage`` the storage is closed and reopened and the root pointer reloaded),
and keeps old versions to prove they never change.  After every step the index
must ``validate`` against the collection's elements, and every supported term drawn
from the current and previously seen keys must give exactly the scan result (and
``ordered_plan`` must give the sorted scan for ``RangeIndex``).

Catches: an index that goes stale in one mutator of one collection, an index lost
or duplicated by save/reload, a lazily loaded index answering differently from the
in-memory one, an old version affected by later mutations, and key extraction
failing on missing or foreign-typed attributes.
"""
import random
import shutil
import tempfile
from collections import Counter

import pytest
from hypothesis import HealthCheck, settings, strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoNotSupportedError
from proto_db.indexing import KeySpec
from proto_db.indexing.equality import EqualityIndex
from proto_db.indexing.range import RangeIndex
from proto_db.query.plans import QueryPlan
from proto_db.storage.file import FileStorage
from proto_db.storage.memory import MemoryStorage
from proto_db.values import MISSING
from tests.contracts.collection_contract import COLLECTION_FACTORIES
from tests.contracts.index_contract import IndexContract, exact_value_key, mixed_value
from tests.support.persistence import DirectContext

INDEX_NAME = "idx"
#: Cap on remembered elements used to draw terms: bounds the cost of a step.
SEEN_LIMIT = 40
PROFILE = settings(max_examples=40, stateful_step_count=40, deadline=None, database=None, derandomize=True,
                   suppress_health_check=list(HealthCheck))


# -- record generation -----------------------------------------------------------------------

def make_record(rng: random.Random, dotted: bool) -> DBObject:
    """A record keyed by ``a`` (or ``a.b``): the key is sometimes missing, ``None`` or of a foreign type."""
    attrs: dict = {"n": rng.randrange(0, 5)}
    if dotted:
        r = rng.random()
        if r < 0.55:
            attrs["a"] = DBObject(b=mixed_value(rng))
        elif r < 0.65:
            attrs["a"] = DBObject(c=1)          # a present, b missing
        elif r < 0.75:
            attrs["a"] = None
        elif r < 0.85:
            attrs["a"] = rng.choice([5, "x", (1, 2)])   # a is not a record
        # else: a missing
    elif rng.random() < 0.85:
        attrs["a"] = mixed_value(rng)
    return DBObject(**attrs)


# -- record-aware mutators --------------------------------------------------------------------
# Same shape as the registered mutators: (collection, rng, new_element) -> new collection or None.

def _members(coll):
    return list(coll.elements())


def _list_mutators():
    def append_last(c, r, new):
        return c.append_last(new)

    def append_first(c, r, new):
        return c.append_first(new)

    def insert_at(c, r, new):
        return c.insert_at(r.randrange(-c.count, c.count + 1), new)

    def set_at(c, r, new):
        return None if c.count == 0 else c.set_at(r.randrange(-c.count, c.count), new)

    def remove_at(c, r, new):
        return None if c.count == 0 else c.remove_at(r.randrange(-c.count, c.count))

    def extend(c, r, new):
        return c.extend([new, new])

    return dict(append_last=append_last, append_first=append_first, insert_at=insert_at, set_at=set_at,
                remove_at=remove_at, extend=extend)


def _dictionary_mutators():
    def set_new(c, r, new):
        return c.set_at(f"k{r.randrange(0, 1000)}", new)

    def set_existing(c, r, new):
        keys = list(c.keys())
        return None if not keys else c.set_at(r.choice(keys), new)

    def remove_present(c, r, new):
        keys = list(c.keys())
        return None if not keys else c.remove_at(r.choice(keys))

    return dict(set_new=set_new, set_existing=set_existing, remove_present=remove_present)


def _set_mutators():
    def add_new(c, r, new):
        return c.add(new)

    def add_existing(c, r, new):
        m = _members(c)
        return None if not m else c.add(r.choice(m))

    def remove_present(c, r, new):
        m = _members(c)
        return None if not m else c.remove(r.choice(m))

    return dict(add_new=add_new, add_existing=add_existing, remove_present=remove_present)


def _repeated_mutators():
    def add_new_key(c, r, new):
        return c.set_at(f"k{r.randrange(0, 1000)}", new)

    def add_to_key(c, r, new):
        keys = list(c.keys())
        return None if not keys else c.set_at(r.choice(keys), new)

    def readd_record(c, r, new):
        records = [(k, v) for k, cs in c.items() for v in cs.elements()]
        return None if not records else c.set_at(*r.choice(records))

    def remove_record(c, r, new):
        records = [(k, v) for k, cs in c.items() for v in cs.elements()]
        return None if not records else c.remove_record(*r.choice(records))

    def remove_key(c, r, new):
        keys = list(c.keys())
        return None if not keys else c.remove_at(r.choice(keys))

    return dict(add_new_key=add_new_key, add_to_key=add_to_key, readd_record=readd_record,
                remove_record=remove_record, remove_key=remove_key)


RECORD_MUTATORS = {"List": _list_mutators(), "Dictionary": _dictionary_mutators(),
                   "HashDictionary": _dictionary_mutators(), "Set": _set_mutators(),
                   "CountedSet": _set_mutators(), "RepeatedKeysDictionary": _repeated_mutators()}
RECORD_MUTATORS["CountedSet"]["remove_all_copies"] = (
    lambda c, r, new: None if c.count == 0 else _remove_fully(c, r))


def _remove_fully(coll, rng):
    element = rng.choice(_members(coll))
    while coll.has(element):
        coll = coll.remove(element)
    return coll


# -- key kinds, index kinds, storages --------------------------------------------------------

KEY_KINDS = {"element": KeySpec.element(), "path": KeySpec.path("a"), "dotted": KeySpec.path("a.b")}
INDEX_KINDS = {"equality": EqualityIndex, "range": RangeIndex}


class MemoryBackend:
    """Storage backend whose 'reopen' keeps the same in-memory storage."""

    def __init__(self):
        self.storage = MemoryStorage()

    def reopen(self):
        """Nothing to reopen: the in-memory storage keeps every record."""

    def close(self):
        """Close the storage."""
        self.storage.close()


class FileBackend:
    """Storage backend over a temporary directory; 'reopen' closes and reopens the space."""

    def __init__(self):
        self._dir = tempfile.mkdtemp(prefix="protodb-collidx-")
        self.storage = FileStorage(self._dir + "/space", durability="none")

    def reopen(self):
        """Flush, close and reopen the space (a new storage object over the same files)."""
        self.storage.flush()
        self.storage.close()
        self.storage = FileStorage.open_existing(self._dir + "/space", durability="none")

    def close(self):
        """Close the storage and remove the directory."""
        self.storage.close()
        shutil.rmtree(self._dir, ignore_errors=True)


BACKENDS = {"memory": MemoryBackend, "file": FileBackend}


# -- the state machine --------------------------------------------------------------------------

class CollectionIndexMachine(RuleBasedStateMachine):
    """Base machine; subclasses fix ``factory``, ``index_cls``, ``key_spec``, ``key_kind`` and ``backend_cls``."""

    factory = None
    index_cls = None
    key_spec = None
    key_kind = None
    backend_cls = None

    def __init__(self):
        super().__init__()
        self.backend = self.backend_cls()
        self.context = DirectContext(self.backend.storage)
        self.coll = self.factory.empty()
        self.seen: dict = {}        # exact key -> detached stand-in element (deduplicated, oldest evicted)
        self.removed: list = []     # stand-ins of keys that the last mutation removed
        self.indexed = False        # model: should the collection carry the index?
        self.history: list = []     # (version, element keys, term answers) kept for later re-checks
        self.rng = random.Random(0)
        self.contract = IndexContract(
            name="collections-with-indexes", index_factory=self.index_cls, element_strategy=lambda r: None,
            key_specs=(self.key_spec,), exact_key=exact_value_key if self.key_kind == "element" else None)
        self.mutators = (self.factory.mutators if self.key_kind == "element"
                         else RECORD_MUTATORS[self.factory.name])

    def teardown(self):
        try:
            self.recheck_history()
        finally:
            self.backend.close()

    # -- helpers --

    def new_index(self):
        return self.index_cls(INDEX_NAME, self.key_spec)

    def element_keys(self, coll) -> list:
        return [self.contract._key(e) for e in coll.elements()]

    def standin(self, key):
        """A detached element carrying ``key``: remembered keys must outlive a closed storage."""
        if self.key_kind == "element":
            return key
        return DBObject(a=key) if self.key_kind == "path" else DBObject(a=DBObject(b=key))

    def remember(self, coll) -> None:
        """Remember the keys of ``coll``'s elements; terms are drawn from every key ever seen."""
        for e in coll.elements():
            key = self.key_spec.extract(e)
            if key is not MISSING:
                token = exact_value_key(key)
                self.seen.pop(token, None)          # re-insert: most recently seen last
                self.seen[token] = self.standin(key)
        while len(self.seen) > SEEN_LIMIT:
            del self.seen[next(iter(self.seen))]

    def seen_elements(self) -> list:
        """The remembered stand-in elements."""
        return list(self.seen.values())

    def check(self, coll, where: str) -> None:
        """Count agrees with the elements; the index (if any) validates and answers like a scan."""
        elements = list(coll.elements())
        assert coll.count == len(elements) == len(coll), where
        index = coll.get_index(INDEX_NAME)
        assert coll.index_names() == ([INDEX_NAME] if self.indexed else []), where
        assert (index is not None) == self.indexed, where
        if index is None:
            return
        assert type(index) is self.index_cls and index.key_spec == self.key_spec, where
        terms = self.contract.terms_for(self.key_spec, self.seen_elements() + elements, self.rng,
                                         focus=elements[:4] + self.removed)
        checked = self.contract.assert_consistent(index, elements, terms, f" {where}")
        assert checked > 0, f"{where}: no term was supported"
        self.check_order(index, elements, where)

    def check_order(self, index, elements, where: str) -> None:
        """RangeIndex iterates in key order; EqualityIndex refuses ordered iteration."""
        c = self.contract
        if not index.supports_order(self.key_spec):
            assert self.index_cls is EqualityIndex, where
            with pytest.raises(ProtoNotSupportedError):
                index.ordered_plan()
            return
        ascending = sorted(elements, key=lambda e: c.order_group(self.key_spec.extract(e)))
        expected = c._runs(self.key_spec, ascending)
        for descending, want in ((False, expected), (True, expected[::-1])):
            plan = index.ordered_plan(descending=descending)
            assert isinstance(plan, QueryPlan)
            assert c._runs(self.key_spec, plan.execute()) == want, f"{where}: ordered_plan({descending})"

    def recheck_history(self) -> None:
        """Every kept version, reloaded from the CURRENT storage, is exactly what it was.

        Elements, index set and recorded index answers are compared on a fresh lazy load of the
        saved pointer, so after a FileStorage reopen this proves old versions survive on disk.
        """
        for ptr, type_name, keys, indexed, answers in self.history:
            version = self.context.load(ptr, type_name)
            assert self.element_keys(version) == keys, "an old version's elements changed"
            index = version.get_index(INDEX_NAME)
            assert (index is not None) == indexed, "an old version's index set changed"
            if index is not None:
                index.validate(list(version.elements()))
                for term, expected in answers:
                    assert self.contract._results(index, term) == expected, \
                        f"an old index answers {term} differently"

    def keep(self) -> None:
        """Save the current version and remember its pointer, content and index answers."""
        index = self.coll.get_index(INDEX_NAME)
        answers = []
        if index is not None:
            terms = self.contract.terms_for(self.key_spec, self.seen_elements(), self.rng)
            answers = [(t, self.contract._results(index, t)) for t in terms if index.supports(t)]
        ptr = self.context.save(self.coll)
        self.history.append((ptr, type(self.coll).__type_name__, self.element_keys(self.coll),
                             index is not None, answers))
        del self.history[:-3]

    # -- rules --

    def mutate_once(self, seed: int) -> None:
        rng = random.Random(seed)
        mutator = self.mutators[rng.choice(sorted(self.mutators))]
        if self.key_kind == "element":
            result = mutator(self.coll, rng)
            new = None if result is None else result[0]
        else:
            new = mutator(self.coll, rng, make_record(rng, self.key_kind == "dotted"))
        if new is None:
            return
        kept = {self.contract._key(e) for e in new.elements()}
        gone = [e for e in self.coll.elements() if self.contract._key(e) not in kept]
        self.removed = [self.standin(k) for k in (self.key_spec.extract(e) for e in gone[:3])
                        if k is not MISSING]
        self.coll = new
        self.remember(new)

    # Three identical rules weight mutation 3:1 against each structural rule (Hypothesis picks
    # rules uniformly), so most steps change content.
    @rule(seed=st.integers(0, 2**31))
    def mutate_a(self, seed):
        self.mutate_once(seed)

    @rule(seed=st.integers(0, 2**31))
    def mutate_b(self, seed):
        self.mutate_once(seed)

    @rule(seed=st.integers(0, 2**31))
    def mutate_c(self, seed):
        self.mutate_once(seed)

    @precondition(lambda self: not self.indexed)
    @rule()
    def add_index(self):
        """Attach the index to the collection as it is (empty or not)."""
        self.coll = self.coll.add_index(self.new_index())
        self.indexed = True

    @precondition(lambda self: self.indexed)
    @rule()
    def remove_index(self):
        self.coll = self.coll.remove_index(INDEX_NAME)
        self.indexed = False

    @rule()
    def keep_version(self):
        self.keep()

    @precondition(lambda self: self.history)
    @rule()
    def recheck_old(self):
        """Reload every kept version from storage and compare it with what was recorded."""
        self.recheck_history()

    @rule()
    def save_keep(self):
        """Save, and keep working on the in-memory (now saved) version."""
        self.context.save(self.coll)

    @rule()
    def reload(self):
        """Save, publish the root and continue with the lazily loaded version."""
        ptr = self.context.save(self.coll)
        loaded = self.context.load(ptr, type(self.coll).__type_name__)
        assert type(loaded) is type(self.coll)
        assert self.element_keys(loaded) == self.element_keys(self.coll)
        self.coll = loaded

    @rule()
    def reopen_storage(self):
        """Publish the version as the space root, reopen the storage and reload the root pointer."""
        storage = self.backend.storage
        ptr = self.context.save(self.coll)
        root = storage.read_root()
        storage.publish_root(ptr, None if root is None else root.epoch)
        before = self.element_keys(self.coll)
        self.recheck_history()
        self.remember(self.coll)
        type_name = type(self.coll).__type_name__
        self.backend.reopen()
        self.context = DirectContext(self.backend.storage)
        root = self.backend.storage.read_root()
        self.coll = self.context.load(root.pointer, type_name)
        assert self.element_keys(self.coll) == before
        self.recheck_history()      # kept versions are read back from the reopened storage

    @invariant()
    def consistent(self):
        self.check(self.coll, "invariant")



_missing = [f.name for f in COLLECTION_FACTORIES if f.name != "ToySet" and f.name not in RECORD_MUTATORS]
assert not _missing, (f"collections {_missing} are registered but have no entry in RECORD_MUTATORS "
                      f"(tests/contracts/test_collections_with_indexes.py); add record-aware mutators")


def _machine(factory, index_name, key_name, backend_name) -> type:
    name = f"{factory.name}_{index_name}_{key_name}_{backend_name}"
    cls = type(f"Machine_{name}", (CollectionIndexMachine,), {
        "factory": factory, "index_cls": INDEX_KINDS[index_name], "key_spec": KEY_KINDS[key_name],
        "key_kind": key_name, "backend_cls": BACKENDS[backend_name]})
    return cls


def _register_machines() -> None:
    """One test class per combination, so a failure names the exact collection, index, key and storage."""
    for factory in COLLECTION_FACTORIES:
        if factory.name == "ToySet":   # the contract's test double, not a shipped collection
            continue
        for index_name in INDEX_KINDS:
            for key_name in KEY_KINDS:
                for backend_name in BACKENDS:
                    machine = _machine(factory, index_name, key_name, backend_name)
                    case = machine.TestCase
                    case.settings = PROFILE
                    case.__name__ = case.__qualname__ = (
                        f"Test_{factory.name}_{index_name}_{key_name}_{backend_name}")
                    globals()[case.__name__] = case


_register_machines()
