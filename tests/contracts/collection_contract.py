"""Behavioural contract every IndexedCollection must satisfy (spec section 8.1).

The maintainer's requirement: every collection supports associated indexes and no
mutator ever returns a collection with stale indexes.  This module holds:

* :class:`CollectionFactory` and the registry :data:`COLLECTION_FACTORIES`.  Each
  task that adds a collection appends an entry in the REGISTRATIONS section at the
  bottom of this file.
* ``check_*`` functions, run by ``tests/unit/test_collection_contract.py`` for
  every registered factory, every mutator and several seeds.

A mutator is a callable ``(collection, rng) -> (new_collection, expected_events)``
or ``None`` when it does not apply to that collection (e.g. removing from an
empty collection).  ``expected_events`` is the exact sequence of element-level
index events the mutation implies: ``("add", e)``, ``("remove", e)``,
``("replace", old, new)``.  An empty sequence declares a no-op on the content:
the mutator may then return the receiver itself or a new collection with the
same content; otherwise it must return a new object.  ConcurrentOptimized
collections log intent (D22): every mutator call, no-ops included, returns a
new version with a longer op log, and a no-op reuses the receiver's indexes.
Their factories also provide ``rebase_model`` so that ``check_rebase`` can
verify ``rebase_onto`` against an independent model.
"""
from __future__ import annotations

import random
from collections import Counter
from dataclasses import dataclass, field
from typing import Callable

from proto_db.collections.base import ConcurrentOptimized, IndexedCollection
from proto_db.query.plans import CollectionScanPlan
from proto_db.storage.memory import MemoryStorage
from proto_db.values import order_key
from tests.support.persistence import DirectContext
from tests.support.recording_index import RecordingIndex

#: Index names attached by the contract; two indexes prove every index gets every hook.
INDEX_NAMES = ("idx_a", "idx_b")
SEEDS = range(12)


@dataclass(frozen=True)
class CollectionFactory:
    """A collection kind under contract.

    ``empty()`` returns a new empty collection; ``populate(collection, rng)``
    returns a version with some random content, built through public mutators;
    ``mutators`` maps a name to a mutator (see the module docstring);
    ``element_key`` maps an element to a hashable key that is equal for equal
    elements (default ``order_key``; override for unorderable elements);
    ``rebase_model(theirs, op_log) -> Counter`` (required for ConcurrentOptimized
    collections) returns the element-key multiset expected from replaying the
    net intent of ``op_log`` onto collection ``theirs``, computed independently
    of the collection's own replay code.
    """

    name: str
    empty: Callable[[], IndexedCollection]
    populate: Callable[[IndexedCollection, random.Random], IndexedCollection]
    mutators: dict = field(default_factory=dict)
    element_key: Callable = order_key
    rebase_model: Callable | None = None


COLLECTION_FACTORIES: list[CollectionFactory] = []


def register(factory: CollectionFactory) -> CollectionFactory:
    """Add ``factory`` to the registry (names must be unique)."""
    assert all(f.name != factory.name for f in COLLECTION_FACTORIES), factory.name
    COLLECTION_FACTORIES.append(factory)
    return factory


def mutator_cases():
    """``(factory, mutator_name, seed)`` triples for parametrisation, with readable ids."""
    import pytest
    return [pytest.param(f, m, s, id=f"{f.name}-{m}-{s}")
            for f in COLLECTION_FACTORIES for m in f.mutators for s in SEEDS]


def rebase_cases():
    """``(factory, seed)`` pairs for every ConcurrentOptimized factory."""
    import pytest
    return [pytest.param(f, s, id=f"{f.name}-{s}") for f in COLLECTION_FACTORIES
            if isinstance(f.empty(), ConcurrentOptimized) for s in SEEDS]


def factory_cases():
    """``(factory, seed)`` pairs for parametrisation, with readable ids."""
    import pytest
    return [pytest.param(f, s, id=f"{f.name}-{s}") for f in COLLECTION_FACTORIES for s in SEEDS]


# -- helpers ------------------------------------------------------------------------------

def _content(factory, coll) -> Counter:
    return Counter(factory.element_key(e) for e in coll.elements())


def _sequence(factory, coll) -> list:
    return [factory.element_key(e) for e in coll.elements()]


def indexed_sample(factory: CollectionFactory, rng: random.Random) -> IndexedCollection:
    """A populated collection with the contract indexes, attached before or after populating."""
    coll = factory.empty()
    if rng.random() < 0.5:
        for name in INDEX_NAMES:
            coll = coll.add_index(RecordingIndex(name))
        return factory.populate(coll, rng)
    coll = factory.populate(coll, rng)
    for name in INDEX_NAMES:
        coll = coll.add_index(RecordingIndex(name))
    return coll


def assert_indexes_consistent(factory, coll) -> None:
    """Every index's view of the elements equals the collection content (no stale index)."""
    content = _content(factory, coll)
    for name in coll.index_names():
        assert coll.get_index(name).live(factory.element_key) == content, f"stale index {name}"


def assert_round_trip(factory, coll, context=None) -> None:
    """Saving and lazily loading ``coll`` preserves elements, count and indexes; op logs are dropped.

    ``context`` defaults to a fresh one; pass the context ``coll`` was loaded from
    when it shares already-saved nodes with a stored version.
    """
    context = context if context is not None else DirectContext(MemoryStorage())
    ptr = context.save(coll)
    loaded = context.load(ptr, type(coll).__type_name__)
    assert type(loaded) is type(coll)
    assert _sequence(factory, loaded) == _sequence(factory, coll)
    assert loaded.count == coll.count == len(loaded)
    assert loaded.index_names() == coll.index_names()
    for name in coll.index_names():
        assert loaded.get_index(name).events == coll.get_index(name).events
    if isinstance(coll, ConcurrentOptimized):
        assert loaded._op_log == ()


# -- checks -------------------------------------------------------------------------------

def check_mutator(factory: CollectionFactory, mutator_name: str, seed: int) -> None:
    """One mutator applied to one random indexed collection satisfies the contract."""
    rng = random.Random(seed)
    coll = indexed_sample(factory, rng)
    assert_indexes_consistent(factory, coll)
    before_seq = _sequence(factory, coll)
    before_count = coll.count
    before_indexes = coll.indexes
    before_events = {n: coll.get_index(n).events for n in coll.index_names()}
    before_log = getattr(coll, "_op_log", None)

    result = factory.mutators[mutator_name](coll, rng)
    if result is None:
        return
    new, expected = result
    expected = tuple(expected)

    assert isinstance(new, type(coll))
    if expected:
        assert new is not coll, "a mutation must return a new collection"
    elif new is not coll:
        assert _content(factory, new) == _content(factory, coll)
    if isinstance(coll, ConcurrentOptimized):
        # Intent logging: every call yields a new version with a longer log; a no-op does no index work.
        assert new is not coll, "a ConcurrentOptimized mutator must return a new version"
        assert len(new._op_log) > len(before_log), "the mutator did not log its intent"
        assert new._op_log[:len(before_log)] == before_log
        if not expected:
            assert new.indexes is coll.indexes, "a no-op must reuse the receiver's indexes"

    # The receiver is unchanged: elements, count, index set and every index.
    assert _sequence(factory, coll) == before_seq
    assert coll.count == before_count
    assert coll.indexes is before_indexes
    assert getattr(coll, "_op_log", None) == before_log
    for name, events in before_events.items():
        assert coll.get_index(name).events == events

    # Every index saw exactly the implied events, and none is stale.
    assert new.index_names() == list(INDEX_NAMES)
    for name in INDEX_NAMES:
        assert new.get_index(name).events_since(coll.get_index(name)) == expected, name
    assert new.count == len(list(new.elements())) == len(new)
    assert_indexes_consistent(factory, new)
    assert_round_trip(factory, new)


def check_index_management(factory: CollectionFactory, seed: int) -> None:
    """add_index / remove_index / get_index / as_query_plan behave on any content."""
    rng = random.Random(seed)
    coll = factory.populate(factory.empty(), rng)
    assert coll.index_names() == [] and len(coll.indexes) == 0
    assert coll.get_index("idx_a") is None

    # Content held by the passed index object must never leak into the collection.
    preloaded = RecordingIndex("idx_x").build(list(coll.elements()))
    for e in coll.elements():
        preloaded = preloaded.with_added(e)
    assert_indexes_consistent(factory, coll.add_index(preloaded))

    with_a = coll.add_index(RecordingIndex("idx_a"))
    assert with_a is not coll and coll.index_names() == []
    assert _sequence(factory, with_a) == _sequence(factory, coll)
    built = with_a.get_index("idx_a")
    assert [factory.element_key(e) for e in built.events[0][1]] == _sequence(factory, coll)
    assert built.events[0][0] == "build" and len(built.events) == 1

    with_ab = with_a.add_index(RecordingIndex("idx_b"))
    assert with_ab.index_names() == ["idx_a", "idx_b"] and with_a.index_names() == ["idx_a"]

    without_a = with_ab.remove_index("idx_a")
    assert without_a.index_names() == ["idx_b"] and with_ab.index_names() == ["idx_a", "idx_b"]
    assert _sequence(factory, without_a) == _sequence(factory, coll)

    plan = with_ab.as_query_plan()
    assert isinstance(plan, CollectionScanPlan)
    assert [factory.element_key(e) for e in plan.execute()] == _sequence(factory, coll)
    explained = plan.explain()
    assert explained["plan"] == "CollectionScan"
    assert explained["indexes"] == ["idx_a", "idx_b"]
    assert explained["rows"] == coll.count
    assert plan.estimate().rows == coll.count
    assert_round_trip(factory, with_ab)


def check_mutator_sequence(factory: CollectionFactory, seed: int, steps: int = 40) -> None:
    """A random sequence of mutations keeps every index consistent at every step."""
    rng = random.Random(seed)
    coll = indexed_sample(factory, rng)
    names = sorted(factory.mutators)
    for _ in range(steps):
        result = factory.mutators[rng.choice(names)](coll, rng)
        if result is None:
            continue
        coll, _ = result
        assert_indexes_consistent(factory, coll)
        assert coll.count == len(list(coll.elements()))
    assert_round_trip(factory, coll)


def _snapshot(coll):
    context = DirectContext(MemoryStorage())
    return context.load(context.save(coll), type(coll).__type_name__)


def _state(factory, coll):
    return (_sequence(factory, coll), coll.count, coll.indexes, coll._op_log,
            {n: coll.get_index(n).events for n in coll.index_names()})


def check_rebase(factory: CollectionFactory, seed: int, steps: int = 15) -> None:
    """``mine.rebase_onto(theirs)`` from a common snapshot equals the intent model and keeps indexes."""
    assert factory.rebase_model is not None, f"{factory.name} is ConcurrentOptimized but has no rebase_model"
    rng = random.Random(seed)
    snapshot = _snapshot(indexed_sample(factory, rng))
    assert snapshot._op_log == ()
    names = sorted(factory.mutators)

    def mutate(coll):
        for _ in range(rng.randrange(0, steps)):
            result = factory.mutators[rng.choice(names)](coll, rng)
            if result is not None:
                coll = result[0]
        return coll

    mine, theirs = mutate(snapshot), mutate(snapshot)
    mine_before, theirs_before = _state(factory, mine), _state(factory, theirs)

    merged = mine.rebase_onto(theirs)

    assert _state(factory, mine) == mine_before and _state(factory, theirs) == theirs_before
    assert type(merged) is type(mine)
    assert _content(factory, merged) == factory.rebase_model(theirs, mine._op_log)
    assert merged.count == len(list(merged.elements()))
    assert merged.index_names() == theirs.index_names()
    assert_indexes_consistent(factory, merged)
    assert_round_trip(factory, merged, snapshot.context)


# == REGISTRATIONS ========================================================================
# Tasks that add a collection append its factory below.

def _toy_factory() -> CollectionFactory:
    from tests.support.toy_collection import ToySet

    def populate(coll, rng):
        for _ in range(rng.randrange(0, 30)):
            coll = coll.add(rng.randrange(0, 60))
        return coll

    def fresh(coll, rng):
        while True:
            e = rng.randrange(0, 200)
            if not coll.has(e):
                return e

    def member(coll, rng):
        members = list(coll.elements())
        return rng.choice(members) if members else None

    def add_new(coll, rng):
        e = fresh(coll, rng)
        return coll.add(e), [("add", e)]

    def add_existing(coll, rng):
        e = member(coll, rng)
        return None if e is None else (coll.add(e), [])

    def discard_present(coll, rng):
        e = member(coll, rng)
        return None if e is None else (coll.discard(e), [("remove", e)])

    def discard_absent(coll, rng):
        return coll.discard(fresh(coll, rng)), []

    def replace(coll, rng):
        old = member(coll, rng)
        if old is None:
            return None
        new = fresh(coll, rng)
        return coll.replace(old, new), [("replace", old, new)]

    def rebase_model(theirs, op_log):
        members = {order_key(e) for e in theirs.elements()}
        for kind, e in op_log:
            (members.add if kind == "add" else members.discard)(order_key(e))
        return Counter(members)

    return CollectionFactory(
        name="ToySet", empty=ToySet, populate=populate, rebase_model=rebase_model,
        mutators={"add_new": add_new, "add_existing": add_existing, "discard_present": discard_present,
                  "discard_absent": discard_absent, "replace": replace})


register(_toy_factory())


# -- Task 3.3a: List, Dictionary, HashDictionary ------------------------------------------

def _list_factory() -> CollectionFactory:
    from proto_db.collections.lists import List

    def populate(coll, rng):
        for _ in range(rng.randrange(0, 30)):
            coll = coll.insert_at(rng.randrange(0, coll.count + 1), rng.randrange(0, 40))
        return coll

    def position(coll, rng):
        return None if coll.count == 0 else rng.randrange(-coll.count, coll.count)

    def insert_at(coll, rng):
        i, v = rng.randrange(-coll.count, coll.count + 1), rng.randrange(0, 40)
        return coll.insert_at(i, v), [("add", v)]

    def append_first(coll, rng):
        v = rng.randrange(0, 40)
        return coll.append_first(v), [("add", v)]

    def append_last(coll, rng):
        v = rng.randrange(0, 40)
        return coll.append_last(v), [("add", v)]

    def set_at(coll, rng):
        i = position(coll, rng)
        if i is None:
            return None
        v = rng.randrange(0, 40)
        return coll.set_at(i, v), [("replace", coll.get_at(i), v)]

    def remove_at(coll, rng):
        i = position(coll, rng)
        return None if i is None else (coll.remove_at(i), [("remove", coll.get_at(i))])

    def extend(coll, rng):
        items = [rng.randrange(0, 40) for _ in range(rng.randrange(0, 5))]
        return coll.extend(items), [("add", v) for v in items]

    return CollectionFactory(
        name="List", empty=List, populate=populate,
        mutators={"insert_at": insert_at, "append_first": append_first, "append_last": append_last,
                  "set_at": set_at, "remove_at": remove_at, "extend": extend})


def _dictionary_like_factory(name, cls) -> CollectionFactory:
    """Factory for a key -> value collection with set_at/remove_at/has (Dictionary, HashDictionary)."""

    def populate(coll, rng):
        for _ in range(rng.randrange(0, 30)):
            coll = coll.set_at(rng.choice([rng.randrange(0, 50), f"s{rng.randrange(0, 20)}"]),
                               rng.randrange(0, 15))
        return coll

    def fresh_key(coll, rng):
        while True:
            k = rng.choice([rng.randrange(0, 200), f"s{rng.randrange(0, 200)}", float(rng.randrange(0, 200))])
            if not coll.has(k):
                return k

    def existing_key(coll, rng):
        ks = list(coll.keys())
        return rng.choice(ks) if ks else None

    def set_new(coll, rng):
        k, v = fresh_key(coll, rng), rng.randrange(0, 15)
        return coll.set_at(k, v), [("add", v)]

    def set_existing(coll, rng):
        k = existing_key(coll, rng)
        if k is None:
            return None
        v = rng.randrange(0, 15)
        # An equal key of another type (1 vs 1.0) addresses the same entry.
        probe = float(k) if isinstance(k, int) and rng.random() < 0.5 else k
        return coll.set_at(probe, v), [("replace", coll.get_at(k), v)]

    def remove_present(coll, rng):
        k = existing_key(coll, rng)
        return None if k is None else (coll.remove_at(k), [("remove", coll.get_at(k))])

    def remove_absent(coll, rng):
        return coll.remove_at(fresh_key(coll, rng)), []

    def rebase_model(theirs, op_log):
        mapping = {order_key(k): v for k, v in theirs.items()}
        for op in op_log:
            if op[0] == "set":
                mapping[order_key(op[1])] = op[2]
            elif op[0] == "remove":
                mapping.pop(order_key(op[1]), None)
        return Counter(order_key(v) for v in mapping.values())

    return CollectionFactory(
        name=name, empty=cls, populate=populate, rebase_model=rebase_model,
        mutators={"set_new": set_new, "set_existing": set_existing, "remove_present": remove_present,
                  "remove_absent": remove_absent})


def _register_3_3a() -> None:
    from proto_db.collections.dictionaries import Dictionary
    from proto_db.collections.hash_dictionaries import HashDictionary
    register(_list_factory())
    register(_dictionary_like_factory("Dictionary", Dictionary))
    register(_dictionary_like_factory("HashDictionary", HashDictionary))


_register_3_3a()
