"""Stateful model tests: List vs ``list``, Dictionary and HashDictionary vs ``dict`` (Task 3.3a).

Each machine applies random mutations to a collection and to a Python model, and
from time to time saves the current version through a ``DirectContext`` and goes
on with the lazily loaded stub, over MemoryStorage and over FileStorage (a fresh
temporary directory per run).  Every previously produced version is re-checked
at the end of a run to prove that no operation mutates its receiver.
"""
import shutil
import tempfile
from decimal import Decimal

from hypothesis import settings, strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

from proto_db.collections.dictionaries import Dictionary
from proto_db.collections.hash_dictionaries import HashDictionary
from proto_db.collections.lists import List
from proto_db.storage.file import FileStorage
from proto_db.storage.memory import MemoryStorage
from proto_db.values import order_key
from tests.support.persistence import DirectContext

SM_SETTINGS = settings(max_examples=40, stateful_step_count=35, deadline=None)

keys = st.one_of(
    st.integers(-5, 30),
    st.integers(2**60 - 3, 2**60 + 3),
    st.sampled_from([True, False, None, 1.0, 2.5, Decimal("3"), Decimal("0.5")]),
    st.text(max_size=3),
    st.tuples(st.integers(0, 3), st.text(max_size=1)),
)
values = st.one_of(st.integers(), st.text(max_size=4), st.none(), st.lists(st.integers(), max_size=2))


class _Persisting(RuleBasedStateMachine):
    """Shared save/reload machinery; subclasses choose the storage with ``storage_kind``."""

    storage_kind = "memory"

    def __init__(self):
        super().__init__()
        self._dir = None
        if self.storage_kind == "file":
            self._dir = tempfile.mkdtemp(prefix="protodb-model-")
            self.storage = FileStorage(self._dir + "/space", durability="none")
        else:
            self.storage = MemoryStorage()
        self.context = DirectContext(self.storage)
        self.history = []

    def teardown(self):
        for coll, expected in self.history:
            assert self.snapshot(coll) == expected
        self.storage.close()
        if self._dir is not None:
            shutil.rmtree(self._dir, ignore_errors=True)

    def record(self, coll, model_snapshot):
        self.history.append((coll, model_snapshot))

    def reloaded(self, coll):
        return self.context.load(self.context.save(coll), type(coll).__type_name__)


# -- List ---------------------------------------------------------------------------------

class ListMachine(_Persisting):
    """``List`` behaves like a Python list."""

    def __init__(self):
        super().__init__()
        self.coll = List()
        self.model = []

    def snapshot(self, coll):
        return list(coll)

    def commit(self, coll, model):
        self.record(self.coll, list(self.model))
        self.coll, self.model = coll, model

    @rule(data=st.data(), v=values)
    def insert_at(self, data, v):
        i = data.draw(st.integers(-len(self.model), len(self.model)))
        model = list(self.model)
        model.insert(i if i >= 0 else len(model) + i, v)
        self.commit(self.coll.insert_at(i, v), model)

    @rule(v=values)
    def append_first(self, v):
        self.commit(self.coll.append_first(v), [v] + self.model)

    @rule(v=values)
    def append_last(self, v):
        self.commit(self.coll.append_last(v), self.model + [v])

    @precondition(lambda self: self.model)
    @rule(data=st.data(), v=values)
    def set_at(self, data, v):
        i = data.draw(st.integers(-len(self.model), len(self.model) - 1))
        model = list(self.model)
        model[i] = v
        self.commit(self.coll.set_at(i, v), model)

    @precondition(lambda self: self.model)
    @rule(data=st.data())
    def remove_at(self, data):
        i = data.draw(st.integers(-len(self.model), len(self.model) - 1))
        model = list(self.model)
        del model[i]
        self.commit(self.coll.remove_at(i), model)

    @rule(vs=st.lists(values, max_size=5))
    def extend(self, vs):
        self.commit(self.coll.extend(vs), self.model + vs)

    @rule(start=st.none() | st.integers(-8, 8), stop=st.none() | st.integers(-8, 8))
    def slice(self, start, stop):
        self.commit(self.coll.slice(start, stop), self.model[start:stop])

    @rule(n=st.integers(-6, 6), head=st.booleans())
    def head_tail(self, n, head):
        if head:
            self.commit(self.coll.head(n), self.model[:n])
        else:
            self.commit(self.coll.tail(n), self.model[n:])

    @rule()
    def persist(self):
        self.commit(self.reloaded(self.coll), list(self.model))

    @invariant()
    def matches(self):
        assert list(self.coll) == self.model
        assert self.coll.count == len(self.model)
        if self.model:
            assert self.coll.get_at(len(self.model) - 1) == self.model[-1]
            assert self.coll.get_at(0) == self.model[0]


# -- Dictionaries -------------------------------------------------------------------------

class _DictMachine(_Persisting):
    """A dictionary behaves like a ``dict`` keyed by ``order_key`` (1 and 1.0 are one key)."""

    cls = Dictionary
    ordered = True

    def __init__(self):
        super().__init__()
        self.coll = self.cls()
        self.model = {}  # order_key -> (first key object, value)

    def snapshot(self, coll):
        return sorted(((order_key(k), repr(v)) for k, v in coll.items()))

    def model_snapshot(self, model):
        return sorted((ok, repr(v)) for ok, (_, v) in model.items())

    def commit(self, coll, model):
        self.record(self.coll, self.model_snapshot(self.model))
        self.coll, self.model = coll, model

    @rule(k=keys, v=values)
    def set_at(self, k, v):
        model = dict(self.model)
        ok = order_key(k)
        model[ok] = (model[ok][0] if ok in model else k, v)
        self.commit(self.coll.set_at(k, v), model)

    @rule(k=keys)
    def remove_at(self, k):
        model = dict(self.model)
        model.pop(order_key(k), None)
        self.commit(self.coll.remove_at(k), model)

    @precondition(lambda self: self.model)
    @rule(data=st.data())
    def remove_present(self, data):
        ok = data.draw(st.sampled_from(sorted(self.model)))
        model = dict(self.model)
        key, _ = model.pop(ok)
        self.commit(self.coll.remove_at(key), model)

    @rule()
    def persist(self):
        self.commit(self.reloaded(self.coll), dict(self.model))

    @rule(k=keys)
    def lookups(self, k):
        ok = order_key(k)
        assert self.coll.has(k) == (ok in self.model) == (k in self.coll)
        expected = self.model[ok][1] if ok in self.model else "absent"
        assert self.coll.get_at(k, "absent") == expected

    @invariant()
    def matches(self):
        items = list(self.coll.items())
        assert self.coll.count == len(self.model) == len(items)
        assert {order_key(k): (k, v) for k, v in items} == self.model
        if self.ordered:
            assert [order_key(k) for k, _ in items] == sorted(self.model)
        assert [repr(v) for v in self.coll.elements()] == [repr(v) for _, v in items]


class DictionaryMachine(_DictMachine):
    cls = Dictionary


class HashDictionaryMachine(_DictMachine):
    cls = HashDictionary
    ordered = False


class ListFileMachine(ListMachine):
    storage_kind = "file"


class DictionaryFileMachine(DictionaryMachine):
    storage_kind = "file"


class HashDictionaryFileMachine(HashDictionaryMachine):
    storage_kind = "file"


TestListModel = ListMachine.TestCase
TestListModel.settings = SM_SETTINGS
TestListModelFile = ListFileMachine.TestCase
TestListModelFile.settings = SM_SETTINGS
TestDictionaryModel = DictionaryMachine.TestCase
TestDictionaryModel.settings = SM_SETTINGS
TestDictionaryModelFile = DictionaryFileMachine.TestCase
TestDictionaryModelFile.settings = SM_SETTINGS
TestHashDictionaryModel = HashDictionaryMachine.TestCase
TestHashDictionaryModel.settings = SM_SETTINGS
TestHashDictionaryModelFile = HashDictionaryFileMachine.TestCase
TestHashDictionaryModelFile.settings = SM_SETTINGS
