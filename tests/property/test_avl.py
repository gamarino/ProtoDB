"""Property and unit tests for the persistent AVL machinery (proto_db/collections/avl.py).

Hypothesis state machines compare positional trees with a Python list and keyed
trees with a sorted dict; ``check_invariants`` runs after every operation and
every previously produced root is re-checked to prove that no operation mutates
its inputs.
"""
import inspect
import sys

import pytest
from hypothesis import settings, strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

from proto_db.collections import avl
from proto_db.collections.avl import (
    UNBOUNDED, KeyNode, PosNode, check_invariants, key_from_sorted, key_get, key_iter, key_iter_range,
    key_put, key_remove, pos_concat, pos_count, pos_from_iterable, pos_get, pos_insert,
    pos_iter, pos_remove, pos_set, pos_slice,
)
from proto_db.errors import ProtoCorruptionError, ProtoUserError, ProtoValidationError
from proto_db.storage.memory import MemoryStorage
from proto_db.values import MISSING, order_key
from tests.support.persistence import DirectContext

SM_SETTINGS = settings(max_examples=60, stateful_step_count=40, deadline=None)


# -- positional state machine -------------------------------------------------------------

class PositionalMachine(RuleBasedStateMachine):
    """A positional AVL tree behaves like a Python list and never mutates old roots."""

    def __init__(self):
        super().__init__()
        self.root = None
        self.model = []
        self.history = []  # (root, expected contents) of every root produced so far

    def _commit(self, root, model):
        check_invariants(root)
        assert list(pos_iter(root)) == model
        assert pos_count(root) == len(model)
        self.history.append((self.root, list(self.model)))
        self.root, self.model = root, model

    @rule(data=st.data(), value=st.integers())
    def insert(self, data, value):
        i = data.draw(st.integers(0, len(self.model)))
        model = list(self.model)
        model.insert(i, value)
        self._commit(pos_insert(self.root, i, value), model)

    @precondition(lambda self: self.model)
    @rule(data=st.data(), value=st.integers())
    def set(self, data, value):
        i = data.draw(st.integers(0, len(self.model) - 1))
        model = list(self.model)
        model[i] = value
        self._commit(pos_set(self.root, i, value), model)

    @precondition(lambda self: self.model)
    @rule(data=st.data())
    def remove(self, data):
        i = data.draw(st.integers(0, len(self.model) - 1))
        model = list(self.model)
        del model[i]
        self._commit(pos_remove(self.root, i), model)

    @precondition(lambda self: self.model)
    @rule(data=st.data())
    def get(self, data):
        i = data.draw(st.integers(0, len(self.model) - 1))
        assert pos_get(self.root, i) == self.model[i]

    @rule(data=st.data())
    def slice(self, data):
        start = data.draw(st.integers(0, len(self.model)))
        stop = data.draw(st.integers(start, len(self.model)))
        part = pos_slice(self.root, start, stop)
        check_invariants(part)
        assert list(pos_iter(part)) == self.model[start:stop]
        if data.draw(st.booleans()):
            self._commit(part, self.model[start:stop])

    @rule(values=st.lists(st.integers(), max_size=12), front=st.booleans())
    def concat(self, values, front):
        other = pos_from_iterable(values)
        check_invariants(other)
        if front:
            self._commit(pos_concat(other, self.root), values + self.model)
        else:
            self._commit(pos_concat(self.root, other), self.model + values)

    @invariant()
    def old_roots_unchanged(self):
        for root, contents in self.history[-5:]:
            check_invariants(root)
            assert list(pos_iter(root)) == contents


TestPositional = PositionalMachine.TestCase
TestPositional.settings = SM_SETTINGS


# -- keyed state machine ------------------------------------------------------------------

# 1.0 and 2.0 collide with 1 and 2 under order_key, exactly as they do as dict keys.
keys = st.one_of(st.integers(-20, 20), st.sampled_from(["a", "b", "c", "x", 1.0, 2.0, 2.5, MISSING]), st.none())
# None and MISSING are ordinary bounds; only UNBOUNDED leaves a side open.
bounds = st.one_of(st.just(UNBOUNDED), st.none(), st.just(MISSING), st.integers(-25, 25),
                   st.sampled_from(["a", "b", "z"]))


def _sorted_items(model):
    return sorted(model.items(), key=lambda kv: order_key(kv[0]))


class KeyedMachine(RuleBasedStateMachine):
    """A keyed AVL tree behaves like a dict sorted by ``order_key`` and never mutates old roots."""

    def __init__(self):
        super().__init__()
        self.root = None
        self.model = {}
        self.history = []

    def _commit(self, root, model):
        check_invariants(root)
        assert list(key_iter(root)) == _sorted_items(model)
        self.history.append((self.root, _sorted_items(self.model)))
        self.root, self.model = root, model

    @rule(key=keys, value=st.integers())
    def put(self, key, value):
        root, replaced, old = key_put(self.root, key, value)
        assert replaced == (key in self.model)
        assert old == self.model.get(key)
        model = dict(self.model)
        model[key] = value
        self._commit(root, model)

    @rule(key=keys)
    def remove(self, key):
        root, removed, old = key_remove(self.root, key)
        assert removed == (key in self.model)
        assert old == self.model.get(key)
        if not removed:
            assert root is self.root
        model = dict(self.model)
        model.pop(key, None)
        self._commit(root, model)

    @rule(key=keys)
    def get(self, key):
        sentinel = object()
        assert key_get(self.root, key, sentinel) == self.model.get(key, sentinel)

    @rule(lo=bounds, hi=bounds, lo_inc=st.booleans(), hi_inc=st.booleans())
    def iter_range(self, lo, hi, lo_inc, hi_inc):
        def inside(k):
            ok = order_key(k)
            if lo is not UNBOUNDED:
                if ok < order_key(lo) or (ok == order_key(lo) and not lo_inc):
                    return False
            if hi is not UNBOUNDED:
                if ok > order_key(hi) or (ok == order_key(hi) and not hi_inc):
                    return False
            return True
        expected = [kv for kv in _sorted_items(self.model) if inside(kv[0])]
        assert list(key_iter_range(self.root, lo, hi, lo_inc, hi_inc)) == expected

    @rule(pairs=st.dictionaries(st.integers(-50, 50), st.integers(), max_size=30))
    def rebuild_from_sorted(self, pairs):
        model = dict(pairs)
        self._commit(key_from_sorted(_sorted_items(model)), model)

    @invariant()
    def old_roots_unchanged(self):
        for root, contents in self.history[-5:]:
            check_invariants(root)
            assert list(key_iter(root)) == contents


TestKeyed = KeyedMachine.TestCase
TestKeyed.settings = SM_SETTINGS


# -- unit tests ---------------------------------------------------------------------------

def test_empty_tree_is_none():
    assert pos_count(None) == 0
    assert list(pos_iter(None)) == []
    assert list(key_iter(None)) == []
    check_invariants(None)
    assert key_get(None, 1) is None
    assert key_remove(None, 1) == (None, False, None)


@pytest.mark.parametrize("bad", [-1, 3, 1.0, True, "0"])
def test_positional_index_errors(bad):
    root = pos_from_iterable([1, 2, 3])
    with pytest.raises(ProtoValidationError):
        pos_get(root, bad)
    with pytest.raises(ProtoValidationError):
        pos_set(root, bad, 0)
    with pytest.raises(ProtoValidationError):
        pos_remove(root, bad)


def test_positional_insert_and_slice_bounds():
    root = pos_from_iterable([1, 2, 3])
    with pytest.raises(ProtoValidationError):
        pos_insert(root, 4, 0)
    with pytest.raises(ProtoValidationError):
        pos_insert(root, -1, 0)
    for start, stop in [(-1, 2), (2, 1), (0, 4)]:
        with pytest.raises(ProtoValidationError):
            pos_slice(root, start, stop)
    assert pos_slice(root, 1, 1) is None


def test_unorderable_key_rejected():
    with pytest.raises(ProtoValidationError):
        key_put(None, [1], 1)
    with pytest.raises(ProtoValidationError):
        key_from_sorted([(2, "a"), (1, "b")])
    with pytest.raises(ProtoValidationError):
        key_from_sorted([(1, "a"), (1, "b")])


def test_equal_keys_of_different_types_are_one_key():
    root, _, _ = key_put(None, 1, "int")
    root, replaced, old = key_put(root, 1.0, "float")
    assert replaced and old == "int"
    items = list(key_iter(root))
    assert items == [(1, "float")] and type(items[0][0]) is int  # existing key kept, like dict


def test_nodes_are_immutable():
    node = pos_from_iterable([1])
    with pytest.raises(ProtoUserError):
        node.value = 2
    with pytest.raises(ProtoUserError):
        del node.value
    knode = key_from_sorted([(1, 2)])
    with pytest.raises(ProtoUserError):
        knode.key = 3


def test_stable_type_names():
    assert PosNode.__type_name__ == "proto_db.avl.PosNode"
    assert KeyNode.__type_name__ == "proto_db.avl.KeyNode"


def _limited_recursion(extra=250):
    """Context manager: recursion limit = current depth + ``extra``."""
    class _Ctx:
        def __enter__(self):
            self.old = sys.getrecursionlimit()
            sys.setrecursionlimit(len(inspect.stack(0)) + extra)

        def __exit__(self, *exc):
            sys.setrecursionlimit(self.old)
    return _Ctx()


def test_large_tree_operations_stay_shallow():
    n = 100_000
    root = pos_from_iterable(range(n))
    check_invariants(root)
    assert root.height <= 18
    with _limited_recursion():
        assert sum(1 for _ in pos_iter(root)) == n
        for i in (0, 1, n // 2, n - 1):
            assert pos_get(root, i) == i
        r2 = pos_insert(root, n // 3, -1)
        r2 = pos_remove(r2, 0)
        r2 = pos_set(r2, 10, "x")
        part = pos_slice(r2, 1000, 90_000)
        joined = pos_concat(part, root)
    check_invariants(r2)
    check_invariants(part)
    check_invariants(joined)
    assert pos_count(part) == 89_000 and pos_count(joined) == 189_000
    assert list(pos_iter(root)) == list(range(n))


def test_large_keyed_tree_operations_stay_shallow():
    n = 100_000
    root = key_from_sorted((i, i * 2) for i in range(n))
    check_invariants(root)
    with _limited_recursion():
        assert sum(1 for _ in key_iter(root)) == n
        assert key_get(root, 777) == 1554
        r2, replaced, _ = key_put(root, 777, 0)
        r2, removed, _ = key_remove(r2, 5)
        rng = list(key_iter_range(root, 500, 510, False, True))
    assert replaced and removed
    assert [k for k, _ in rng] == list(range(501, 511))
    check_invariants(r2)


def test_sequential_inserts_stay_balanced():
    root = None
    for i in range(5000):
        root = pos_insert(root, pos_count(root), i)
    check_invariants(root)
    assert root.height <= 1.45 * 13 + 2
    kroot = None
    for i in range(5000):
        kroot, _, _ = key_put(kroot, i, i)
    check_invariants(kroot)


# -- check_invariants detects violations ----------------------------------------------------

def _forge(cls, **fields):
    """Build a node with arbitrary (possibly inconsistent) fields, bypassing the constructor."""
    node = cls.__new__(cls)
    avl.Atom.__init__(node)
    for k, v in fields.items():
        object.__setattr__(node, k, v)
    return node


def test_check_invariants_detects_bad_height_count_balance_and_order():
    leaf = pos_from_iterable([1])
    with pytest.raises(ProtoCorruptionError, match="height"):
        check_invariants(_forge(PosNode, value=0, left=leaf, right=None, height=5, count=2))
    with pytest.raises(ProtoCorruptionError, match="count"):
        check_invariants(_forge(PosNode, value=0, left=leaf, right=None, height=2, count=7))
    deep = pos_from_iterable([1, 2, 3])
    with pytest.raises(ProtoCorruptionError, match="balance"):
        check_invariants(_forge(PosNode, value=0, left=deep, right=None, height=3, count=4))
    k1 = key_from_sorted([(5, "a")])
    with pytest.raises(ProtoCorruptionError, match="order"):
        check_invariants(_forge(KeyNode, key=1, value="b", left=k1, right=None, height=2, count=2))
    with pytest.raises(ProtoCorruptionError, match="kind"):
        check_invariants(_forge(KeyNode, key=9, value="b", left=leaf, right=None, height=2, count=2))


# -- persistence --------------------------------------------------------------------------

class _CountingStorage(MemoryStorage):
    def __init__(self):
        super().__init__()
        self.reads = 0

    def get_atom(self, ptr):
        self.reads += 1
        return super().get_atom(ptr)


def test_positional_persistence_roundtrip_with_lazy_loading():
    storage = _CountingStorage()
    n = 10_000
    root = pos_from_iterable(range(n))
    ptr = DirectContext(storage).save(root)

    loaded = DirectContext(storage).load(ptr, PosNode.__type_name__)
    assert storage.reads == 0
    assert pos_get(loaded, 1234) == 1234
    assert storage.reads <= 2 * 15  # one root-to-leaf path, plus siblings read for counts
    assert list(pos_iter(loaded)) == list(range(n))
    check_invariants(loaded)

    # Modify a partially loaded tree, save the new version and reload it.
    fresh = DirectContext(storage).load(ptr, PosNode.__type_name__)
    v2 = pos_insert(pos_remove(fresh, 0), 5000, "mid")
    ptr2 = DirectContext(storage).save(v2)
    again = DirectContext(storage).load(ptr2, PosNode.__type_name__)
    expected = list(range(1, n))
    expected.insert(5000, "mid")
    assert list(pos_iter(again)) == expected
    check_invariants(again)


def test_keyed_persistence_roundtrip():
    storage = MemoryStorage()
    n = 10_000
    root = key_from_sorted((i, str(i)) for i in range(n))
    ptr = DirectContext(storage).save(root)
    loaded = DirectContext(storage).load(ptr, KeyNode.__type_name__)
    assert key_get(loaded, 4321) == "4321"
    v2, _, _ = key_put(loaded, -1, "neg")
    ptr2 = DirectContext(storage).save(v2)
    again = DirectContext(storage).load(ptr2, KeyNode.__type_name__)
    assert list(key_iter(again)) == [(-1, "neg")] + [(i, str(i)) for i in range(n)]
    check_invariants(again)


def test_none_and_missing_are_real_range_bounds():
    root = key_from_sorted([(MISSING, "m"), (None, "n"), (1, "one")])
    assert [v for _, v in key_iter_range(root, UNBOUNDED, None, True, True)] == ["m", "n"]
    assert [v for _, v in key_iter_range(root, None, UNBOUNDED, False, True)] == ["one"]
    assert [v for _, v in key_iter_range(root, None, 5, False, False)] == ["one"]
    assert [v for _, v in key_iter_range(root, MISSING, None, True, False)] == ["m"]
    assert [v for _, v in key_iter_range(root)] == ["m", "n", "one"]


def test_check_invariants_shared_child_and_cycle_raise_corruption():
    leaf = pos_from_iterable([1])
    # A shared child is a DAG, not a tree; it must not crash with KeyError.
    shared = _forge(PosNode, value=0, left=leaf, right=leaf, height=2, count=3)
    check_invariants(shared)  # structurally consistent; traversed per path
    a = _forge(PosNode, value=0, left=None, right=None, height=2, count=2)
    object.__setattr__(a, "left", a)  # cycle
    with pytest.raises(ProtoCorruptionError):
        check_invariants(a)
    huge = _forge(PosNode, value=0, left=None, right=None, height=10**9, count=1)
    with pytest.raises(ProtoCorruptionError, match="height"):
        check_invariants(huge)


# -- order-key level range iteration, ranks and reverse iteration (Task 4.3, D30) ---------

def test_key_iter_okey_range_with_rank_bounds():
    """Catches order-key bounds that are not honoured (range scans limited to one type rank)."""
    from proto_db.collections.avl import key_iter_okey_range
    from proto_db.values import rank_bounds
    keys = [MISSING, None, False, True, 1, 2.5, 3, "a", "b", (1,)]
    root = key_from_sorted((k, i) for i, k in enumerate(keys))
    low, high = rank_bounds(0)
    assert [k for k, _ in key_iter_okey_range(root, low, high)] == [1, 2.5, 3]
    assert [k for k, _ in key_iter_okey_range(root, low, order_key(3), True, False)] == [1, 2.5]
    assert [k for k, _ in key_iter_okey_range(root, order_key(1), high, False, True)] == [2.5, 3]
    assert [k for k, _ in key_iter_okey_range(root, order_key(None), None, False)] == keys[2:]
    assert [k for k, _ in key_iter_okey_range(root)] == keys


@pytest.mark.parametrize("n", [0, 1, 2, 7, 100])
def test_key_count_below_matches_a_linear_count(n):
    """Catches a wrong rank: RangeIndex estimates count key groups in a range with it."""
    from proto_db.collections.avl import key_count_below
    root = key_from_sorted((2 * i, i) for i in range(n))
    for probe in range(-2, 2 * n + 2):
        k = order_key(probe)
        assert key_count_below(root, k) == sum(1 for i in range(n) if order_key(2 * i) < k)
        assert key_count_below(root, k, inclusive=True) == sum(1 for i in range(n) if order_key(2 * i) <= k)
    assert key_count_below(root, (99,)) == n and key_count_below(root, (-1,)) == 0


def test_key_iter_reversed_is_the_exact_reverse():
    """Catches descending ordered iteration that is not the reverse of key order."""
    from proto_db.collections.avl import key_iter_reversed
    for n in (0, 1, 5, 300):
        root = key_from_sorted((i, -i) for i in range(n))
        assert list(key_iter_reversed(root)) == list(key_iter(root))[::-1]


def test_key_from_sorted_okeys_matches_key_from_sorted_and_checks_order():
    """Catches the bulk builder with precomputed order keys building a different or unordered tree."""
    from proto_db.collections.avl import key_from_sorted_okeys
    pairs = [(MISSING, 0), (None, 1), (1, 2), (2.5, 3), ("a", 4), ((1, "b"), 5)]
    tree = key_from_sorted_okeys((order_key(k), k, v) for k, v in pairs)
    check_invariants(tree)
    assert list(key_iter(tree)) == list(key_iter(key_from_sorted(pairs)))
    assert key_from_sorted_okeys([]) is None
    with pytest.raises(ProtoValidationError):
        key_from_sorted_okeys([(order_key(2), 2, "x"), (order_key(1), 1, "y")])
