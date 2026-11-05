"""Occurrence weights of keyed AVL trees (Task 8.4b, D56): ``CountedKeyNode`` and rank access.

A ``CountedKeyNode`` holds a ``(payload, count)`` value and persists ``weight``, the
occurrences of its subtree (the sum of the counts).  Rank functions find occurrence r in
O(log n).  A Hypothesis state machine compares a weighted tree with a sorted list model
after every write (put, replace, remove, bulk build) and checks every rank function
against the model, ascending and descending.
"""
import pytest
from hypothesis import settings, strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule

from proto_db.collections import avl
from proto_db.collections.avl import CountedKeyNode, KeyNode, check_invariants
from proto_db.errors import ProtoCorruptionError, ProtoValidationError
from proto_db.storage.memory import MemoryStorage
from proto_db.values import order_key
from tests.support.persistence import DirectContext

SM_SETTINGS = settings(max_examples=60, stateful_step_count=40, deadline=None)


def _expand(model: dict, descending=False):
    """The occurrence sequence of a model {key: count}: each node's occurrences in a run."""
    keys = sorted(model, key=order_key, reverse=descending)
    return [(k, i) for k in keys for i in range(model[k])]


def _tree_rows(root, start, descending=False):
    out = []
    for key, value, skip in avl.key_iter_from_weight(root, start, descending):
        out.extend((key, i) for i in range(skip, value[1]))
    return out


class WeightedMachine(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self.root = None
        self.model = {}
        self.history = []

    def _commit(self, root, model):
        self.history.append((self.root, self.model))
        self.root, self.model = root, model

    @rule(key=st.integers(-20, 20), count=st.integers(1, 4))
    def put(self, key, count):
        self._commit(avl.key_put(self.root, key, ("p", count), node_class=CountedKeyNode)[0],
                     {**self.model, key: count})

    @rule(key=st.integers(-20, 20))
    def remove(self, key):
        model = dict(self.model)
        model.pop(key, None)
        self._commit(avl.key_remove(self.root, key)[0], model)

    @rule(pairs=st.dictionaries(st.integers(-20, 20), st.integers(1, 4), max_size=25))
    def rebuild(self, pairs):
        triples = [(order_key(k), k, ("p", n)) for k, n in sorted(pairs.items())]
        self._commit(avl.key_from_sorted_okeys(triples, node_class=CountedKeyNode), dict(pairs))

    @invariant()
    def weights_exact(self):
        root = self.root
        check_invariants(root)
        assert root is None or type(root) is CountedKeyNode
        total = sum(self.model.values())
        assert avl.key_weight(root) == total
        for desc in (False, True):
            rows = _expand(self.model, desc)
            for r in range(0, total + 1):
                assert _tree_rows(root, r, desc) == rows[r:]
        for bound in range(-22, 23):
            for inclusive in (False, True):
                expected = sum(n for k, n in self.model.items() if k < bound or (inclusive and k == bound))
                assert avl.key_weight_below(root, order_key(bound), inclusive) == expected

    @invariant()
    def old_roots_unchanged(self):
        for root, model in self.history[-5:]:
            assert avl.key_weight(root) == sum(model.values())


TestWeightedMachine = WeightedMachine.TestCase
TestWeightedMachine.settings = SM_SETTINGS


def test_plain_keynode_trees_stay_plain():
    root = None
    for k in range(10):
        root = avl.key_put(root, k, ("p", 1))[0]
    assert type(root) is KeyNode
    with pytest.raises(ProtoValidationError):
        avl.key_weight(root)


def test_existing_root_class_wins_over_node_class():
    root = avl.key_put(None, 1, ("p", 1))[0]
    root = avl.key_put(root, 2, ("p", 1), node_class=CountedKeyNode)[0]
    assert type(root) is KeyNode and type(root.right) is KeyNode


def test_value_must_carry_a_count():
    with pytest.raises(ProtoValidationError):
        CountedKeyNode(1, "no pair")
    with pytest.raises(ProtoValidationError):
        CountedKeyNode(1, ("p", -1))
    with pytest.raises(ProtoValidationError):
        CountedKeyNode(1, ("p", True))


def test_rank_out_of_range_is_rejected():
    root = avl.key_put(None, 1, ("p", 2), node_class=CountedKeyNode)[0]
    assert list(avl.key_iter_from_weight(root, 2)) == []
    with pytest.raises(ProtoValidationError):
        list(avl.key_iter_from_weight(root, 3))
    with pytest.raises(ProtoValidationError):
        list(avl.key_iter_from_weight(root, -1))


def test_weights_survive_a_round_trip_through_storage():
    ctx = DirectContext(MemoryStorage())
    triples = [(order_key(k), k, ("p", k % 3 + 1)) for k in range(200)]
    root = avl.key_from_sorted_okeys(triples, node_class=CountedKeyNode)
    root._bind(ctx)
    ptr = ctx.save(root)
    loaded = ctx.load(ptr, CountedKeyNode.__type_name__)
    check_invariants(loaded)
    assert avl.key_weight(loaded) == sum(k % 3 + 1 for k in range(200))
    assert [k for k, _, _ in avl.key_iter_from_weight(loaded, 150)][:3] == [75, 76, 77]  # keys 0..74 hold 150


def test_check_invariants_catches_a_wrong_weight():
    root = avl.key_from_sorted_okeys([(order_key(k), k, ("p", 1)) for k in range(7)], node_class=CountedKeyNode)
    object.__setattr__(root.left, "weight", 99)
    with pytest.raises(ProtoCorruptionError, match="weight"):
        check_invariants(root)
