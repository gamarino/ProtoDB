"""Unit tests for proto_db.collections.base: IndexSet, IndexedCollection, ConcurrentOptimized."""
import random

import pytest

from proto_db.collections.base import ConcurrentOptimized, IndexedCollection, IndexSet, net_effect
from proto_db.errors import ProtoConflictError, ProtoUserError, ProtoValidationError
from proto_db.storage.memory import MemoryStorage
from tests.contracts.collection_contract import CollectionFactory, check_mutator
from tests.support.persistence import DirectContext
from tests.support.recording_index import RecordingIndex
from tests.support.toy_collection import ToySet


def toy(*members, indexes=()):
    s = ToySet()
    for name in indexes:
        s = s.add_index(RecordingIndex(name))
    for m in members:
        s = s.add(m)
    return s


# -- IndexSet -----------------------------------------------------------------------------

class TestIndexSet:
    def test_empty(self):
        s = IndexSet()
        assert s.names() == [] and len(s) == 0 and s.get("x") is None and "x" not in s

    def test_empty_hooks_return_self(self):
        s = IndexSet()
        assert s.on_add(1) is s and s.on_remove(1) is s and s.on_replace(1, 2) is s

    def test_with_index_and_without(self):
        a, b = RecordingIndex("b"), RecordingIndex("a")
        s0 = IndexSet()
        s1 = s0.with_index(a)
        s2 = s1.with_index(b)
        assert s0.names() == [] and s1.names() == ["b"] and s2.names() == ["a", "b"]
        assert s2.get("a") is b and "a" in s2 and len(s2) == 2
        s3 = s2.without("b")
        assert s3.names() == ["a"] and s2.names() == ["a", "b"]

    def test_duplicate_and_absent_names_raise(self):
        s = IndexSet().with_index(RecordingIndex("a"))
        with pytest.raises(ProtoValidationError):
            s.with_index(RecordingIndex("a"))
        with pytest.raises(ProtoValidationError):
            s.without("zzz")

    @pytest.mark.parametrize("bad", [None, "", 3, b"a"])
    def test_invalid_index_name(self, bad):
        class Nameless:
            name = bad
        with pytest.raises(ProtoValidationError):
            IndexSet().with_index(Nameless())

    def test_index_without_name_attribute(self):
        with pytest.raises(ProtoValidationError):
            IndexSet().with_index(object())

    def test_hooks_apply_to_every_index_and_keep_receiver(self):
        s = IndexSet().with_index(RecordingIndex("a")).with_index(RecordingIndex("b"))
        s2 = s.on_add(1).on_remove(1).on_replace(2, 3)
        for name in ("a", "b"):
            assert s.get(name).events == ()
            assert s2.get(name).events == (("add", 1), ("remove", 1), ("replace", 2, 3))

    def test_hook_returning_a_renamed_index_is_rejected(self):
        class Renaming(RecordingIndex):
            __type_name__ = "tests.unit.Renaming"

            def with_added(self, element):
                return RecordingIndex("other")
        s = IndexSet().with_index(Renaming("a"))
        with pytest.raises(ProtoValidationError):
            s.on_add(1)

    def test_immutable(self):
        s = IndexSet()
        with pytest.raises(ProtoUserError):
            s.root = None

    def test_round_trip(self):
        s = IndexSet().with_index(RecordingIndex("a", (("add", 1),))).with_index(RecordingIndex("b"))
        ctx = DirectContext(MemoryStorage())
        loaded = ctx.load(ctx.save(s), IndexSet.__type_name__)
        assert loaded.names() == ["a", "b"]
        assert loaded.get("a").events == (("add", 1),)


# -- IndexedCollection --------------------------------------------------------------------

class TestIndexedCollection:
    def test_defaults(self):
        s = ToySet()
        assert s.count == 0 and len(s) == 0 and list(s) == [] and s.index_names() == []
        assert isinstance(s.indexes, IndexSet)

    def test_iteration_is_elements(self):
        assert list(toy(3, 1, 2)) == [1, 2, 3]

    def test_add_index_builds_over_current_elements(self):
        s = toy(1, 2)
        s2 = s.add_index(RecordingIndex("i"))
        assert s2.get_index("i").events == (("build", (1, 2)),)
        assert s.index_names() == [] and s2.index_names() == ["i"]

    def test_add_index_duplicate_name_raises_before_building(self):
        built = []

        class Spy(RecordingIndex):
            __type_name__ = "tests.unit.Spy"

            def build(self, elements):
                built.append(1)
                return super().build(elements)
        s = toy(1, indexes=("i",))
        with pytest.raises(ProtoValidationError):
            s.add_index(Spy("i"))
        assert built == []

    def test_add_index_rejects_renaming_build(self):
        class Bad(RecordingIndex):
            __type_name__ = "tests.unit.BadBuild"

            def build(self, elements):
                return RecordingIndex("other")
        with pytest.raises(ProtoValidationError):
            toy(1).add_index(Bad("i"))

    def test_remove_index(self):
        s = toy(1, indexes=("i", "j"))
        assert s.remove_index("i").index_names() == ["j"]
        with pytest.raises(ProtoValidationError):
            s.remove_index("nope")

    def test_with_rejects_unknown_fields(self):
        with pytest.raises(ProtoValidationError):
            ToySet()._with(bogus=1)

    def test_with_keeps_context(self):
        ctx = DirectContext(MemoryStorage())
        s = ToySet(context=ctx)
        assert s.add(1).context is ctx

    def test_immutable(self):
        s = ToySet()
        with pytest.raises(ProtoUserError):
            s.count = 3
        with pytest.raises(ProtoUserError):
            del s.count

    def test_elements_is_abstract(self):
        class Bare(IndexedCollection):
            __type_name__ = "tests.unit.Bare"
            _fields = ("count", "indexes")
        with pytest.raises(NotImplementedError):
            list(Bare().elements())

    def test_fields_must_include_count_and_indexes(self):
        with pytest.raises(ProtoUserError):
            class Missing(IndexedCollection):
                __type_name__ = "tests.unit.MissingFields"
                _fields = ("root",)

    def test_mixin_must_precede_indexed_collection(self):
        with pytest.raises(ProtoUserError):
            class Wrong(IndexedCollection, ConcurrentOptimized):
                __type_name__ = "tests.unit.WrongOrder"
                _fields = ("count", "indexes")

    def test_stub_has_no_op_log_and_loads_lazily(self):
        ctx = DirectContext(MemoryStorage())
        s = toy(1, 2, indexes=("i",))
        assert s._op_log
        loaded = ctx.load(ctx.save(s), ToySet.__type_name__)
        assert loaded._op_log == ()
        assert list(loaded) == [1, 2] and loaded.get_index("i").events == s.get_index("i").events
        # A mutation of a loaded version keeps working and persists again.
        again = loaded.add(3)
        reloaded = ctx.load(ctx.save(again), ToySet.__type_name__)
        assert list(reloaded) == [1, 2, 3]
        assert reloaded.get_index("i").events[-1] == ("add", 3)


def test_contract_catches_a_stale_index():
    """A collection whose mutator forgets the index hook fails the contract."""
    class Forgetful(ToySet):
        __type_name__ = "tests.unit.Forgetful"

        def discard(self, e):
            from proto_db.collections import avl
            root, removed, _ = avl.key_remove(self.root, e)
            if not removed:
                return self
            return self._with(root=root, count=self.count - 1)  # indexes left stale

    def populate(coll, rng):
        for i in range(5):
            coll = coll.add(i)
        return coll

    factory = CollectionFactory(
        name="Forgetful", empty=Forgetful, populate=populate,
        mutators={"discard": lambda c, rng: (c.discard(0), [("remove", 0)])})
    with pytest.raises(AssertionError):
        check_mutator(factory, "discard", 0)


# -- ConcurrentOptimized ------------------------------------------------------------------

class TestNetEffect:
    def test_last_op_per_key_in_order_of_last_occurrence(self):
        ops = [("add", 1), ("add", 2), ("remove", 1), ("add", 3), ("add", 2)]
        assert net_effect(ops) == [("remove", 1), ("add", 3), ("add", 2)]

    def test_equal_keys_of_different_types_are_one_key(self):
        assert net_effect([("add", 1), ("remove", 1.0)]) == [("remove", 1.0)]

    def test_custom_key(self):
        ops = [("set", "a", 1), ("set", "b", 2), ("set", "a", 3)]
        assert net_effect(ops, key=lambda op: op[1]) == [("set", "b", 2), ("set", "a", 3)]

    def test_empty(self):
        assert net_effect(()) == []


class TestRebase:
    def test_logged_is_immutable_extension(self):
        s1 = toy(1)
        s2 = s1.add(2)
        assert s1._op_log == (("add", 1),) and s2._op_log == (("add", 1), ("add", 2))

    def test_no_op_mutations_do_not_log(self):
        s = toy(1)
        assert s.add(1)._op_log == s._op_log and s.discard(9)._op_log == s._op_log

    def test_rebase_replays_net_effect(self):
        base = toy(1, 2, 3)
        ctx = DirectContext(MemoryStorage())
        stored = ctx.load(ctx.save(base), ToySet.__type_name__)  # snapshot: empty op log
        mine = stored.add(10).discard(1).add(1).discard(2).add(11).discard(11)
        theirs = stored.add(20).discard(3)
        merged = mine.rebase_onto(theirs)
        assert list(merged) == [1, 10, 20]
        assert list(theirs) == [1, 2, 20] and list(mine) == [1, 3, 10]

    def test_rebase_maintains_indexes(self):
        stored = _snapshot(toy(1, 2, indexes=("i",)))
        mine = stored.add(5).discard(1)
        theirs = stored.add(7)
        merged = mine.rebase_onto(theirs)
        assert list(merged) == [2, 5, 7]
        assert merged.get_index("i").live() == RecordingIndex("x", (("build", (2, 5, 7)),)).live()

    def test_rebase_onto_other_type_raises(self):
        with pytest.raises(ProtoValidationError):
            toy(1).rebase_onto(IndexSet())

    def test_rebase_replays_index_addition(self):
        stored = _snapshot(toy(1, 2))
        mine = stored.add_index(RecordingIndex("i")).add(3)
        theirs = stored.add(4)
        merged = mine.rebase_onto(theirs)
        assert merged.index_names() == ["i"] and list(merged) == [1, 2, 3, 4]
        assert merged.get_index("i").live() == RecordingIndex("x", (("build", (1, 2, 3, 4)),)).live()

    def test_rebase_index_added_on_both_sides_conflicts(self):
        stored = _snapshot(toy(1))
        mine = stored.add_index(RecordingIndex("i"))
        theirs = stored.add_index(RecordingIndex("i"))
        with pytest.raises(ProtoConflictError):
            mine.rebase_onto(theirs)

    def test_rebase_index_removal(self):
        stored = _snapshot(toy(1, indexes=("i", "j")))
        mine = stored.remove_index("i")
        assert mine.rebase_onto(stored.add(2)).index_names() == ["j"]
        # Already removed concurrently: a no-op.
        assert mine.rebase_onto(stored.remove_index("i")).index_names() == ["j"]

    def test_rebase_index_redefinition(self):
        stored = _snapshot(toy(1, indexes=("i",)))
        mine = stored.remove_index("i").add_index(RecordingIndex("i"))
        merged = mine.rebase_onto(stored.add(2))
        assert merged.index_names() == ["i"]
        assert merged.get_index("i").events == (("build", (1, 2)),)

    def test_rebase_index_added_then_removed_is_no_op(self):
        stored = _snapshot(toy(1))
        mine = stored.add_index(RecordingIndex("i")).remove_index("i")
        theirs = stored.add_index(RecordingIndex("i"))
        assert mine.rebase_onto(theirs).index_names() == ["i"]

    def test_rebase_random_against_model(self):
        for seed in range(20):
            rng = random.Random(seed)
            start = {rng.randrange(30) for _ in range(10)}
            stored = _snapshot(toy(*sorted(start), indexes=("i",)))
            mine, theirs = stored, stored
            model_mine = {}
            for _ in range(30):
                e = rng.randrange(30)
                # Net effect relative to the snapshot: no-op mutations are not logged.
                if rng.random() < 0.5:
                    if not mine.has(e):
                        model_mine[e] = True
                    mine = mine.add(e)
                else:
                    if mine.has(e):
                        model_mine[e] = False
                    mine = mine.discard(e)
                f = rng.randrange(30)
                theirs = theirs.add(f) if rng.random() < 0.5 else theirs.discard(f)
            expected = set(theirs)
            for e, present in model_mine.items():
                (expected.add if present else expected.discard)(e)
            merged = mine.rebase_onto(theirs)
            assert list(merged) == sorted(expected)
            assert merged.get_index("i").live() == RecordingIndex("x", (("build", tuple(merged)),)).live()


def _snapshot(coll):
    """Save and reload ``coll`` so that the version starts with an empty op log, as a transaction would."""
    ctx = DirectContext(MemoryStorage())
    return ctx.load(ctx.save(coll), type(coll).__type_name__)
