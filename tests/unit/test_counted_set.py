"""Unit tests for proto_db.collections.sets.CountedSet (spec section 8.2, D26)."""
import random
import threading
from collections import Counter
from decimal import Decimal

import pytest

from proto_db.collections import avl
from proto_db.collections.base import ConcurrentOptimized
from proto_db.collections.sets import CountedSet
from proto_db.errors import ProtoUserError, ProtoValidationError
from proto_db.storage.memory import MemoryStorage
from proto_db.values import hash_key, order_key
from tests.support.persistence import DirectContext
from tests.support.recording_index import RecordingIndex


class CollidingCountedSet(CountedSet):
    """Four buckets only, so that collision handling is exercised on every operation."""

    __type_name__ = "tests.unit.CollidingCountedSet"

    def _bucket_hash(self, element):
        return hash_key(element) % 4


both = pytest.mark.parametrize("cls", [CountedSet, CollidingCountedSet])


def make(items=(), indexes=(), cls=CountedSet):
    s = cls()
    for name in indexes:
        s = s.add_index(RecordingIndex(name))
    for e in items:
        s = s.add(e)
    return s


def reload(coll, context=None):
    context = context or DirectContext(MemoryStorage())
    return context.load(context.save(coll), type(coll).__type_name__)


def counts(coll):
    return {order_key(e): n for e, n in coll.items()}


def literal(start: dict, ops) -> dict:
    """Independent model: apply counter ops one at a time, clamping at zero."""
    model = Counter(start)
    for op in ops:
        k = order_key(op[1])
        if op[0] == "add":
            model[k] += 1
        elif op[0] == "remove":
            if model[k] > 0:
                model[k] -= 1
        else:  # ("transform", e, removes, adds): logged by a replayed merge
            model[k] = max(model[k] - op[2], 0) + op[3]
    return dict(+model)


class TestBasics:
    def test_empty(self):
        s = CountedSet()
        assert s.count == 0 and s.total_count == 0 and list(s) == [] and s.count_of("a") == 0

    def test_concurrent_optimized(self):
        assert isinstance(CountedSet(), ConcurrentOptimized)

    @both
    def test_add_counts(self, cls):
        s = make(["a", "b", "a", "a"], cls=cls)
        assert s.count == 2 and s.total_count == 4 and len(s) == 2
        assert s.count_of("a") == 3 and s.count_of("b") == 1 and s.count_of("c") == 0
        assert sorted(s.elements()) == ["a", "b"]  # each distinct element once
        assert s.has("a") and "b" in s and not s.has("c")

    @both
    def test_remove_decrements_then_deletes(self, cls):
        s = make(["a", "a", "b"], cls=cls)
        r = s.remove("a")
        assert r.count_of("a") == 1 and r.count == 2 and r.total_count == 2
        r = r.remove("a")
        assert not r.has("a") and r.count == 1 and r.total_count == 1 and list(r) == ["b"]
        assert s.count_of("a") == 2  # receiver unchanged

    @both
    def test_remove_absent_is_content_noop(self, cls):
        s = make(["a", "a"], cls=cls)
        r = s.remove("zz")
        assert r is not s and r.root is s.root and r.total_count == 2 and r.count == 1

    def test_element_constraints(self):
        for bad in ([1], {"a": 1}, float("nan")):
            with pytest.raises(ProtoValidationError):
                CountedSet().add(bad)
            with pytest.raises(ProtoValidationError):
                CountedSet().count_of(bad)

    def test_immutable(self):
        with pytest.raises(ProtoUserError):
            make([1]).total_count = 5


class TestRegressions:
    @both
    def test_no_double_count(self, cls):
        """0.1 bug: adding an existing element counted it twice / counts drifted after reload."""
        context = DirectContext(MemoryStorage())
        s = make(["x"], cls=cls)
        s = reload(s, context).add("x")
        assert s.count_of("x") == 2 and s.total_count == 2 and s.count == 1
        s = reload(s, context)
        assert s.count_of("x") == 2 and s.total_count == 2 and s.count == 1
        s = reload(s.add("x").remove("x"), context)
        assert s.count_of("x") == 2 and s.total_count == 2 and s.count == 1

    @both
    def test_bool_and_int_distinct(self, cls):
        s = make([1, True, 1.0, Decimal(1)], cls=cls)
        assert s.count == 2 and s.count_of(1) == 3 and s.count_of(True) == 1 and s.total_count == 4

    def test_receiver_unchanged(self):
        s = make(["a"])
        snapshot = (s.root, s.count, s.total_count)
        s.add("a")
        s.add("b")
        s.remove("a")
        assert (s.root, s.count, s.total_count) == snapshot and s.count_of("a") == 1


class TestIndexEvents:
    @both
    def test_events_only_on_appear_and_disappear(self, cls):
        s = make(["a", "b", "b"], indexes=("i",), cls=cls)
        base = s.get_index("i")
        assert s.add("c").get_index("i").events_since(base) == (("add", "c"),)
        assert s.add("a").indexes is s.indexes  # 1 -> 2: no event
        assert s.remove("b").indexes is s.indexes  # 2 -> 1: no event
        assert s.remove("a").get_index("i").events_since(base) == (("remove", "a"),)
        assert s.remove("zz").indexes is s.indexes
        assert s.get_index("i").live() == Counter(order_key(e) for e in ["a", "b"])


class TestRebase:
    @both
    def test_concurrent_increments_both_count(self, cls):
        """D26: counters merge by net delta, so two concurrent increments are both kept."""
        snapshot = reload(make(["a"], indexes=("i",), cls=cls))
        theirs = snapshot.add("a").add("b")
        mine = snapshot.add("a").add("c").add("c")
        merged = mine.rebase_onto(theirs)
        assert counts(merged) == counts(make(["a", "a", "a", "b", "c", "c"]))
        assert merged.total_count == 6
        assert merged.get_index("i").live() == Counter(order_key(e) for e in merged)

    @both
    def test_decrements_clamp_at_zero(self, cls):
        snapshot = reload(make(["a", "a"], cls=cls))
        theirs = snapshot.remove("a").remove("a")
        mine = snapshot.remove("a")
        assert mine.rebase_onto(theirs).count_of("a") == 0

    def test_removes_beyond_zero_then_add(self):
        """Within one log, a remove that found nothing does not cancel a later add (serial equivalence)."""
        snapshot = reload(make(["a"]))
        mine = snapshot.remove("a").remove("a").add("a")
        assert mine.count_of("a") == 1
        assert mine.rebase_onto(snapshot).count_of("a") == 1
        # Serially after theirs {a: 2}: remove -> 1, remove -> 0, add -> 1.
        assert mine.rebase_onto(snapshot.add("a")).count_of("a") == 1
        assert mine.rebase_onto(snapshot.add("a").add("a")).count_of("a") == 2
        assert mine.rebase_onto(snapshot.remove("a")).count_of("a") == 1

    def test_rebase_matches_serial_replay(self):
        """Rebasing equals replaying mine's ops one by one after theirs (random logs)."""
        rng = random.Random(7)
        for _ in range(200):
            start = [rng.randrange(4) for _ in range(rng.randrange(6))]
            snapshot = make(start)._with(_op_log=())
            theirs, mine = snapshot, snapshot
            for _ in range(rng.randrange(8)):
                e = rng.randrange(4)
                theirs = theirs.add(e) if rng.random() < 0.5 else theirs.remove(e)
            for _ in range(rng.randrange(10)):
                e = rng.randrange(4)
                mine = mine.add(e) if rng.random() < 0.5 else mine.remove(e)
            merged = mine.rebase_onto(theirs)
            assert counts(merged) == literal(counts(theirs), mine._op_log)
            assert merged.total_count == sum(n for _, n in merged.items())


class TestBulk:
    @both
    def test_from_iterable(self, cls):
        s = cls.from_iterable(["a", "b", "a", 1, 1.0, True])
        assert s.count == 4 and s.total_count == 6 and s._op_log == ()
        assert s.count_of("a") == 2 and s.count_of(1) == 2 and s.count_of(True) == 1
        avl.check_invariants(s.root)
        assert cls.from_iterable([]).count == 0

    def test_large_bulk_round_trip(self):
        n = 20_000
        loaded = reload(CountedSet.from_iterable(i % 1000 for i in range(n)))
        assert loaded.count == 1000 and loaded.total_count == n
        assert all(loaded.count_of(i) == 20 for i in range(0, 1000, 37))


class TestFreeThreading:
    def test_concurrent_readers(self):
        n = 800
        base = reload(CountedSet.from_iterable(i % n for i in range(3 * n)))
        errors, barrier = [], threading.Barrier(8)

        def reader(seed):
            barrier.wait()
            for _ in range(3):
                if base.count != n or base.total_count != 3 * n or sum(1 for _ in base) != n:
                    errors.append("totals")
                for i in range(seed, n, 31):
                    if base.count_of(i) != 3:
                        errors.append(i)
                derived = base.add(seed).remove(seed + 1)
                if derived.count_of(seed) != 4 or base.count_of(seed) != 3:
                    errors.append(f"derive {seed}")

        threads = [threading.Thread(target=reader, args=(s,)) for s in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []


class TestReviewFixes:
    def test_replay_is_one_step_per_element(self):
        """The composed transform is applied as one adjustment: no remove/add pair when the element survives."""
        snapshot = reload(make(["a", "b", "b"], indexes=("i",)))
        theirs = snapshot.add("c")
        mine = snapshot.remove("a").add("a").remove("b").add("b").add("b")
        merged = mine.rebase_onto(theirs)
        assert merged.get_index("i").events_since(theirs.get_index("i")) == ()
        assert merged.count_of("b") == 3 and merged.count_of("a") == 1
        assert len(merged._op_log) == len(theirs._op_log) + 2  # one logged step per element

    def test_random_replay_emits_no_events_for_survivors(self):
        rng = random.Random(3)
        for _ in range(150):
            snapshot = make([rng.randrange(4) for _ in range(rng.randrange(6))], indexes=("i",))
            snapshot = snapshot._with(_op_log=())
            theirs, mine = snapshot, snapshot
            for _ in range(rng.randrange(6)):
                e = rng.randrange(4)
                theirs = theirs.add(e) if rng.random() < 0.5 else theirs.remove(e)
            for _ in range(rng.randrange(10)):
                e = rng.randrange(4)
                mine = mine.add(e) if rng.random() < 0.5 else mine.remove(e)
            merged = mine.rebase_onto(theirs)
            assert counts(merged) == literal(counts(theirs), mine._op_log)
            events = merged.get_index("i").events_since(theirs.get_index("i"))
            assert len(events) == len({order_key(ev[1]) for ev in events})  # at most one event per element
            for kind, e in events:
                assert (kind == "add") == (not theirs.has(e) and merged.has(e))
            # A merged version can itself be rebased (its log holds transform ops).
            assert counts(merged.rebase_onto(theirs)) == literal(counts(theirs), merged._op_log)
