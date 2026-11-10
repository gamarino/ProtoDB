"""Unit tests for proto_db.collections.lists.List (spec section 8.2, D23)."""
import pytest

from proto_db.collections import avl
from proto_db.collections.base import ConcurrentOptimized
from proto_db.collections.lists import List
from proto_db.errors import ProtoUserError, ProtoValidationError
from proto_db.storage.memory import MemoryStorage
from tests.support.persistence import DirectContext
from tests.support.recording_index import RecordingIndex


def make(*items, indexes=()):
    lst = List()
    for name in indexes:
        lst = lst.add_index(RecordingIndex(name))
    for item in items:
        lst = lst.append_last(item)
    return lst


def reload(coll):
    context = DirectContext(MemoryStorage())
    return context.load(context.save(coll), type(coll).__type_name__)


class TestBasics:
    def test_empty(self):
        lst = List()
        assert lst.count == 0 and len(lst) == 0 and list(lst) == [] and list(lst.elements()) == []

    def test_not_concurrent_optimized(self):
        """Positional writes do not merge: a List root is last-writer-wins (D23)."""
        assert not isinstance(List(), ConcurrentOptimized)

    def test_append_and_get(self):
        lst = make("a", "b", "c")
        assert list(lst) == ["a", "b", "c"]
        assert [lst.get_at(i) for i in range(3)] == ["a", "b", "c"]
        assert lst.get_at(-1) == "c" and lst.get_at(-3) == "a"

    def test_get_out_of_range_raises(self):
        lst = make(1, 2)
        for i in (2, -3, 100):
            with pytest.raises(ProtoValidationError):
                lst.get_at(i)
        with pytest.raises(ProtoValidationError):
            List().get_at(0)
        with pytest.raises(ProtoValidationError):
            lst.get_at(True)

    def test_append_first(self):
        assert list(make(2, 3).append_first(1)) == [1, 2, 3]

    def test_insert_at(self):
        lst = make(1, 2, 3)
        assert list(lst.insert_at(0, 0)) == [0, 1, 2, 3]
        assert list(lst.insert_at(3, 4)) == [1, 2, 3, 4]
        assert list(lst.insert_at(-1, 9)) == [1, 2, 9, 3]
        with pytest.raises(ProtoValidationError):
            lst.insert_at(4, 0)
        with pytest.raises(ProtoValidationError):
            lst.insert_at(-4, 0)

    def test_set_at(self):
        lst = make(1, 2, 3)
        assert list(lst.set_at(1, 20)) == [1, 20, 3]
        assert list(lst.set_at(-1, 30)) == [1, 2, 30]
        assert list(lst) == [1, 2, 3]
        with pytest.raises(ProtoValidationError):
            lst.set_at(3, 0)

    def test_remove_at(self):
        lst = make(1, 2, 3)
        assert list(lst.remove_at(0)) == [2, 3]
        assert list(lst.remove_at(-1)) == [1, 2]
        assert lst.remove_at(1).count == 2
        with pytest.raises(ProtoValidationError):
            lst.remove_at(3)
        with pytest.raises(ProtoValidationError):
            List().remove_at(0)

    def test_extend(self):
        lst = make(1, 2)
        assert list(lst.extend([3, 4])) == [1, 2, 3, 4]
        assert list(lst.extend(make(5, 6))) == [1, 2, 5, 6]
        assert list(lst.extend(lst)) == [1, 2, 1, 2]
        assert list(lst.extend(x for x in (7,))) == [1, 2, 7]
        assert list(lst.extend([])) == [1, 2]

    def test_contains(self):
        lst = make(1, "x", (1, 2))
        assert 1 in lst and 1.0 in lst and "x" in lst and (1, 2) in lst
        assert True not in lst and 2 not in lst

    def test_values_are_canonicalised(self):
        lst = make(bytearray(b"ab"))
        assert lst.get_at(0) == b"ab" and type(lst.get_at(0)) is bytes
        with pytest.raises(ProtoValidationError):
            List().append_last(object())
        with pytest.raises(ProtoValidationError):
            List().extend([1, object()])

    def test_unorderable_values_are_storable(self):
        lst = make([1, 2], {"a": 1})
        assert list(lst) == [[1, 2], {"a": 1}]
        assert list(reload(lst)) == [[1, 2], {"a": 1}]

    def test_immutable(self):
        lst = make(1)
        with pytest.raises(ProtoUserError):
            lst.count = 5
        with pytest.raises(ProtoUserError):
            lst.root = None

    def test_mutators_never_change_receiver(self):
        lst = make(1, 2, 3)
        lst.append_last(4), lst.append_first(0), lst.insert_at(1, 9), lst.set_at(0, 7)
        lst.remove_at(0), lst.extend([5]), lst.slice(0, 1)
        assert list(lst) == [1, 2, 3] and lst.count == 3

    def test_structure_is_balanced(self):
        lst = List()
        for i in range(500):
            lst = lst.insert_at(lst.count // 2, i)
        avl.check_invariants(lst.root)
        assert lst.root.height <= 1.45 * 9 + 2


class TestSlicing:
    def test_slice_python_semantics(self):
        items = list(range(10))
        lst = List.from_iterable(items)
        cases = [(None, None), (2, 5), (-3, None), (None, -2), (5, 2), (-100, 100), (3, 3), (0, 10),
                 (11, 20), (-1, -5)]
        for start, stop in cases:
            assert list(lst.slice(start, stop)) == items[start:stop], (start, stop)
            assert lst.slice(start, stop).count == len(items[start:stop])

    def test_head_and_tail(self):
        items = list(range(6))
        lst = List.from_iterable(items)
        for n in (-10, -2, 0, 3, 6, 9):
            assert list(lst.head(n)) == items[:n], n
            assert list(lst.tail(n)) == items[n:], n

    def test_slice_rebuilds_indexes(self):
        lst = make(1, 2, 3, 4, indexes=("i",))
        part = lst.slice(1, 3)
        assert part.index_names() == ["i"]
        assert part.get_index("i").events == (("build", (2, 3)),)
        assert lst.get_index("i").events[-1] == ("add", 4)

    def test_slice_without_indexes(self):
        assert make(1, 2).slice(0, 1).index_names() == []


class TestIndexEvents:
    def test_events_per_mutator(self):
        lst = make(1, 2, indexes=("i",))
        base = lst.get_index("i")
        assert lst.append_last(3).get_index("i").events_since(base) == (("add", 3),)
        assert lst.insert_at(1, 9).get_index("i").events_since(base) == (("add", 9),)
        assert lst.set_at(0, 5).get_index("i").events_since(base) == (("replace", 1, 5),)
        assert lst.remove_at(1).get_index("i").events_since(base) == (("remove", 2),)
        assert lst.extend([7, 8]).get_index("i").events_since(base) == (("add", 7), ("add", 8))


class TestBulkAndPersistence:
    def test_from_iterable(self):
        lst = List.from_iterable(["a", 1, None])
        assert list(lst) == ["a", 1, None] and lst.count == 3
        assert List.from_iterable([]).count == 0
        with pytest.raises(ProtoValidationError):
            List.from_iterable([object()])

    def test_from_iterable_context(self):
        context = DirectContext(MemoryStorage())
        lst = List.from_iterable([1], context=context)
        assert lst.context is context
        assert list(context.load(context.save(lst), List.__type_name__)) == [1]

    def test_round_trip_and_mutate_loaded(self):
        lst = make(1, 2, 3, indexes=("i",))
        loaded = reload(lst)
        assert list(loaded) == [1, 2, 3] and loaded.get_at(1) == 2 and loaded.count == 3
        assert list(loaded.insert_at(1, 9)) == [1, 9, 2, 3]
        assert loaded.get_index("i").events == lst.get_index("i").events

    def test_large_bulk_list_has_no_recursion_problem(self):
        n = 100_000
        lst = List.from_iterable(range(n))
        assert lst.count == n and lst.get_at(n - 1) == n - 1 and lst.get_at(54321) == 54321
        avl.check_invariants(lst.root)
        loaded = reload(lst)
        assert loaded.get_at(77777) == 77777
        total = 0
        for i, v in enumerate(loaded):
            assert v == i
            total += 1
        assert total == n
        assert list(lst.extend(range(5)).tail(-5)) == [0, 1, 2, 3, 4]
