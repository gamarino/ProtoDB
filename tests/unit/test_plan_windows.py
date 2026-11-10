"""Rank access through the plan protocol (Task 8.4b, D56): scans, Limit and Count.

``window(start, stop)`` must equal ``islice(execute(), start, stop)`` and ``exact_rows()``
the number of rows, whenever a plan offers them; ``Limit`` and ``Count`` use them, so
``skip(o).take(p)`` over an index or a List does not produce the rows before ``o``.
"""
import pytest

from proto_db.collections import avl
from proto_db.collections.dictionaries import Dictionary
from proto_db.collections.lists import List
from proto_db.collections.sets import Set
from proto_db.indexing import RangeIndex
from proto_db.query.linq import F, from_collection
from proto_db.query.plans import CollectionScanPlan, Count, Filter, IndexPlan, Limit
from proto_db.query.expressions import And
from proto_db.query.terms import Term


def _check(plan):
    rows = list(plan.execute())
    assert plan.exact_rows() == len(rows)
    for a in range(0, len(rows) + 2):
        for size in (0, 1, 2, 5, len(rows) + 3):
            assert list(plan.window(a, a + size)) == rows[a:a + size]
        assert list(plan.window(a)) == rows[a:]


def test_list_and_dictionary_scans_offer_windows():
    lst = List.from_iterable(range(23))
    _check(CollectionScanPlan(lst))
    d = Dictionary.from_items((k, -k) for k in range(17))
    _check(CollectionScanPlan(d))


def test_other_scans_do_not_claim_windows():
    plan = CollectionScanPlan(Set.from_iterable(range(5)))
    assert plan.window(0, 2) is None and plan.exact_rows() is None


def test_positional_iteration_from_a_position():
    root = avl.pos_from_iterable(range(40))
    for start in range(41):
        assert list(avl.pos_iter_from(root, start)) == list(range(start, 40))
    keyed = avl.key_from_sorted((k, str(k)) for k in range(30))
    for start in range(31):
        assert [k for k, _ in avl.key_iter_from(keyed, start)] == list(range(start, 30))


def test_limit_over_a_window_does_not_run_the_producer():
    index = RangeIndex("k", "k").build([{"k": i} for i in range(1000)])
    inner = index.ordered_plan()
    produced = []

    def producer():
        for e in inner.execute():
            produced.append(e)
            yield e
    plan = Limit(IndexPlan(index, None, producer, kind="IndexOrderedScan", estimate=inner.estimate(),
                           rank=(inner.window, inner.exact_rows)), 3, 900)
    assert list(plan.execute()) == [{"k": 900}, {"k": 901}, {"k": 902}]
    assert produced == []           # served by rank: no row before the window was produced
    no_rank = Limit(IndexPlan(index, None, producer, kind="IndexOrderedScan", estimate=inner.estimate()), 3, 900)
    assert list(no_rank.execute()) == [{"k": 900}, {"k": 901}, {"k": 902}]
    assert len(produced) == 903


def test_count_uses_exact_rows():
    index = RangeIndex("k", "k").build([{"k": i % 10} for i in range(1000)])
    plan = Count(index.plan(Term("k", ">=", 7)))
    assert list(plan.execute()) == [300]
    assert list(Count(Filter(index.ordered_plan(), Term("k", "==", 3))).execute()) == [100]


def test_filters_seek_only_for_one_indexed_term_over_an_ordered_scan():
    index = RangeIndex("k", "k").build([{"k": i, "j": i % 3} for i in range(10)])
    _check(Filter(index.ordered_plan(), Term("k", ">", 3)))
    _check(Filter(index.ordered_plan(True), Term("k", "<=", 6)))
    assert Filter(index.ordered_plan(), Term("j", "==", 1)).window(0, 1) is None
    assert Filter(index.ordered_plan(), And(Term("k", ">", 3), Term("k", "<", 8))).window(0, 1) is None
    assert Filter(index.plan(Term("k", ">", 1)), Term("k", ">", 3)).window(0, 1) is None


def test_linq_skip_take_over_an_ordered_index():
    lst = List.from_iterable({"k": (i * 37) % 101} for i in range(101)).add_index(RangeIndex("k", "k"))
    q = from_collection(lst).order_by(F.k).skip(50).take(4)
    assert [e["k"] for e in q] == [50, 51, 52, 53]
    assert "Limit" in str(q.explain())


def test_window_arguments_are_validated():
    plan = CollectionScanPlan(List.from_iterable(range(3)))
    with pytest.raises(Exception):
        plan.window(-1, 2)
    with pytest.raises(Exception):
        plan.window(0, "x")


def test_a_subclass_redefining_elements_is_not_windowed():
    class Odd(List):
        __type_name__ = "tests.plan_windows.Odd"

        def elements(self):
            return (v for v in super().elements() if v % 2)
    plan = CollectionScanPlan(Odd.from_iterable(range(6)))
    assert plan.window(0, 2) is None and plan.exact_rows() is None
    assert list(Limit(plan, 2, 1).execute()) == [3, 5]
