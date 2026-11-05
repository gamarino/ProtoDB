"""Rank access on key-group indexes (Task 8.4b, D56): windows and exact row counts.

For every operator an index answers, and for its ordered scans, ``plan.window(a, b)``
must equal ``list(plan.execute())[a:b]`` and ``plan.exact_rows()`` must equal the number
of rows, on indexes reached by every write path (bulk build, insertions, removals,
replacements).  The recount checker verifies every weight after each path.
"""
import pytest
from hypothesis import given, settings, strategies as st

from proto_db.collections import avl
from proto_db.collections.avl import KeyNode
from proto_db.indexing import EqualityIndex, RangeIndex
from proto_db.query.terms import Term
from proto_db.values import MISSING
from tests.support.index_weights import assert_exact_weights

KEYS = st.one_of(st.integers(-6, 6), st.none(), st.just(MISSING), st.sampled_from(["a", "b", "c"]),
                 st.just([1, 2]), st.just(float("nan")))


def _element(key, tag):
    return {"tag": tag} if key is MISSING else {"k": key, "tag": tag}


ELEMENTS = st.lists(st.tuples(KEYS, st.integers(0, 3)), max_size=40).map(
    lambda pairs: [_element(k, t) for k, t in pairs])

OPERANDS = st.one_of(st.integers(-7, 7), st.sampled_from(["a", "b", "z"]))


def _terms(draw):
    v = draw(OPERANDS)
    w = draw(OPERANDS.filter(lambda x: type(x) is type(v)))
    lo, hi = sorted([v, w])
    terms = [Term("k", op, v) for op in ("==", "!=", "<", "<=", ">", ">=")]
    terms += [Term("k", op, (lo, hi)) for op in ("between[]", "between()", "between(]", "between[)")]
    terms += [Term("k", "in", (v, w, v)), Term("k", "?N"), Term("k", "?!N"), Term("k", "==", None)]
    return terms


def _check_plan(plan, data):
    rows = list(plan.execute())
    assert plan.exact_rows() == len(rows)
    n = len(rows)
    starts = sorted({0, 1, n // 2, max(n - 1, 0), n, data.draw(st.integers(0, n + 2))})
    for a in starts:
        for size in (0, 1, 3, n + 5):
            assert list(plan.window(a, a + size)) == rows[a:a + size], (plan.explain(), a, size)
    assert list(plan.window(0, None)) == rows


def _check_index(index, data):
    assert_exact_weights(index)
    for term in _terms(data.draw):
        if index.supports(term):
            _check_plan(index.plan(term), data)
    if isinstance(index, RangeIndex):
        for desc in (False, True):
            _check_plan(index.ordered_plan(desc), data)


@settings(max_examples=150, deadline=None)
@given(elements=ELEMENTS, data=st.data())
def test_built_index_windows_equal_execution(elements, data):
    for cls in (RangeIndex, EqualityIndex):
        _check_index(cls("i", "k").build(elements), data)


@settings(max_examples=100, deadline=None)
@given(elements=ELEMENTS, removals=st.lists(st.integers(0, 39), max_size=15),
       replacements=st.lists(st.tuples(st.integers(0, 39), KEYS), max_size=10), data=st.data())
def test_incremental_writes_keep_weights_exact(elements, removals, replacements, data):
    index = RangeIndex("i", "k")
    held = []
    for e in elements:
        index = index.with_added(e)
        held.append(e)
    assert_exact_weights(index)
    for i in removals:
        if held:
            e = held.pop(i % len(held))
            index = index.with_removed(e)
    assert_exact_weights(index)
    for i, key in replacements:
        if held:
            j = i % len(held)
            new = _element(key, 9)
            index = index.with_replaced(held[j], new)
            held[j] = new
    _check_index(index, data)
    index.validate(held)


def test_window_bounds_are_validated():
    plan = RangeIndex("i", "k").build([{"k": 1}, {"k": 2}]).ordered_plan()
    assert list(plan.window(5, 9)) == []
    with pytest.raises(Exception):
        plan.window(-1, 2)


def test_an_unweighted_index_offers_no_window_and_still_answers():
    index = RangeIndex("i", "k").build([{"k": i % 4} for i in range(20)])
    groups = avl.key_from_sorted((k, (avl.key_from_sorted(avl.key_iter(g[0])), g[1]))
                                 for k, g in avl.key_iter(index.groups))
    assert type(groups) is KeyNode
    old = index._with(groups=groups)
    plan = old.plan(Term("k", ">=", 2))
    assert plan.window(0, 3) is None and plan.exact_rows() is None
    assert list(plan.execute()) == list(index.plan(Term("k", ">=", 2)).execute())
    # Writes keep the old tree's class: the index stays unweighted until it is rebuilt.
    assert type(old.with_added({"k": 9}).groups) is KeyNode
    assert_exact_weights(old.empty().build(old.iter_ordered()))


@settings(max_examples=120, deadline=None)
@given(elements=ELEMENTS, data=st.data())
def test_sorted_and_filtered_index_plans_window_by_rank(elements, data):
    """The planner's shapes for "predicate + order by the indexed key": Sort over an index plan,
    and a Filter of one term over an ordered scan.  Both must window by rank and equal execution."""
    from proto_db.query.plans import Filter, Sort
    index = RangeIndex("i", "k").build(elements)
    for term in _terms(data.draw):
        for desc in (False, True):
            if index.supports(term):
                sorted_plan = Sort(index.plan(term), [("k", desc)])
                assert sorted_plan.window(0, 0) is not None
                _check_plan(sorted_plan, data)
            filtered = Filter(index.ordered_plan(desc), term)
            if index.supports(term):
                assert filtered.window(0, 0) is not None
            _check_plan(filtered, data) if filtered.window(0, 0) is not None else None
    # A sort by another key, or by two keys, is not served by the index.
    assert Sort(index.plan(Term("k", ">", 0)), [("tag", False)]).window(0, 1) is None
    assert Sort(index.plan(Term("k", ">", 0)), ["k", "tag"]).window(0, 1) is None
    assert Filter(index.ordered_plan(), Term("tag", "==", 1)).window(0, 1) is None
