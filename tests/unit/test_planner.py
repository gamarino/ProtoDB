"""Unit and property tests for the query planner, proto_db.query.planner (Task 6.2, spec section 10, D37).

Catches: a planner that picks a dearer access path when a cheaper index exists
(or breaks estimate ties non-deterministically); one that drops a conjunct
instead of keeping it as a residual filter, or shows a residual in
``explain()`` that differs from the conjuncts it did not answer with an index;
an OR planned as a Union although one branch has no index (a branch would be
lost), or a Union that loses or duplicates List duplicates; ORDER BY that
ignores a RangeIndex able to order (ascending and descending), uses one for a
multi-key order, or returns another order than ``Sort`` over a scan; LIMIT and
OFFSET applied before ordering; complement rewrites under ``Not`` that change
the answer for MISSING keys (D36 amendment: only ``Not(?N)`` <-> ``?!N`` is
exact); a ``near[]`` term evaluated by a residual filter or silently dropped
when no index supports it; equal-but-distinct values (``True``, ``1``,
``1.0``, ``Decimal("1")``) collapsed or confused; per-collection state cached
across versions; and an ``explain()`` that is not the tree ``execute()`` runs.
The Hypothesis property checks, for random collections (with duplicates),
random index sets and random expressions, that the planned result multiset
equals a scan filtered by ``evaluate``.
"""
import json
import math
from collections import Counter
from decimal import Decimal

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from proto_db.collections.lists import List
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoNotSupportedError, ProtoValidationError
from proto_db.indexing import KeySpec, element_identity
from proto_db.indexing.equality import EqualityIndex
from proto_db.indexing.range import RangeIndex
from proto_db.query import plans as plans_module
from proto_db.query.expressions import And, Not, Or, Term, evaluate
from proto_db.query.planner import normalize, plan_query
from proto_db.query.plans import (CollectionScanPlan, Estimate, Filter, IndexPlan, Limit, QueryPlan, Sort, Union,
                                  VectorSearch)
from proto_db.query.terms import (BETWEEN, CONTAINS, EQ, GE, GT, IN, IS_NONE, IS_TRUE, LE, LT, NE, NEAR, NOT_NONE,
                                  NOT_TRUE, NearQuery)
from proto_db.values import MISSING, exact_token
from tests.support.scan_index import ScanIndex


def r(**kw):
    return DBObject(**kw)


def ident(e):
    return element_identity(e), exact_token(e)


def ms(elements):
    """Exact multiset of a result (equal-but-distinct plain values stay apart)."""
    return Counter(ident(e) for e in elements)


def reference(collection, expr):
    """The scan oracle: every element of the collection for which ``evaluate`` holds."""
    if expr is None:
        return list(collection.elements())
    return [e for e in collection.elements() if evaluate(expr, e)]


def check(plan, collection, expr):
    """The plan returns the scan's multiset, twice, and explains as JSON with its own estimate."""
    assert isinstance(plan, QueryPlan)
    expected = ms(reference(collection, expr))
    assert ms(plan.execute()) == expected
    assert ms(plan.execute()) == expected
    info = plan.explain()
    json.dumps(info)
    assert info["estimate"] == plan.estimate().describe()
    return info


def node_names(info):
    """Every plan name of an explain tree, depth first."""
    out = [info["plan"]]
    for child in info["children"]:
        out.extend(node_names(child))
    return out


def find(info, name):
    """The first explain node named ``name`` (depth first), else None."""
    if info["plan"] == name:
        return info
    for child in info["children"]:
        hit = find(child, name)
        if hit is not None:
            return hit
    return None


# 200 records: ``x`` unique 0..199, ``y`` in 0..3, ``z`` in 0..9; every 10th record lacks ``y``.
RECORDS = [r(x=i, z=i % 10, name=f"n{i}", **({} if i % 10 == 0 else {"y": i % 4})) for i in range(200)]


def records(*indexes):
    coll = List.from_iterable(RECORDS)
    for index in indexes:
        coll = coll.add_index(index)
    return coll


# -- source handling and validation ------------------------------------------------------------------

def test_no_where_is_the_scan_itself():
    coll = records(EqualityIndex("by_x", "x"))
    plan = plan_query(coll)
    assert isinstance(plan, CollectionScanPlan) and plan.collection is coll
    check(plan, coll, None)


def test_collection_scan_plan_source_uses_the_collection_indexes():
    coll = records(EqualityIndex("by_x", "x"))
    scan = coll.as_query_plan()
    plan = plan_query(scan, Term("x", EQ, 7))
    assert isinstance(plan, IndexPlan) and plan.index is coll.get_index("by_x")
    check(plan, coll, Term("x", EQ, 7))


def test_other_plan_sources_get_a_filter_on_top():
    coll = records(EqualityIndex("by_x", "x"))
    source = Limit(coll.as_query_plan(), 50)
    expr = Term("x", EQ, 7)
    plan = plan_query(source, expr)
    assert isinstance(plan, Filter) and plan.source is source and plan.expr == expr
    assert [e.x for e in plan.execute()] == [7]


def test_where_accepts_the_list_syntax():
    coll = records(EqualityIndex("by_x", "x"))
    plan = plan_query(coll, ["x", "==", 3, "z", "<", 5])
    info = check(plan, coll, And(Term("x", EQ, 3), Term("z", LT, 5)))
    assert info["plan"] == "Filter" and info["predicate"] == Term("z", LT, 5).describe()


def test_invalid_arguments_raise():
    coll = records()
    with pytest.raises(ProtoValidationError):
        plan_query([1, 2, 3])
    with pytest.raises(ProtoValidationError):
        plan_query(coll, where="x == 1")
    for bad in (5, [("x", "yes")], [("x", True, 1)], [], [(3, False)]):
        with pytest.raises(ProtoValidationError):
            plan_query(coll, order_by=bad)
    for limit, offset in ((-1, 0), (1.5, 0), (True, 0), (1, -1), (None, -2)):
        with pytest.raises(ProtoValidationError):
            plan_query(coll, limit=limit, offset=offset)


# -- access path selection --------------------------------------------------------------------------

def test_picks_the_cheapest_index_and_keeps_the_rest_as_residual():
    coll = records(EqualityIndex("by_y", "y"), EqualityIndex("by_x", "x"), RangeIndex("by_z", "z"))
    expr = And(Term("y", EQ, 1), Term("z", EQ, 3), Term("x", IN, (3, 13, 23, 4)))
    plan = plan_query(coll, expr)
    info = check(plan, coll, expr)
    assert info["plan"] == "Filter"
    assert info["predicate"] == And(Term("y", EQ, 1), Term("z", EQ, 3)).describe()
    leaf = info["children"][0]
    assert leaf["plan"] == "IndexLookup" and leaf["index"]["index"] == "by_x"
    assert sorted(e.x for e in plan.execute()) == [13]


def test_cheaper_index_wins_over_a_dearer_one_on_the_same_term():
    # ScanIndex estimates a full pass (rows = cost = n): the EqualityIndex is cheaper although "a" < "b".
    coll = records(ScanIndex("a_scan", KeySpec.path("x")), EqualityIndex("b_eq", "x"))
    plan = plan_query(coll, Term("x", EQ, 5))
    assert isinstance(plan, IndexPlan) and plan.explain()["index"]["index"] == "b_eq"


def test_estimate_ties_break_by_index_name():
    coll = records(RangeIndex("m_range", "x"), EqualityIndex("k_eq", "x"), EqualityIndex("z_eq", "x"))
    assert coll.get_index("m_range").estimate(Term("x", EQ, 5)) == coll.get_index("k_eq").estimate(Term("x", EQ, 5))
    for _ in range(3):
        plan = plan_query(coll, Term("x", EQ, 5))
        assert plan.explain()["index"]["index"] == "k_eq"


def test_ne_term_alone_prefers_the_index_complement_over_scan_plus_filter():
    coll = records(EqualityIndex("by_z", "z"))
    expr = Term("z", NE, 3)
    info = check(plan_query(coll, expr), coll, expr)
    # D30: != costs n + G + 1 = 211 while scan + residual filter costs 2n = 400.
    assert info["plan"] == "IndexComplement"


def test_and_keeps_every_unindexed_conjunct_in_order():
    coll = records(EqualityIndex("by_x", "x"))
    residual = [Term("name", CONTAINS, "1"), Term("z", IS_TRUE), Not(Term("y", LT, 2)), Term("z", NOT_TRUE)]
    expr = And(residual[0], Term("x", IN, tuple(range(0, 200, 7))), *residual[1:])
    info = check(plan_query(coll, expr), coll, expr)
    assert info["plan"] == "Filter" and info["predicate"] == And(*residual).describe()
    assert info["children"][0]["plan"] == "IndexLookup"
    assert node_names(info) == ["Filter", "IndexLookup"]


def test_duplicate_conjuncts_are_planned_once():
    coll = records(EqualityIndex("by_x", "x"))
    expr = And(Term("x", EQ, 5), Term("z", LT, 8), Term("x", EQ, 5.0), Term("z", LT, 8))
    info = check(plan_query(coll, expr), coll, expr)
    assert info["plan"] == "Filter" and info["predicate"] == Term("z", LT, 8).describe()


def test_no_index_means_scan_plus_filter_of_the_whole_predicate():
    coll = records()
    expr = And(Term("x", EQ, 5), Term("z", LT, 8))
    info = check(plan_query(coll, expr), coll, expr)
    assert node_names(info) == ["Filter", "CollectionScan"] and info["predicate"] == expr.describe()


# -- disjunctions ---------------------------------------------------------------------------------------

def test_or_fully_indexed_uses_union():
    coll = records(EqualityIndex("by_x", "x"), RangeIndex("by_z", "z"))
    expr = Or(Term("x", EQ, 5), And(Term("z", EQ, 3), Term("y", EQ, 1)), Term("x", IN, (6, 7)))
    info = check(plan_query(coll, expr), coll, expr)
    assert info["plan"] == "Union"
    kinds = [c["plan"] for c in info["children"]]
    assert kinds == ["IndexLookup", "Filter", "IndexLookup"]
    assert info["children"][1]["predicate"] == Term("y", EQ, 1).describe()


def test_or_with_an_unindexed_branch_falls_back_to_scan():
    coll = records(EqualityIndex("by_x", "x"))
    expr = Or(Term("x", EQ, 5), Term("z", EQ, 3))
    info = check(plan_query(coll, expr), coll, expr)
    assert node_names(info) == ["Filter", "CollectionScan"] and info["predicate"] == expr.describe()


def test_or_with_a_negated_branch_falls_back_to_scan():
    coll = records(EqualityIndex("by_x", "x"))
    expr = Or(Term("x", EQ, 5), Not(Term("x", LT, 190)))
    info = check(plan_query(coll, expr), coll, expr)
    assert node_names(info) == ["Filter", "CollectionScan"]


def test_unselective_union_loses_to_the_scan():
    coll = records(EqualityIndex("by_z", "z"))
    expr = Or(*[Term("z", EQ, v) for v in range(10)])
    info = check(plan_query(coll, expr), coll, expr)
    assert node_names(info) == ["Filter", "CollectionScan"]


def test_or_conjunct_inside_and_is_an_access_path():
    coll = records(EqualityIndex("by_x", "x"))
    expr = And(Term("z", LT, 9), Or(Term("x", EQ, 5), Term("x", EQ, 9)))
    info = check(plan_query(coll, expr), coll, expr)
    assert info["plan"] == "Filter" and info["predicate"] == Term("z", LT, 9).describe()
    assert info["children"][0]["plan"] == "Union"


def test_union_keeps_list_duplicates_exactly():
    a, b, c = r(k=1, t="p"), r(k=2, t="q"), r(k=1, t="q")
    filler = [r(k=9, t="z") for _ in range(100)]  # makes the Union cheaper than scan + filter
    coll = List.from_iterable([a, a, b, c, c, c, b] + filler).add_index(EqualityIndex("by_k", "k")) \
        .add_index(EqualityIndex("by_t", "t"))
    for expr in (Or(Term("k", EQ, 1), Term("t", EQ, "q")),   # c matches both branches, three times
                 Or(Term("k", EQ, 1), Term("t", EQ, "p")),   # a twice through both branches
                 Or(Term("k", EQ, 2), Term("t", EQ, "zz"))):  # b twice through one branch only
        plan = plan_query(coll, expr)
        assert isinstance(plan, Union)
        check(plan, coll, expr)
    got = Counter(id(e) for e in plan_query(coll, Or(Term("k", EQ, 1), Term("t", EQ, "q"))).execute())
    assert sorted(got.values()) == [2, 2, 3]


# -- negation ------------------------------------------------------------------------------------------

def test_normalize_de_morgan_and_presence_only():
    x, y = Term("x", EQ, 1), Term("y", LT, 2)
    assert normalize(Not(And(x, y))) == Or(Not(x), Not(y))
    assert normalize(Not(Or(x, y))) == And(Not(x), Not(y))
    assert normalize(Not(Term("x", IS_NONE))) == Term("x", NOT_NONE)
    assert normalize(Not(Term("x", NOT_NONE))) == Term("x", IS_NONE)
    assert normalize(Not(Not(x))) == x
    assert normalize(Not(x)) == Not(x)  # never x != 1: MISSING satisfies Not(x == 1) only
    assert normalize(Not(Term("x", IS_TRUE))) == Not(Term("x", IS_TRUE))
    assert normalize(And(x, And(y, Not(Or(x, Term("x", IS_NONE)))))) == And(x, y, Not(x), Term("x", NOT_NONE))
    with pytest.raises(ProtoValidationError):
        normalize("x")


def test_not_presence_uses_the_index_and_not_comparison_stays_residual():
    coll = records(RangeIndex("by_y", "y"), EqualityIndex("by_x", "x"))
    expr = Not(Term("y", NOT_NONE))  # == y ?N: 20 rows lack y
    info = check(plan_query(coll, expr), coll, expr)
    assert info["plan"] == "IndexLookup" and info["term"]["op"] == IS_NONE
    expr = And(Term("x", IN, (0, 1, 2, 3, 10)), Not(Term("y", EQ, 1)))
    info = check(plan_query(coll, expr), coll, expr)
    assert info["plan"] == "Filter" and info["predicate"] == Not(Term("y", EQ, 1)).describe()
    # x = 0 and x = 10 lack y: Not(y == 1) is true for them, unlike y != 1.
    assert sorted(e.x for e in plan_query(coll, expr).execute()) == [0, 2, 3, 10]


def test_not_over_and_becomes_a_scan_filtered_by_the_disjunction():
    coll = records(EqualityIndex("by_x", "x"))
    expr = Not(And(Term("x", GE, 0), Term("y", EQ, 1)))
    info = check(plan_query(coll, expr), coll, expr)
    assert info["plan"] == "Filter" and info["predicate"] == normalize(expr).describe()


def test_not_over_or_of_presence_terms_is_indexed():
    coll = records(EqualityIndex("by_y", "y"), EqualityIndex("by_x", "x"))
    expr = Not(Or(Term("y", IS_NONE), Term("x", NOT_NONE)))  # == y ?!N and x ?N: empty
    info = check(plan_query(coll, expr), coll, expr)
    assert "CollectionScan" not in node_names(info)


# -- equal-but-distinct and odd values ------------------------------------------------------------------

MIXED = [True, 1, 1.0, Decimal("1"), Decimal("1.00"), 10 ** 16 + 1, 10 ** 16, None, 0, False, "1", 2.5]


@pytest.mark.parametrize("make_index", [EqualityIndex, RangeIndex])
def test_mixed_type_values_by_element(make_index):
    coll = List.from_iterable(MIXED + MIXED[:4]).add_index(make_index("e", KeySpec.element()))
    for expr in (Term(None, EQ, 1), Term(None, EQ, True), Term(None, IN, (True, 10 ** 16 + 1)),
                 Term(None, NE, 1), Term(None, IS_NONE), Or(Term(None, EQ, 1), Term(None, EQ, False)),
                 And(Term(None, NOT_NONE), Not(Term(None, EQ, 1))), Term(None, EQ, 10 ** 16)):
        check(plan_query(coll, expr), coll, expr)
    got = ms(plan_query(coll, Term(None, EQ, 1)).execute())
    assert got == ms([1, 1.0, Decimal("1"), Decimal("1.00"), 1, 1.0, Decimal("1")])


def test_mixed_type_values_and_missing_by_path():
    items = [r(v=v) for v in MIXED] + [r(w=1), r(), r(v=None)]
    coll = List.from_iterable(items).add_index(RangeIndex("by_v", "v"))
    for expr in (Term("v", GE, 1), Term("v", LT, 10 ** 16 + 1), Term("v", BETWEEN, (0, 1)), Term("v", IS_NONE),
                 Not(Term("v", EQ, 1)), Term("v", NE, None), Or(Term("v", IS_NONE), Term("v", GT, 2))):
        check(plan_query(coll, expr), coll, expr)
    plan = plan_query(coll, Term("v", IS_NONE))
    assert len(list(plan.execute())) == 4  # None twice, the absent v twice


# -- ORDER BY, LIMIT, OFFSET --------------------------------------------------------------------------------

def keys_of(plan, path):
    spec = KeySpec.for_term_path(path)
    return [spec.extract(e) for e in plan.execute()]


def sorted_keys(collection, expr, keys):
    return [tuple(KeySpec.for_term_path(p).extract(e) for p, _ in keys)
            for e in Sort(Filter(collection.as_query_plan(), expr) if expr is not None
                          else collection.as_query_plan(), keys).execute()]


@pytest.mark.parametrize("descending", [False, True])
def test_order_by_uses_a_range_index_when_present(descending):
    coll = records(RangeIndex("by_y", "y"))
    plan = plan_query(coll, order_by=[("y", descending)])
    info = check(plan, coll, None)
    assert info["plan"] == "IndexOrderedScan" and info["descending"] is descending
    assert [(k,) for k in keys_of(plan, "y")] == sorted_keys(coll, None, [("y", descending)])


def test_order_by_single_path_string_and_filter_on_ordered_scan():
    coll = records(RangeIndex("by_y", "y"))
    expr = Term("z", LT, 8)
    plan = plan_query(coll, expr, order_by="y")
    info = check(plan, coll, expr)
    assert node_names(info) == ["Filter", "IndexOrderedScan"]
    assert [(k,) for k in keys_of(plan, "y")] == sorted_keys(coll, expr, [("y", False)])


def test_order_by_without_ordering_index_sorts():
    coll = records(EqualityIndex("by_y", "y"))  # an EqualityIndex does not order
    plan = plan_query(coll, order_by=[("y", True)])
    info = check(plan, coll, None)
    assert info["plan"] == "Sort" and info["keys"] == [{"key": "y", "descending": True}]
    assert [(k,) for k in keys_of(plan, "y")] == sorted_keys(coll, None, [("y", True)])


def test_order_by_two_keys_sorts_even_with_an_index_on_the_first():
    coll = records(RangeIndex("by_y", "y"))
    keys = [("y", False), ("x", True)]
    plan = plan_query(coll, order_by=keys)
    info = check(plan, coll, None)
    assert info["plan"] == "Sort"
    spec_y, spec_x = KeySpec.path("y"), KeySpec.path("x")
    assert [(spec_y.extract(e), spec_x.extract(e)) for e in plan.execute()] == sorted_keys(coll, None, keys)


def test_selective_where_then_sort_beats_ordered_scan():
    coll = records(RangeIndex("by_y", "y"), EqualityIndex("by_x", "x"))
    expr = Term("x", IN, (5, 6, 7))
    plan = plan_query(coll, expr, order_by=[("y", False)])
    info = check(plan, coll, expr)
    assert node_names(info) == ["Sort", "IndexLookup"]


def test_range_and_order_on_the_same_key_picks_by_cost():
    coll = records(RangeIndex("by_x", "x"))
    narrow, wide = Term("x", LT, 5), Term("x", GE, 0)
    info = check(plan_query(coll, narrow, order_by=[("x", True)]), coll, narrow)
    assert node_names(info) == ["Sort", "IndexRange"]
    plan = plan_query(coll, wide, order_by=[("x", True)])
    info = check(plan, coll, wide)
    assert node_names(info) == ["Filter", "IndexOrderedScan"]
    assert keys_of(plan, "x") == list(range(199, -1, -1))


def test_limit_and_offset_apply_after_ordering():
    coll = records(RangeIndex("by_x", "x"))
    plan = plan_query(coll, Term("z", EQ, 3), order_by=[("x", True)], limit=3, offset=2)
    info = plan.explain()
    assert info["plan"] == "Limit" and info["limit"] == 3 and info["offset"] == 2
    assert keys_of(plan, "x") == [173, 163, 153]
    plan = plan_query(coll, order_by="x", offset=197)
    assert plan.explain()["limit"] is None and keys_of(plan, "x") == [197, 198, 199]
    assert isinstance(plan_query(coll, limit=None, offset=0), CollectionScanPlan)
    assert keys_of(plan_query(coll, order_by="x", limit=0), "x") == []


# -- near[] -------------------------------------------------------------------------------------------------

class NearScanIndex(ScanIndex):
    """Test index answering ``near[]`` on its key by brute-force L2 kNN over its stored multiset."""

    __type_name__ = "tests.unit.test_planner.NearScanIndex"

    def supports(self, term):
        return self._on_key(term) and term.op == NEAR

    def estimate(self, term):
        self._check_supported(term)
        return Estimate(rows=term.value.k or 0, cost=self.content.total_count)

    def plan(self, term):
        self._check_supported(term)
        content, spec, q, k = self.content, self.key_spec, term.value.vector, term.value.k

        def produce():
            ranked = sorted(content.items(), key=lambda item: math.dist(spec.extract(item[0]), q))
            for element, n in ranked[:k]:
                for _ in range(n):
                    yield element

        return IndexPlan(self, term, produce, kind="VectorKnn")


class FilteredNearScanIndex(NearScanIndex):
    """NearScanIndex that can restrict its kNN to a residual predicate (ruling R27)."""

    __type_name__ = "tests.unit.test_planner.FilteredNearScanIndex"

    def supports_filtered(self, term):
        return self.supports(term)

    def filtered_plan(self, term, where):
        self._check_supported(term)
        content, spec, q, k = self.content, self.key_spec, term.value.vector, term.value.k

        def produce():
            eligible = [item for item in content.items() if evaluate(where, item[0])]
            ranked = sorted(eligible, key=lambda item: math.dist(spec.extract(item[0]), q))
            for element, n in ranked[:k]:
                for _ in range(n):
                    yield element

        return IndexPlan(self, term, produce, kind="VectorKnn")


VECTORS = [r(id=i, v=(float(i), 0.0), z=i % 3) for i in range(20)]


def near(k=3, at=4.1):
    return Term("v", NEAR, NearQuery((at, 0.0), k=k))


def test_near_without_index_raises():
    coll = List.from_iterable(VECTORS).add_index(EqualityIndex("by_z", "z"))
    for expr in (near(), And(near(), Term("z", EQ, 1)), Or(near(), Term("z", EQ, 1))):
        with pytest.raises(ProtoNotSupportedError, match="near"):
            plan_query(coll, expr)
    with pytest.raises(ProtoNotSupportedError, match="near"):
        plan_query(coll.as_query_plan(), near())
    with pytest.raises(ProtoNotSupportedError, match="near"):
        plan_query(Limit(coll.as_query_plan(), 5), near())


def test_near_with_index_uses_vector_search_never_a_filter():
    coll = List.from_iterable(VECTORS).add_index(NearScanIndex("vec", KeySpec.path("v"))).add_index(EqualityIndex("by_z", "z"))
    plan = plan_query(coll, near())
    assert isinstance(plan, VectorSearch)
    assert sorted(e.id for e in plan.execute()) == [3, 4, 5]
    # near(k) AND P is the k nearest among the elements satisfying P (R27): an index that cannot restrict its
    # search refuses, rather than post-filtering to fewer than k elements.
    with pytest.raises(ProtoNotSupportedError, match="restrict"):
        plan_query(coll, And(Term("z", EQ, 1), near()))
    filtered = List.from_iterable(VECTORS).add_index(FilteredNearScanIndex("vec", KeySpec.path("v")))
    plan = plan_query(filtered, And(Term("z", EQ, 1), near()))
    info = plan.explain()
    assert node_names(info) == ["VectorSearch", "VectorKnn"] and info["where"] == Term("z", EQ, 1).describe()
    assert sorted(e.id for e in plan.execute()) == [1, 4, 7]
    # threshold-only near[] keeps the exact post-filter
    plan = plan_query(coll, And(Term("z", EQ, 1), Term("v", NEAR, NearQuery((4.1, 0.0), threshold=1.0))))
    assert node_names(plan.explain())[:2] == ["Filter", "VectorSearch"]
    plan = plan_query(coll, And(near(k=5), near(k=3, at=6.0)))  # both forced: Intersect of the two searches
    assert plan.explain()["plan"] == "Intersect" and sorted(e.id for e in plan.execute()) == [5, 6]


def test_near_in_a_disjunction_forces_a_union_with_a_filtered_scan_branch():
    coll = List.from_iterable(VECTORS).add_index(NearScanIndex("vec", KeySpec.path("v")))
    expr = Or(near(k=2), Term("id", EQ, 17))
    plan = plan_query(coll, expr)
    info = plan.explain()
    assert info["plan"] == "Union"
    assert [c["plan"] for c in info["children"]] == ["VectorSearch", "Filter"]
    assert sorted(e.id for e in plan.execute()) == [4, 5, 17]
    with pytest.raises(ProtoNotSupportedError, match="near"):
        plan_query(coll, Not(near()))
    with pytest.raises(ProtoNotSupportedError, match="near"):
        plan_query(coll, Not(And(near(), Term("id", EQ, 1))))


def test_near_where_with_order_by_never_filters_on_the_ordered_scan():
    coll = List.from_iterable(VECTORS).add_index(NearScanIndex("vec", KeySpec.path("v"))).add_index(RangeIndex("by_id", "id"))
    plan = plan_query(coll, near(k=3), order_by=[("id", True)])
    assert node_names(plan.explain()) == ["Sort", "VectorSearch", "VectorKnn"]
    assert [e.id for e in plan.execute()] == [5, 4, 3]


# -- explain() is the executed tree; no state across versions -----------------------------------------------

def _children(plan):
    for attr in ("source", "inner"):
        if hasattr(plan, attr):
            return [getattr(plan, attr)]
    return list(getattr(plan, "sources", ()))


def tree(plan):
    """The node tree as nested (name-from-explain, children) built from the plan objects."""
    return [plan] + [n for child in _children(plan) for n in tree(child)]


def test_explain_is_exactly_the_executed_node_tree(monkeypatch):
    coll = records(EqualityIndex("by_x", "x"), RangeIndex("by_z", "z"), RangeIndex("by_y", "y"))
    executed = []
    classes = {cls for cls in vars(plans_module).values()
               if isinstance(cls, type) and issubclass(cls, QueryPlan) and "execute" in vars(cls)}
    for cls in classes:
        original = vars(cls)["execute"]

        def wrapped(self, _original=original):
            executed.append(self)
            return _original(self)

        monkeypatch.setattr(cls, "execute", wrapped)
    cases = [(And(Term("x", IN, (1, 2, 3)), Term("name", CONTAINS, "2")), None, None),
             (Or(Term("x", EQ, 4), And(Term("z", EQ, 3), Term("y", EQ, 1))), None, None),
             (Term("z", GE, 0), [("z", True)], 7),
             (Or(Term("x", EQ, 4), Term("name", EQ, "n5")), [("y", False), ("x", True)], 4)]
    for expr, order_by, limit in cases:
        plan = plan_query(coll, expr, order_by=order_by, limit=limit)
        nodes = tree(plan)
        executed.clear()
        list(plan.execute())
        # A node that explain() marks "by_rank" seeks its source subtree instead of executing it
        # (D56): its subtree is read, by rank, and is not expected among the executed nodes.
        expected = []

        def collect(node):
            expected.append(node)
            if not node.explain().get("by_rank"):
                for kid in _children(node):
                    collect(kid)
        collect(plan)
        assert {id(n) for n in executed} == {id(n) for n in expected}
        info = plan.explain()

        def walk(node, info):
            assert info["plan"] == node.explain()["plan"]
            kids = _children(node)
            assert len(kids) == len(info["children"])
            for kid, kid_info in zip(kids, info["children"]):
                walk(kid, kid_info)

        walk(plan, info)
        assert info == plan.explain()  # deterministic


def test_planning_is_per_version():
    v1 = records(EqualityIndex("by_x", "x"))
    expr = Term("x", EQ, 500)
    assert list(plan_query(v1, expr).execute()) == []
    v2 = v1.append_last(r(x=500))
    plan2 = plan_query(v2, expr)
    assert [e.x for e in plan2.execute()] == [500]
    assert list(plan_query(v1, expr).execute()) == []
    v3 = v2.remove_index("by_x")
    assert node_names(plan_query(v3, expr).explain()) == ["Filter", "CollectionScan"]
    assert [e.x for e in plan2.execute()] == [500]  # a plan keeps answering for its version


# -- property: never drops a predicate ----------------------------------------------------------------------

VALUES = [None, True, False, 0, 1, 1.0, Decimal("1"), 2, 3.5, 10 ** 16 + 1, "a", "b", MISSING]
OPS_BY_KIND = [EQ, NE, LT, LE, GT, GE, IS_NONE, NOT_NONE, IS_TRUE, CONTAINS]
operands = st.sampled_from([v for v in VALUES if v is not MISSING])


def _record(values):
    return r(**{k: v for k, v in zip(("a", "b"), values) if v is not MISSING})


records_st = st.lists(st.tuples(st.sampled_from(VALUES), st.sampled_from(VALUES)).map(_record), min_size=20, max_size=60)


@st.composite
def terms(draw):
    path = draw(st.sampled_from(["a", "b"]))
    op = draw(st.sampled_from(OPS_BY_KIND + [IN, BETWEEN]))
    if op in (IS_NONE, NOT_NONE, IS_TRUE):
        return Term(path, op)
    if op == IN:
        return Term(path, op, tuple(draw(st.lists(operands, min_size=1, max_size=3))))
    if op == BETWEEN:
        return Term(path, op, draw(st.sampled_from([(0, 2), (1, 10 ** 17), ("a", "b"), (False, True)])))
    return Term(path, op, draw(operands))


expressions = st.recursive(terms(), lambda inner: st.one_of(
    st.lists(inner, min_size=2, max_size=3).map(lambda ops: And(*ops)),
    st.lists(inner, min_size=2, max_size=3).map(lambda ops: Or(*ops)),
    inner.map(Not)), max_leaves=6)

index_sets = st.lists(st.tuples(st.sampled_from(["eq", "range"]), st.sampled_from(["a", "b"])), min_size=1, max_size=3,
                      unique=True)


@settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(items=records_st, dup=st.integers(0, 3), specs=index_sets, expr=expressions,
       order=st.sampled_from([None, [("a", False)], [("b", True)], [("a", True), ("b", False)]]))
def test_property_planned_result_equals_the_scan(items, dup, specs, expr, order):
    items = items + items[:dup]  # List duplicates (same atoms twice)
    coll = List.from_iterable(items)
    for kind, path in specs:
        coll = coll.add_index((EqualityIndex if kind == "eq" else RangeIndex)(f"{kind}_{path}", path))
    plan = plan_query(coll, expr, order_by=order)
    got = list(plan.execute())
    assert ms(got) == ms(reference(coll, expr))
    if order is not None:
        assert [tuple(KeySpec.path(p).extract(e) for p, _ in order) for e in got] == sorted_keys(coll, expr, order)
    json.dumps(plan.explain())
