"""Unit tests for the physical plan nodes of proto_db.query.plans (Task 6.1, spec section 10, D30, D36).

Catches: a node whose ``execute()`` differs from a Python reference, a plan that
can be executed only once or whose iterators share state, ``explain()`` output
that is not the documented nested JSON shape (or that hides the
PythonFilter), estimates that ignore the D30 cost model (scan = rows, Sort
n log n, Limit capping rows), Union/Intersect that collapse equal-but-distinct
plain values (``1``, ``1.0``, ``Decimal("1.00")``, R19) or lose duplicates of
one source (multiset semantics: Union = max multiplicity, Intersect = min),
Sort that is unstable or orders unorderable keys anywhere but last (R18), a
Filter that accepts a ``near[]`` term it cannot evaluate, a PythonFilter whose
exception is swallowed into an empty result (the 0.1 defect), and plans that
give different results when executed concurrently from 8 threads.
"""
import json
import math
import threading
from collections import Counter
from decimal import Decimal

import pytest

from proto_db.collections.lists import List
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoNotSupportedError, ProtoUserError, ProtoValidationError
from proto_db.indexing import KeySpec, element_identity
from proto_db.indexing.equality import EqualityIndex
from proto_db.indexing.range import RangeIndex
from proto_db.query.expressions import And, Not, Or, Term
from proto_db.query.plans import (FILTER_SELECTIVITY, UNNEST_FANOUT, CollectionScanPlan, Count, Estimate, Filter,
                                  GroupBy, IndexPlan, Intersect, Join, Limit, Project, PythonFilter, QueryPlan, Sort,
                                  Union, Unnest, VectorSearch, ordered_index_scan)
from proto_db.query.terms import EQ, GE, GT, IN, LT, NEAR, NearQuery
from proto_db.values import MISSING, exact_token


def r(**kw):
    return DBObject(**kw)


PEOPLE = [r(name="ann", age=30, city="Rome", tags=["a", "b"]), r(name="bob", age=25, city="Oslo", tags=[]),
          r(name="cid", age=35, city="Rome"), r(name="dan", city="Oslo", tags=["c"]),
          r(name="eve", age=25, city=None, tags=["a"]), r(name="fay", age=40.0, city="Lima", tags=["b", "b"])]


def scan(elements):
    return List.from_iterable(elements).as_query_plan()


def people():
    return scan(PEOPLE)


def ident(e):
    return element_identity(e), exact_token(e)


def ms(elements):
    """Exact multiset of a result (equal-but-distinct plain values stay apart)."""
    return Counter(ident(e) for e in elements)


def check_shape(info):
    """explain() shape: plan name, estimate, children, recursively; JSON-serialisable."""
    assert isinstance(info["plan"], str)
    assert set(info["estimate"]) == {"rows", "cost"}
    assert isinstance(info["children"], list)
    for child in info["children"]:
        check_shape(child)
    json.dumps(info)


def check_plan(plan, expected, *, ordered=True):
    """Result equals the reference, re-executable with fresh iterators, explain and estimate sane."""
    assert isinstance(plan, QueryPlan)
    first, second = plan.execute(), plan.execute()
    assert first is not second
    a = list(first)
    b = list(second)
    if ordered:
        assert a == expected and b == expected
    else:
        assert ms(a) == ms(expected) and ms(b) == ms(expected)
    info = plan.explain()
    check_shape(info)
    est = plan.estimate()
    assert isinstance(est, Estimate) and est.rows >= 0 and est.cost >= 0
    assert info["estimate"] == {"rows": est.rows, "cost": est.cost}
    return info


# -- leaves --------------------------------------------------------------------------------------

def test_collection_scan_explain_has_estimate_and_children():
    info = check_plan(people(), PEOPLE)
    assert info["plan"] == "CollectionScan" and info["children"] == [] and info["rows"] == 6
    assert people().estimate() == Estimate(6, 6)


def test_index_plan_lookup_range_and_ordered_scan():
    coll = List.from_iterable(PEOPLE).add_index(RangeIndex("by_age", "age")).add_index(EqualityIndex("by_city", "city"))
    by_age, by_city = coll.get_index("by_age"), coll.get_index("by_city")
    lookup = by_city.plan(Term("city", EQ, "Rome"))
    info = check_plan(lookup, [p for p in PEOPLE if p.city == "Rome"], ordered=False)
    assert info["plan"] == "IndexLookup" and info["term"] == {"path": "city", "op": EQ, "value": "Rome"}
    rng = by_age.plan(Term("age", GE, 30))
    info = check_plan(rng, [p for p in PEOPLE if Term("age", GE, 30).matches(p)], ordered=False)
    assert info["plan"] == "IndexRange"

    ordered = ordered_index_scan(by_age, KeySpec.path("age"))
    ages = [KeySpec.path("age").extract(p) for p in ordered.execute()]
    assert ages[0] is MISSING and ages[1:] == [25, 25, 30, 35, 40.0]
    info = check_plan(ordered, list(ordered.execute()))
    assert info["plan"] == "IndexOrderedScan" and info["descending"] is False
    down = ordered_index_scan(by_age, "age", descending=True)
    down_ages = [KeySpec.path("age").extract(p) for p in down.execute()]
    assert down_ages[:-1] == [40.0, 35, 30, 25, 25] and down_ages[-1] is MISSING
    with pytest.raises(ProtoNotSupportedError):
        ordered_index_scan(by_city, "city")
    with pytest.raises(ProtoNotSupportedError):
        ordered_index_scan(by_age, "city")


# -- VectorSearch (stub over any index supporting near[]) -------------------------------------------

class BruteVectorIndex:
    """A duck-typed index answering near[] by brute-force L2 kNN over (id, vector) pairs."""

    def __init__(self, items):
        self.items = tuple(items)

    def supports(self, term):
        return term.op == NEAR and term.path == "v"

    def estimate(self, term):
        return Estimate(rows=term.value.k or len(self.items), cost=len(self.items))

    def explain(self):
        return {"index": "vec", "kind": "BruteVectorIndex", "key": "v"}

    def plan(self, term):
        q, k = term.value.vector, term.value.k
        items = self.items

        def produce():
            ranked = sorted(items, key=lambda it: math.dist(it.v, q))
            yield from ranked[:k]

        return IndexPlan(self, term, produce, kind="VectorKnn")


def test_vector_search_wraps_the_index_plan():
    items = [r(id=i, v=(float(i), 0.0)) for i in range(10)]
    idx = BruteVectorIndex(items)
    term = Term("v", NEAR, NearQuery((3.2, 0.0), k=3))
    plan = VectorSearch(idx, term)
    info = check_plan(plan, [items[3], items[4], items[2]])
    assert info["plan"] == "VectorSearch" and info["index"]["kind"] == "BruteVectorIndex"
    assert info["term"]["value"]["k"] == 3 and info["children"][0]["plan"] == "VectorKnn"
    assert plan.estimate() == Estimate(3, 10)
    with pytest.raises(ProtoValidationError, match="near"):
        VectorSearch(idx, Term("v", EQ, 1))
    with pytest.raises(ProtoNotSupportedError):
        VectorSearch(idx, Term("w", NEAR, NearQuery((1.0,), k=1)))


# -- Filter / PythonFilter --------------------------------------------------------------------------

def test_filter_matches_reference_and_estimates_selectivity():
    expr = And(Term("city", EQ, "Rome"), Or(Term("age", LT, 33), Not(Term("tags", "?!N"))))
    plan = Filter(people(), expr)
    info = check_plan(plan, [PEOPLE[0], PEOPLE[2]])
    assert info["predicate"] == expr.describe() and info["children"][0]["plan"] == "CollectionScan"
    est = plan.estimate()
    s = FILTER_SELECTIVITY
    sel = s * (1 - (1 - s) * (1 - (1 - s)))
    assert est.rows == pytest.approx(6 * sel) and est.cost == 6 + 6


def test_filter_accepts_prefix_lists_and_rejects_near():
    assert list(Filter(people(), ["city", EQ, "Oslo"]).execute()) == [PEOPLE[1], PEOPLE[3]]
    with pytest.raises(ProtoValidationError, match="near"):
        Filter(people(), Term("v", NEAR, NearQuery((1.0,), k=1)))
    with pytest.raises(ProtoValidationError):
        Filter(people(), "city == 'Oslo'")
    with pytest.raises(ProtoValidationError, match="QueryPlan"):
        Filter([1, 2], Term(None, EQ, 1))


def test_python_filter_is_explicit_and_never_swallows():
    plan = PythonFilter(people(), lambda p: p.name.startswith("e"), "name starts with e")
    info = check_plan(plan, [PEOPLE[4]])
    assert info["plan"] == "PythonFilter" and info["description"] == "name starts with e"
    boom = PythonFilter(people(), lambda p: p.age > 1, "age > 1")  # dan has no age: None > 1
    with pytest.raises(TypeError):
        list(boom.execute())
    with pytest.raises(ProtoValidationError):
        PythonFilter(people(), "nope", "x")
    with pytest.raises(ProtoValidationError):
        PythonFilter(people(), bool, "")


# -- Union / Intersect: multiset semantics by exact identity ----------------------------------------

def test_union_max_multiplicity_and_equal_but_distinct_values():
    left = scan([1, 1, 2, 1.0, Decimal("1.00")])
    right = scan([1, 2, 2, 3, 1.0, 1.0])
    u = Union(left, right)
    out = list(u.execute())
    expected = Counter({ident(1): 2, ident(2): 2, ident(1.0): 2, ident(Decimal("1.00")): 1, ident(3): 1})
    assert ms(out) == expected
    # first-seen order: left fully, then the extra occurrences of right
    assert [exact_token(x) for x in out] == [exact_token(x) for x in [1, 1, 2, 1.0, Decimal("1.00"), 2, 3, 1.0]]
    info = check_plan(u, out)
    assert info["plan"] == "Union" and len(info["children"]) == 2
    assert u.estimate().rows == 11


def test_union_of_same_element_from_two_branches_yields_it_once():
    coll = List.from_iterable(PEOPLE)
    rome = Filter(coll.as_query_plan(), Term("city", EQ, "Rome"))
    young = Filter(coll.as_query_plan(), Term("age", LT, 31))
    out = list(Union(rome, young).execute())
    assert out == [PEOPLE[0], PEOPLE[2], PEOPLE[1], PEOPLE[4]]


def test_intersect_min_multiplicity():
    a = scan([1, 1, 1, 2, 1.0, 3, Decimal("1.00")])
    b = scan([1, 1, 2, 2, 1.0, Decimal("1")])
    c = scan([2, 1, 1, 1, 1.0])
    out = list(Intersect(a, b, c).execute())
    assert [exact_token(x) for x in out] == [exact_token(x) for x in [1, 1, 2, 1.0]]
    info = check_plan(Intersect(a, b, c), out)
    assert info["plan"] == "Intersect" and len(info["children"]) == 3
    assert Intersect(a, b).estimate().rows == 6


def test_union_intersect_need_two_plans():
    for cls in (Union, Intersect):
        with pytest.raises(ProtoValidationError):
            cls(people())
        with pytest.raises(ProtoValidationError):
            cls(people(), [1])


def test_union_intersect_with_unorderable_elements_and_atoms():
    lists = scan([[1, 2], {"a": 1}, [1, 2], float("nan")])
    other = scan([[1, 2], {"a": 1}])
    assert len(list(Union(lists, other).execute())) == 4
    out = list(Intersect(lists, other).execute())
    assert out == [[1, 2], {"a": 1}]


# -- Project / Sort / Limit / Count -----------------------------------------------------------------

def test_project_fields_mapping_and_function():
    info = check_plan(Project(people(), ["name", "age"]),
                      [{"name": p.name, "age": p.age} if p.has_defined_attr("age") else {"name": p.name}
                       for p in PEOPLE])
    assert info["fields"] == {"name": "name", "age": "age"}
    assert list(Project(people(), {"n": "name"}).execute())[0] == {"n": "ann"}
    info = check_plan(Project(people(), lambda p: p.name.upper(), description="upper name"),
                      [p.name.upper() for p in PEOPLE])
    assert info["function"] == "upper name"
    with pytest.raises(ProtoValidationError):
        Project(people(), [])
    with pytest.raises(ProtoValidationError):
        Project(people(), 5)


def test_sort_is_stable_total_and_puts_unorderable_last():
    data = [r(k=2, i=0), r(k=[1], i=1), r(i=2), r(k="a", i=3), r(k=1, i=4), r(k=None, i=5), r(k=2.0, i=6),
            r(k=float("nan"), i=7), r(k=True, i=8)]
    up = [e.i for e in Sort(scan(data), [("k", False)]).execute()]
    assert up == [2, 5, 8, 4, 0, 6, 3, 1, 7]
    down = [e.i for e in Sort(scan(data), [("k", True)]).execute()]
    assert down == [1, 7, 3, 0, 6, 4, 8, 5, 2]  # unorderable first, ties keep input order
    info = check_plan(Sort(scan(data), ["k"]), list(Sort(scan(data), ["k"]).execute()))
    assert info["keys"] == [{"key": "k", "descending": False}]


def test_sort_multi_key_and_function_keys():
    plan = Sort(people(), [("city", False), ("age", True)])
    age = KeySpec.path("age").extract
    expected = sorted(PEOPLE, key=lambda p: -1 if age(p) is MISSING else -age(p))  # age desc, missing last
    expected = sorted(expected, key=lambda p: (p.city is not None, p.city or ""))
    assert [p.name for p in plan.execute()] == [p.name for p in expected]
    by_len = Sort(people(), [(lambda p: len(p.name), False)], description="name length")
    assert [p.name for p in by_len.execute()] == [p.name for p in PEOPLE]
    assert by_len.explain()["keys"] == [{"key": "name length", "descending": False}]
    n = 6
    assert plan.estimate() == Estimate(n, n + n * math.log2(n))
    with pytest.raises(ProtoValidationError):
        Sort(people(), [])
    with pytest.raises(ProtoValidationError):
        Sort(people(), [("a", "yes")])
    with pytest.raises(ProtoValidationError):
        Sort(people(), [("a", True, 1)])


def test_limit_offset_and_estimate():
    info = check_plan(Limit(people(), 2, offset=1), PEOPLE[1:3])
    assert info["limit"] == 2 and info["offset"] == 1
    assert Limit(people(), 2, offset=1).estimate().rows == 2
    assert Limit(people(), 10, offset=5).estimate().rows == 1
    assert Limit(people(), 10, offset=50).estimate().rows == 0
    assert list(Limit(people(), 0).execute()) == []
    for bad in ((-1, 0), (1, -1), (True, 0), (1.5, 0)):
        with pytest.raises(ProtoValidationError):
            Limit(people(), bad[0], offset=bad[1])


def test_count_yields_one_int():
    info = check_plan(Count(Filter(people(), Term("city", EQ, "Oslo"))), [2])
    assert info["plan"] == "Count" and info["estimate"]["rows"] == 1
    assert list(Count(scan([])).execute()) == [0]


# -- GroupBy / Join / Unnest -------------------------------------------------------------------------

def test_group_by_with_aggregations():
    plan = GroupBy(people(), "city", {"n": ("count", None), "ages": ("count", "age"), "total": ("sum", "age"),
                                      "oldest": ("max", "age"), "youngest": ("min", "age"), "mean": ("avg", "age"),
                                      "names": ("collect", "name"), "first": (lambda vs: vs[0], "name")})
    out = list(plan.execute())
    assert [g["key"] for g in out] == ["Rome", "Oslo", None, "Lima"]
    rome, oslo, none, lima = out
    assert rome == {"key": "Rome", "n": 2, "ages": 2, "total": 65, "oldest": 35, "youngest": 30, "mean": 32.5,
                    "names": ("ann", "cid"), "first": "ann"}
    assert oslo["n"] == 2 and oslo["ages"] == 1 and oslo["total"] == 25 and oslo["mean"] == 25
    assert none["key"] is None and lima["oldest"] == 40.0
    info = check_plan(plan, out)
    assert "lambda" in info["aggregations"]["first"]["op"]
    assert info["aggregations"]["total"] == {"op": "sum", "path": "age"}
    assert info["key"] == "city"


def test_group_by_merges_equal_keys_and_keeps_missing_apart():
    data = [r(k=1, v=1), r(k=1.0, v=2), r(v=3), r(k=None, v=4), r(k=[1], v=5), r(k=[1], v=6)]
    out = list(GroupBy(scan(data), "k", {"s": ("sum", "v")}).execute())
    assert [(g["key"], g["s"]) for g in out] == [(1, 3), (MISSING, 3), (None, 4), ([1], 11)]


def test_group_by_rejects_bad_specs_and_values():
    with pytest.raises(ProtoValidationError):
        GroupBy(people(), "city", {"key": ("count", None)})
    with pytest.raises(ProtoValidationError):
        GroupBy(people(), "city", {"x": ("median", "age")})
    with pytest.raises(ProtoValidationError, match="number"):
        list(GroupBy(people(), "city", {"x": ("sum", "name")}).execute())
    with pytest.raises(ProtoValidationError):
        list(GroupBy(scan([r(v=1.5), r(v=Decimal(1))]), lambda e: 0, {"x": ("sum", "v")}).execute())


def test_join_inner_and_left():
    cities = scan([r(city="Rome", country="IT"), r(city="Oslo", country="NO"), r(city="Rome", country="VA"),
                   r(city=None, country="?"), r(country="none")])
    inner = Join(people(), cities, "city", "city")
    out = [(a.name, b.country) for a, b in inner.execute()]
    assert out == [("ann", "IT"), ("ann", "VA"), ("bob", "NO"), ("cid", "IT"), ("cid", "VA"), ("dan", "NO"),
                   ("eve", "?")]
    info = check_plan(inner, list(inner.execute()))
    assert info["kind"] == "inner" and len(info["children"]) == 2
    left = Join(people(), cities, "city", "city", kind="left")
    pairs = list(left.execute())
    assert (PEOPLE[5], MISSING) in pairs and len(pairs) == 8
    with pytest.raises(ProtoValidationError):
        Join(people(), cities, "city", "city", kind="outer")


def test_unnest():
    plan = Unnest(people(), "tags")
    info = check_plan(plan, ["a", "b", "c", "a", "b", "b"])
    assert info["path"] == "tags"
    assert plan.estimate().rows == 6 * UNNEST_FANOUT
    pairs = list(Unnest(people(), "tags", with_parent=True).execute())
    assert pairs[0] == (PEOPLE[0], "a") and len(pairs) == 6
    with pytest.raises(ProtoValidationError, match="list or tuple"):
        list(Unnest(people(), "name").execute())


# -- immutability, composition and concurrency ------------------------------------------------------

def test_plans_are_immutable():
    plan = Filter(people(), Term("age", GT, 1))
    with pytest.raises(ProtoUserError):
        plan.source = None
    with pytest.raises(ProtoUserError):
        plan.extra = 1


def test_composed_plan_explain_is_nested():
    plan = Limit(Sort(Filter(people(), Term("age", IN, (25, 30))), [("name", True)]), 1)
    info = check_plan(plan, [PEOPLE[4]])
    assert [info["plan"], info["children"][0]["plan"], info["children"][0]["children"][0]["plan"],
            info["children"][0]["children"][0]["children"][0]["plan"]] == ["Limit", "Sort", "Filter",
                                                                           "CollectionScan"]


def test_concurrent_execution_gives_identical_results():
    coll = List.from_iterable([r(k=i % 17, v=i) for i in range(400)]).add_index(RangeIndex("k", "k"))
    idx = coll.get_index("k")
    plan = Limit(Sort(Union(Filter(coll.as_query_plan(), Term("v", LT, 300)), idx.plan(Term("k", GE, 10))),
                      [("k", True), ("v", False)]), 250)
    expected = [e.v for e in plan.execute()]
    results, barrier = [None] * 8, threading.Barrier(8)

    def worker(i):
        barrier.wait()
        results[i] = [e.v for e in plan.execute()]

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert all(res == expected for res in results) and len(expected) == 250
