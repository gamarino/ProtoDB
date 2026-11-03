"""Unit tests for the LINQ front end, proto_db.query.linq (Task 6.4, spec section 10, D38).

Catches: the 0.1 regression where LINQ over a collection silently returned
``[]`` (an exception swallowed in the query path); a Queryable that mutates
its receiver; an ``F`` predicate that is not turned into a pure Expression
(so indexes go unused) or that an indexed collection does not answer with an
index plan visible in ``explain()``; a lambda (or an untranslatable ``F``
extra such as ``startswith``/``lower``/arithmetic) that is hidden instead of
appearing as a ``PythonFilter``; an AND of a translatable and a lambda part
that is not split (index + PythonFilter), or an OR/NOT holding a lambda that
is partially indexed or silently dropped instead of becoming one
PythonFilter; exceptions raised by user code that are swallowed; wrong
LINQ semantics (order_by/then_by stability and direction, skip/take
composition, distinct by exact identity, inner/left join, group_by
aggregations, select_many, count/first); and an ``explain()`` that differs
from the tree executed.  It also ports the 0.1 LINQ tests that encode real
behaviour (proto_db/tests/test_linq_between.py, test_linq_groupby.py,
test_linq_indexed_fastpath.py, test_linq_like.py) to the new API.
"""
import json
import random
from collections import Counter
from datetime import datetime
from decimal import Decimal

import pytest

from proto_db.collections.lists import List
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoNotSupportedError, ProtoUserError, ProtoValidationError
from proto_db.indexing.equality import EqualityIndex
from proto_db.indexing.range import RangeIndex
from proto_db.query.expressions import And, Not, Or, Term, evaluate
from proto_db.query.linq import Agg, F, Field, Predicate, Queryable, from_collection, path_of
from proto_db.query.plans import Distinct, Limit, QueryPlan, ValuesScan, element_key
from proto_db.query.terms import NearQuery
from proto_db.values import MISSING


# -- helpers ---------------------------------------------------------------------------------------

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


def ms(elements):
    """Exact multiset of results (identity + exact token)."""
    return Counter(element_key(e) for e in elements)


def explained(q):
    """explain() is JSON-serialisable and equals the explain of the plan that runs."""
    info = q.explain()
    json.dumps(info)
    assert info == q.plan().explain()
    return info


# 2000 records like the 0.1 indexed fast-path test; deterministic.
_RND = random.Random(123)
ROWS = [DBObject(id=i + 1, category=f"c{_RND.randrange(50)}", status=f"s{_RND.randrange(30)}",
                 value=_RND.randint(0, 100000)) for i in range(2000)]


def indexed():
    coll = List.from_iterable(ROWS)
    for index in (EqualityIndex("by_id", "id"), EqualityIndex("by_category", "category"),
                  EqualityIndex("by_status", "status"), RangeIndex("by_value", "value")):
        coll = coll.add_index(index)
    return coll


USERS = [
    {"id": 1, "first_name": "Alice", "last_name": "Zeus", "age": 30, "country": "ES", "status": "active",
     "email": "a@example.com", "last_login": 5},
    {"id": 2, "first_name": "Bob", "last_name": "Young", "age": 17, "country": "AR", "status": "inactive",
     "email": "b@example.com", "last_login": 10},
    {"id": 3, "first_name": "Carol", "last_name": "Xavier", "age": 25, "country": "US", "status": "active",
     "email": "c@example.com", "last_login": 2},
    {"id": 4, "first_name": "Dan", "last_name": "White", "age": 22, "country": "AR", "status": "active",
     "email": "d@example.com", "last_login": 7},
]

AGES = [{"id": 1, "age": 9}, {"id": 2, "age": 10}, {"id": 3, "age": 15}, {"id": 4, "age": 20},
        {"id": 5, "age": 21}, {"id": 6, "age": None}]


def ids(rows):
    return [r["id"] for r in rows]


# -- the 0.1 regression and sources ------------------------------------------------------------------

def test_regression_linq_over_a_collection_returns_the_rows_not_an_empty_list():
    coll = indexed()
    assert len(from_collection(coll).to_list()) == 2000
    rows = from_collection(coll).where(F.category == "c7").to_list()
    expected = [r for r in ROWS if r.category == "c7"]
    assert rows and ms(rows) == ms(expected)
    assert ms(from_collection(coll).where(lambda r: r.value > 50000)) == ms(r for r in ROWS if r.value > 50000)


def test_sources_collection_plan_iterable_and_queryable():
    coll = indexed()
    assert explained(from_collection(coll))["plan"] == "CollectionScan"
    plan = coll.as_query_plan()
    assert from_collection(plan).where(F.id == 5).explain()["plan"] == "IndexLookup"
    limited = Limit(plan, 10)
    q = from_collection(limited).where(F.id == 5)
    assert node_names(q.explain()) == ["Filter", "Limit", "CollectionScan"]
    assert [r.id for r in q] == [5]
    # Iterable sources: a ValuesScan over a materialised tuple, never an index.
    gen = (u for u in USERS)
    q = from_collection(gen)
    info = explained(q.where(F.age > 18))
    assert node_names(info) == ["Filter", "ValuesScan"]
    assert q.to_list() == USERS and q.to_list() == USERS  # materialised once, re-executable
    base = from_collection(USERS)
    assert from_collection(base) is base


def test_invalid_sources_raise():
    for bad in ("abc", b"abc", {"a": 1}, 5, None):
        with pytest.raises(ProtoValidationError):
            from_collection(bad)


def test_queryable_is_immutable():
    q = from_collection(USERS)
    filtered = q.where(F.age > 18)
    ordered = filtered.order_by(F.age)
    assert isinstance(filtered, Queryable) and filtered is not q
    assert q.to_list() == USERS
    assert ids(filtered) == [1, 3, 4]
    assert ids(ordered) == [4, 3, 1]
    assert ids(filtered) == [1, 3, 4]
    for method, args in (("select", (F.id,)), ("skip", (1,)), ("take", (1,)), ("distinct", ()),
                         ("group_by", (F.country,)), ("select_many", (F.tags,)),
                         ("join", (USERS, F.id, F.id))):
        assert getattr(q, method)(*args) is not q
    assert q.to_list() == USERS
    with pytest.raises(ProtoUserError):
        q.extra = 1
    with pytest.raises(ProtoUserError):
        del q._source


# -- the F builder -------------------------------------------------------------------------------

def test_field_paths():
    assert (F.a == 1).expression == Term("a", "==", 1)
    assert (F.a.b.c == 1).expression == Term("a.b.c", "==", 1)
    assert (F["a"]["b"] == 1).expression == Term("a.b", "==", 1)
    assert (F["between"] > 1).expression == Term("between", ">", 1)
    assert (F() > 1).expression == Term(None, ">", 1)
    assert path_of(F.a.b) == "a.b" and path_of(F()) is None
    with pytest.raises(AttributeError):
        F.a._private
    with pytest.raises(ProtoValidationError):
        F[3]
    with pytest.raises(ProtoValidationError):
        F["a.b"]


def test_field_predicates_are_pure_terms():
    cases = [
        (F.a == 1, Term("a", "==", 1)), (F.a != 1, Term("a", "!=", 1)), (F.a < 1, Term("a", "<", 1)),
        (F.a <= 1, Term("a", "<=", 1)), (F.a > 1, Term("a", ">", 1)), (F.a >= 1, Term("a", ">=", 1)),
        (F.a.in_([1, 2]), Term("a", "in", (1, 2))), (F.a.in_({2, 1}), Term("a", "in", {1, 2})),
        (F.a.between(1, 5), Term("a", "between[]", (1, 5))),
        (F.a.between(1, 5, inclusive=(False, False)), Term("a", "between()", (1, 5))),
        (F.a.between_closed(1, 5), Term("a", "between[]", (1, 5))),
        (F.a.between_open(1, 5), Term("a", "between()", (1, 5))),
        (F.a.between_left_open(1, 5), Term("a", "between(]", (1, 5))),
        (F.a.between_right_open(1, 5), Term("a", "between[)", (1, 5))),
        (F.a.range(1, 5, bounds="[)"), Term("a", "between[)", (1, 5))),
        (F.a.contains("x"), Term("a", "contains", "x")),
        (F.a.is_none(), Term("a", "?N")), (F.a.not_none(), Term("a", "?!N")),
        (F.a.is_true(), Term("a", "?T")), (F.a.not_true(), Term("a", "?!T")),
        (F.v.near([1.0, 0.0], k=3), Term("v", "near[]", NearQuery((1.0, 0.0), k=3))),
        (F.v.near([1.0, 0.0], threshold=0.5, metric="l2"),
         Term("v", "near[]", NearQuery((1.0, 0.0), threshold=0.5, metric="l2"))),
    ]
    for predicate, term in cases:
        assert isinstance(predicate, Predicate) and predicate.expression == term
    for bad in ("<>", ["[", "]"], None):
        with pytest.raises(ProtoValidationError):
            F.a.range(1, 5, bounds=bad)
    for bad in ((1, 1), (True,), [True, True], "[]"):
        with pytest.raises(ProtoValidationError):
            F.a.between(1, 5, inclusive=bad)


def test_combinators_build_expressions():
    a, b, c = F.a == 1, F.b == 2, F.c == 3
    assert (a & b).expression == And(Term("a", "==", 1), Term("b", "==", 2))
    assert (a | b | c).expression == Or(Term("a", "==", 1), Term("b", "==", 2), Term("c", "==", 3))
    assert (~a).expression == Not(Term("a", "==", 1))
    assert (~(a & b) | c).expression == Or(Not(And(Term("a", "==", 1), Term("b", "==", 2))), Term("c", "==", 3))
    # a lambda anywhere makes the predicate impure
    assert ((a & (lambda r: True)).expression) is None
    assert (((lambda r: True) | a).expression) is None
    with pytest.raises(ProtoValidationError):
        a & 5
    with pytest.raises(ProtoUserError):
        bool(a)  # `F.a == 1 and F.b == 2` would silently drop a predicate


def test_predicates_evaluate_like_the_scan_oracle():
    rows = AGES + [{"id": 7}]
    for predicate in (F.age > 10, ~(F.age > 10), F.age.is_none(), (F.age >= 10) & (F.age < 21),
                      (F.age < 10) | F.age.is_none()):
        assert [predicate(r) for r in rows] == [evaluate(predicate.expression, r) for r in rows]


# -- index use and PythonFilter visibility -------------------------------------------------------

def test_indexed_equality_uses_the_index():  # port of test_001_equality_pk_uses_indexed_search
    target = ROWS[100].id
    q = from_collection(indexed()).where(F.id == target)
    info = explained(q)
    assert info["plan"] == "IndexLookup" and info["index"]["index"] == "by_id"
    assert [r.id for r in q.to_list()] == [target]


def test_indexed_in_uses_the_index():  # port of test_002_in_uses_indexed_search
    vals = [ROWS[10].category, ROWS[20].category]
    q = from_collection(indexed()).where(F.category.in_(vals))
    info = explained(q)
    assert info["plan"] == "IndexLookup" and info["index"]["index"] == "by_category"
    assert ms(q) == ms(r for r in ROWS if r.category in vals)


def test_indexed_between_uses_a_range_and_executes():  # port of test_003 (0.1 could not execute it)
    q = from_collection(indexed()).where(F.value.between_open(1000, 20000))
    info = explained(q)
    assert info["plan"] == "IndexRange" and info["index"]["index"] == "by_value"
    assert ms(q) == ms(r for r in ROWS if 1000 < r.value < 20000)


def test_indexed_and_uses_one_index_and_a_residual():  # port of test_004_and_merge_intersection
    cat, st = ROWS[5].category, ROWS[7].status
    q = from_collection(indexed()).where((F.category == cat) & (F.status == st) & F.value.between(100, 90000))
    info = explained(q)
    assert info["plan"] == "Filter" and info["children"][0]["plan"] in ("IndexLookup", "IndexRange")
    assert ms(q) == ms(r for r in ROWS if r.category == cat and r.status == st and 100 <= r.value <= 90000)


def test_indexed_or_uses_a_union():  # port of test_005_or_merge_union
    c1, c2 = ROWS[15].category, ROWS[25].category
    q = from_collection(indexed()).where((F.category == c1) | (F.category == c2))
    info = explained(q)
    assert info["plan"] == "Union" and node_names(info).count("IndexLookup") == 2
    assert ms(q) == ms(r for r in ROWS if r.category in (c1, c2))


def test_consecutive_wheres_are_merged_into_one_planned_predicate():
    q = from_collection(indexed()).where(F.status == "s3").where(F.id == 77)
    info = explained(q)
    assert info["plan"] in ("IndexLookup", "Filter") and find(info, "CollectionScan") is None
    assert ms(q) == ms(r for r in ROWS if r.status == "s3" and r.id == 77)


def test_where_accepts_expressions_and_the_list_syntax():
    coll = indexed()
    assert ms(from_collection(coll).where(Term("id", "==", 9))) == ms([ROWS[8]])
    assert ms(from_collection(coll).where(["id", "<", 4])) == ms(ROWS[:3])
    with pytest.raises(ProtoValidationError):
        from_collection(coll).where(5)


def test_lambda_becomes_a_visible_python_filter():
    q = from_collection(indexed()).where(lambda r: r.value % 7 == 0)
    info = explained(q)
    assert node_names(info) == ["PythonFilter", "CollectionScan"]
    assert "<lambda>" in info["description"] and "test_linq.py" in info["description"]
    assert ms(q) == ms(r for r in ROWS if r.value % 7 == 0)


def test_mixed_and_is_split_into_index_and_python_filter():
    for predicate in ((F.category == "c3") & (lambda r: r.value > 50000),
                      (lambda r: r.value > 50000) & (F.category == "c3")):
        q = from_collection(indexed()).where(predicate)
        info = explained(q)
        assert node_names(info) == ["PythonFilter", "IndexLookup"]
        assert ms(q) == ms(r for r in ROWS if r.category == "c3" and r.value > 50000)


def test_mixed_and_with_several_parts():
    lam1 = lambda r: r.value > 1000  # noqa: E731
    lam2 = lambda r: r.value < 90000  # noqa: E731
    predicate = (F.category == "c3") & lam1 & (F.status != "s1") & lam2
    q = from_collection(indexed()).where(predicate)
    info = explained(q)
    assert node_names(info) == ["PythonFilter", "PythonFilter", "Filter", "IndexLookup"]
    assert ms(q) == ms(r for r in ROWS if r.category == "c3" and 1000 < r.value < 90000 and r.status != "s1")


def test_or_or_not_with_a_lambda_is_one_whole_python_filter():
    lam = lambda r: r.value > 99000  # noqa: E731
    q = from_collection(indexed()).where((F.category == "c3") | lam)
    info = explained(q)
    assert node_names(info) == ["PythonFilter", "CollectionScan"]
    assert "category == 'c3'" in info["description"] and "<lambda>" in info["description"]
    assert ms(q) == ms(r for r in ROWS if r.category == "c3" or r.value > 99000)
    q = from_collection(indexed()).where(~((F.category == "c3") & lam))
    assert node_names(explained(q)) == ["PythonFilter", "CollectionScan"]
    assert ms(q) == ms(r for r in ROWS if not (r.category == "c3" and r.value > 99000))
    # AND at top level whose impure part is an OR: the pure part is indexed, the OR is one PythonFilter
    q = from_collection(indexed()).where((F.status == "s2") & ((F.category == "c3") | lam))
    assert node_names(explained(q)) == ["PythonFilter", "IndexLookup"]
    assert ms(q) == ms(r for r in ROWS if r.status == "s2" and (r.category == "c3" or r.value > 99000))


def test_untranslatable_field_extras_become_python_filters():
    cases = [
        (F.first_name.startswith("A"), lambda u: u["first_name"].startswith("A"), "first_name.startswith('A')"),
        (F.email.endswith("c.com"), lambda u: u["email"].endswith("c.com"), "email.endswith('c.com')"),
        (F.first_name.lower() == "bob", lambda u: u["first_name"].lower() == "bob", "first_name.lower() == 'bob'"),
        (F.last_name.upper().startswith("X"), lambda u: u["last_name"].upper().startswith("X"), None),
        (F.first_name.length() > 3, lambda u: len(u["first_name"]) > 3, "first_name.length() > 3"),
        ((F.age + F.last_login) > 30, lambda u: u["age"] + u["last_login"] > 30, None),
        ((F.age * 2 - 10) >= 40, lambda u: u["age"] * 2 - 10 >= 40, None),
        ((F.age / 2) < 10, lambda u: u["age"] / 2 < 10, None),
        ((F.age - 40).abs() < 15, lambda u: abs(u["age"] - 40) < 15, None),
        (F.age < F.last_login * 3, lambda u: u["age"] < u["last_login"] * 3, None),
        ((F.first_name.lower()).in_(["alice", "dan"]), lambda u: u["first_name"].lower() in ("alice", "dan"), None),
    ]
    for predicate, reference, description in cases:
        assert predicate.expression is None
        q = from_collection(USERS).where(predicate)
        info = explained(q)
        assert node_names(info) == ["PythonFilter", "ValuesScan"]
        if description is not None:
            assert info["description"] == description
        assert q.to_list() == [u for u in USERS if reference(u)]


def test_computed_values_follow_missing_and_none_semantics():
    rows = [{"id": 1, "s": "Ab"}, {"id": 2, "s": None}, {"id": 3}]
    assert ids(from_collection(rows).where(F.s.lower() == "ab")) == [1]
    assert ids(from_collection(rows).where(F.s.lower().is_none())) == [2, 3]
    assert ids(from_collection(rows).where(F.s.startswith("A"))) == [1]
    assert ids(from_collection(rows).where((F.s + "x") == "Abx")) == [1]


def test_near_without_a_vector_index_raises_instead_of_falling_back():
    coll = indexed()
    with pytest.raises(ProtoNotSupportedError):
        from_collection(coll).where(F.v.near([1.0, 0.0], k=2)).to_list()
    with pytest.raises(ProtoNotSupportedError):
        F.v.lower().near([1.0], k=1)
    with pytest.raises(ProtoNotSupportedError):  # a near[] term cannot run inside a PythonFilter
        from_collection(coll).where(F.v.near([1.0], k=1) | (lambda r: True))


# -- errors propagate ----------------------------------------------------------------------------

def test_errors_raised_by_user_code_propagate():
    # 0.1 test_lambda_chained_comparison: the None age makes the lambda raise; 0.1 hid it.
    with pytest.raises(TypeError):
        from_collection(AGES).where(lambda x: 10 <= x["age"] <= 20).to_list()
    guarded = from_collection(AGES).where(lambda x: x["age"] is not None and 10 <= x["age"] <= 20)
    assert ids(guarded) == [2, 3, 4]

    class Boom(Exception):
        pass

    def boom(_):
        raise Boom()

    coll = indexed()
    for q in (from_collection(coll).where((F.category == "c3") & boom),
              from_collection(coll).where((F.category == "c3") | boom),
              from_collection(coll).select(boom),
              from_collection(coll).order_by(boom),
              from_collection(coll).group_by(boom),
              from_collection(coll).select_many(boom)):
        with pytest.raises(Boom):
            q.to_list()
    with pytest.raises(Boom):
        from_collection(coll).where(boom).count()
    with pytest.raises(Boom):
        from_collection(coll).where(boom).first_or_none()
    with pytest.raises(AttributeError):
        from_collection([{"s": 5}]).where(F.s.lower() == "x").to_list()


# -- 0.1 between tests (test_linq_between.py) ----------------------------------------------------

def test_between_inclusive_default():
    assert ids(from_collection(AGES).where(F.age.between(10, 20))) == [2, 3, 4]


def test_between_exclusive():
    assert ids(from_collection(AGES).where(F.age.between(10, 20, inclusive=(False, False)))) == [3]


def test_between_sugars():
    q = from_collection(AGES)
    assert q.where(F.age.between_closed(10, 20)).count() == 3
    assert q.where(F.age.between_open(10, 20)).count() == 1
    assert q.where(F.age.between_left_open(10, 20)).count() == 2
    assert q.where(F.age.between_right_open(10, 20)).count() == 2


def test_between_range_bounds():
    assert ids(from_collection(AGES).where(F.age.range(10, 20, bounds="[)"))) == [2, 3]


def test_datetime_between():
    start, end = datetime(2024, 1, 1), datetime(2024, 1, 3)
    data = [{"d": datetime(2023, 12, 31)}, {"d": datetime(2024, 1, 1)}, {"d": datetime(2024, 1, 2)},
            {"d": datetime(2024, 1, 3)}, {"d": datetime(2024, 1, 4)}]
    assert len(from_collection(data).where(F.d.between(start, end)).to_list()) == 3
    assert len(from_collection(data).where(F.d.between(start, end, inclusive=(True, False))).to_list()) == 2


# -- ordering, windows ---------------------------------------------------------------------------

PRODUCTS = [{"id": 1, "category": "A", "price": 10}, {"id": 2, "category": "A", "price": 20},
            {"id": 3, "category": "B", "price": 5}, {"id": 4, "category": "B", "price": 5},
            {"id": 5, "category": "A", "price": 10}, {"id": 6, "category": "C"}]


def test_order_by_then_by_stability_and_direction():  # extends 0.1 test_then_by
    q = from_collection(PRODUCTS)
    assert ids(q.order_by(F.price)) == [6, 3, 4, 1, 5, 2]  # MISSING first (order_key), ties keep input order
    assert ids(q.order_by(F.price, descending=True)) == [2, 1, 5, 3, 4, 6]
    assert ids(q.order_by(F.category).then_by(F.price, descending=True)) == [2, 1, 5, 3, 4, 6]
    assert ids(q.order_by("category", descending=True).then_by("price")) == [6, 3, 4, 1, 5, 2]
    assert ids(q.order_by(lambda p: -p["id"])) == [6, 5, 4, 3, 2, 1]
    assert ids(q.order_by(F.category).then_by(lambda p: -p["id"])) == [5, 2, 1, 4, 3, 6]
    # a later order_by re-sorts the whole sequence stably: the earlier keys break its ties
    assert ids(q.order_by(F.price).order_by(F.category)) == ids(q.order_by(F.category).then_by(F.price))
    info = explained(q.order_by(F.category).then_by(F.price, descending=True))
    assert info["plan"] == "Sort" and [k["descending"] for k in info["keys"]] == [False, True]


def test_then_by_needs_a_preceding_order_by():
    q = from_collection(PRODUCTS)
    for bad in (q, q.where(F.id > 1), q.order_by(F.id).take(2), q.order_by(F.id).where(F.id > 1)):
        with pytest.raises(ProtoUserError):
            bad.then_by(F.price)


def test_order_by_can_stream_an_ordered_index():
    q = from_collection(indexed()).order_by(F.value, descending=True).take(5)
    info = explained(q)
    assert info["plan"] == "Limit" and find(info, "IndexOrderedScan") is not None
    expected = sorted(ROWS, key=lambda r: r.value, reverse=True)[:5]
    assert [r.value for r in q] == [r.value for r in expected]


def test_skip_and_take_compose_like_slicing():
    data = list(range(12))
    rnd = random.Random(7)
    for _ in range(200):
        q, ref = from_collection(data), list(data)
        for _ in range(rnd.randrange(1, 5)):
            n = rnd.randrange(0, 6)
            if rnd.random() < 0.5:
                q, ref = q.skip(n), ref[n:]
            else:
                q, ref = q.take(n), ref[:n]
        assert q.to_list() == ref
        assert sum(1 for name in node_names(q.explain()) if name == "Limit") <= 1
    for bad in (-1, 1.5, True, None):
        with pytest.raises(ProtoValidationError):
            from_collection(data).take(bad)
        with pytest.raises(ProtoValidationError):
            from_collection(data).skip(bad)


def test_where_after_take_is_applied_on_top():
    q = from_collection(list(range(10))).take(5).where(F() > 2)
    info = explained(q)
    assert node_names(info) == ["Filter", "Limit", "ValuesScan"]
    assert q.to_list() == [3, 4]
    q = from_collection(list(range(10))).where(lambda x: x % 2 == 0).order_by(F(), descending=True).skip(1).take(2)
    assert node_names(explained(q)) == ["Limit", "PythonFilter", "Sort", "ValuesScan"]  # path key: planner sorts
    assert q.to_list() == [6, 4]


def test_order_and_window_are_pushed_into_the_planner():
    q = from_collection(indexed()).where(F.category == "c3").order_by(F.value).skip(2).take(3)
    info = explained(q)
    assert node_names(info)[:2] == ["Limit", "Sort"] and info["limit"] == 3 and info["offset"] == 2
    expected = sorted((r for r in ROWS if r.category == "c3"), key=lambda r: r.value)[2:5]
    assert [r.id for r in q] == [r.id for r in expected]


# -- select, distinct ----------------------------------------------------------------------------

def test_select_forms():
    q = from_collection(USERS + [{"id": 5}])
    assert q.select(F.email).to_list()[:2] == ["a@example.com", "b@example.com"]
    assert q.select(F.email).to_list()[4] is MISSING
    assert q.select("id").to_list() == [1, 2, 3, 4, 5]
    assert q.select([F.id, "age"]).to_list()[:2] == [{"id": 1, "age": 30}, {"id": 2, "age": 17}]
    assert q.select({"who": F.first_name}).to_list()[4] == {}
    assert q.select(lambda u: u["id"] * 10).to_list() == [10, 20, 30, 40, 50]
    info = explained(q.select({"n": F.id, "a": "age"}))
    assert info["plan"] == "Project" and info["fields"] == {"n": "id", "a": "age"}
    full = q.select({"id": lambda x: x["id"], "name": F.first_name + " " + F.last_name, "age": F.age})
    info = explained(full)
    assert info["plan"] == "Project" and "first_name + ' ' + last_name" in info["function"]
    assert full.to_list()[0] == {"id": 1, "name": "Alice Zeus", "age": 30}
    assert full.to_list()[4] == {"id": 5}
    with pytest.raises(ProtoValidationError):
        q.select(5)


def test_where_after_select_filters_the_projection():
    q = from_collection(USERS).select({"name": F.first_name, "adult": F.age >= 18}).where(F.adult.is_true())
    info = explained(q)
    assert node_names(info) == ["Filter", "Project", "ValuesScan"]
    assert [r["name"] for r in q] == ["Alice", "Carol", "Dan"]


def test_distinct_keeps_equal_but_distinct_values_apart():
    data = [1, 1.0, Decimal("1"), True, 1, 1.0, "1", None, None, {"a": 1}, {"a": 1}, (1,), [1]]
    q = from_collection(data).distinct()
    assert node_names(explained(q)) == ["Distinct", "ValuesScan"]
    out = q.to_list()
    assert len(out) == 9
    assert [type(v) for v in out[:4]] == [int, float, Decimal, bool]


def test_distinct_and_count():  # port of test_linq_like.test_distinct_and_count
    emails = from_collection(USERS + USERS).select(F.email).distinct().to_list()
    assert emails == [u["email"] for u in USERS]
    assert from_collection(USERS).where(F.status == "active").count() == 3


def test_filter_projection_pagination():  # port of test_linq_like.test_filter_projection_pagination
    q = (from_collection(USERS)
         .where((F.age >= 18) & F.country.in_(["ES", "AR"]))
         .order_by(F.last_login, descending=True)
         .select({"id": lambda x: x["id"], "name": F.first_name + " " + F.last_name})
         .take(20))
    assert node_names(explained(q)) == ["Limit", "Project", "Sort", "Filter", "ValuesScan"]
    assert q.to_list() == [{"id": 4, "name": "Dan White"}, {"id": 1, "name": "Alice Zeus"}]


def test_lambda_where_with_window():  # port of test_linq_like.test_policies_warn (no policies: explicit)
    items = [{"score": 0.9}, {"score": 0.0}]
    q = from_collection(items).where(lambda x: (x.get("score") or 0) > 0.8).take(100)
    assert node_names(explained(q)) == ["Limit", "PythonFilter", "ValuesScan"]
    assert q.to_list() == [{"score": 0.9}]


# -- join, group_by, select_many ------------------------------------------------------------------

CUSTOMERS = [{"cid": 1, "name": "ann"}, {"cid": 2, "name": "bo"}, {"cid": 3, "name": "cy"}]
ORDERS = [{"oid": 10, "cid": 1, "status": "completed", "total": 10.0},
          {"oid": 11, "cid": 1, "status": "completed", "total": 15.0},
          {"oid": 12, "cid": 2, "status": "completed", "total": 8.0},
          {"oid": 13, "cid": 2, "status": "cancelled", "total": 9.0},
          {"oid": 14, "cid": 9, "status": "completed", "total": 1.0}]


def test_join_inner_and_left():
    inner = from_collection(CUSTOMERS).join(ORDERS, F.cid, "cid")
    info = explained(inner)
    assert info["plan"] == "Join" and info["kind"] == "inner"
    assert [(c["cid"], o["oid"]) for c, o in inner] == [(1, 10), (1, 11), (2, 12), (2, 13)]
    left = from_collection(CUSTOMERS).join(from_collection(ORDERS).where(F.status == "completed"),
                                           F.cid, F.cid, kind="left")
    assert [(c["cid"], o if o is MISSING else o["oid"]) for c, o in left] == [(1, 10), (1, 11), (2, 12),
                                                                            (3, MISSING)]
    assert node_names(left.explain()) == ["Join", "ValuesScan", "Filter", "ValuesScan"]
    by_fn = from_collection(CUSTOMERS).join(ORDERS, lambda c: c["cid"], lambda o: o["cid"])
    assert len(by_fn.to_list()) == 4
    with pytest.raises(ProtoValidationError):
        from_collection(CUSTOMERS).join(ORDERS, F.cid, F.cid, kind="outer")


def test_group_by_with_aggregates():  # port of test_linq_groupby.test_group_by_with_aggregates
    q = (from_collection(ORDERS)
         .where(F.status == "completed")
         .group_by(F.cid, {"customer_id": Agg.key(), "orders": Agg.count(), "sum": Agg.sum(F.total),
                           "avg": Agg.average(F.total), "lo": Agg.min("total"), "hi": Agg.max(F.total)})
         .order_by(F.sum, descending=True)
         .take(10))
    info = explained(q)
    assert find(info, "GroupBy") is not None
    assert q.to_list() == [
        {"customer_id": 1, "orders": 2, "sum": 25.0, "avg": 12.5, "lo": 10.0, "hi": 15.0},
        {"customer_id": 2, "orders": 1, "sum": 8.0, "avg": 8.0, "lo": 8.0, "hi": 8.0},
        {"customer_id": 9, "orders": 1, "sum": 1.0, "avg": 1.0, "lo": 1.0, "hi": 1.0},
    ]


def test_group_by_defaults_and_raw_aggregations():
    q = from_collection(ORDERS).group_by(F.status)
    out = q.to_list()
    assert [g["key"] for g in out] == ["completed", "cancelled"]
    assert [o["oid"] for o in out[0]["items"]] == [10, 11, 12, 14]
    q = from_collection(ORDERS).group_by("cid", {"n": ("count", None), "avg": Agg.avg("total"),
                                                 "ids": Agg.collect(F.oid)})
    assert node_names(explained(q)) == ["GroupBy", "ValuesScan"]
    assert q.to_list()[0] == {"key": 1, "n": 2, "avg": 12.5, "ids": (10, 11)}
    by_fn = from_collection(ORDERS).group_by(lambda o: o["total"] > 9, {"n": Agg.count()})
    assert by_fn.to_list() == [{"key": True, "n": 2}, {"key": False, "n": 3}]
    with pytest.raises(ProtoValidationError):
        from_collection(ORDERS).group_by(F.cid, {"x": "sum"})


def test_select_many():
    data = [{"id": 1, "tags": ["a", "b"]}, {"id": 2, "tags": []}, {"id": 3}, {"id": 4, "tags": ("c",)}]
    q = from_collection(data).select_many(F.tags)
    assert node_names(explained(q)) == ["Unnest", "ValuesScan"]
    assert q.to_list() == ["a", "b", "c"]
    assert from_collection(data).select_many(lambda r: [r["id"]] * 2).to_list() == [1, 1, 2, 2, 3, 3, 4, 4]
    pairs = from_collection(data).select_many("tags", with_parent=True).to_list()
    assert [(p["id"], t) for p, t in pairs] == [(1, "a"), (1, "b"), (4, "c")]


def test_select_many_top_n_per_category():
    # Port of the 0.1 test_select_many_plan.test_top_n_per_category: a per-element subquery,
    # materialized with to_list() (Unnest flattens lists, tuples and collections).
    categories = [{"id": 1, "name": "Electronics"}, {"id": 2, "name": "Books"}]
    products = [{"name": "Laptop", "cat": 1, "price": 1000}, {"name": "Mouse", "cat": 1, "price": 20},
                {"name": "Monitor", "cat": 1, "price": 300}, {"name": "Cookbook", "cat": 2, "price": 30},
                {"name": "Sci-Fi Novel", "cat": 2, "price": 25}, {"name": "Pamphlet", "cat": 2, "price": 1}]
    q = (from_collection(categories).order_by(F.name)
         .select_many(lambda c: from_collection(products).where(F.cat == c["id"])
                      .order_by(F.price, descending=True).take(2).to_list(), with_parent=True)
         .select(lambda pair: (pair[0]["name"], pair[1]["name"])))
    assert q.to_list() == [("Books", "Cookbook"), ("Books", "Sci-Fi Novel"), ("Electronics", "Laptop"),
                           ("Electronics", "Monitor")]
    assert from_collection(categories).select_many(lambda c: []).to_list() == []


# -- terminals -----------------------------------------------------------------------------------

def test_count_first_and_first_or_none():
    coll = indexed()
    q = from_collection(coll)
    assert q.count() == 2000
    assert q.where(F.category == "c3").count() == sum(1 for r in ROWS if r.category == "c3")
    assert q.count(F.id < 11) == 10
    assert q.order_by(F.value).first().value == min(r.value for r in ROWS)
    assert q.first(F.id == 42).id == 42
    assert q.where(F.id == -1).first_or_none() is None
    assert q.first_or_none(F.id == 3).id == 3
    with pytest.raises(ProtoUserError):
        q.where(F.id == -1).first()
    assert node_names(q.where(F.id == 3).explain_count()) == ["Count", "IndexLookup"]


def test_iteration_executes_the_explained_plan():
    q = from_collection(indexed()).where(F.category == "c1").order_by(F.id).take(3)
    plan = q.plan()
    assert isinstance(plan, QueryPlan)
    assert [r.id for r in q] == [r.id for r in plan.execute()]
    assert q.explain() == plan.explain()


# -- new plan nodes ------------------------------------------------------------------------------

def test_values_scan_and_distinct_nodes():
    scan = ValuesScan([3, 1, 3])
    assert list(scan.execute()) == [3, 1, 3] and list(scan.execute()) == [3, 1, 3]
    assert scan.explain()["plan"] == "ValuesScan" and scan.explain()["rows"] == 3
    assert scan.estimate().rows == 3
    d = Distinct(scan)
    assert list(d.execute()) == [3, 1]
    assert d.explain()["children"][0]["plan"] == "ValuesScan"
    with pytest.raises(ProtoUserError):
        d.source = scan
    with pytest.raises(ProtoValidationError):
        Distinct([1, 2])


# -- review fix round 1 ----------------------------------------------------------------------------

def test_missing_is_public_api():
    from proto_db import query
    from proto_db.query import linq
    assert linq.MISSING is MISSING and query.MISSING is MISSING
    assert repr(MISSING) == "MISSING" and not MISSING


def test_builder_names_are_child_fields_not_internals():
    assert isinstance(F.file.path, Field) and path_of(F.file.path) == "file.path"
    assert path_of(F()) is None and path_of(F.a.b) == "a.b"
    data = [{"id": 1, "file": {"path": "/b", "size": 2}}, {"id": 2, "file": {"path": "/a", "size": 1}}]
    q = from_collection(data)
    assert q.select(F.file.path).to_list() == ["/b", "/a"]
    assert ids(q.order_by(F.file.path)) == [2, 1]
    assert [g["key"] for g in q.group_by(F.file.path, {"n": Agg.count()})] == ["/b", "/a"]
    assert ids(q.where(F.file.path == "/a")) == [2]
    assert [(l["id"], r["id"]) for l, r in q.join(data, F.file.path, F.file.path)] == [(1, 1), (2, 2)]


def test_bound_builder_methods_are_rejected_as_keys_and_predicates():
    q = from_collection([{"name": "x", "v": [1.0]}])
    calls = [lambda: q.select(F.name.startswith), lambda: q.select({"s": F.name.lower}),
             lambda: q.order_by(F.v.near), lambda: q.order_by(F.name).then_by(F.name.upper),
             lambda: q.group_by(F.name.length), lambda: q.join(q, F.name.abs, F.name),
             lambda: q.select_many(F.v.contains), lambda: q.where(F.name.is_none),
             lambda: q.group_by(F.name, {"n": Agg.count(F.name.lower)}), lambda: (F.a == 1) & F.b.is_true]
    for call in calls:
        with pytest.raises(ProtoValidationError, match=r'F\["'):
            call()


def test_group_by_key_rename_keeps_any_aggregation_name():
    q = from_collection(ORDERS).group_by(F.cid, {"customer": Agg.key(), "n.c": Agg.count(), "@x": Agg.sum(F.total)})
    assert q.to_list()[0] == {"customer": 1, "n.c": 2, "@x": 25.0}
    assert node_names(explained(q)) == ["Project", "GroupBy", "ValuesScan"]


def test_impure_leading_predicate_still_lets_the_planner_order():
    lam = lambda r: r.value % 3 == 0  # noqa: E731
    q = from_collection(indexed()).where((F.id > 0) & lam).order_by(F.value).take(7)
    info = explained(q)
    assert info["plan"] == "Limit" and info["children"][0]["plan"] == "PythonFilter"
    assert find(info, "IndexOrderedScan") is not None and find(info, "Sort") is None
    expected = sorted((r for r in ROWS if r.id > 0 and r.value % 3 == 0), key=lambda r: r.value)[:7]
    assert [r.value for r in q] == [r.value for r in expected]
    # a callable order key still needs a Sort above the PythonFilter
    q = from_collection(indexed()).where((F.id > 0) & lam).order_by(lambda r: -r.value).take(3)
    assert node_names(explained(q))[:3] == ["Limit", "Sort", "PythonFilter"]


def test_near_and_lambda_split_like_two_wheres():
    coll = indexed()
    q = from_collection(coll).where(F.v.near([1.0, 0.0], k=2) & (lambda r: True))  # no error at where()
    with pytest.raises(ProtoNotSupportedError, match="index"):  # the planner: no vector index
        q.to_list()
    with pytest.raises(ProtoNotSupportedError, match="OR/NOT"):
        from_collection(coll).where(F.v.near([1.0], k=1) | (lambda r: True))


def test_in_rejects_a_mapping():
    with pytest.raises(ProtoValidationError):
        F.a.in_({"x": 1})


def test_queryable_of_a_queryable_keeps_its_operators():
    gen_rows = iter(list(USERS))
    base = from_collection(gen_rows).where(F.age > 18)
    again = Queryable(base)
    assert again.explain() == base.explain() and ids(again) == ids(base) == [1, 3, 4]
