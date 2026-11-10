"""Unit tests for proto_db.query.expressions (Task 6.1, spec section 10, D28, D36).

Catches: the 0.1 prefix syntax not accepted (binary '&'/'|' in a prefix
stream, '!' unary, implicit top-level AND, nested lists, ternary between and
near[]), malformed input compiled into something instead of raising a precise
``ProtoValidationError``, And/Or nodes that do not flatten or are not
hashable/immutable, ``Not(Not(e))`` not simplified, ``evaluate`` disagreeing
with ``Term.matches`` (D28) or a Python reference truth table on MISSING, None
and mixed types, missing short-circuit in And/Or, and ``describe()`` output that
is not JSON-serialisable.
"""
import itertools
import json
from decimal import Decimal

import pytest

from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoNotSupportedError, ProtoValidationError
from proto_db.query import expressions as ex
from proto_db.query.expressions import And, Expression, Not, Or, Term, evaluate, iter_terms
from proto_db.query.terms import (BETWEEN, BETWEEN_EXCL_BOTH, BETWEEN_EXCL_HIGH, BETWEEN_EXCL_LOW, CONTAINS, EQ,
                                  GE, GT, IN, IS_NONE, IS_TRUE, LE, LT, NE, NEAR, NOT_NONE, NOT_TRUE, NearQuery)

A1 = Term("a", EQ, 1)
B2 = Term("b", GT, 2)
C3 = Term("c", LT, 3)


# -- node construction ----------------------------------------------------------------------

def test_term_is_an_expression_and_stays_hashable():
    assert isinstance(A1, Expression)
    assert hash(A1) == hash(Term("a", EQ, 1.0)) and A1 == Term("a", EQ, 1.0)
    assert {A1, Term("a", EQ, 1)} == {A1}


def test_and_or_flatten_nested_same_type():
    assert And(And(A1, B2), C3) == And(A1, B2, C3)
    assert And(A1, And(B2, C3)).operands == (A1, B2, C3)
    assert Or(Or(A1, B2), Or(C3, A1)).operands == (A1, B2, C3, A1)
    # different types are not flattened into each other
    assert And(Or(A1, B2), C3).operands == (Or(A1, B2), C3)


def test_and_or_need_two_expression_operands():
    for cls in (And, Or):
        with pytest.raises(ProtoValidationError, match="at least two"):
            cls(A1)
        with pytest.raises(ProtoValidationError, match="at least two"):
            cls()
        with pytest.raises(ProtoValidationError, match="Expression"):
            cls(A1, ["a", "==", 1])


def test_not_not_simplifies():
    assert Not(Not(A1)) is A1
    assert Not(Not(Not(A1))) == Not(A1)
    with pytest.raises(ProtoValidationError, match="Expression"):
        Not("a")


def test_nodes_are_immutable_hashable_and_comparable():
    e = And(A1, Or(B2, Not(C3)))
    assert hash(e) == hash(And(A1, Or(B2, Not(C3))))
    assert e == And(A1, Or(B2, Not(C3))) and e != Or(A1, Or(B2, Not(C3)))
    assert And(A1, B2) != And(B2, A1)  # order is kept (evaluation order)
    assert len({e, And(A1, Or(B2, Not(C3)))}) == 1
    for node in (e, Not(A1), Or(A1, B2)):
        with pytest.raises(AttributeError):
            node.operands = ()
        with pytest.raises(AttributeError):
            node.extra = 1


def test_repr_is_readable():
    assert repr(Not(A1)) == f"Not({A1!r})"
    assert repr(And(A1, B2)) == f"And({A1!r}, {B2!r})"
    assert repr(Or(A1, B2)) == f"Or({A1!r}, {B2!r})"


def test_describe_is_json_serialisable():
    e = And(Term("d", EQ, Decimal("1.50")), Or(Term("t", BETWEEN, (1, 5)), Not(Term("n", IS_NONE))),
            Term("v", NEAR, NearQuery((1, 0), k=3)))
    d = e.describe()
    assert d["and"][0] == {"path": "d", "op": EQ, "value": "Decimal('1.50')"}
    assert d["and"][1]["or"][0]["value"] == [1, 5]
    assert d["and"][1]["or"][1] == {"not": {"path": "n", "op": IS_NONE, "value": None}}
    assert d["and"][2]["value"] == {"vector": [1.0, 0.0], "k": 3, "threshold": None, "metric": None}
    json.dumps(d)


def test_iter_terms_walks_the_tree_in_order():
    assert list(iter_terms(And(A1, Or(B2, Not(C3))))) == [A1, B2, C3]
    assert list(iter_terms(A1)) == [A1]


# -- compile (0.1 prefix syntax) ---------------------------------------------------------------

@pytest.mark.parametrize("tokens, expected", [
    (["a", EQ, 1], Term("a", EQ, 1)),
    (["a", NE, "x"], Term("a", NE, "x")),
    (["a", LT, 1], Term("a", LT, 1)),
    (["a", LE, 1], Term("a", LE, 1)),
    (["a", GT, 1], Term("a", GT, 1)),
    (["a", GE, 1], Term("a", GE, 1)),
    (["a", IN, [1, 2]], Term("a", IN, (1, 2))),
    (["a", CONTAINS, "x"], Term("a", CONTAINS, "x")),
    (["a", IS_NONE], Term("a", IS_NONE)),
    (["a", NOT_NONE], Term("a", NOT_NONE)),
    (["a", IS_TRUE], Term("a", IS_TRUE)),
    (["a", NOT_TRUE], Term("a", NOT_TRUE)),
    (["a", BETWEEN, 1, 5], Term("a", BETWEEN, (1, 5))),
    (["a", BETWEEN_EXCL_BOTH, 1, 5], Term("a", BETWEEN_EXCL_BOTH, (1, 5))),
    (["a", BETWEEN_EXCL_LOW, 1, 5], Term("a", BETWEEN_EXCL_LOW, (1, 5))),
    (["a", BETWEEN_EXCL_HIGH, 1, 5], Term("a", BETWEEN_EXCL_HIGH, (1, 5))),
    (["v", NEAR, [1, 0], 0.8], Term("v", NEAR, NearQuery((1.0, 0.0), threshold=0.8, metric="cosine"))),
    (["v", NEAR, {"vector": [1, 0], "k": 2}], Term("v", NEAR, NearQuery((1.0, 0.0), k=2))),
    (["v", NEAR, NearQuery((1, 0), k=2, metric="l2")], Term("v", NEAR, NearQuery((1, 0), k=2, metric="l2"))),
    ([None, EQ, 5], Term(None, EQ, 5)),
    (["a.b.c", EQ, 5], Term("a.b.c", EQ, 5)),
])
def test_compile_every_operator(tokens, expected):
    compiled = Expression.compile(tokens)
    assert compiled == expected
    assert Expression.compile(compiled.to_list()) == compiled


def test_compile_prefix_stream_operators():
    assert Expression.compile(["&", "a", EQ, 1, "b", GT, 2]) == And(A1, B2)
    assert Expression.compile(["|", "a", EQ, 1, "b", GT, 2]) == Or(A1, B2)
    assert Expression.compile(["!", "a", EQ, 1]) == Not(A1)
    assert Expression.compile(["!", "!", "a", EQ, 1]) == A1
    # '&' is binary in a prefix stream: the third term joins through the implicit AND
    assert Expression.compile(["&", "a", EQ, 1, "b", GT, 2, "c", LT, 3]) == And(A1, B2, C3)
    assert Expression.compile(["|", "a", EQ, 1, "&", "b", GT, 2, "c", LT, 3]) == Or(A1, And(B2, C3))
    assert Expression.compile(["&", "|", "a", EQ, 1, "b", GT, 2, "c", LT, 3]) == And(Or(A1, B2), C3)


def test_compile_implicit_and_and_nested_lists():
    assert Expression.compile(["a", EQ, 1, "b", GT, 2]) == And(A1, B2)
    assert Expression.compile([["a", EQ, 1], ["b", GT, 2]]) == And(A1, B2)
    assert Expression.compile(["|", ["a", EQ, 1], ["&", ["b", GT, 2], ["c", LT, 3]]]) == Or(A1, And(B2, C3))
    assert Expression.compile([["&", "a", EQ, 1, "b", GT, 2], "c", LT, 3]) == And(A1, B2, C3)
    assert Expression.compile(["!", ["|", "a", EQ, 1, "b", GT, 2]]) == Not(Or(A1, B2))
    assert Expression.compile([[["a", EQ, 1]]]) == A1
    # already compiled expressions are accepted as tokens
    assert Expression.compile(["&", A1, ["b", GT, 2]]) == And(A1, B2)
    assert Expression.compile(A1) is A1


def test_compile_round_trips_nested_trees():
    trees = [And(A1, Or(B2, Not(C3))), Or(Not(And(A1, B2)), C3, Term("x", BETWEEN, (1, 9))),
             Not(Or(Term("v", NEAR, NearQuery((1, 2), k=4)), Term("y", IN, ("a", "b"))))]
    for tree in trees:
        assert Expression.compile(tree.to_list()) == tree


@pytest.mark.parametrize("tokens, message", [
    ([], "empty"),
    ("a == 1", "list"),
    (("a", EQ, 1), "list"),
    (["a"], "operator"),
    (["a", "=~", 1], "Unknown operator '=~'"),
    (["a", EQ], "needs a value"),
    (["a", BETWEEN, 1], "needs a lower and an upper bound"),
    (["v", NEAR, [1, 0]], "near"),
    (["&", "a", EQ, 1], "'&' needs two operands"),
    (["|"], "'|' needs two operands"),
    (["!"], "'!' needs an operand"),
    ([5, EQ, 1], "Expected a path"),
    ([["a", EQ, 1], 7], "Expected a path"),
    ([[]], "empty"),
    (["a", EQ, [1, 2]], "orderable"),
    (["a", BETWEEN, 1, "z"], "one type rank"),
    (["a", IS_NONE, 3], "Expected a path"),
    (["a", IN, 3], "list, tuple or set"),
])
def test_compile_rejects_invalid_input(tokens, message):
    with pytest.raises(ProtoValidationError, match=message):
        Expression.compile(tokens)


# -- evaluate ---------------------------------------------------------------------------------------

def rec(**kw):
    return DBObject(**kw)


SAMPLES = [rec(), rec(a=None), rec(a=1), rec(a=1.0), rec(a=Decimal("1.00")), rec(a=True), rec(a=0), rec(a=7),
           rec(a="x"), rec(a=[1, 2]), rec(a=float("nan")), {"a": 3}, {"b": 1}, 5, None]

TERMS = [Term("a", EQ, 1), Term("a", NE, 1), Term("a", LT, 5), Term("a", GE, 1), Term("a", IS_NONE),
         Term("a", NOT_NONE), Term("a", IN, (1, "x")), Term("a", BETWEEN, (0, 3)), Term("a", IS_TRUE),
         Term("a", NOT_TRUE), Term("a", CONTAINS, 2), Term(None, EQ, 5)]


def test_evaluate_term_is_the_scan_oracle():
    for term, element in itertools.product(TERMS, SAMPLES):
        assert evaluate(term, element) is term.matches(element)


def test_evaluate_truth_tables():
    for t1, t2 in itertools.product(TERMS, repeat=2):
        for e in SAMPLES:
            m1, m2 = t1.matches(e), t2.matches(e)
            assert evaluate(And(t1, t2), e) is (m1 and m2)
            assert evaluate(Or(t1, t2), e) is (m1 or m2)
            assert evaluate(Not(t1), e) is (not m1)
            assert evaluate(Not(And(t1, t2)), e) is (not (m1 and m2))


def test_not_of_a_comparison_on_a_missing_key_is_true():
    # D28: MISSING satisfies no comparison, so the negation is true (documented in D36).
    assert evaluate(Term("a", LT, 5), rec()) is False
    assert evaluate(Not(Term("a", LT, 5)), rec()) is True
    assert evaluate(Term("a", NE, 5), rec()) is False
    assert evaluate(Not(Term("a", EQ, 5)), rec()) is True


def test_mixed_types_and_equal_but_distinct_values():
    t = Term("a", EQ, 1)
    assert [evaluate(t, e) for e in (rec(a=1), rec(a=1.0), rec(a=Decimal("1.00")), rec(a=True))] == \
        [True, True, True, False]
    assert evaluate(Term("a", GT, 0), rec(a="x")) is False  # cross-rank ordering never matches


def test_and_or_short_circuit(monkeypatch):
    calls = []
    original = Term.matches

    def spy(self, element):
        calls.append(self)
        return original(self, element)

    monkeypatch.setattr(Term, "matches", spy)
    e = rec(a=1, b=0)
    assert evaluate(And(Term("a", EQ, 2), Term("b", EQ, 0)), e) is False
    assert calls == [Term("a", EQ, 2)]
    calls.clear()
    assert evaluate(Or(Term("a", EQ, 1), Term("b", EQ, 9)), e) is True
    assert calls == [Term("a", EQ, 1)]


def test_evaluate_near_is_not_a_scan_predicate():
    with pytest.raises(ProtoNotSupportedError):
        evaluate(Term("v", NEAR, NearQuery((1, 0), k=1)), rec(v=(1, 0)))


def test_evaluate_rejects_non_expressions():
    with pytest.raises(ProtoValidationError, match="Expression"):
        evaluate(["a", EQ, 1], rec(a=1))


# -- operators added for the 0.1 vocabulary (contains, ?T, ?!T) -------------------------------------

def test_contains_semantics():
    t = Term("a", CONTAINS, 2)
    assert t.matches(rec(a=[1, 2])) and t.matches(rec(a=(2.0,))) and t.matches(rec(a=[[0], 2]))
    assert not t.matches(rec(a=[1, 3])) and not t.matches(rec()) and not t.matches(rec(a=None))
    assert not t.matches(rec(a=2)) and not t.matches(rec(a={"x": 2}))
    assert not Term("a", CONTAINS, True).matches(rec(a=[1]))  # True is not 1 (D28)
    s = Term("a", CONTAINS, "ell")
    assert s.matches(rec(a="hello")) and not s.matches(rec(a="help")) and not s.matches(rec(a=b"ell"))
    assert Term("a", CONTAINS, b"el").matches(rec(a=b"hello"))
    with pytest.raises(ProtoValidationError):
        Term("a", CONTAINS, [1])


def test_truthiness_operators():
    t, f = Term("a", IS_TRUE), Term("a", NOT_TRUE)
    for value, truthy in ((1, True), (0, False), ("", False), ("x", True), (None, False), ([], False),
                          ([0], True), (Decimal("0"), False), (float("nan"), True)):
        assert t.matches(rec(a=value)) is truthy and f.matches(rec(a=value)) is (not truthy)
    # MISSING matches neither (absence is not a value, D28)
    assert not t.matches(rec()) and not f.matches(rec())
    with pytest.raises(ProtoValidationError):
        Term("a", IS_TRUE, 1)


def test_module_reexports_term():
    assert ex.Term is Term


# -- fix round 1: equality follows values_equal; contains on collections --------------------------

def test_term_equality_and_hash_follow_values_equal():
    assert Term("x", EQ, True) != Term("x", EQ, 1)
    assert Term("x", IN, (True,)) != Term("x", IN, (1,))
    assert Term("x", BETWEEN, (True, True)) != Term("x", BETWEEN, (1, 1))
    assert len({Term("x", EQ, True), Term("x", EQ, 1)}) == 2
    assert Term("x", EQ, 1) == Term("x", EQ, 1.0) == Term("x", EQ, Decimal("1.00"))
    assert hash(Term("x", EQ, 1)) == hash(Term("x", EQ, 1.0)) == hash(Term("x", EQ, Decimal("1.00")))
    assert Term("x", IN, (1, 2)) == Term("x", IN, (1.0, Decimal(2)))
    assert Term("x", EQ, (True, 1)) != Term("x", EQ, (1, 1))
    assert Term("v", NEAR, NearQuery((1, 0), k=2)) == Term("v", NEAR, {"vector": (1.0, 0.0), "k": 2})
    assert And(Term("x", EQ, True), B2) != And(Term("x", EQ, 1), B2)
    assert Not(Term("x", EQ, True)) != Not(Term("x", EQ, 1))
    # equal terms match the same elements
    assert Term("x", EQ, True).matches({"x": 1}) is False and Term("x", EQ, 1).matches({"x": 1}) is True


def test_contains_on_dicts_and_collection_atoms():
    from proto_db.collections.dictionaries import Dictionary
    from proto_db.collections.lists import List
    from proto_db.collections.sets import CountedSet, Set
    t = Term("c", CONTAINS, 2)
    assert t.matches({"c": {2: "a"}}) and t.matches({"c": {2.0: "a"}}) and not t.matches({"c": {"x": 2}})
    assert not Term("c", CONTAINS, "2").matches({"c": {2: 1}})
    assert t.matches(rec(c=List.from_iterable([1, 2.0, [3]])))
    assert not t.matches(rec(c=List.from_iterable([1, [2], True])))
    assert not Term("c", CONTAINS, True).matches(rec(c=List.from_iterable([1])))
    assert t.matches(rec(c=Set.from_iterable([2, 3]))) and not t.matches(rec(c=Set.from_iterable([3])))
    assert t.matches(rec(c=CountedSet.from_iterable([2, 2]))) and not t.matches(rec(c=CountedSet()))
    d = Dictionary().set_at("k", 2)
    assert Term("c", CONTAINS, "k").matches(rec(c=d)) and not t.matches(rec(c=d))  # keys, not values
    nested = rec(inner=rec(c=List.from_iterable(["a", 2])))
    assert Term("inner.c", CONTAINS, 2).matches(nested)
    for other in (None, 2, 2.5, rec(c=1)):
        assert not t.matches({"c": other})
    assert evaluate(t, rec()) is False
