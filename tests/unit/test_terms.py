"""Unit tests for proto_db.query.terms, the minimal term model and scan oracle (Task 4.1, D28).

Catches: a scan oracle that disagrees with values.py semantics (1 == 1.0,
True != 1), MISSING matching a comparison, IS_NONE not covering both MISSING and
None, range operators matching values of another type rank (None < 5), wrong
bound handling in the BETWEEN variants, and terms accepted with values that no
index could answer (MISSING, unorderable, malformed bounds).
"""
import dataclasses
from decimal import Decimal

import pytest

from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoNotSupportedError, ProtoValidationError
from proto_db.indexing import KeySpec, register_extractor
from proto_db.query.terms import (BETWEEN, BETWEEN_EXCL_BOTH, BETWEEN_EXCL_HIGH, BETWEEN_EXCL_LOW, EQ, GE,
                                  GT, IN, IS_NONE, LE, LT, NE, NEAR, NOT_NONE, OPERATORS, NearQuery, Term)
from proto_db.values import MISSING


def rec(**kw):
    return DBObject(**kw)


ABSENT, NONE, ONE, ONE_F, TRUE, FIVE, A_STR, A_LIST = (
    rec(), rec(x=None), rec(x=1), rec(x=1.0), rec(x=True), rec(x=5), rec(x="a"), rec(x=[1]))
ALL = [ABSENT, NONE, ONE, ONE_F, TRUE, FIVE, A_STR, A_LIST]


def matching(term):
    return [e for e in ALL if term.matches(e)]


def test_operator_vocabulary_uses_the_0_1_spellings():
    assert (EQ, NE, LT, LE, GT, GE, IN) == ("==", "!=", "<", "<=", ">", ">=", "in")
    assert (BETWEEN, BETWEEN_EXCL_BOTH, BETWEEN_EXCL_LOW, BETWEEN_EXCL_HIGH) == (
        "between[]", "between()", "between(]", "between[)")
    assert (IS_NONE, NOT_NONE, NEAR) == ("?N", "?!N", "near[]")
    assert set(OPERATORS) == {EQ, NE, LT, LE, GT, GE, IN, BETWEEN, BETWEEN_EXCL_BOTH, BETWEEN_EXCL_LOW,
                              BETWEEN_EXCL_HIGH, IS_NONE, NOT_NONE, NEAR, "contains", "?T", "?!T"}


def test_term_is_frozen_and_compares_by_value():
    t = Term("x", EQ, 1)
    assert t == Term("x", EQ, 1) and hash(t) == hash(Term("x", EQ, 1))
    assert t != Term("y", EQ, 1)
    with pytest.raises(dataclasses.FrozenInstanceError):
        t.value = 2
    assert t.key_spec == KeySpec.path("x")
    assert Term(None, EQ, 1).key_spec == KeySpec.element()


def test_equality_follows_values_semantics():
    assert matching(Term("x", EQ, 1)) == [ONE, ONE_F]  # 1 == 1.0, True is not 1
    assert matching(Term("x", EQ, Decimal(1))) == [ONE, ONE_F]
    assert matching(Term("x", EQ, True)) == [TRUE]
    assert matching(Term("x", EQ, None)) == [NONE]  # None is a value; MISSING is not None


def test_missing_never_matches_a_comparison():
    for term in (Term("x", NE, 1), Term("x", LT, 100), Term("x", GE, -100), Term("x", IN, (1, None)),
                 Term("x", BETWEEN, (-10, 10)), Term("x", EQ, None), Term("x", NE, None)):
        assert not term.matches(ABSENT), term


def test_not_equal_is_present_and_not_equal():
    assert matching(Term("x", NE, 1)) == [NONE, TRUE, FIVE, A_STR, A_LIST]
    assert matching(Term("x", NE, None)) == [ONE, ONE_F, TRUE, FIVE, A_STR, A_LIST]


def test_is_none_covers_missing_and_none():
    assert matching(Term("x", IS_NONE)) == [ABSENT, NONE]
    assert matching(Term("x", NOT_NONE)) == [ONE, ONE_F, TRUE, FIVE, A_STR, A_LIST]


def test_range_operators_stay_within_the_value_rank():
    assert matching(Term("x", LT, 5)) == [ONE, ONE_F]  # not None, not True
    assert matching(Term("x", LE, 5)) == [ONE, ONE_F, FIVE]
    assert matching(Term("x", GT, 1)) == [FIVE]  # not "a"
    assert matching(Term("x", GE, 1)) == [ONE, ONE_F, FIVE]
    assert matching(Term("x", GT, "")) == [A_STR]
    assert matching(Term("x", LT, None)) == [] and matching(Term("x", LE, None)) == [NONE]
    assert matching(Term("x", GE, False)) == [TRUE]


def test_between_variants():
    assert matching(Term("x", BETWEEN, (1, 5))) == [ONE, ONE_F, FIVE]
    assert matching(Term("x", BETWEEN_EXCL_BOTH, (1, 5))) == []
    assert matching(Term("x", BETWEEN_EXCL_LOW, (1, 5))) == [FIVE]
    assert matching(Term("x", BETWEEN_EXCL_HIGH, (1, 5))) == [ONE, ONE_F]
    assert matching(Term("x", BETWEEN, [0, 1])) == [ONE, ONE_F]  # a list of bounds is accepted
    assert Term("x", BETWEEN, [0, 1]).value == (0, 1)


def test_in():
    t = Term("x", IN, [5, "a", 9])
    assert t.value == (5, "a", 9)
    assert matching(t) == [FIVE, A_STR]
    assert matching(Term("x", IN, frozenset({1}))) == [ONE, ONE_F]
    assert matching(Term("x", IN, ())) == []


def test_unorderable_keys_only_match_presence_and_not_equal():
    assert A_LIST in matching(Term("x", NOT_NONE))
    assert A_LIST in matching(Term("x", NE, 1))
    for op, value in ((EQ, 1), (LT, 100), (GT, -1), (IN, (1,)), (BETWEEN, (-10, 10))):
        assert not Term("x", op, value).matches(A_LIST)


def test_path_none_is_the_element_itself():
    assert Term(None, EQ, 3).matches(3) and not Term(None, EQ, 3).matches(4)
    assert Term(None, IS_NONE).matches(None) and Term(None, IS_NONE).matches(MISSING)
    r = rec(a=1)
    assert Term(None, EQ, r).matches(r) and not Term(None, EQ, r).matches(rec(a=1))


def _double_x(element):
    x = KeySpec.path("x").extract(element)
    return x * 2 if type(x) is int else MISSING


register_extractor("tests.terms.double_x", _double_x)


def test_dotted_and_extractor_paths():
    assert Term("a.b", EQ, 2).matches(rec(a={"b": 2}))
    t = Term("@tests.terms.double_x", EQ, 10)
    assert t.key_spec == KeySpec.extractor("tests.terms.double_x")
    assert matching(t) == [FIVE]


def test_near_is_not_answered_by_the_scan_oracle():
    t = Term("v", NEAR, NearQuery(vector=[1.0, 0.0], k=3))
    with pytest.raises(ProtoNotSupportedError):
        t.matches(rec(v=[1.0, 0.0]))


def test_near_values_are_canonical_and_hashable():
    q = NearQuery(vector=[1, 0.5], k=2, metric="cosine")
    assert q.vector == (1.0, 0.5) and all(type(x) is float for x in q.vector)
    t = Term("v", NEAR, q)
    assert hash(t) == hash(Term("v", NEAR, NearQuery(vector=(1.0, 0.5), k=2, metric="cosine")))
    assert {t: 1}[Term("v", NEAR, {"vector": [1.0, 0.5], "k": 2, "metric": "cosine"})] == 1
    assert Term("v", NEAR, {"vector": [1.0], "threshold": 0.3}).value == NearQuery((1.0,), threshold=0.3)
    assert NearQuery((1.0,), k=1).metric is None  # None = the index's default metric


@pytest.mark.parametrize("kwargs", [
    {"vector": [], "k": 1}, {"vector": [1.0]}, {"vector": [1.0], "k": 0}, {"vector": [1.0], "k": True},
    {"vector": [1.0], "k": 1.5}, {"vector": [float("nan")], "k": 1}, {"vector": [float("inf")], "k": 1},
    {"vector": [True], "k": 1}, {"vector": ["1"], "k": 1}, {"vector": 1.0, "k": 1},
    {"vector": [1.0], "threshold": float("nan")}, {"vector": [1.0], "threshold": "0.1"},
    {"vector": [1.0], "k": 1, "metric": "manhattan"},
])
def test_near_query_validation(kwargs):
    with pytest.raises(ProtoValidationError):
        NearQuery(**kwargs)


@pytest.mark.parametrize("value", [([1.0, 0.0], 3), [1.0], 5, {"vector": [1.0]}, {"vector": [1.0], "k": 1, "x": 2}])
def test_near_terms_need_a_near_query(value):
    with pytest.raises(ProtoValidationError):
        Term("v", NEAR, value)


def test_in_over_a_set_is_stored_in_a_canonical_order():
    a = Term("x", IN, {3, "b", 1, 2.5, "a"})
    b = Term("x", IN, frozenset(["a", 2.5, 1, "b", 3]))
    assert a == b and hash(a) == hash(b)
    assert a.value == (1, 2.5, 3, "a", "b")
    assert Term("x", IN, [3, 1]).value == (3, 1)  # sequences keep the caller's order


@pytest.mark.parametrize("path, op, value", [
    ("x", "bogus", 1), ("", EQ, 1), ("a..b", EQ, 1), (3, EQ, 1),
    ("x", EQ, MISSING), ("x", EQ, [1]), ("x", LT, {"a": 1}), ("x", EQ, float("nan")), ("x", EQ, object()),
    ("x", IN, 5), ("x", IN, "abc"), ("x", IN, (1, MISSING)), ("x", IN, ([1],)),
    ("x", BETWEEN, 5), ("x", BETWEEN, (1,)), ("x", BETWEEN, (1, 2, 3)), ("x", BETWEEN, (1, "z")),
    ("x", BETWEEN_EXCL_LOW, (MISSING, 1)), ("x", IS_NONE, 1), ("x", NOT_NONE, 0),
])
def test_invalid_terms_are_rejected(path, op, value):
    with pytest.raises(ProtoValidationError):
        Term(path, op, value)


def test_describe():
    assert Term("x", EQ, 1).describe() == {"path": "x", "op": "==", "value": 1}
    assert Term(None, IS_NONE).describe() == {"path": None, "op": "?N", "value": None}
