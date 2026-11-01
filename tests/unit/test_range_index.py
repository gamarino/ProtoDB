"""Unit tests for RangeIndex (Task 4.3, spec section 8.3, D28, D30).

Catches: range operators leaking across type ranks (``None < 5`` or
``"a" > 1`` matching, against D28), wrong bound inclusivity in the four
``between`` variants, mixed numeric types (int, float, Decimal) ordered
inexactly, ordered iteration out of the values total order (MISSING < None <
bool < number < ..., unorderable keys after every orderable one, descending the
exact reverse), range estimates that ignore the tree's subtree counts, a
``validate`` that misses corruption, and a bulk ``build`` that differs from the
fold of ``with_added``.  The generic behaviour is covered by the index contract.
"""
import threading
from collections import Counter
from datetime import date
from decimal import Decimal

import pytest

from proto_db.collections import avl
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoCorruptionError, ProtoNotSupportedError
from proto_db.indexing import KeySpec, element_identity
from proto_db.indexing.range import RangeIndex
from proto_db.query.terms import (BETWEEN, BETWEEN_EXCL_BOTH, BETWEEN_EXCL_HIGH, BETWEEN_EXCL_LOW, EQ, GE, GT,
                                  IN, IS_NONE, LE, LT, NE, NEAR, NOT_NONE, NearQuery, Term, is_orderable)
from proto_db.values import MISSING, FrozenList, order_key


def ids(elements):
    return Counter(element_identity(e) for e in elements)


def run(index, term):
    return ids(index.plan(term).execute())


def scan(elements, term):
    return ids(e for e in elements if term.matches(e))


MIXED = [None, None, False, True, -2, 0, 1, 1.0, Decimal("1.5"), 2, 2.5, 3, 10**20, float("inf"), "", "a",
         "b", "b", b"x", date(2026, 1, 1), (1, "a"), (2,), FrozenList([1]), float("nan")]


def test_definition_and_type_name():
    index = RangeIndex("by_age", "age")
    assert index.key_spec == KeySpec.path("age")
    assert RangeIndex.__type_name__ == "proto_db.indexing.RangeIndex"


def test_supported_operators_and_ordering():
    index = RangeIndex("by_age", "age")
    for op, value in ((EQ, 1), (IN, (1, 2)), (NE, 1), (LT, 1), (LE, 1), (GT, 1), (GE, 1), (BETWEEN, (1, 2)),
                      (BETWEEN_EXCL_BOTH, (1, 2)), (BETWEEN_EXCL_LOW, (1, 2)), (BETWEEN_EXCL_HIGH, (1, 2)),
                      (IS_NONE, None), (NOT_NONE, None)):
        assert index.supports(Term("age", op, value)), op
    near = Term("age", NEAR, NearQuery((1.0,), k=1))
    assert not index.supports(near)
    with pytest.raises(ProtoNotSupportedError):
        index.plan(near)
    assert not index.supports(Term("other", LT, 1))
    assert index.supports_order(KeySpec.path("age"))
    assert not index.supports_order(KeySpec.path("other"))
    assert not index.supports_order(KeySpec.element())


def test_range_operators_stay_within_the_operand_rank():
    index = RangeIndex("v", KeySpec.element()).build(MIXED)
    assert sorted(run(index, Term(None, LT, 5))) == sorted(ids([-2, 0, 1, 1.0, Decimal("1.5"), 2, 2.5, 3]))
    assert run(index, Term(None, GT, "a")) == ids(["b", "b"])
    assert run(index, Term(None, GE, False)) == ids([False, True])
    assert run(index, Term(None, LE, None)) == ids([None, None])
    assert run(index, Term(None, GT, None)) == Counter()
    assert run(index, Term(None, LT, (2,))) == ids([(1, "a")])
    assert run(index, Term(None, GT, 3)) == ids([10**20, float("inf")])
    for op in (LT, LE, GT, GE):
        for pivot in (None, False, True, 0, 1, 1.5, Decimal("1.5"), 3, "b", b"x", date(2026, 1, 1), (2,)):
            term = Term(None, op, pivot)
            assert run(index, term) == scan(MIXED, term), term


def test_between_variants_inclusivity():
    index = RangeIndex("v", KeySpec.element()).build(MIXED)
    for op in (BETWEEN, BETWEEN_EXCL_BOTH, BETWEEN_EXCL_LOW, BETWEEN_EXCL_HIGH):
        for lo, hi in ((1, 2.5), (1.0, 1), (0, 10**20), (2, 1), ("a", "b"), (False, True)):
            term = Term(None, op, (lo, hi))
            assert run(index, term) == scan(MIXED, term), term
    assert sum(run(index, Term(None, BETWEEN, (1, 2))).values()) == 4  # 1, 1.0, 1.5, 2
    assert sum(run(index, Term(None, BETWEEN_EXCL_BOTH, (1, 2))).values()) == 1


def test_presence_equality_and_unorderable_keys():
    elements = [DBObject(age=a) for a in MIXED] + [DBObject(), DBObject(age=[1, 2])]
    index = RangeIndex("by_age", "age").build(elements)
    for term in (Term("age", IS_NONE), Term("age", NOT_NONE), Term("age", EQ, 1), Term("age", IN, (1, "b", None)),
                 Term("age", NE, 1), Term("age", NE, None), Term("age", LT, 2), Term("age", EQ, None)):
        assert run(index, term) == scan(elements, term), term


def test_ordered_iteration_follows_the_total_order():
    elements = [DBObject(age=a) for a in MIXED] + [DBObject(), DBObject(age=[1, 2])]
    index = RangeIndex("by_age", "age").build(elements)

    def group(e):
        k = KeySpec.path("age").extract(e)
        return (0, order_key(k)) if is_orderable(k) else (1,)

    up = [group(e) for e in index.ordered_plan().execute()]
    assert up == sorted(up) and len(up) == len(elements)
    assert up[0] == (0, order_key(MISSING)) and up[-1] == (1,)
    down = [group(e) for e in index.ordered_plan(descending=True).execute()]
    assert down == up[::-1]
    assert [group(e) for e in index.iter_ordered(descending=True)] == down
    plan = index.ordered_plan()
    assert plan.estimate().rows == len(elements)
    assert plan.explain()["plan"] == "IndexOrderedScan"


def test_range_estimates_use_subtree_counts():
    elements = list(range(100)) * 2  # 100 keys, two occurrences each
    index = RangeIndex("v", KeySpec.element()).build(elements)
    assert index.estimate(Term(None, EQ, 5)).rows == 2
    assert index.estimate(Term(None, LT, 10)).rows == 20
    assert index.estimate(Term(None, BETWEEN_EXCL_HIGH, (10, 20))).rows == 20
    assert index.estimate(Term(None, GT, 1000)).rows == 0
    assert index.estimate(Term(None, LT, "a")).rows == 0  # another rank: nothing
    small, large = index.estimate(Term(None, LT, 10)), index.estimate(Term(None, LT, 90))
    assert small.cost < large.cost < len(elements) + 100 + 10


def test_validate_detects_corruption():
    elements = [DBObject(age=a) for a in (1, 2, 2, None)] + [DBObject()]
    index = RangeIndex("by_age", "age").build(elements)
    index.validate(elements)
    with pytest.raises(ProtoCorruptionError):
        index.validate(elements[1:])
    group = avl.key_get(index.groups, 1)
    misfiled = avl.key_put(avl.key_remove(index.groups, 1)[0], 7, group)[0]
    with pytest.raises(ProtoCorruptionError):
        index._with(groups=misfiled).validate(elements)


def test_build_equals_fold():
    elements = MIXED * 2 + [DBObject(age=3)]
    built = RangeIndex("v", KeySpec.element()).build(elements)
    folded = RangeIndex("v", KeySpec.element())
    for e in elements:
        folded = folded.with_added(e)
    for term in (Term(None, LT, 3), Term(None, GE, "a"), Term(None, NE, 1), Term(None, NOT_NONE),
                 Term(None, IS_NONE)):
        assert run(built, term) == run(folded, term), term
    assert ids(built.ordered_plan().execute()) == ids(folded.ordered_plan().execute()) == ids(elements)
    built.validate(elements)
    folded.validate(elements)


def test_readers_unaffected_by_concurrent_writers():
    """8 threads run range queries on one version while 4 threads derive new versions."""
    base = [DBObject(age=i % 50, n=i) for i in range(300)]
    index = RangeIndex("by_age", "age").build(base)
    terms = [Term("age", LT, 10), Term("age", BETWEEN, (5, 25)), Term("age", GE, 40), Term("age", EQ, 7)]
    expected = {t: scan(base, t) for t in terms}
    barrier = threading.Barrier(12)
    errors: list = []
    derived: list = []

    def reader():
        barrier.wait()
        try:
            for _ in range(15):
                for t in terms:
                    assert run(index, t) == expected[t]
                assert sum(1 for _ in index.ordered_plan().execute()) == len(base)
        except BaseException as e:  # reported below
            errors.append(e)
            raise

    def writer(seed):
        barrier.wait()
        try:
            version, model = index, list(base)
            for i in range(60):
                if i % 3 == 2:
                    version = version.with_removed(model.pop((seed * 17 + i) % len(model)))
                else:
                    e = DBObject(age=(seed * 7 + i) % 60)
                    version = version.with_added(e)
                    model.append(e)
            derived.append((version, model))
        except BaseException as e:  # reported below
            errors.append(e)
            raise

    threads = [threading.Thread(target=reader) for _ in range(8)] + \
              [threading.Thread(target=writer, args=(s,)) for s in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, repr(errors[0])
    for version, model in derived:
        version.validate(model)
