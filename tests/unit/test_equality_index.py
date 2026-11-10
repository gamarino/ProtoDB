"""Unit tests for EqualityIndex (Task 4.2, spec section 8.3, D30).

Catches: a wrong operator set (claiming ranges or ordering), duplicates
collapsed or removed all at once, MISSING keys lost (``?N`` disagreeing with a
scan) or returned by ``!=``, ``1``/``1.0`` split into two keys or ``True``
merged with ``1``, unorderable keys (lists, dicts, NaN) or unorderable elements
breaking the index, removal of an absent element passing silently, estimates
that are not exact for lookups or that let ``!=`` undercut a scan, a ``validate``
that misses stale or miscounted entries, a bulk ``build`` that differs from the
fold of ``with_added``, and readers disturbed by concurrent derivation of new
versions.  The generic behaviour is covered by the index contract
(tests/unit/test_index_contract.py runs it for every registered kind).
"""
import threading
from collections import Counter
from decimal import Decimal

import pytest

from proto_db.collections import avl
from proto_db.collections.lists import List
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoCorruptionError, ProtoNotSupportedError, ProtoValidationError
from proto_db.indexing import KeySpec, element_identity
from proto_db.indexing.equality import EqualityIndex
from proto_db.query.plans import CollectionScanPlan
from proto_db.query.terms import (BETWEEN, EQ, GE, GT, IN, IS_NONE, LE, LT, NE, NEAR, NOT_NONE, NearQuery,
                                  Term)
from proto_db.storage.memory import MemoryStorage
from proto_db.values import FrozenDict, FrozenList
from tests.support.persistence import DirectContext


def ids(elements):
    return Counter(element_identity(e) for e in elements)


def run(index, term):
    return ids(index.plan(term).execute())


def scan(elements, term):
    return ids(e for e in elements if term.matches(e))


def people():
    return [DBObject(name="a", city="Paris"), DBObject(name="b", city="Rome"), DBObject(name="c"),
            DBObject(name="d", city=None), DBObject(name="e", city="Paris"), DBObject(name="f", city=1),
            DBObject(name="g", city=1.0), DBObject(name="h", city=True), DBObject(name="i", city=[1, 2])]


def test_definition_type_name_and_str_key():
    index = EqualityIndex("by_city", "city")
    assert index.key_spec == KeySpec.path("city")
    assert EqualityIndex.__type_name__ == "proto_db.indexing.EqualityIndex"
    assert EqualityIndex("e", KeySpec.element()).key_spec == KeySpec.element()
    with pytest.raises(ProtoValidationError):
        EqualityIndex("bad", 3)


def test_supported_operators():
    index = EqualityIndex("by_city", "city")
    for op, value in ((EQ, "x"), (IN, ("x", 1)), (NE, "x"), (IS_NONE, None), (NOT_NONE, None)):
        assert index.supports(Term("city", op, value)), op
    for op, value in ((LT, 1), (LE, 1), (GT, 1), (GE, 1), (BETWEEN, (1, 2)),
                      (NEAR, NearQuery((1.0,), k=1))):
        term = Term("city", op, value)
        assert not index.supports(term), op
        with pytest.raises(ProtoNotSupportedError):
            index.plan(term)
        with pytest.raises(ProtoNotSupportedError):
            index.estimate(term)
    assert not index.supports(Term("zip", EQ, "x"))
    assert not index.supports_order(KeySpec.path("city"))
    with pytest.raises(ProtoNotSupportedError):
        index.ordered_plan()


def test_answers_match_a_scan_with_value_semantics():
    elements = people()
    index = EqualityIndex("by_city", "city").build(elements)
    terms = [Term("city", EQ, "Paris"), Term("city", EQ, 1), Term("city", EQ, Decimal(1)), Term("city", EQ, True),
             Term("city", EQ, None), Term("city", IN, ("Rome", 1.0, 1)), Term("city", NE, "Paris"),
             Term("city", NE, None), Term("city", IS_NONE), Term("city", NOT_NONE), Term("city", EQ, "nowhere")]
    for term in terms:
        assert run(index, term) == scan(elements, term), term
    assert sum(run(index, Term("city", EQ, 1)).values()) == 2  # 1 and 1.0 are one key, True is not
    assert sum(run(index, Term("city", IN, (1, 1.0))).values()) == 2  # an operand twice: each element once


def test_missing_keys_are_recorded_and_never_match_ne():
    elements = people()
    index = EqualityIndex("by_city", "city").build(elements)
    missing = DBObject(name="c")
    assert element_identity(elements[2]) in run(index, Term("city", IS_NONE))
    assert element_identity(elements[2]) not in run(index, Term("city", NE, "Paris"))
    assert element_identity(elements[2]) not in run(index, Term("city", NOT_NONE))
    grown = index.with_added(missing)
    assert sum(run(grown, Term("city", IS_NONE)).values()) == 3


def test_duplicates_are_counted():
    x = DBObject(city="Oslo")
    index = EqualityIndex("by_city", "city").build([x, x, DBObject(city="Oslo")])
    assert sum(run(index, Term("city", EQ, "Oslo")).values()) == 3
    once = index.with_removed(x)
    assert run(once, Term("city", EQ, "Oslo"))[element_identity(x)] == 1
    assert sum(run(once, Term("city", EQ, "Oslo")).values()) == 2
    assert run(once.with_removed(x), Term("city", EQ, "Oslo"))[element_identity(x)] == 0
    plain = EqualityIndex("v", KeySpec.element()).build([3, 3, 3.0, True])
    assert sum(run(plain, Term(None, EQ, 3)).values()) == 3
    assert sum(run(plain.with_removed(3.0), Term(None, EQ, 3)).values()) == 2


def test_removing_an_absent_element_raises():
    index = EqualityIndex("by_city", "city").build([DBObject(city="Oslo")])
    with pytest.raises(ProtoValidationError):
        index.with_removed(DBObject(city="Oslo"))  # same key, another element
    with pytest.raises(ProtoValidationError):
        index.with_removed(DBObject(city="Rome"))
    with pytest.raises(ProtoValidationError):
        EqualityIndex("v", KeySpec.element()).with_removed([1])


def test_unorderable_keys_and_elements():
    elements = [FrozenList([1]), FrozenList([1]), FrozenDict({"a": 1}), float("nan"), (FrozenList([2]), 1),
                FrozenDict({"city": "Paris"}), FrozenDict({"city": "Paris", "n": 2}), 4]
    by_value = EqualityIndex("v", KeySpec.element()).build(elements)
    by_city = EqualityIndex("c", "city").build(elements)
    for index, path in ((by_value, None), (by_city, "city")):
        for term in (Term(path, NE, 4), Term(path, NOT_NONE), Term(path, IS_NONE), Term(path, EQ, "Paris"),
                     Term(path, EQ, 4)):
            assert run(index, term) == scan(elements, term), term
        index.validate(elements)
    assert sum(run(by_city, Term("city", EQ, "Paris")).values()) == 2
    after = by_value.with_removed(FrozenList([1])).with_removed(float("nan"))
    rest = [FrozenList([1]), FrozenDict({"a": 1}), (FrozenList([2]), 1), FrozenDict({"city": "Paris"}),
            FrozenDict({"city": "Paris", "n": 2}), 4]
    after.validate(rest)
    assert run(after, Term(None, NOT_NONE)) == scan(rest, Term(None, NOT_NONE))


def test_estimates():
    elements = people()
    n = len(elements)
    index = EqualityIndex("by_city", "city").build(elements)
    assert index.estimate(Term("city", EQ, "Paris")).rows == 2
    assert index.estimate(Term("city", EQ, "nowhere")).rows == 0
    assert index.estimate(Term("city", IN, ("Paris", "Rome", "Paris"))).rows == 3
    assert index.estimate(Term("city", IS_NONE)).rows == 2
    assert index.estimate(Term("city", NOT_NONE)).rows == n - 2
    ne = index.estimate(Term("city", NE, "Paris"))
    assert ne.rows == n - 1 - 2  # minus MISSING and the two Paris
    coll = List.from_iterable(elements)
    scan_cost = CollectionScanPlan(coll).estimate().cost
    assert ne.cost > scan_cost, "a != plan must not undercut a scan (spec: scan preferred)"
    eq = index.estimate(Term("city", EQ, "Paris"))
    assert eq.rows <= eq.cost < scan_cost
    for term in (Term("city", EQ, "Paris"), Term("city", NE, 1), Term("city", NOT_NONE)):
        assert index.plan(term).estimate() == index.estimate(term)


def test_explain_reports_the_structure():
    index = EqualityIndex("by_city", "city").build(people())
    info = index.explain()
    assert info["index"] == "by_city" and info["kind"] == "EqualityIndex" and info["key"] == "city"
    assert info["entries"] == len(people()) and info["keys"] >= 5
    plan = index.plan(Term("city", EQ, "Paris"))
    assert plan.explain()["plan"] == "IndexLookup"


def test_validate_detects_stale_and_miscounted_entries():
    elements = people()
    index = EqualityIndex("by_city", "city").build(elements)
    index.validate(elements)
    with pytest.raises(ProtoCorruptionError):
        index.validate(elements[:-1])  # an entry no current element has
    with pytest.raises(ProtoCorruptionError):
        index.validate(elements + [elements[0]])  # count too low
    with pytest.raises(ProtoCorruptionError):
        index._with(total_count=index.total_count + 1).validate(elements)
    # An element filed under another key than its own.
    rome = DBObject(city="Rome")
    good = EqualityIndex("by_city", "city").build([rome])
    group = avl.key_get(good.groups, "Rome")
    misfiled = avl.key_put(avl.key_remove(good.groups, "Rome")[0], "Paris", group)[0]
    with pytest.raises(ProtoCorruptionError):
        good._with(groups=misfiled).validate([rome])
    # A broken tree.
    with pytest.raises(ProtoCorruptionError):
        good._with(groups=avl.KeyNode("Rome", group, avl.KeyNode("Zz", group))).validate([rome])


def test_build_equals_fold_and_ignores_the_receiver():
    elements = people() * 3
    built = EqualityIndex("by_city", "city").build(elements)
    folded = EqualityIndex("by_city", "city")
    for e in elements:
        folded = folded.with_added(e)
    terms = [Term("city", EQ, "Paris"), Term("city", NE, "Rome"), Term("city", IS_NONE), Term("city", NOT_NONE)]
    for term in terms:
        assert run(built, term) == run(folded, term)
    assert built.total_count == folded.total_count == len(elements)
    assert built.build([]).total_count == 0
    built.validate(elements)


def test_persistence_roundtrip_keeps_answers():
    elements = people()
    index = EqualityIndex("by_city", "city").build(elements)
    context = DirectContext(MemoryStorage())
    loaded = context.load(context.save(index), EqualityIndex.__type_name__)
    for term in (Term("city", EQ, "Paris"), Term("city", IS_NONE), Term("city", NE, 1)):
        assert run(loaded, term) == run(index, term)
    loaded.validate(elements)
    loaded.with_removed(elements[0]).validate(elements[1:])


def test_readers_unaffected_by_concurrent_writers():
    """8 threads query one version while 4 threads derive new versions from it."""
    base_elements = [DBObject(city=f"c{i % 7}", n=i) for i in range(200)]
    index = EqualityIndex("by_city", "city").build(base_elements)
    terms = [Term("city", EQ, f"c{i}") for i in range(8)] + [Term("city", NE, "c1"), Term("city", NOT_NONE)]
    expected = {t: scan(base_elements, t) for t in terms}
    barrier = threading.Barrier(12)
    errors: list = []
    derived: list = []

    def reader():
        barrier.wait()
        try:
            for _ in range(20):
                for t in terms:
                    assert run(index, t) == expected[t]
        except BaseException as e:  # reported below
            errors.append(e)
            raise

    def writer(seed):
        barrier.wait()
        try:
            version, model = index, list(base_elements)
            for i in range(60):
                if i % 3 == 2:
                    victim = model.pop((seed * 31 + i) % len(model))
                    version = version.with_removed(victim)
                else:
                    e = DBObject(city=f"c{(seed + i) % 9}", n=-i)
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
    assert len(derived) == 4
    for version, model in derived:
        version.validate(model)
    index.validate(base_elements)
