"""Exhaustive property: a planned query returns what a full scan returns (spec section 10, Task 6.3).

Catches: any access path (index lookup, complement, range, Union, ordered index
scan, residual filter) that drops, duplicates or invents an element for some
mix of value types (``True``, ``1``, ``1.0``, ``Decimal("1")``, ``10**16 + 1``,
``None``, a missing attribute, NaN, unorderable ``FrozenList``); an ``order_by``
whose key sequence differs from a stable sort of the scan; a ``limit``/``offset``
window that is not a window of that order; a plan that explains as non-JSON or
answers differently the second time; and a collection that answers differently
after being saved and reloaded as lazy stubs.

The oracle is independent of the planner: ``evaluate`` over ``collection.elements()``,
then Python's stable ``sort`` over the order-key slots.  Equal-key ties may be
returned in any order, so a window is checked by its key sequence plus, per key
group, exact equality when the group lies wholly inside the window and
sub-multiset inclusion when the window cuts it.

Set ``PROTODB_HYPOTHESIS_PROFILE=thorough`` for a manual, much larger run.
"""
import json
import os
import threading
from collections import Counter
from datetime import date
from decimal import Decimal

import pytest
from hypothesis import HealthCheck, event, given, settings
from hypothesis import strategies as st

from proto_db.collections.dictionaries import Dictionary, RepeatedKeysDictionary
from proto_db.collections.hash_dictionaries import HashDictionary
from proto_db.collections.lists import List
from proto_db.collections.sets import CountedSet, Set
from proto_db.core.dbobject import DBObject
from proto_db.indexing import KeySpec, element_identity
from proto_db.indexing.equality import EqualityIndex
from proto_db.indexing.range import RangeIndex
from proto_db.query.expressions import And, Not, Or, Term, evaluate
from proto_db.query.planner import plan_query
from proto_db.query.terms import (BETWEEN, BETWEEN_EXCL_BOTH, BETWEEN_EXCL_HIGH, BETWEEN_EXCL_LOW, CONTAINS, EQ, GE,
                                  GT, IN, IS_NONE, IS_TRUE, LE, LT, NE, NOT_NONE, NOT_TRUE)
from proto_db.storage.memory import MemoryStorage
from proto_db.values import MISSING, FrozenList, exact_token, is_orderable, order_key
from tests.support.persistence import DirectContext

THOROUGH = os.environ.get("PROTODB_HYPOTHESIS_PROFILE") == "thorough"
EXAMPLES_PER_KIND = 1500 if THOROUGH else 450

# -- value pools --------------------------------------------------------------------------------------------

D1, D2 = date(2026, 1, 1), date(2026, 1, 3)
FIELD_VALUES = [True, False, 0, 1, 1.0, Decimal("1"), Decimal("1.0"), 2, 3, -1, 0.5, 2.5, 10 ** 16, 10 ** 16 + 1,
                None, "a", "b", "", "ab", b"x", b"", D1, D2, (1, "t"), (0, "a"), FrozenList([1]), float("nan"), MISSING]
OPERANDS = [v for v in FIELD_VALUES if v is not MISSING and is_orderable(v)]
BETWEEN_PAIRS = [(0, 2), (1, 10 ** 17), (-1, 1.0), ("a", "b"), ("", "ab"), (False, True), (b"", b"x"), (D1, D2),
                 (0.5, 2.5), ((0, "a"), (1, "t")), (Decimal("1"), 3)]
BETWEEN_OPS = [BETWEEN, BETWEEN_EXCL_BOTH, BETWEEN_EXCL_LOW, BETWEEN_EXCL_HIGH]
PATHS = ["a", "b", "c.d", "a", "b", None, "zz"]  # "zz" is never indexed nor present
INDEX_PATHS = ["a", "b", "c.d", None]

field_value = st.sampled_from(FIELD_VALUES)


@st.composite
def records(draw):
    """A DBObject with ``a``, a sometimes missing/None ``b`` and a nested ``c.d`` (DBObject or dict), or a plain value."""
    if draw(st.integers(0, 9)) == 0:
        return draw(st.sampled_from([v for v in FIELD_VALUES if v is not MISSING]))
    attrs = {}
    a, b, d = draw(field_value), draw(field_value), draw(field_value)
    if a is not MISSING:
        attrs["a"] = a
    if b is not MISSING:
        attrs["b"] = b
    nest = draw(st.integers(0, 3))
    if d is not MISSING and nest == 0:
        attrs["c"] = DBObject(d=d)
    elif d is not MISSING and nest == 1:
        attrs["c"] = {"d": d}
    elif nest == 2:
        attrs["c"] = None
    return DBObject(**attrs)


@st.composite
def terms(draw):
    """A Term over every supported operator, on indexed, unindexed and absent paths."""
    path = draw(st.sampled_from(PATHS))
    op = draw(st.sampled_from([EQ, EQ, NE, LT, LE, GT, GE, IN, IN, IS_NONE, NOT_NONE, IS_TRUE, NOT_TRUE, CONTAINS]
                              + BETWEEN_OPS))
    if op in (IS_NONE, NOT_NONE, IS_TRUE, NOT_TRUE):
        return Term(path, op)
    if op == IN:
        return Term(path, op, draw(st.lists(st.sampled_from(OPERANDS), min_size=0, max_size=4)))
    if op in BETWEEN_OPS:
        return Term(path, op, draw(st.sampled_from(BETWEEN_PAIRS)))
    if op == CONTAINS:
        return Term(path, op, draw(st.sampled_from(["a", "b", 1, b"x", 0])))
    return Term(path, op, draw(st.sampled_from(OPERANDS)))


expressions = st.recursive(
    terms(),
    lambda inner: st.one_of(st.lists(inner, min_size=2, max_size=3).map(lambda ops: And(*ops)),
                            st.lists(inner, min_size=2, max_size=3).map(lambda ops: Or(*ops)),
                            inner.map(Not)),
    max_leaves=8)

sizes = st.sampled_from([0, 3, 12, 40, 40, 90])


@st.composite
def indexable_terms(draw):
    """A selective Term an index can answer (path a/b/c.d/element; ==, in, ranges, presence)."""
    path = draw(st.sampled_from(["a", "b", "c.d", None]))
    op = draw(st.sampled_from([EQ, IN, LT, GE, IS_NONE, NOT_NONE] + BETWEEN_OPS))
    if op in (IS_NONE, NOT_NONE):
        return Term(path, op)
    if op == IN:
        return Term(path, op, draw(st.lists(st.sampled_from(OPERANDS), min_size=1, max_size=3)))
    if op in BETWEEN_OPS:
        return Term(path, op, draw(st.sampled_from(BETWEEN_PAIRS)))
    return Term(path, op, draw(st.sampled_from(OPERANDS)))


#: Shapes that favour index use: a lone term, an Or of terms (Union), an And of terms, a Not over a term.
targeted = st.one_of(
    indexable_terms(),
    st.lists(indexable_terms(), min_size=2, max_size=3).map(lambda ops: Or(*ops)),
    st.lists(indexable_terms(), min_size=2, max_size=3).map(lambda ops: And(*ops)),
    st.lists(st.one_of(indexable_terms(), st.lists(indexable_terms(), min_size=2, max_size=2).map(lambda o: Or(*o))),
             min_size=2, max_size=3).map(lambda ops: And(*ops)),
    indexable_terms().map(Not))

index_specs = st.lists(st.tuples(st.sampled_from([EqualityIndex, RangeIndex]), st.sampled_from(INDEX_PATHS)),
                       max_size=4, unique=True).flatmap(
    lambda specs: st.just(specs) if specs else st.sampled_from([[(EqualityIndex, "a"), (RangeIndex, "b")],
                                                                [(RangeIndex, "a")], []]))
order_keys = st.lists(st.tuples(st.sampled_from(PATHS), st.booleans()), max_size=2, unique_by=lambda k: k[0])

# -- collection builders -------------------------------------------------------------------------------------


def _by_keys(cls, keyed):
    def build(items):
        coll = cls()
        for i, item in enumerate(items):
            coll = coll.set_at(i if not keyed else i % 5, item)
        return coll
    return build


BUILDERS = {
    "List": List.from_iterable,
    "Set": Set.from_iterable,
    "CountedSet": CountedSet.from_iterable,
    "Dictionary": _by_keys(Dictionary, False),
    "HashDictionary": _by_keys(HashDictionary, False),
    "RepeatedKeysDictionary": _by_keys(RepeatedKeysDictionary, True),
}

# -- the oracle ----------------------------------------------------------------------------------------------


def ident(e):
    """Exact identity of a result element (equal-but-distinct plain values stay apart)."""
    return element_identity(e), exact_token(e)


def slot(value):
    """Ascending sort slot of a key (mirrors the documented total order: orderable by order_key, rest last)."""
    return (0, order_key(value)) if is_orderable(value) else (1,)


def key_slots(element, order):
    return tuple(slot(KeySpec.for_term_path(path).extract(element)) for path, _ in order)


def scan_sorted(collection, expr, order):
    """Scan filtered by ``evaluate``, then a stable multi-pass sort (last key first)."""
    rows = [e for e in collection.elements() if expr is None or evaluate(expr, e)]
    for i in range(len(order) - 1, -1, -1):
        spec = KeySpec.for_term_path(order[i][0])
        rows.sort(key=lambda e: slot(spec.extract(e)), reverse=order[i][1])
    return rows


def assert_window(got, oracle, order, limit, offset):
    """``got`` is a valid ``order``-ed window ``[offset:offset+limit]`` of ``oracle``."""
    stop = None if limit is None else offset + limit
    window = oracle[offset:stop]
    assert len(got) == len(window)
    if not order:  # unordered window: a sub-multiset of the scan of the right size
        assert not (Counter(map(ident, got)) - Counter(map(ident, oracle)))
        return
    assert [key_slots(e, order) for e in got] == [key_slots(e, order) for e in window]
    total, inside = Counter(key_slots(e, order) for e in oracle), Counter(key_slots(e, order) for e in window)
    by_key_oracle, by_key_got = {}, {}
    for e in oracle:
        by_key_oracle.setdefault(key_slots(e, order), Counter())[ident(e)] += 1
    for e in got:
        by_key_got.setdefault(key_slots(e, order), Counter())[ident(e)] += 1
    for key, members in by_key_got.items():
        if inside[key] == total[key]:  # group wholly inside the window: exact
            assert members == by_key_oracle[key]
        else:  # the window cuts the tie group: any members of it
            assert not (members - by_key_oracle[key])


# -- access path census ----------------------------------------------------------------------------------------

_census = Counter()
_census_lock = threading.Lock()


def names(info):
    out = [info["plan"]]
    for child in info["children"]:
        out.extend(names(child))
    return out


def classify(info) -> str:
    """The access path of an explain tree: ordered index, union, single/multiple index, or scan."""
    found = names(info)
    if any("Ordered" in n for n in found):
        return "ordered"
    if "Union" in found:
        return "union"
    if any(n.startswith("Index") for n in found):
        return "index"
    return "scan"


def check_query(collection, expr, order, limit, offset):
    """Plan, execute twice and compare with the oracle; return the access path."""
    plan = plan_query(collection, expr, order_by=order or None, limit=limit, offset=offset)
    oracle = scan_sorted(collection, expr, order)
    got = list(plan.execute())
    if limit is None and not offset:
        assert Counter(map(ident, got)) == Counter(map(ident, oracle))
    if order:
        assert [key_slots(e, order) for e in got] == [key_slots(e, order) for e in
                                                      (oracle if limit is None and not offset else oracle[offset:None if limit is None else offset + limit])]
    assert_window(got, oracle, order, limit, offset)
    again = list(plan.execute())
    assert Counter(map(ident, again)) == Counter(map(ident, got))
    if order:
        assert [key_slots(e, order) for e in again] == [key_slots(e, order) for e in got]
    info = plan.explain()
    json.dumps(info)
    return classify(info)


def reload(collection):
    """The collection saved and loaded back as lazy stubs."""
    context = DirectContext(MemoryStorage())
    return context.load(context.save(collection), type(collection).__type_name__)


# -- the property ------------------------------------------------------------------------------------------------

SETTINGS = settings(max_examples=EXAMPLES_PER_KIND, deadline=None, derandomize=True, database=None,
                    suppress_health_check=list(HealthCheck))


@pytest.mark.parametrize("kind", sorted(BUILDERS))
@SETTINGS
@given(items=sizes.flatmap(lambda n: st.lists(records(), min_size=n, max_size=n)), dups=st.integers(0, 3), specs=index_specs,
       expr=st.one_of(st.none(), expressions, targeted, targeted), order=order_keys,
       limit=st.one_of(st.none(), st.integers(0, 6)), offset=st.integers(0, 5))
def test_planned_query_equals_scan(kind, items, dups, specs, expr, order, limit, offset):
    if kind in ("Set", "CountedSet", "RepeatedKeysDictionary"):  # members must be orderable
        items = [e for e in items if is_orderable(e)]
    items = items + items[:dups]  # the same atoms twice (List duplicates; folded by sets)
    collection = BUILDERS[kind](items)
    for index_cls, path in specs:
        name = f"{index_cls.__name__}_{path}"
        collection = collection.add_index(index_cls(name, KeySpec.element() if path is None else path))
    path_used = check_query(collection, expr, order, limit, offset)
    with _census_lock:
        _census[path_used] += 1
    event(f"path={path_used}")
    persisted = reload(collection)
    assert check_query(persisted, expr, order, limit, offset) == path_used
    assert sorted(persisted.index_names()) == sorted(collection.index_names())


def test_every_access_path_was_exercised():
    if not _census:
        pytest.skip("the property did not run in this session")
    missing = {"scan", "index", "union", "ordered"} - set(_census)
    assert not missing, f"access paths never chosen: {sorted(missing)}; census {dict(_census)}"
    print(f"access path census: {dict(_census)}")
