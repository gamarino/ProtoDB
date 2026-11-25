"""Every read path of the upper layers records through the model (spec R21, R22, R37; review N2;
section 12 item 5).

Explicit interleavings only: open A, read through the path, B commits a change of what A read,
A writes another root and commits: ``ProtoConflictError`` naming the expected unit.  Where the path
records finer than the whole collection, B changing something outside what it read lets A commit.

Catches: a query, a paginator (in-process, LINQ or portable) or a semantic query whose read is not
recorded or is recorded on the wrong unit; a result the query cache serves that records less than
(or other than) the execution that produced it; a cost estimate that records a read; a read through
a snapshot or of the semantic catalog that enters the read set (R21, R37).

The audit of every site is ``docs/superpowers/specs/2026-11-24-read-path-audit.md``.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from proto_db.errors import ProtoConflictError
from proto_db.indexing.equality import EqualityIndex
from proto_db.indexing.keyspec import KeySpec
from proto_db.preconditions import Unit, UnitKind
from proto_db.errors import ProtoUnexpectedError
from proto_db.query import plans as plan_nodes
from proto_db.query.cache import SERVED_TYPES, QueryCache, cache_key, record_served_reads
from proto_db.query.linq import from_collection
from proto_db.query.pager import Pager
from proto_db.query.planner import plan_query
from proto_db.query.plans import (CollectionScanPlan, Count, Distinct, Filter, GroupBy, Intersect, Join, Limit,
                                  Project, PythonFilter, QueryPlan, Sort, Union, Unnest, ValuesScan)
from proto_db.query.terms import EQ, Term
from proto_db.semantic import semantic
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from tests.unit.semantic_modules import MAPPING

pytestmark = [pytest.mark.timeout(120)]


@pytest.fixture
def space():
    s = ObjectSpace(MemoryStorage())
    yield s
    s.close()


@pytest.fixture
def db(space):
    return space.new_database("db")


def whole(root):
    return Unit(UnitKind.WHOLE, root)


def group(root, index, key):
    return Unit(UnitKind.INDEX_GROUP, root, key=key, index=index)


def seed(db, name="d", **items):
    """Root ``name``: a Dictionary of ``items`` with the EqualityIndex ``e`` over its values."""
    def build(tr):
        d = tr.new_dictionary()
        for k, v in items.items():
            d = d.set_at(k, v)
        tr.set_root_object(name, d.add_index(EqualityIndex("e", KeySpec.element())))
    db.run(build)


def write(db, name="d", **items):
    """Commit ``items`` into root ``name``."""
    def change(tr):
        d = tr.get_root_object(name)
        for k, v in items.items():
            d = d.set_at(k, v)
        tr.set_root_object(name, d)
    db.run(change)


def report(tr):
    """A write of another root, so the reading transaction validates at commit."""
    tr.set_root_object("report", tr.new_object(done=True))


def conflict_unit(tr):
    report(tr)
    with pytest.raises(ProtoConflictError) as e:
        tr.commit()
    return e.value.unit


def commits(tr):
    report(tr)
    tr.commit()


def reads(tr) -> tuple:
    """What ``tr`` recorded: its units (expected values left out) and its ranges."""
    log = tr._read_log
    return frozenset(log.preconditions()), frozenset(log.ranges())


ONE = Term(None, EQ, 1)


# -- queries ------------------------------------------------------------------------------------

def test_query_read_then_concurrent_change_conflicts(db):
    seed(db, a=1, b=2, c=5)
    # A lookup answered by the index group of 1: a change in that group conflicts on the group.
    a = db.new_transaction()
    assert list(plan_query(a.get_root_object("d"), ONE).execute()) == [1]
    write(db, x=1)
    assert conflict_unit(a) == group("d", "e", 1)
    # A change outside the group read commits (the read recorded the group, not the collection).
    a = db.new_transaction()
    assert list(plan_query(a.get_root_object("d"), ONE).execute()) == [1, 1]
    write(db, c=6)
    commits(a)
    # A query without an index path (a scan) reads the whole collection.
    a = db.new_transaction()
    assert list(plan_query(a.get_root_object("d"), Term(None, "!=", 1)).execute())
    write(db, z=9)
    assert conflict_unit(a) == whole("d")


def _planned(d):
    return from_collection(d).where(ONE)


def _hand_built(d):
    return Filter(CollectionScanPlan(d), ONE)


def _window(d):
    return from_collection(d).skip(1).take(1)


def _counted_window(d):
    return Count(plan_query(d, limit=1, offset=1))


@pytest.mark.parametrize("build, changed, unit", [
    (_planned, {"x": 1}, group("d", "e", 1)),
    (_hand_built, {"z": 9}, whole("d")),
    (_window, {"a": 3}, None),          # a range from the first key to the window's last one
    (_counted_window, {"z": 9}, whole("d")),
], ids=["planned", "hand_built", "window", "counted_window"])
def test_cached_query_result_registers_same_reads(space, db, build, changed, unit):
    seed(db, a=1, b=2, c=1)
    cache = QueryCache(1 << 20, space._space_load)
    # P answers the query (a miss): what it records is what the query reads.
    p = db.new_transaction()
    first = cache.result(build(p.get_root_object("d")))
    assert first.key is not None and cache.stats()["misses"] == 1
    expected = reads(p)
    p.abort()
    # A gets the same result from the cache (a hit) and records the same reads.
    a = db.new_transaction()
    second = cache.result(build(a.get_root_object("d")))
    assert second is first and cache.stats()["hits"] == 1
    assert reads(a) == expected
    write(db, **changed)
    got = conflict_unit(a)
    if unit is None:
        assert got.kind is UnitKind.RANGE and got.root == "d"
    else:
        assert got == unit


def _joined(d):
    return Join(CollectionScanPlan(d), plan_query(d, ONE), None, None)


def _union(d):
    return Union(plan_query(d, ONE), plan_query(d, Term(None, EQ, 2)))


def _nested_window(d):
    return Limit(plan_query(d, limit=3), 1)        # executes rows 0..0 of a planned window of rows 0..2


@pytest.mark.parametrize("build, changed, unit, covers", [
    (_joined, {"z": 9}, whole("d"), False),
    (_union, {"y": 2}, group("d", "e", 2), False),
    (_nested_window, {"b": 3}, None, True),        # inside the planned window, after the executed row
], ids=["join", "union", "nested_window"])
def test_cached_composite_plans_register_reads(space, db, build, changed, unit, covers):
    """A Join, a Union and a Limit over a planned window: a hit records the miss's reads; for the nested
    window those cover the narrower range the miss's execution read (the outer Limit narrows it)."""
    seed(db, a=1, b=2, c=1, e=7)
    cache = QueryCache(1 << 20, space._space_load)
    p = db.new_transaction()
    cache.result(build(p.get_root_object("d")))
    missed = reads(p)
    p.abort()
    a = db.new_transaction()
    cache.result(build(a.get_root_object("d")))
    assert cache.stats()["hits"] == 1
    hit = reads(a)
    assert hit[0] == missed[0]
    if covers:
        # The miss recorded its execution's narrower range too; the hit records the inner window's range,
        # which contains it (both start at the first key; the hit's ends at or after the miss's).
        assert hit[1] < missed[1] and len(hit[1]) == 1
        [wide] = hit[1]
        assert all(r.low is None and r.high <= wide.high for r in missed[1])
    else:
        assert hit == missed
    write(db, **changed)
    got = conflict_unit(a)
    if unit is None:
        assert got.kind is UnitKind.RANGE and got.root == "d"
    else:
        assert got == unit


def _plan_classes() -> set:
    """Every concrete QueryPlan class of proto_db.query.plans."""
    found, stack = set(), [QueryPlan]
    while stack:
        cls = stack.pop()
        for sub in cls.__subclasses__():
            stack.append(sub)
            if sub.__module__ == plan_nodes.__name__ and not getattr(sub, "__abstractmethods__", None) \
                    and not sub.__name__.startswith("_"):
                found.add(sub)
    return found


def test_record_served_reads_handles_every_cacheable_node(db):
    """Parity: every plan node type the cache key accepts is handled by name by record_served_reads, and
    the representative instances below cover every plan class (a new class fails here until added)."""
    seed(db, a=1, b=2, c=1)
    a = db.new_transaction()
    d = a.get_root_object("d")
    scan, lookup = CollectionScanPlan(d), plan_query(d, ONE)
    samples = {
        CollectionScanPlan: scan, ValuesScan: ValuesScan([1]), plan_nodes.IndexPlan: lookup,
        Filter: Filter(scan, ONE), PythonFilter: PythonFilter(scan, bool, "bool"),
        Union: Union(scan, lookup), Intersect: Intersect(scan, lookup), Project: Project(scan, {"v": "real"}),
        Sort: Sort(scan, [None]), Limit: Limit(scan, 1), Count: Count(scan), Distinct: Distinct(scan),
        GroupBy: GroupBy(scan, None, {"n": ("count", None)}), Join: Join(scan, lookup, None, None),
        Unnest: Unnest(scan, "items"),
    }
    vector = plan_nodes.VectorSearch
    missing = _plan_classes() - set(samples) - {vector}
    assert not missing, f"add a sample of {missing} and handle it in record_served_reads"
    assert vector in SERVED_TYPES       # its cache key needs a deterministic vector index; handled as an index plan
    for cls, sample in samples.items():
        key, reason = cache_key(sample)
        if key is not None:
            assert cls in SERVED_TYPES, f"{cls.__name__} is cacheable but not handled by name"
    a.abort()


def test_unknown_plan_node_records_whole_or_raises(db):
    """Fail-closed (P-A): a node type record_served_reads does not know records the whole collection of every
    scan reachable through its attributes, and raises when none is reachable; it never records nothing."""
    seed(db, a=1, b=2, c=1)

    class Wrapper(QueryPlan):
        def __init__(self, inner):
            self.parts = (inner,)
            self._freeze()

        def execute(self):
            return self.parts[0].execute()

        def explain(self):
            return {}

        def estimate(self):
            return self.parts[0].estimate()

    a = db.new_transaction()
    d = a.get_root_object("d")
    record_served_reads(Wrapper(Filter(CollectionScanPlan(d), ONE)))
    assert whole("d") in a._read_log
    with pytest.raises(ProtoUnexpectedError):
        record_served_reads(Wrapper(ValuesScan([1])))
    a.abort()


@pytest.mark.parametrize("build", [_planned, _window], ids=["planned", "window"])
def test_cached_query_hit_outside_the_read_commits(space, db, build):
    seed(db, a=1, b=2, c=5)
    cache = QueryCache(1 << 20, space._space_load)
    p = db.new_transaction()
    cache.result(build(p.get_root_object("d")))
    p.abort()
    a = db.new_transaction()
    cache.result(build(a.get_root_object("d")))
    assert cache.stats()["hits"] == 1 and whole("d") not in a._read_log
    write(db, c=6)          # outside the group of 1, and after the window's last key (b)
    commits(a)


def test_plan_estimate_records_nothing(db):
    seed(db, a=1, b=2, c=3)
    a = db.new_transaction()
    plan = plan_query(a.get_root_object("d"), limit=1)
    before = reads(a)
    assert whole("d") not in a._read_log
    plan.estimate()
    Limit(CollectionScanPlan(a.get_root_object("d")), 1).estimate()
    assert reads(a) == before
    # explain() shows the scan's row count: that is a read of the whole collection.
    plan.explain()
    assert whole("d") in a._read_log
    a.abort()


# -- paginators ---------------------------------------------------------------------------------

def test_pager_read_then_concurrent_change_conflicts(space, db):
    seed(db, a=1, b=2, c=1)
    # By rank over a scan: the pager knows the row count, the whole collection.
    a = db.new_transaction()
    pager = Pager(a.get_root_object("d").as_query_plan(), 2)
    assert pager.by_rank and pager.page(0) == (1, 2)
    write(db, z=9)
    assert conflict_unit(a) == whole("d")
    # Materialized through the cache: a pager whose result is a hit records what the miss did.
    cache = QueryCache(1 << 20, space._space_load)
    p = db.new_transaction()
    assert Pager(_hand_built(p.get_root_object("d")), 1, cache=cache).page(0) == (1,)
    p.abort()
    a = db.new_transaction()
    pager = Pager(_hand_built(a.get_root_object("d")), 1, cache=cache)
    assert not pager.by_rank and pager.page(1) == (1,) and cache.stats()["hits"] == 1
    write(db, z=9)
    assert conflict_unit(a) == whole("d")


def test_transaction_pager_records_reads(db):
    seed(db, a=1, b=2, c=1)
    a = db.new_transaction()
    pager = a.pager("d", where=ONE, page_size=1)
    assert whole("d") in a._read_log
    assert pager.page(0) == (1,)
    write(db, z=9)
    assert conflict_unit(a) == whole("d")


def test_linq_pager_records_reads(db):
    seed(db, a=1, b=2, c=5)
    a = db.new_transaction()
    pager = from_collection(a.get_root_object("d")).where(ONE).pager(1)
    assert pager.page(0) == (1,)
    assert group("d", "e", 1) in a._read_log and whole("d") not in a._read_log
    write(db, x=1)
    assert conflict_unit(a) == group("d", "e", 1)
    # A change outside the group the pager read commits.
    a = db.new_transaction()
    assert from_collection(a.get_root_object("d")).where(ONE).pager(1).page(0) == (1,)
    write(db, c=6)
    commits(a)


# -- the semantic layer -------------------------------------------------------------------------

ORDER = "acme.sales.Order"


@pytest.fixture
def sdb():
    space = ObjectSpace(MemoryStorage(), modules_mapping=MAPPING)
    try:
        database = space.new_database("db")
        database.run(lambda tr: tr.install_module("acme_sales"))
        yield database
    finally:
        space.close()


def _sales(sdb):
    """A customer with two orders (numbers 1 and 6) and a product: ``(customer, product, first, sixth)``
    handles."""
    def build(tr):
        s = semantic(tr)
        company = s.create("acme.core.Company", name="ACME")
        customer = s.create("acme.sales.Customer", name="Bob", credit_limit=Decimal("1000"), company=company)
        product = s.create("acme.catalog.Product", name="widget", list_price=Decimal("10"), company=company)
        orders = [s.create(ORDER, number=n, date=date(2026, n, 10), company=company, customer=customer)
                  for n in (1, 6)]
        return customer, product, orders[0], orders[1]
    return sdb.run(build)


def _update(sdb, handle, **changes):
    sdb.run(lambda tr: semantic(tr).update(handle, **changes))


SIXTH = ["==", ["attr", "number"], ["const", 6]]


def test_semantic_query_read_then_concurrent_change_conflicts(sdb):
    customer, product, first, sixth = _sales(sdb)
    query = {"from": ["via", ["cell", customer.name], "orders"], "where": SIXTH}
    # The query reads the customer's cell and the cell of every order it follows.
    a = sdb.new_transaction()
    assert [v.entity.name for v in semantic(a).query(query)["rows"]] == [sixth.name]
    assert Unit(UnitKind.CELL, key=sixth.name) in a._read_log
    _update(sdb, sixth, date=date(2026, 6, 11))
    assert conflict_unit(a) == Unit(UnitKind.CELL, key=sixth.name)
    # A cell the query did not reach (the product) changes: A commits.
    a = sdb.new_transaction()
    assert [v.entity.name for v in semantic(a).query(query)["rows"]] == [sixth.name]
    _update(sdb, product, list_price=Decimal("12"))
    commits(a)


def test_semantic_root_query_records_the_whole_root(sdb):
    def build(tr):
        company = semantic(tr).create("acme.core.Company", name="ACME")
        tr.set_root_object("companies", tr.new_list().append_last(company))
    sdb.run(build)
    a = sdb.new_transaction()
    assert len(semantic(a).query({"from": ["root", "companies"]})["rows"]) == 1
    assert whole("companies") in a._read_log
    sdb.run(lambda tr: tr.set_root_object(
        "companies", tr.get_root_object("companies").append_last(semantic(tr).create("acme.core.Company",
                                                                                     name="Other"))))
    assert conflict_unit(a) == whole("companies")


def test_semantic_describe_from_snapshot_records_nothing(sdb):
    customer, product, first, sixth = _sales(sdb)
    a = sdb.new_transaction()
    snapshot = sdb.snapshot_at(a.snapshot_ref)
    s = semantic(snapshot)
    assert s.describe(ORDER)
    assert s.query({"from": ["via", ["cell", customer.name], "orders"], "where": SIXTH})["rows"]
    # The catalog of A's own session is a reserved root, read outside the model (R37).
    assert semantic(a).describe(ORDER)
    assert reads(a) == (frozenset(), frozenset())
    _update(sdb, sixth, date=date(2026, 6, 12))
    commits(a)


# -- N2: composing the semantic catalog runs no application code (R37, P-C) ----------------------------

def test_semantic_catalog_merge_runs_no_user_code(sdb, monkeypatch):
    """A commit that installs a module merges the semantic catalog under the space root, composing the
    module's contributions there.  Its descriptor, invariant and intent spec subclasses override the methods
    the composition reads: none is called, under the root or elsewhere in the composition, and the stored
    catalog holds the engine's reading of the declarations, not the overrides'."""
    from proto_db.semantic import store
    from proto_db.semantic.descriptors import Field, Invariant
    from proto_db.transactions import Transaction
    from tests.unit.semantic_modules import acme_overrides

    composed = []
    contributions = store.module_contributions

    def spy(registry, name):
        composed.append((name, Transaction._holding_root_lock()))
        return contributions(registry, name)
    monkeypatch.setattr(store, "module_contributions", spy)
    acme_overrides.CALLS.clear()
    sdb.run(lambda tr: tr.install_module("acme_overrides"))
    assert ("acme_overrides", True) in composed            # the merge composed the module under the root
    assert [call for call in acme_overrides.CALLS if call[1]] == []
    assert acme_overrides.CALLS == []
    with sdb.new_transaction() as tr:
        model, _ = store.read_stored(tr._objects)
    [extension] = model["modules"]["acme_overrides"]["extensions"]
    elements = {e["name"]: e for e in extension["elements"]}
    assert elements["note"] == Field(str, meaning="A free note on the order").element("note")
    engine_invariant = Invariant("number >= 0", name="number_not_negative", meaning="An order number is not negative")
    assert elements["number_not_negative"] == engine_invariant.element("entity")
    assert elements["review"]["meaning"] == "Mark the order as reviewed"


class _NotedDict(dict):
    copied = []

    def __deepcopy__(self, memo):
        _NotedDict.copied.append(True)
        return _NotedDict(self)


class _NotedStr(str):
    def __deepcopy__(self, memo):
        _NotedDict.copied.append(True)
        return _NotedStr(self)


@pytest.mark.parametrize("dunder, value", [("__label__", _NotedDict(und="Order")), ("__meaning__", _NotedStr("x"))],
                         ids=["dict", "str"])
def test_extension_item_never_deep_copies_a_module_value(dunder, value):
    """A semantic dunder holding an object of a module's class (a ``__deepcopy__`` of its own) is refused,
    never copied through it: the composition copies plain data only."""
    from types import SimpleNamespace

    from proto_db.errors import ProtoValidationError
    from proto_db.semantic import compose
    _NotedDict.copied.clear()
    extension = SimpleNamespace(semantic={dunder: value}, definitions={}, registry_name="acme.sales.Order",
                                module="acme_x")
    with pytest.raises(ProtoValidationError):
        compose._extension_item(None, extension, frozenset())
    assert _NotedDict.copied == []
