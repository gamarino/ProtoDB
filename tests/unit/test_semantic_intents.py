"""Intent transactions behind a flag, on one node (spec section 9; rulings R260, R261, R269 to R271, R278;
Task 8; preflight F21, F22, F25, F26, F37).

Catches: a flag that changes the bytes or the behaviour of code that does not set it; two orders of one
customer that conflict on the customer cell instead of being replayed one after the other; a fine read of
one attribute that conflicts with a change of another attribute of the same cell, or that misses a change of
the attribute it read; a condition read (a query, a navigation) that misses a change of the collection it
scanned; a replay that draws a UUID, or creates cells named otherwise than the recording told the caller; an
``__intentLog`` that misses an operation, keeps an older commit's records, or names a version other than the
one it is in; a redefined intent whose ``_super()`` chain runs a module's implementation twice; an intent
whose returned version does not update its target; ``intent_context()`` usable outside an intent; a
``requires`` decided at record time (a conflict, or a stale yes) instead of at replay; the flag accepted on a
clustered space; a raw write that silently overwrites, or is overwritten by, the cells the log touches; a
group-commit leader that deadlocks, or replays without its intent context, when it replays another thread's
intents.  No sleep decides anything (R82): concurrent commits are ordered by the test, and the group-commit
round by its queue.

Every entity the tests create is complete (every ``one`` role set, preflight F28).
"""
from __future__ import annotations

import threading
from datetime import date
from decimal import Decimal
from unittest import mock

import pytest

from proto_db.catalog.atoms import INTENT_LOG_ROOT, intent_log_root, intent_records
from proto_db.errors import (ProtoConflictError, ProtoNotSupportedError, ProtoUserError,
                             ProtoValidationError)
from proto_db.semantic import IntentContext, Version, intent_context, semantic
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import Database, ObjectSpace, Transaction
from tests.unit.semantic_modules import MAPPING
from tests.unit.semantic_modules import acme_audit
from tests.unit.test_semantic_classes import _PLAIN_DIGEST_ON_THE_BASE, _plain_workload

pytestmark = [pytest.mark.timeout(120)]

ORDER, LINE, PRODUCT = "acme.sales.Order", "acme.sales.OrderLine", "acme.catalog.Product"


def _database(*modules):
    space = ObjectSpace(MemoryStorage(), modules_mapping=MAPPING)
    database = space.new_database("db")
    database.run(lambda tr: [tr.install_module(name) for name in ("acme_sales",) + modules])
    return space, database


@pytest.fixture
def db():
    space, database = _database()
    try:
        yield database
    finally:
        space.close()


@pytest.fixture
def audited():
    space, database = _database("acme_audit")
    acme_audit.RUNS.clear()
    try:
        yield database
    finally:
        space.close()


def _world(db):
    """A company with one customer and one product, committed in direct mode: their handles."""
    def build(tr):
        s = semantic(tr)
        company = s.create("acme.core.Company", name="ACME")
        customer = s.create("acme.sales.Customer", name="Bob", credit_limit=Decimal("1000"), company=company)
        product = s.create(PRODUCT, name="widget", list_price=Decimal("10"), company=company)
        return company, customer, product
    return db.run(build)


def _order(s, company, customer, number, day=date(2026, 6, 10)):
    return s.create(ORDER, number=number, date=day, company=company, customer=customer)


def _read(db, fn):
    tr = db.new_transaction()
    try:
        return fn(semantic(tr), tr)
    finally:
        tr.abort()


def _log(tr) -> tuple:
    """The IntentRecords of the version ``tr`` reads, as ``(intent, target, arguments, transaction)``."""
    log = intent_log_root(tr._objects)
    return None if log is None else tuple((r.intent, r.target, dict(r.arguments), r.transaction)
                                          for r in intent_records(log))


# -- without the flag ------------------------------------------------------------------------------------

def test_without_the_flag_nothing_changes(tmp_path, db):
    # F37: the oracle is the literal digest of a fixed non-semantic workload computed on the base commit
    # b99285b, by the helper the class tests pin it with; the same workload with intents=False stated
    # explicitly writes the same bytes.
    assert _plain_workload(tmp_path / "default") == _PLAIN_DIGEST_ON_THE_BASE
    original = Database.run

    def run(self, fn, *args, **kwargs):
        return original(self, fn, *args, intents=False, **kwargs)
    with mock.patch.object(Database, "run", run):
        assert _plain_workload(tmp_path / "explicit") == _PLAIN_DIGEST_ON_THE_BASE
    # Direct mode keeps today's rules: a raw write of a cell a session operation wrote is allowed, and a
    # database that never used intents has no __intentLog.
    company, customer, product = _world(db)
    tr = db.new_transaction()
    assert tr.intents is False
    order = _order(semantic(tr), company, customer, 1)
    tr.set_mutable(order.name, tr.get_mutable(order.name).with_(number=2))
    tr.commit()
    assert _read(db, lambda s, tr: (s.get(order).number, _log(tr))) == (2, None)
    for wrong in (1, None, "yes"):
        with pytest.raises(ProtoValidationError, match="intents"):
            db.new_transaction(intents=wrong)
        with pytest.raises(ProtoValidationError, match="intents"):
            db.run(lambda tr: None, intents=wrong)


# -- what commits together ------------------------------------------------------------------------------

def test_two_orders_for_one_customer_both_commit(db):
    company, customer, product = _world(db)
    first, second = db.new_transaction(intents=True), db.new_transaction(intents=True)     # one snapshot
    orders = []
    for tr, number, quantity in ((first, 1, "2"), (second, 2, "3")):
        s = semantic(tr)
        order = _order(s, company, customer, number)
        s.get(order).add_line(product, Decimal(quantity), Decimal("10"))       # the brief's signature
        assert s.get(order).total == Decimal(quantity) * 10                     # the record view shows it
        orders.append(order)
    first.commit()
    second.commit()                                                              # no conflict: replayed

    def check(s, tr):
        bob = s.get(customer)
        assert {v.entity for v in bob.orders} == set(orders)
        assert bob.committed == Decimal("50")
        assert [s.get(o).total for o in orders] == [Decimal("20"), Decimal("30")]
        assert {v.entity for v in s.get(company).orders} == set(orders)
        assert len(s.get(product).order_lines) == 2
    _read(db, check)


def test_reading_price_does_not_conflict_with_a_new_sale(db):
    company, customer, product = _world(db)
    tr = db.new_transaction(intents=True)
    s = semantic(tr)
    price = s.get(product).list_price                       # a fine read of (product, list_price)
    order = _order(s, company, customer, 1)
    s.get(order).add_line(product, Decimal("1"), price)

    def sale(other):                                        # a concurrent sale: product.order_lines changes
        t = semantic(other)
        sold = _order(t, company, customer, 2)
        t.relate(LINE, order=sold, product=product, quantity=Decimal("4"), price=Decimal("10"))
    db.run(sale)
    tr.commit()
    _read(db, lambda s, tr: (len(s.get(product).order_lines) == 2 and s.get(customer).committed == Decimal("50"))
          or pytest.fail("both sales are kept"))


def test_reading_a_changed_attribute_conflicts(db):
    company, customer, product = _world(db)
    tr = db.new_transaction(intents=True)
    s = semantic(tr)
    assert s.get(product).list_price == Decimal("10")
    _order(s, company, customer, 1)
    db.run(lambda other: semantic(other).update(product, list_price=Decimal("12")))
    with pytest.raises(ProtoConflictError, match=r"'list_price'.*acme\.catalog\.Product#"):
        tr.commit()
    assert _read(db, lambda s, tr: len(s.get(customer).orders)) == 0          # nothing of it is published
    # A field read at its default (stored nowhere, ABSENT) conflicts when a concurrent commit stores it.
    order = db.run(lambda other: _order(semantic(other), company, customer, 5))
    tr = db.new_transaction(intents=True)
    assert semantic(tr).get(order).state == "open"
    semantic(tr).update(order, date=date(2026, 6, 12))
    db.run(lambda other: semantic(other).update(order, state="open"))
    with pytest.raises(ProtoConflictError, match="'state'"):
        tr.commit()


def test_condition_read_conflicts_when_the_collection_changed(db):
    company, customer, product = _world(db)
    query = {"from": ["via", ["cell", customer.name], "orders"], "aggregate": ["count"]}

    def decide(tr):
        s = semantic(tr)
        if s.query(query)["rows"] == [{"count": 0}]:       # the result decides what the transaction writes
            _order(s, company, customer, 1)

    tr = db.new_transaction(intents=True)
    decide(tr)
    db.run(lambda other: _order(semantic(other), company, customer, 7))
    with pytest.raises(ProtoConflictError):
        tr.commit()
    # A navigation of a many end is a condition read of that end too.
    tr = db.new_transaction(intents=True)
    s = semantic(tr)
    if len(s.get(customer).orders) == 1:
        _order(s, company, customer, 2)
    db.run(lambda other: _order(semantic(other), company, customer, 8))
    with pytest.raises(ProtoConflictError, match="'orders'"):
        tr.commit()
    # Without a concurrent change the same decision commits.
    tr = db.new_transaction(intents=True)
    s = semantic(tr)
    if len(s.get(customer).orders) == 2:
        _order(s, company, customer, 3)
    tr.commit()
    assert _read(db, lambda s, tr: len(s.get(customer).orders)) == 3


# -- the log and its replay ---------------------------------------------------------------------------

def test_replay_is_deterministic_and_draws_no_uuid(audited, monkeypatch):
    db = audited
    company, customer, product = _world(db)
    tr = db.new_transaction(intents=True)
    s = semantic(tr)
    order = _order(s, company, customer, 1)
    follow = s.get(order).follow_up(2)                      # a creation inside an intent
    gadget = s.create(PRODUCT, name="gadget", list_price=Decimal("3"), company=company)
    db.run(lambda other: semantic(other).update(customer, credit_limit=Decimal("900")))   # replayed elsewhere

    # R260: an entity's identity is its cell name, drawn at record time.  The patch covers every place the
    # semantic layer draws one; a record's oid (storage identity, core/atom.py) is drawn by every new
    # record, and the replay builds new records (a role binds the version current at replay), so the
    # patch leaves the uuid module itself alone.
    class _NoDraw:
        @staticmethod
        def uuid4():
            raise AssertionError("the replay drew an entity's UUID")
    from proto_db.semantic import intents as intents_module, session as session_module
    monkeypatch.setattr(intents_module, "uuid", _NoDraw)
    monkeypatch.setattr(session_module, "uuid", _NoDraw)
    tr.commit()
    monkeypatch.undo()
    with pytest.raises(AssertionError, match="entity's UUID"):            # the patch covers the draw
        with mock.patch.object(intents_module, "uuid", _NoDraw):
            tr2 = db.new_transaction(intents=True)
            try:
                semantic(tr2).create("acme.core.Company", name="X")
            finally:
                tr2.abort()

    def check(s, tr):
        assert s.get(order).number == 1 and s.get(follow).number == 2 and s.get(gadget).name == "gadget"
        assert {v.entity for v in s.get(customer).orders} == {order, follow}
        assert tr.get_mutable(follow.name).customer == customer
        records = _log(tr)
        assert [r[0] for r in records] == [(ORDER, "create"), (ORDER, "follow_up"), (PRODUCT, "create")]
        assert records[1][2] == {"number": 2, "#created": [follow.name]}       # the drawn cell travels
        assert records[2][1] == gadget.name
    _read(db, check)


def test_a_replay_that_creates_otherwise_than_recorded_conflicts(audited):
    # The replay never draws (R260): when it creates more entities than were recorded, or of another type,
    # the commit conflicts instead of drawing a name, or of publishing a cell the caller was never told of.
    # The divergence is made by editing the recording, the one deterministic way to reach it.
    db = audited
    company, customer, product = _world(db)
    order = db.run(lambda other: _order(semantic(other), company, customer, 1))
    for tamper, message in ((lambda created: created.clear(), "creates more"),
                            (lambda created: created.__setitem__(0, f"{PRODUCT}#{'0' * 32}"), "recorded")):
        tr = db.new_transaction(intents=True)
        follow = semantic(tr).get(order).follow_up(2)
        assert follow.name.startswith(ORDER + "#")
        tamper(tr._intent_log.entries[-1].created)
        with pytest.raises(ProtoConflictError, match=message):
            tr.commit()
    # Unchanged recordings replay their own cells, also two follow-ups of one order from one snapshot.
    first, second = db.new_transaction(intents=True), db.new_transaction(intents=True)
    handles = [semantic(first).get(order).follow_up(2), semantic(second).get(order).follow_up(3)]
    first.commit()
    second.commit()
    assert _read(db, lambda s, tr: {v.entity: v.number for v in s.get(customer).orders}) == {
        order: 1, handles[0]: 2, handles[1]: 3}


def test_intent_log_records_each_operation(db):
    company, customer, product = _world(db)
    other_company = db.run(lambda tr: semantic(tr).create("acme.core.Company", name="Other"))
    tr = db.new_transaction(intents=True)
    s = semantic(tr)
    order = _order(s, company, customer, 1)
    s.get(order).add_line(product, Decimal("2"), Decimal("10.50"))
    s.update(order, date=date(2026, 6, 11))
    s.relate(LINE, order=order, product=product, quantity=Decimal("1"), price=Decimal("9"))
    s.unlink(customer, "company", company)
    s.link(customer, "company", other_company)
    s.invoke(order, "confirm")
    tr.commit()

    def check(s, tr):
        version = tr._db_root.version
        assert _log(tr) == (
            ((ORDER, "create"), order.name, {"number": 1, "date": date(2026, 6, 10), "company": company.name,
                                             "customer": customer.name}, version),
            ((ORDER, "add_line"), order.name, {"product": product.name, "quantity": Decimal("2"),
                                               "price": Decimal("10.50")}, version),
            ((ORDER, "update"), order.name, {"date": date(2026, 6, 11)}, version),
            ((LINE, "relate"), order.name, {"order": order.name, "product": product.name,
                                            "quantity": Decimal("1"), "price": Decimal("9")}, version),
            (("acme.sales.Customer", "unlink"), customer.name, {"role": "company", "target": company.name},
             version),
            (("acme.sales.Customer", "link"), customer.name, {"role": "company", "target": other_company.name},
             version),
            ((ORDER, "confirm"), order.name, {}, version))
        assert s.get(order).state == "confirmed" and s.get(order).total == Decimal("30.00")
        assert s.get(customer).company.entity == other_company
    _read(db, check)
    # The next commit's log holds its own records only; a commit without the flag writes none, and the
    # version before keeps its log (history is the chain of versions).
    db.run(lambda tr: semantic(tr).update(order, date=date(2026, 6, 12)), intents=True)
    assert _read(db, lambda s, tr: [r[0] for r in _log(tr)]) == [(ORDER, "update")]
    db.run(lambda tr: tr.set_root_object("plain", 1))
    assert _read(db, lambda s, tr: (_log(tr), tr._objects.has(INTENT_LOG_ROOT))) == (None, False)
    assert [r.intent for r in intent_records(intent_log_root(db.history()[1].root.objects))] == [(ORDER, "update")]


def test_add_line_super_chain_runs_once_per_module(audited):
    db = audited
    company, customer, product = _world(db)
    tr = db.new_transaction(intents=True)
    s = semantic(tr)
    order = _order(s, company, customer, 1)
    s.get(order).add_line(product, Decimal("2"), Decimal("10"))
    assert acme_audit.RUNS == [threading.current_thread().name]       # recorded: each module once
    assert len(s.get(order).lines) == 1
    tr.commit()
    assert len(acme_audit.RUNS) == 2                                  # replayed: each module once more
    assert _read(db, lambda s, tr: (len(s.get(order).lines), s.get(order).total)) == (1, Decimal("20"))


def test_confirm_returning_with_updates_the_target(db):
    company, customer, product = _world(db)
    order = db.run(lambda tr: _order(semantic(tr), company, customer, 1))
    tr = db.new_transaction(intents=True)
    s = semantic(tr)
    result = s.get(order).confirm()
    assert isinstance(result, Version) and result.entity == order and result.state == "confirmed"
    assert s.get(order).state == "confirmed"
    tr.commit()
    assert _read(db, lambda s, tr: s.get(order).state) == "confirmed"
    # Direct mode applies an intent at once, under today's rules.
    order2 = db.run(lambda tr: _order(semantic(tr), company, customer, 2))
    db.run(lambda tr: semantic(tr).get(order2).confirm())
    assert _read(db, lambda s, tr: (s.get(order2).state, _log(tr))) == ("confirmed", None)


def test_intent_context_outside_an_intent_raises(db):
    company, customer, product = _world(db)
    with pytest.raises(ProtoUserError, match="only while an intent runs"):
        intent_context()
    tr = db.new_transaction(intents=True)
    s = semantic(tr)
    order = _order(s, company, customer, 1)
    s.get(order).confirm()
    with pytest.raises(ProtoUserError, match="only while an intent runs"):
        intent_context()                                    # the context does not outlive the call
    with tr:                                                # the reader block resolves the record
        with pytest.raises(ProtoUserError, match="SemanticSession"):
            s.get(order).record.confirm()                   # the stored record: a version no session gave
        tr.abort()
    assert isinstance(IntentContext("t"), IntentContext)


def test_session_writes_inside_an_intent_go_through_the_context(audited):
    db = audited
    company, customer, product = _world(db)
    tr = db.new_transaction(intents=True)
    s = semantic(tr)
    order = _order(s, company, customer, 1)
    with mock.patch.object(acme_audit, "RUNS", new=_Raiser(lambda: s.update(order, date=date(2026, 1, 1)))):
        with pytest.raises(ProtoUserError, match="intent_context"):
            s.get(order).add_line(product, Decimal("1"), Decimal("1"))
    assert len(s.get(order).lines) == 0 and s.get(order).date == date(2026, 6, 10)    # nothing of it applied
    tr.commit()
    assert _read(db, lambda s, tr: [r[0][1] for r in _log(tr)]) == ["create"]


class _Raiser(list):
    """A stand-in for ``acme_audit.RUNS`` whose ``append`` runs ``action`` (a session write from inside the
    running intent)."""

    def __init__(self, action):
        super().__init__()
        self._action = action

    def append(self, item):
        self._action()


def test_requires_is_evaluated_at_replay(db):
    company, customer, product = _world(db)
    order = db.run(lambda tr: _order(semantic(tr), company, customer, 1))
    late, early = db.new_transaction(intents=True), db.new_transaction(intents=True)
    semantic(late).get(order).confirm()                     # requires holds at record time
    semantic(early).get(order).confirm()
    early.commit()
    with pytest.raises(ProtoValidationError, match=r"acme\.sales\.Order\.confirm\.requires") as raised:
        late.commit()
    # F26: the record-time check is a courtesy that records no fine read; the replay decides (Task 9 makes
    # the class ProtoConstraintError).
    assert not isinstance(raised.value, ProtoConflictError)
    assert "Confirm the order" in str(raised.value)
    tr = db.new_transaction(intents=True)
    with pytest.raises(ProtoValidationError, match=r"acme\.sales\.Order\.confirm\.requires"):
        semantic(tr).get(order).confirm()                   # false at record time: refused at once
    tr.abort()


def test_cluster_space_refuses_intents():
    from tests.unit.test_semantic_catalog_store import _NodeRoot
    storage = MemoryStorage()
    plain = ObjectSpace(storage)
    plain.new_database("db")
    space = ObjectSpace(storage, coordinator=_NodeRoot((0, storage.read_root())))
    try:
        database = Database(space, "db")
        with pytest.raises(ProtoNotSupportedError, match="cluster"):
            database.new_transaction(intents=True)
        with pytest.raises(ProtoNotSupportedError, match="cluster"):
            database.run(lambda tr: None, intents=True)
        with pytest.raises(ProtoNotSupportedError, match="cluster"):
            Transaction(database, intents=True)
    finally:
        space.close()
        plain.close()


def test_raw_write_of_a_logged_cell_is_refused(db):
    company, customer, product = _world(db)
    tr = db.new_transaction(intents=True)
    s = semantic(tr)
    order = _order(s, company, customer, 1)
    for name in (order.name, customer.name, company.name):              # created, linked, linked
        with pytest.raises(ProtoUserError, match="intent log"):
            tr.set_mutable(name, 1)
        with pytest.raises(ProtoUserError, match="intent log"):
            tr.remove_mutable(name)
    with pytest.raises(ProtoUserError, match="intent log"):
        tr.new_mutable(order.name, 1)
    tr.new_mutable("plain", 1)                                           # other raw writes keep today's rules
    tr.set_mutable("plain", 2)
    tr.commit()
    assert _read(db, lambda s, tr: (tr.get_mutable("plain"), s.get(order).number)) == (2, 1)
    # The other order: a raw write first, then an operation that touches that cell.
    tr = db.new_transaction(intents=True)
    tr.set_mutable(product.name, tr.get_mutable(product.name))
    with pytest.raises(ProtoUserError, match="intent log"):
        semantic(tr).update(product, list_price=Decimal("11"))
    tr.abort()


# -- a group-commit leader replays another thread's intents ----------------------------------------------

def test_a_group_commit_leader_replays_another_threads_intents(audited, monkeypatch):
    """T0's round holds the root lock while T1 and T2, two intent transactions, queue behind it; whichever of
    them leads the next round builds the other one's commit, so its replay (its intent included) runs in
    the leader's thread while the owner's thread holds its transaction's lock and waits.  Neither waits for
    the other (no cycle), both commit, and the replayed intent sees its context.  The order is set by the
    group queue, with no sleep."""
    from proto_db import transactions as T
    db = audited
    company, customer, product = _world(db)
    orders = db.run(lambda tr: [_order(semantic(tr), company, customer, n) for n in (1, 2)])
    space = db.space
    group = T._group_of(space.storage)
    original = Transaction._build_root
    owner, held, release = {}, threading.Event(), threading.Event()

    def build(self, current, stamp=None):
        if owner.get(id(self)) == "T0":
            held.set()
            assert release.wait(60)
        return original(self, current, stamp)
    monkeypatch.setattr(Transaction, "_build_root", build)
    outcome = {}

    def commit(name, order):
        tr = db.new_transaction(intents=order is not None)
        owner[id(tr)] = name
        if order is None:
            tr.set_root_object("t0", 1)
        else:
            semantic(tr).get(order).add_line(product, Decimal("1"), Decimal("10"))
        try:
            tr.commit()
            outcome[name] = "committed"
        except BaseException as error:          # reported to the test
            outcome[name] = error
    first = threading.Thread(target=commit, args=("T0", None), name="T0", daemon=True)
    first.start()
    assert held.wait(60)
    queued = [threading.Thread(target=commit, args=(n, o), name=n, daemon=True)
              for n, o in (("T1", orders[0]), ("T2", orders[1]))]
    for thread in queued:
        thread.start()
    with group.cond:            # both wait in the queue behind T0's round: one leads the next round
        assert group.cond.wait_for(lambda: len(group.queue) == 2, timeout=60)
    release.set()
    for thread in [first] + queued:
        thread.join(60)
    assert [t.name for t in [first] + queued if t.is_alive()] == [], "a commit hung"
    assert outcome == {"T0": "committed", "T1": "committed", "T2": "committed"}
    recorded, replayed = acme_audit.RUNS[:2], acme_audit.RUNS[2:]
    assert sorted(recorded) == ["T1", "T2"]
    assert len(replayed) == 2 and len(set(replayed)) == 1             # one leader replayed both
    assert _read(db, lambda s, tr: s.get(customer).committed) == Decimal("20")
    space.close()       # not on failure: a hung commit holds the root lock (the daemon threads die with the run)


def test_taking_the_stored_record_is_a_read_of_the_whole_cell(db):
    # version.record hands the stored record itself to the caller, whose reads of it the session cannot see:
    # taking it records the whole cell (coarse, never wrong).
    company, customer, product = _world(db)
    tr = db.new_transaction(intents=True)
    s = semantic(tr)
    if s.get(product).record.list_price == Decimal("10"):
        _order(s, company, customer, 1)
    db.run(lambda other: semantic(other).update(product, name="widget 2"))   # another attribute of the cell
    with pytest.raises(ProtoConflictError, match=r"the cell 'acme\.catalog\.Product#"):
        tr.commit()
    tr = db.new_transaction(intents=True)
    version = semantic(tr).get(product)
    tr.abort()
    assert version.record.name == "widget 2"                                  # after the end: no read recorded
