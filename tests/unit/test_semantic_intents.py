"""The semantic session's intents and operations on the cell path (spec section 9; R53, R278).

The intents mode (``intents=True``) is gone (R53): every session operation reads and writes the transaction's cells
through ``get_mutable``, ``set_mutable``, ``new_mutable`` and ``remove_mutable``, and the unit of conflict is the
cell (R3).  What these tests catch: a session whose bytes differ from the plain workload's; two orders of one
customer that do not conflict on the customer cell, or a ``Database.run`` that does not retry them; a read of a cell
that misses a concurrent change of it; an intent whose ``_super()`` chain runs a module's implementation twice; an
intent whose returned version does not update its target; ``intent_context()`` usable outside an intent; a
``requires`` that is not decided when the intent is called, or a retried ``ProtoConstraintError``; a raw access to a
cell the session touched that is refused; a member read that runs inside another thread's operation; a catalog root
change that a transaction's session writes miss.  No sleep decides anything (R82): concurrent commits are ordered by
the test.

Every entity the tests create is complete (every ``one`` role set, preflight F28).
"""
from __future__ import annotations

import threading
from datetime import date
from decimal import Decimal
from unittest import mock

import pytest

from proto_db.errors import ProtoConflictError, ProtoConstraintError, ProtoUserError, ProtoValidationError
from proto_db.semantic import IntentContext, Version, intent_context, semantic
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace, Transaction
from tests.support.roots import reserved_root_names
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


# -- the plain workload ---------------------------------------------------------------------------------

def test_the_session_writes_the_plain_workloads_bytes(tmp_path, db):
    # F37: the oracle is the literal digest of a fixed non-semantic workload computed on the base commit b99285b.
    assert _plain_workload(tmp_path / "default") == _PLAIN_DIGEST_ON_THE_BASE
    # A raw write of a cell a session operation wrote is allowed, and no __intentLog is written (R53, R55).
    company, customer, product = _world(db)
    tr = db.new_transaction()
    order = _order(semantic(tr), company, customer, 1)
    tr.set_mutable(order.name, tr.get_mutable(order.name).with_(number=2))
    tr.commit()
    assert _read(db, lambda s, tr: s.get(order).number) == 2
    assert "__intentLog" not in reserved_root_names(db)


# -- what commits together ------------------------------------------------------------------------------

def test_two_orders_for_one_customer_conflict_on_the_cell_and_run_retries(db):
    # R3: the unit is the cell.  Two orders from one snapshot both write the customer cell, so the second
    # commit conflicts; Database.run retries it on the new state and both orders land.
    company, customer, product = _world(db)
    first, second = db.new_transaction(), db.new_transaction()                  # one snapshot
    orders = []
    for tr, number, quantity in ((first, 1, "2"), (second, 2, "3")):
        s = semantic(tr)
        order = _order(s, company, customer, number)
        s.get(order).add_line(product, Decimal(quantity), Decimal("10"))
        assert s.get(order).total == Decimal(quantity) * 10
        orders.append(order)
    first.commit()
    with pytest.raises(ProtoConflictError):
        second.commit()
    attempts = []

    def place(tr):
        attempts.append(1)
        s = semantic(tr)
        order = _order(s, company, customer, 2)
        s.get(order).add_line(product, Decimal("3"), Decimal("10"))
        if len(attempts) == 1:                              # a concurrent commit on the customer's cell
            db.run(lambda other: semantic(other).update(customer, credit_limit=Decimal("900")))
        return order
    retried = db.run(place)
    assert len(attempts) == 2

    def check(s, tr):
        bob = s.get(customer)
        assert {v.entity for v in bob.orders} == {orders[0], retried}
        assert bob.committed == Decimal("50")
        assert len(s.get(product).order_lines) == 2
    _read(db, check)


def test_reading_a_changed_cell_conflicts(db):
    company, customer, product = _world(db)
    tr = db.new_transaction()
    s = semantic(tr)
    assert s.get(product).list_price == Decimal("10")
    _order(s, company, customer, 1)
    db.run(lambda other: semantic(other).update(product, list_price=Decimal("12")))
    with pytest.raises(ProtoConflictError):
        tr.commit()
    assert _read(db, lambda s, tr: len(s.get(customer).orders)) == 0          # nothing of it is published


def test_navigating_a_many_end_conflicts_when_the_collection_changed(db):
    company, customer, product = _world(db)
    tr = db.new_transaction()
    s = semantic(tr)
    if len(s.get(customer).orders) == 0:
        _order(s, company, customer, 1)
    db.run(lambda other: _order(semantic(other), company, customer, 7))
    with pytest.raises(ProtoConflictError):
        tr.commit()
    # Without a concurrent change the same decision commits.
    tr = db.new_transaction()
    s = semantic(tr)
    if len(s.get(customer).orders) == 1:
        _order(s, company, customer, 2)
    tr.commit()
    assert _read(db, lambda s, tr: len(s.get(customer).orders)) == 2


# -- intents --------------------------------------------------------------------------------------------

def test_an_intent_creates_inside_a_transaction(audited):
    db = audited
    company, customer, product = _world(db)
    tr = db.new_transaction()
    s = semantic(tr)
    order = _order(s, company, customer, 1)
    follow = s.get(order).follow_up(2)                      # a creation inside an intent
    gadget = s.create(PRODUCT, name="gadget", list_price=Decimal("3"), company=company)
    tr.commit()

    def check(s, tr):
        assert s.get(order).number == 1 and s.get(follow).number == 2 and s.get(gadget).name == "gadget"
        assert {v.entity for v in s.get(customer).orders} == {order, follow}
        assert tr.get_mutable(follow.name).customer == customer
    _read(db, check)


def test_add_line_super_chain_runs_once_per_module(audited):
    db = audited
    company, customer, product = _world(db)
    tr = db.new_transaction()
    s = semantic(tr)
    order = _order(s, company, customer, 1)
    s.get(order).add_line(product, Decimal("2"), Decimal("10"))
    assert acme_audit.RUNS == [threading.current_thread().name]       # each module once
    assert len(s.get(order).lines) == 1
    tr.commit()
    assert len(acme_audit.RUNS) == 1                                  # a commit runs no intent
    assert _read(db, lambda s, tr: (len(s.get(order).lines), s.get(order).total)) == (1, Decimal("20"))


def test_confirm_returning_with_updates_the_target(db):
    company, customer, product = _world(db)
    order = db.run(lambda tr: _order(semantic(tr), company, customer, 1))
    tr = db.new_transaction()
    s = semantic(tr)
    result = s.get(order).confirm()
    assert isinstance(result, Version) and result.entity == order and result.state == "confirmed"
    assert s.get(order).state == "confirmed"
    tr.commit()
    assert _read(db, lambda s, tr: s.get(order).state) == "confirmed"


def test_intent_context_outside_an_intent_raises(db):
    company, customer, product = _world(db)
    with pytest.raises(ProtoUserError, match="only while an intent runs"):
        intent_context()
    tr = db.new_transaction()
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
    tr = db.new_transaction()
    s = semantic(tr)
    order = _order(s, company, customer, 1)
    with mock.patch.object(acme_audit, "RUNS", new=_Raiser(lambda: s.update(order, date=date(2026, 1, 1)))):
        with pytest.raises(ProtoUserError, match="intent_context"):
            s.get(order).add_line(product, Decimal("1"), Decimal("1"))
    assert len(s.get(order).lines) == 0 and s.get(order).date == date(2026, 6, 10)    # nothing of it applied
    tr.commit()


class _Raiser(list):
    """A stand-in for ``acme_audit.RUNS`` whose ``append`` runs ``action`` (a session write from inside the
    running intent)."""

    def __init__(self, action):
        super().__init__()
        self._action = action

    def append(self, item):
        self._action()


def test_requires_is_decided_when_the_intent_is_called(db):
    company, customer, product = _world(db)
    order = db.run(lambda tr: _order(semantic(tr), company, customer, 1))
    db.run(lambda tr: semantic(tr).get(order).confirm())
    tr = db.new_transaction()
    with pytest.raises(ProtoConstraintError, match=r"acme\.sales\.Order\.confirm\.requires") as raised:
        semantic(tr).get(order).confirm()
    tr.abort()
    error = raised.value
    assert not isinstance(error, ProtoConflictError)
    assert error.constraint == "acme.sales.Order.confirm.requires"
    assert error.meaning == "Confirm the order; it accepts no new lines afterwards"
    assert error.entity == order.name and error.values == {"state": "confirmed"}
    assert "Confirm the order" in str(error)
    calls = []

    def confirm(tr):                                        # in Database.run: raised once, never retried
        calls.append(tr)
        semantic(tr).get(order).confirm()
    with pytest.raises(ProtoConstraintError):
        db.run(confirm)
    assert len(calls) == 1


# -- raw access beside the session ---------------------------------------------------------------------

def test_raw_access_to_a_cell_the_session_touched_is_allowed(db):
    company, customer, product = _world(db)
    spare = db.run(lambda tr: tr.new_mutable("spare", 7))
    tr = db.new_transaction()
    s = semantic(tr)
    order = _order(s, company, customer, 1)
    s.get(order).add_line(product, Decimal("2"), Decimal("10"))
    for name in (order.name, customer.name, company.name, product.name):      # created, linked, linked, read
        assert tr.get_mutable(name) is not None
    assert tr.snapshot_mutable(order.name) is None and tr.snapshot_mutable(customer.name) is not None
    assert customer.get(tr) is not None
    assert tr.get_mutable(spare.name) == 7
    assert order.name in tr.mutable_names() and spare.name in tr.mutable_names()
    tr.new_mutable("audit", 0)
    tr.commit()
    assert _read(db, lambda s, t: (t.get_mutable("audit"), s.get(customer).committed)) == (0, Decimal("20"))


class _Watched:
    """The transaction's lock, reporting when a thread finds it held by another (``contended``)."""

    def __init__(self, lock):
        self._lock = lock
        self.contended = threading.Event()

    def acquire(self, blocking=True, timeout=-1):
        if self._lock.acquire(blocking=False):
            return True
        self.contended.set()
        return self._lock.acquire(blocking, timeout)

    def release(self):
        self._lock.release()

    def __enter__(self):
        return self.acquire()

    def __exit__(self, *exc):
        self.release()

    def __getattr__(self, name):
        return getattr(self._lock, name)


def test_member_iteration_waits_for_an_operation_in_progress(db, monkeypatch):
    # Review I2 (Task 6 atomicity): each member read of an iteration happens entirely before or after another
    # thread's operation on the transaction -- never inside it, where it would see writes the failing operation
    # then drops.  Ordered by events: A's operation, holding the lock after its writes, waits until B either
    # met the lock (fixed) or finished reading without it (the defect), then fails.
    from proto_db.semantic import entities as entities_module
    company, customer, _ = _world(db)
    orders = db.run(lambda tr: [_order(semantic(tr), company, customer, n, date(2026, 6, 1)) for n in (1, 2, 3)])
    tr = db.new_transaction()
    s = semantic(tr)
    it = iter(s.get(customer).orders)
    first = next(it)
    watched = _Watched(tr._lock)
    tr._lock = watched
    inside, progress, seen = threading.Event(), threading.Event(), []

    def hold(self, old, new):
        inside.set()
        progress.wait(60)
        raise RuntimeError("the operation fails after its writes")
    monkeypatch.setattr(entities_module.Operations, "_propagate", hold)

    def a():
        with pytest.raises(RuntimeError):
            s.update(next(o for o in orders if o != first.entity), date=date(2030, 1, 1))

    def b():
        try:
            seen.extend(version._record.date for version in it)
        finally:
            watched.contended.set()
            progress.set()
    watched.contended.clear()
    thread_a = threading.Thread(target=a)
    thread_a.start()
    assert inside.wait(60)
    watched.contended.clear()
    original_set = watched.contended.set

    def contended():
        original_set()
        progress.set()
    watched.contended.set = contended
    thread_b = threading.Thread(target=b)
    thread_b.start()
    thread_a.join(60)
    thread_b.join(60)
    tr._lock = watched._lock
    assert seen == [date(2026, 6, 1)] * 2, seen
    monkeypatch.undo()
    assert [v.date for v in s.get(customer).orders] == [date(2026, 6, 1)] * 3
    tr.abort()


def test_a_concurrent_unlink_conflicts_and_the_retry_decides_on_the_new_state(db):
    company, customer, _ = _world(db)
    other = db.run(lambda tr: semantic(tr).create("acme.core.Company", name="Other"))
    a, b = db.new_transaction(), db.new_transaction()
    for tr in (a, b):
        semantic(tr).unlink(customer, "company", company)
        semantic(tr).link(customer, "company", other)
    a.commit()
    with pytest.raises(ProtoConflictError):
        b.commit()
    assert _read(db, lambda s, t: s.navigate(customer, "company").entity) == other
    # The retry decides on the new state: the unlink is refused when it is made.
    attempts = []

    def move(tr):
        attempts.append(1)
        semantic(tr).unlink(customer, "company", company)
    with pytest.raises(ProtoValidationError, match="is not linked"):
        db.run(move)
    assert len(attempts) == 1


def test_the_first_registry_build_of_a_transaction_holds_no_transaction_lock(db, monkeypatch):
    # Final review M5: building the registry may import, and imports never run holding the transaction's
    # lock (review M-5); the first operation of a transaction builds it before taking the lock.
    company, customer, _ = _world(db)
    original = Transaction._resolution_registry
    held = []

    def watched(self):
        if self._registry_memo is None:
            held.append(self._lock._is_owned())
        return original(self)
    monkeypatch.setattr(Transaction, "_resolution_registry", watched)
    tr = db.new_transaction()
    try:
        semantic(tr).update(customer, credit_limit=Decimal("10"))
    finally:
        tr.abort()
    assert held and not any(held)


def test_a_catalog_root_change_during_a_transaction_conflicts(db):
    # The first write of a session enters the catalog roots in the read set: a concurrent install changes them,
    # and the commit is a conflict naming the root.
    company, customer, _ = _world(db)
    tr = db.new_transaction()
    _order(semantic(tr), company, customer, 1)
    db.run(lambda other: other.install_module("acme_audit"))
    with pytest.raises(ProtoConflictError, match="__currentModules"):
        tr.commit()


_RESERVED_PROBE = """
from proto_db.registry import module
from proto_db.semantic import Extension, intent
from proto_db.errors import ProtoValidationError
module("t8_reserved_names", depends=("acme_sales",))
refused = {}
for name in ("create", "update", "link", "unlink", "relate", "follow_up"):
    def body(self):
        return self
    try:
        type("Probe", (Extension,), {"__extends__": "acme.sales.Order", "__module__": __name__,
                                     name: intent(meaning="x")(body)})
    except ProtoValidationError as error:
        refused[name] = str(error)
"""


def test_an_intent_named_like_a_primitive_is_refused():
    # An IntentRecord names its operation by (type, name): an intent named like a primitive
    # (classes.PRIMITIVES) would read as that primitive, so the declaration is refused.  The probe declares
    # its own registry module in a namespace of its own (one declaration per process).
    namespace = {"__name__": "tests.unit.semantic_modules.t8_reserved_names"}
    exec(compile(_RESERVED_PROBE, "<t8_reserved_names>", "exec"), namespace)
    refused = namespace["refused"]
    assert sorted(refused) == sorted(["create", "update", "link", "unlink", "relate"])
    for name, message in refused.items():
        assert f"may not be named {name!r}" in message
