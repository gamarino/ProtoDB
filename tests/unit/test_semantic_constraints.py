"""Declared invariants and cardinality, with errors that name the rule (spec section 10; rulings R272, R273,
R279; Task 9; preflight F25, F27, F28, F29; the plan's Review Focus 3, 4 and 6).

Catches: two orders of one customer that fit its credit limit together but are refused, or that exceed it
together and both commit; a violation reported as a conflict (and so retried), as a ``TypeError`` over an
absent operand, or without the rule's name, meaning and rendered message; a violating commit that publishes
part of itself; a cancelled order that keeps
weighing on the aggregate; an invariant of a supertype's declaration that misses a subtype instance; an
informative invariant (``enforced=False``) that refuses anything; a direct-mode operation that is checked
only at commit, or that leaves its writes behind when it is refused; an entity committed without the one
target its role requires; an enforced invariant over another entity, which no change of that entity would
re-check.  No sleep decides anything (R82): concurrent commits are ordered by the test.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from proto_db.errors import ProtoConflictError, ProtoConstraintError, ProtoNotSupportedError, ProtoValidationError
from proto_db.semantic import semantic
from proto_db.semantic.descriptors import invariant_name
from proto_db.semantic.expressions import parse
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from proto_db.values import MISSING
from tests.unit.semantic_modules import MAPPING

pytestmark = [pytest.mark.timeout(120)]

ORDER, RUSH, PRODUCT, CUSTOMER = "acme.sales.Order", "acme.sales.RushOrder", "acme.catalog.Product", \
    "acme.sales.Customer"
#: The R279 name of the demo's credit invariant, computed as the rule says (never copied from the code).
CREDIT = invariant_name(parse("committed <= credit_limit"))
MEANING = "A customer cannot have open orders above its credit limit"


def _space(*modules):
    space = ObjectSpace(MemoryStorage(), modules_mapping=MAPPING)
    database = space.new_database("db")
    database.run(lambda tr: [tr.install_module(name) for name in ("acme_sales",) + modules])
    return space, database


@pytest.fixture
def db():
    space, database = _space()
    try:
        yield database
    finally:
        space.close()


def _order(s, world, number, amount, *, customer=None, type_name=ORDER):
    """An order of ``world``'s company for ``customer`` (Bob by default) with one line of ``amount``."""
    order = s.create(type_name, number=number, date=date(2026, 6, 10), company=world["company"],
                     customer=world["bob"] if customer is None else customer)
    if amount:
        s.get(order).add_line(world["product"], Decimal("1"), Decimal(amount))
    return order


def _world(db, limit="100", committed="20"):
    """A company, Bob (credit limit ``limit``, with an order of ``committed``), Ann (limit 100, nothing
    committed) and a product, committed in direct mode; every entity complete (F28)."""
    def build(tr):
        s = semantic(tr)
        company = s.create("acme.core.Company", name="ACME")
        world = {"company": company,
                 "bob": s.create(CUSTOMER, name="Bob", credit_limit=Decimal(limit), company=company),
                 "ann": s.create(CUSTOMER, name="Ann", credit_limit=Decimal("100"), company=company),
                 "product": s.create(PRODUCT, name="widget", list_price=Decimal("10"), company=company)}
        world["first"] = _order(s, world, 1, committed)
        return world
    return db.run(build)


def _read(db, fn):
    tr = db.new_transaction()
    try:
        return fn(semantic(tr), tr)
    finally:
        tr.abort()


def _committed(db, customer):
    return _read(db, lambda s, tr: s.get(customer).committed)


def _orders_of(db, customer):
    return _read(db, lambda s, tr: {v.entity for v in s.get(customer).orders})


def _exists(db, handle) -> bool:
    return _read(db, lambda s, tr: tr.get_mutable(handle.name) is not None)


# -- what commits together (the cell is the unit, R3) --------------------------------------------------------

def test_concurrent_orders_within_the_limit_both_commit_through_run(db):
    world = _world(db)
    first, second = db.new_transaction(), db.new_transaction()          # one snapshot
    orders = [_order(semantic(first), world, 2, "30")]
    _order(semantic(second), world, 3, "30")
    first.commit()
    with pytest.raises(ProtoConflictError):                  # both write Bob's cell
        second.commit()
    orders.append(db.run(lambda tr: _order(semantic(tr), world, 3, "30")))       # the retry: 20 + 30 + 30 <= 100
    assert _committed(db, world["bob"]) == Decimal("80")
    assert _orders_of(db, world["bob"]) == {world["first"], *orders}


def test_the_later_order_over_the_limit_fails_with_the_rule(db):
    world = _world(db)
    first = db.new_transaction()
    _order(semantic(first), world, 2, "30")
    first.commit()
    second = db.new_transaction()                           # on the first's version: 20 + 30 + 60 = 110 > 100
    with pytest.raises(ProtoConstraintError) as raised:
        _order(semantic(second), world, 3, "60")            # refused at the operation, not at commit
    error = raised.value
    assert isinstance(error, ProtoValidationError) and not isinstance(error, ProtoConflictError)    # R273
    assert error.code == 10_100
    assert error.constraint == CREDIT == "invariant_9d9f212a"
    assert error.meaning == MEANING
    # R279: {customer} renders the customer's key (its name, F27) and {excess} is left minus right.
    assert error.message == str(error) == "The order exceeds the credit limit of Bob by 10"
    assert error.entity == world["bob"].name
    assert error.values == {"committed": Decimal("110"), "credit_limit": Decimal("100")}
    described = error.describe()
    assert described == {"error": "ProtoConstraintError", "code": 10_100, "constraint": CREDIT, "meaning": MEANING,
                         "message": "The order exceeds the credit limit of Bob by 10", "entity": world["bob"].name,
                         "values": {"committed": Decimal("110"), "credit_limit": Decimal("100")}, "missing": []}
    second.abort()                                          # the refused operation left nothing behind
    assert _committed(db, world["bob"]) == Decimal("50")
    assert len(_orders_of(db, world["bob"])) == 2
    assert "conflict" not in error.message


def test_cancelled_orders_leave_the_aggregate(db):
    world = _world(db)
    second = db.run(lambda tr: _order(semantic(tr), world, 2, "30"))
    third = db.run(lambda tr: _order(semantic(tr), world, 3, "40"))
    assert _committed(db, world["bob"]) == Decimal("90")
    db.run(lambda tr: semantic(tr).update(third, state="cancelled"))       # where becomes false: a delta of -40
    assert _committed(db, world["bob"]) == Decimal("50")
    fourth = db.run(lambda tr: _order(semantic(tr), world, 4, "50"))       # 50 + 50 = 100 <= 100
    assert _committed(db, world["bob"]) == Decimal("100")
    assert _orders_of(db, world["bob"]) == {world["first"], second, third, fourth}
    # The cancelled order's lines no longer move the aggregate.
    db.run(lambda tr: semantic(tr).get(third).add_line(world["product"], Decimal("1"), Decimal("500")))
    assert _committed(db, world["bob"]) == Decimal("100")
    with pytest.raises(ProtoConstraintError, match="by 1$"):
        db.run(lambda tr: _order(semantic(tr), world, 5, "1"))


# -- Review Focus 3: a violation in Database.run -------------------------------------------------------------

def test_constraint_error_is_not_retried_by_run(db):
    world = _world(db)
    calls = []

    def fn(tr):
        calls.append(tr)
        order = _order(semantic(tr), world, 3, "60")        # alone it fits: 20 + 60 <= 100
        if len(calls) == 1:                                 # a concurrent order commits before this one
            db.run(lambda other: _order(semantic(other), world, 2, "30"))
        return order
    with pytest.raises(ProtoConstraintError) as raised:
        db.run(fn)
    assert len(calls) == 2                                  # the conflict was retried; the rule never (R273)
    assert raised.value.constraint == CREDIT
    assert _committed(db, world["bob"]) == Decimal("50")
    assert len(_orders_of(db, world["bob"])) == 2


# -- Review Focus 4: an absent operand -------------------------------------------------------------------------

def test_missing_credit_limit_names_the_operand(db):
    world = _world(db)
    # Created through the session without a limit, the invariant's operands are set and it fails at once.
    with pytest.raises(ProtoConstraintError, match="<missing credit_limit>"):
        db.run(lambda tr: semantic(tr).create(CUSTOMER, name="Carol", company=world["company"]))
    carol = db.run(lambda tr: semantic(tr).create(CUSTOMER, name="Carol", credit_limit=Decimal("100"),
                                                  company=world["company"]))

    def drop_limit(tr):                         # a record written raw without the limit (Review Focus 4)
        record = tr.get_mutable(carol.name)
        tr.set_mutable(carol.name, type(record)(name=record.name, committed=record.committed,
                                                company=record.company))
    db.run(drop_limit)
    order = db.run(lambda tr: _order(semantic(tr), world, 2, None, customer=carol))     # no amount: unchanged
    with pytest.raises(ProtoConstraintError) as raised:
        db.run(lambda tr: semantic(tr).get(order).add_line(world["product"], Decimal("1"), Decimal("5")))
    error = raised.value
    assert error.constraint == CREDIT
    assert error.message == "The order exceeds the credit limit of Carol by <missing credit_limit>"
    assert error.values["committed"] == Decimal("5") and error.values["credit_limit"] is MISSING
    described = error.describe()
    assert described["values"] == {"committed": Decimal("5"), "credit_limit": None}
    assert described["missing"] == ["credit_limit"]
    assert _committed(db, carol) == 0


# -- cardinality (F28: at commit) --------------------------------------------------------------

def test_cardinality_one_is_enforced(db):
    world = _world(db)
    order = world["first"]
    tr = db.new_transaction()
    semantic(tr).unlink(order, "customer", world["bob"])
    with pytest.raises(ProtoConstraintError) as raised:
        tr.commit()
    error = raised.value
    assert error.constraint == "acme.sales.Order.customer.cardinality"
    assert error.meaning == "Every acme.sales.Order has exactly one customer"
    assert error.message == "The order ACME, 1 has no customer (cardinality one)"
    assert error.entity == order.name and error.values == {"customer": MISSING}
    assert not tr.is_active
    assert _read(db, lambda s, tr: s.get(order).customer.entity) == world["bob"]       # nothing published
    # An unlink and a link in one transaction move the end: the order still has exactly one customer.
    db.run(lambda tr: [semantic(tr).unlink(order, "customer", world["bob"]),
                       semantic(tr).link(order, "customer", world["ann"])])
    world["bob"], world["ann"] = world["ann"], world["bob"]
    # A new order without its customer is refused at commit, with nothing published.
    tr = db.new_transaction()
    orphan = semantic(tr).create(ORDER, number=9, company=world["company"])
    with pytest.raises(ProtoConstraintError, match=r"^The order ACME, 9 has no customer"):
        tr.commit()
    assert not _exists(db, orphan)


def test_cardinality_at_least_one_is_enforced():
    space, db = _space("acme_favourites")
    try:
        def build(tr):
            s = semantic(tr)
            company = s.create("acme.core.Company", name="ACME")
            product = s.create(PRODUCT, name="widget", list_price=Decimal("10"), company=company)
            return company, product, s.create(CUSTOMER, name="Dan", credit_limit=Decimal("1"), company=company)
        with pytest.raises(ProtoConstraintError) as raised:
            db.run(build)
        assert raised.value.constraint == "acme.sales.Customer.favourites.cardinality"
        assert raised.value.meaning == "Every acme.sales.Customer has at least one favourites"
        assert raised.value.message == "The customer Dan has no favourites (cardinality 1..*)"

        def complete(tr):
            company, product, customer = build(tr)
            semantic(tr).link(customer, "favourites", product)
            return customer
        customer = db.run(complete)
        assert len(_read(db, lambda s, tr: s.get(customer).favourites)) == 1
    finally:
        space.close()


# -- Review Focus 6, informative constraints, direct mode ------------------------------------------------------

def test_inherited_invariant_binds_subtype_instances():
    space, db = _space("acme_rush")
    try:
        world = _world(db)
        rush = db.run(lambda tr: _order(semantic(tr), world, 2, "30", type_name=RUSH))     # counted: 50
        assert _committed(db, world["bob"]) == Decimal("50")
        with pytest.raises(ProtoConstraintError) as raised:
            db.run(lambda tr: _order(semantic(tr), world, 3, "60", type_name=RUSH))
        assert raised.value.constraint == CREDIT
        assert str(raised.value) == "The order exceeds the credit limit of Bob by 10"
        assert _orders_of(db, world["bob"]) == {world["first"], rush}
        # A rush order's own key is inherited from Order: its cardinality error names it the same way.
        tr = db.new_transaction()
        semantic(tr).unlink(rush, "customer", world["bob"])
        with pytest.raises(ProtoConstraintError, match=r"^The rushorder ACME, 2 has no customer") as raised:
            tr.commit()
        assert raised.value.constraint == "acme.sales.RushOrder.customer.cardinality"
    finally:
        space.close()


def test_informative_constraint_is_not_enforced():
    space, db = _space("acme_informative")
    try:
        world = _world(db)
        db.run(lambda tr: _order(semantic(tr), world, 2, "60"))       # 80: into the margin of 50
        assert _committed(db, world["bob"]) == Decimal("80")
        described = _read(db, lambda s, tr: s.describe(CUSTOMER))
        margin = next(c for c in described["constraints"] if c["name"] == "credit_margin")
        assert margin["enforced"] is False
        with pytest.raises(ProtoConstraintError) as raised:     # the enforced one still holds
            db.run(lambda tr: _order(semantic(tr), world, 3, "30"))
        assert raised.value.constraint == CREDIT
    finally:
        space.close()


def test_direct_mode_checks_at_the_operation(db):
    world = _world(db)
    tr = db.new_transaction()
    s = semantic(tr)
    kept = _order(s, world, 2, "50")                        # 70
    refused = _order(s, world, 3, None)                     # no line yet: nothing changes
    with pytest.raises(ProtoConstraintError) as raised:
        s.get(refused).add_line(world["product"], Decimal("1"), Decimal("40"))      # 110: refused at once
    assert str(raised.value) == "The order exceeds the credit limit of Bob by 10"
    # The refused operation left nothing behind in the transaction; the transaction goes on.
    assert tr.is_active
    assert s.get(world["bob"]).committed == Decimal("70")
    assert s.get(refused).total == 0 and len(s.get(refused).lines) == 0
    assert len(s.get(world["product"]).order_lines) == 2
    tr.commit()
    assert _committed(db, world["bob"]) == Decimal("70")
    assert _orders_of(db, world["bob"]) == {world["first"], kept, refused}
    # The check's reads enter the read set: a concurrent change of the limit makes the commit conflict.
    tr = db.new_transaction()
    _order(semantic(tr), world, 4, "30")                    # 100: within the limit read
    db.run(lambda other: semantic(other).update(world["bob"], credit_limit=Decimal("90")))
    with pytest.raises(ProtoConflictError):
        tr.commit()
    assert _committed(db, world["bob"]) == Decimal("70")


def test_an_enforced_invariant_over_another_entity_is_refused():
    space = ObjectSpace(MemoryStorage(), modules_mapping=MAPPING)
    try:
        db = space.new_database("db")
        db.run(lambda tr: tr.install_module("acme_sales"))
        with pytest.raises(ProtoNotSupportedError, match="order_within_limit"):
            db.run(lambda tr: tr.install_module("acme_far_invariant"))
    finally:
        space.close()


def test_a_stored_cross_entity_invariant_opens_for_reading_and_is_not_enforced(monkeypatch):
    """The refusal is the writer's (composition at install).  A catalog that another writer stored with a
    cross-entity enforced invariant still opens: the invariant is published, describe says it is not enforced
    and why, and no write is refused by it."""
    from proto_db.semantic import store as _store
    space = ObjectSpace(MemoryStorage(), modules_mapping=MAPPING)
    try:
        db = space.new_database("db")
        db.run(lambda tr: tr.install_module("acme_sales"))
        with monkeypatch.context() as patched:          # another writer, without this refusal
            patched.setattr(_store, "check_enforceable", lambda entities: None)
            db.run(lambda tr: tr.install_module("acme_far_invariant"))
        described = _read(db, lambda s, tr: s.describe(ORDER))
        rule = next(c for c in described["constraints"] if c["name"] == "order_within_limit")
        assert rule.get("enforced", True) is True
        assert rule["not_enforced"].startswith("an enforced invariant reads only its entity's own attributes")
        assert "customer.credit_limit" in rule["not_enforced"]
        credit = next(c for c in _read(db, lambda s, tr: s.describe(CUSTOMER))["constraints"])
        assert "not_enforced" not in credit
        world = _world(db, limit="100", committed="20")     # an order of 20 within Bob's 100
        # A cancelled order leaves Bob's aggregate, so a line of 500 keeps Bob's own invariant; the order's
        # total (520) then exceeds its customer's limit, and the published invariant does not refuse it.
        db.run(lambda tr: semantic(tr).update(world["first"], state="cancelled"))
        db.run(lambda tr: semantic(tr).get(world["first"]).add_line(world["product"], Decimal("1"),
                                                                     Decimal("500")))
        assert _read(db, lambda s, tr: s.get(world["first"]).total) == Decimal("520")
        db.run(lambda tr: semantic(tr).get(world["first"]).add_line(world["product"], Decimal("1"),
                                                                     Decimal("1")))
        assert _read(db, lambda s, tr: s.get(world["first"]).total) == Decimal("521")
    finally:
        space.close()


def test_a_session_read_allocates_no_undo_log(db, monkeypatch):
    from proto_db import transactions as T
    world = _world(db)
    made = []
    original = T._SemanticUndo

    class Counting(original):
        __slots__ = ()

        def __init__(self):
            made.append(1)
            super().__init__()
    monkeypatch.setattr(T, "_SemanticUndo", Counting)
    tr = db.new_transaction()
    s = semantic(tr)
    assert s.get(world["bob"]).committed == Decimal("20") and len(s.get(world["bob"]).orders) == 1
    s.query({"from": ["via", ["cell", world["company"].name], "customers"]})
    assert made == []                                       # reads hold the lock, and write nothing
    s.update(world["bob"], credit_limit=Decimal("200"))
    assert made == [1]                                      # the first write of an operation makes it
    tr.abort()


# -- final review I2: a direct-mode semantic write conflicts with a concurrent catalog change ----------------

def test_a_direct_write_conflicts_with_a_concurrent_install_that_adds_an_invariant(db):
    """T1 updates Bob's credit limit to 900 in direct mode on a version without ``acme_credit_cap``; T2
    installs it (``credit_limit <= 500``, enforced) and commits first.  T1's write was checked against a
    catalog that is no longer current, so its commit conflicts, as an intent-mode commit does; the retry
    checks the new invariant.  No committed version violates an invariant its own catalog enforces."""
    world = _world(db)
    t1 = db.new_transaction()
    semantic(t1).update(world["bob"], credit_limit=Decimal("900"))
    db.run(lambda tr: tr.install_module("acme_credit_cap"))
    with pytest.raises(ProtoConflictError):
        t1.commit()
    attempts = []

    def retried(tr):
        attempts.append(1)
        semantic(tr).update(world["bob"], credit_limit=Decimal("900"))
    with pytest.raises(ProtoConstraintError):
        db.run(retried)
    assert len(attempts) == 1
    check = db.new_transaction()
    assert semantic(check).get(world["bob"]).record.credit_limit == Decimal("100")
    check.abort()


def test_a_direct_read_only_transaction_does_not_conflict_with_an_install(db):
    """Only a semantic write enters the catalog roots in the read set: a transaction that only reads
    through the session commits across a concurrent install."""
    world = _world(db)
    t1 = db.new_transaction()
    assert semantic(t1).get(world["bob"]).record.credit_limit == Decimal("100")
    t1.set_root_object("note", "read only")
    db.run(lambda tr: tr.install_module("acme_credit_cap"))
    t1.commit()
