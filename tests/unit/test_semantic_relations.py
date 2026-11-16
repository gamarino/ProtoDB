"""Entities and relations through the semantic session, in direct mode (spec sections 6, 7 and 10; Task 6;
preflight F17, F18, F24, F28 and row 44; Review Focus 6).

Catches: a cell not named ``<type name>#<32 hex digits>``; a create that accepts a missing required field, an
unknown name or a role it does not link; a link or unlink that updates one end only; a relationship atom
copied into each collection instead of shared; a version-bound role that follows the product's current
version instead of the one sold; a query over a maintained-key end that ignores its index, or that returns
the stored entries instead of the members; an update of the keyed attribute that leaves stale keys; a
maintained aggregate recomputed over its collection instead of updated by deltas, or not propagated along
the inverses; a direct-mode commit that does not conflict on a shared order; a role typed by a supertype that
refuses a subtype, or accepts an unrelated type; writes through a session over a snapshot.

Every entity the tests create is complete (every ``one`` role set, preflight F28).
"""
from __future__ import annotations

import re
import threading
from datetime import date
from decimal import Decimal

import pytest

from proto_db.errors import ProtoConflictError, ProtoNotSupportedError, ProtoUserError, ProtoValidationError
from proto_db.semantic import Members, Version, semantic
from proto_db.semantic import expressions
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from proto_db.values import MutableObject
from tests.unit.semantic_modules import MAPPING

from docs.kit.semantic.acme_sales import Order, OrderLine

pytestmark = [pytest.mark.timeout(120)]

ORDER, LINE = "acme.sales.Order", "acme.sales.OrderLine"
JUNE = ["and", [">=", ["attr", "order", "date"], ["const", date(2026, 6, 1)]],
        ["<=", ["attr", "order", "date"], ["const", date(2026, 6, 30)]]]


@pytest.fixture
def db():
    space = ObjectSpace(MemoryStorage(), modules_mapping=MAPPING)
    try:
        database = space.new_database("db")
        database.run(lambda tr: tr.install_module("acme_sales"))
        yield database
    finally:
        space.close()


def _world(s):
    """A company with one customer and one product, every ``one`` role set (F28)."""
    company = s.create("acme.core.Company", name="ACME")
    customer = s.create("acme.sales.Customer", name="Bob", credit_limit=Decimal("1000"), company=company)
    product = s.create("acme.catalog.Product", name="widget", list_price=Decimal("10"), company=company)
    return company, customer, product


def _order(s, company, customer, number, day=date(2026, 6, 10), type_name=ORDER, **fields):
    return s.create(type_name, number=number, date=day, company=company, customer=customer, **fields)


def _line(s, order, product, quantity="1", price="10"):
    return s.relate(LINE, order=order, product=product, quantity=Decimal(quantity), price=Decimal(price))


def _indexes_used(explain) -> set:
    """The names of the indexes a plan in ``explain`` reads."""
    found, stack = set(), [explain]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            if isinstance(node.get("index"), dict):
                found.add(node["index"]["index"])
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)
    return found


# -- creation ---------------------------------------------------------------------------------------------

def test_create_names_the_cell_by_type_and_uuid(db):
    tr = db.new_transaction()
    s = semantic(tr)
    company, customer, product = _world(s)
    order = _order(s, company, customer, 1)
    other = _order(s, company, customer, 2)
    assert isinstance(order, MutableObject)
    assert re.fullmatch(r"acme\.sales\.Order#[0-9a-f]{32}", order.name)
    assert order.name != other.name
    assert re.fullmatch(r"acme\.core\.Company#[0-9a-f]{32}", company.name)
    record = tr.get_mutable(order.name)
    assert type(record) is Order and record.number == 1
    version = s.get(order)
    assert isinstance(version, Version) and version.record is record and version.entity == order
    assert version.number == 1 and version.state == "open"           # a Field default resolves
    tr.commit()
    tr = db.new_transaction()
    assert semantic(tr).get(order).number == 1                        # the cell is stored
    tr.abort()


def test_required_field_missing_is_refused(db):
    tr = db.new_transaction()
    s = semantic(tr)
    company, customer, _ = _world(s)
    with pytest.raises(ProtoValidationError, match=r"acme\.sales\.Order.*required.*number"):
        s.create(ORDER, date=date(2026, 6, 1), company=company, customer=customer)
    with pytest.raises(ProtoValidationError, match=r"no attribute or role 'colour'"):
        s.create(ORDER, number=1, colour="red", company=company, customer=customer)
    with pytest.raises(ProtoValidationError, match=r"'total'.*maintained"):
        s.create(ORDER, number=1, total=Decimal(5), company=company, customer=customer)
    with pytest.raises(ProtoValidationError, match=r"relationship.*relate"):
        s.create(LINE, quantity=Decimal(1), price=Decimal(1))
    with pytest.raises(ProtoValidationError, match=r"required.*price"):
        s.relate(LINE, order=_order(s, company, customer, 3), product=s.create(
            "acme.catalog.Product", name="p", company=company), quantity=Decimal(1))
    tr.abort()


def test_a_type_without_a_python_class_is_not_supported(db):
    db.run(lambda tr: tr.install_module("acme_ghost"))
    tr = db.new_transaction()
    with pytest.raises(ProtoNotSupportedError, match=r"acme\.ghost\.Thing.*no Python class"):
        semantic(tr).create("acme.ghost.Thing", name="x")
    tr.abort()


def test_writes_on_a_snapshot_session_are_refused(db):
    handles = db.run(lambda tr: _world(semantic(tr)))
    tr = db.new_transaction()
    snapshot = db.snapshot_at(tr.snapshot_ref)
    tr.abort()
    s = semantic(snapshot)
    assert s.get(handles[1]).name == "Bob"                            # reads work
    answer = s.query({"from": ["via", ["cell", handles[0].name], "customers"]})
    assert [v.entity for v in answer["rows"]] == [handles[1]] and answer["version"]["number"] == snapshot.version
    with pytest.raises(ProtoUserError, match="read-only"):
        s.create("acme.core.Company", name="Other")
    with pytest.raises(ProtoUserError, match="read-only"):
        s.update(handles[2], list_price=Decimal(1))
    with pytest.raises(ProtoUserError, match="read-only"):
        s.relate(LINE, order=handles[0], product=handles[2], quantity=Decimal(1), price=Decimal(1))


# -- links ------------------------------------------------------------------------------------------------

def test_link_maintains_both_ends(db):
    tr = db.new_transaction()
    s = semantic(tr)
    company, customer, product = _world(s)
    order = _order(s, company, customer, 1)
    assert s.navigate(order, "customer").entity == customer
    assert [v.entity for v in s.navigate(customer, "orders")] == [order]
    assert {v.entity for v in s.navigate(company, "orders")} == {order}
    assert {v.entity for v in s.navigate(company, "customers")} == {customer}
    # One end only is never left: unlink, then link to another customer.
    other = s.create("acme.sales.Customer", name="Ann", credit_limit=Decimal("5"), company=company)
    with pytest.raises(ProtoValidationError, match="unlink"):
        s.link(order, "customer", other)
    s.unlink(customer, "orders", order)                               # either end may be named
    assert s.navigate(order, "customer") is None
    assert len(s.navigate(customer, "orders")) == 0
    s.link(other, "orders", order)
    assert s.navigate(order, "customer").entity == other
    assert [v.entity for v in s.navigate(other, "orders")] == [order]
    with pytest.raises(ProtoValidationError, match="not linked"):
        s.unlink(order, "customer", customer)
    # Roles change through link and unlink only.
    with pytest.raises(ProtoValidationError, match=r"role.*link"):
        s.update(order, customer=customer)
    with pytest.raises(ProtoValidationError, match=r"relationship.*relate"):
        s.link(order, "lines", product)
    tr.commit()
    tr = db.new_transaction()
    s = semantic(tr)
    assert s.get(order).customer.entity == other                    # attribute access navigates
    assert [v.entity for v in s.get(other).orders] == [order]
    tr.abort()


def test_link_of_an_unrelated_type_is_refused(db):
    tr = db.new_transaction()
    s = semantic(tr)
    company, customer, product = _world(s)
    with pytest.raises(ProtoValidationError, match=r"acme\.catalog\.Product.*acme\.sales\.Order"):
        s.link(customer, "orders", product)
    with pytest.raises(ProtoValidationError, match=r"acme\.catalog\.Product.*acme\.sales\.Customer"):
        s.create(ORDER, number=1, company=company, customer=product)
    order = _order(s, company, customer, 1)
    with pytest.raises(ProtoValidationError, match=r"acme\.sales\.Customer.*acme\.catalog\.Product"):
        s.relate(LINE, order=order, product=customer, quantity=Decimal(1), price=Decimal(1))
    assert len(s.navigate(customer, "orders")) == 1                   # nothing was half applied
    tr.abort()


# -- relationships ----------------------------------------------------------------------------------------

def test_relationship_atom_is_shared_by_both_collections(db):
    def build(tr):
        s = semantic(tr)
        company, customer, product = _world(s)
        order = _order(s, company, customer, 1)
        line = _line(s, order, product, "2", "9")
        assert isinstance(line, Version) and type(line.record) is OrderLine and line.entity is None
        return order, product, line.oid
    order, product, oid = db.run(build)
    tr = db.new_transaction()
    s = semantic(tr)
    from_order = list(s.navigate(order, "lines"))
    from_product = list(s.navigate(product, "order_lines"))
    assert [v.oid for v in from_order] == [oid] == [v.oid for v in from_product]
    assert from_order[0].record.atom_pointer == from_product[0].record.atom_pointer
    # Row 44: the maintained-key end yields members (lines), never its stored entries.
    lines = s.get(product).order_lines
    assert isinstance(lines, Members) and not lines.ordered
    assert all(type(v.record) is OrderLine for v in lines)
    assert all(type(entry).__name__ == "DBObject" and set(entry._own) == {"member", "key"}
               for entry in lines.collection)
    assert s.navigate(order, "lines").ordered
    assert from_order[0].order.entity == order                       # navigation from the atom
    assert from_order[0].amount == Decimal(18)                       # a neutral computed of the atom
    tr.abort()


def test_version_binding_keeps_the_product_as_sold(db):
    def sell(tr):
        s = semantic(tr)
        company, customer, product = _world(s)
        order = _order(s, company, customer, 1)
        _line(s, order, product, "1", "9")
        return order, product
    order, product = db.run(sell)
    db.run(lambda tr: semantic(tr).update(product, list_price=Decimal("12")))
    tr = db.new_transaction()
    s = semantic(tr)
    line = list(s.navigate(order, "lines"))[0]
    sold = s.navigate(line, "product")
    assert isinstance(sold, Version) and sold.list_price == Decimal("10")
    assert line.product.list_price == Decimal("10")                  # attribute access, the same
    assert sold.entity == product
    assert s.get(sold.entity).list_price == Decimal("12")            # .entity reaches the current one
    tr.abort()


def test_bindings_of_one_product_in_one_transaction_share_one_version(db):
    """Spec 6 / I1: writes to the product's own ends (each line joins ``order_lines``) never start a new
    bound version; a product created in the transaction binds its record as created."""
    def build(tr):
        s = semantic(tr)
        company, customer, product = _world(s)
        order = _order(s, company, customer, 1)
        for quantity in ("1", "2", "3"):
            _line(s, order, product, quantity)
        return customer, order, product
    customer, order, product = db.run(build)
    tr = db.new_transaction()
    s = semantic(tr)
    bound = [line.product for line in s.navigate(order, "lines")]
    assert len({v.oid for v in bound}) == 1 and len(bound[0].order_lines) == 0
    count = s.query({"from": ["via", ["cell", order.name], "lines", "product"], "aggregate": ["count"]})
    assert count["rows"] == [{"count": 1}]
    committed = s.get(product)
    tr.abort()
    # A product the transaction reads but whose fields it does not change: the committed record it read.
    def more(tr):
        s = semantic(tr)
        second = _order(s, s.get(order).company.entity, customer, 2)
        _line(s, second, product, "4")
        _line(s, second, product, "5")
        return second
    second = db.run(more)
    tr = db.new_transaction()
    s = semantic(tr)
    assert {line.product.oid for line in s.navigate(second, "lines")} == {committed.oid}
    tr.abort()


def test_relate_then_update_keeps_the_old_price(db):
    """I1: a later field change in the same transaction does not move an earlier binding; the bindings after
    it share the first record carrying the new fields."""
    def build(tr):
        s = semantic(tr)
        company, customer, product = _world(s)
        return _order(s, company, customer, 1), product
    order, product = db.run(build)
    def sell(tr):
        s = semantic(tr)
        _line(s, order, product, "1")
        s.update(product, list_price=Decimal("12"))
        _line(s, order, product, "2")
        _line(s, order, product, "3")
    db.run(sell)
    tr = db.new_transaction()
    s = semantic(tr)
    bound = [line.product for line in s.navigate(order, "lines")]
    assert [v.list_price for v in bound] == [Decimal("10"), Decimal("12"), Decimal("12")]
    assert bound[1].oid == bound[2].oid != bound[0].oid
    assert s.get(product).list_price == Decimal("12")
    tr.abort()


def test_sales_by_order_date_use_the_index(db):
    def build(tr):
        s = semantic(tr)
        company, customer, product = _world(s)
        june = []
        for month in range(1, 13):
            order = _order(s, company, customer, month, day=date(2026, month, 10))
            for _ in range(2):
                line = _line(s, order, product)
                if month == 6:
                    june.append(line.oid)
        return product, june
    product, june = db.run(build)
    tr = db.new_transaction()
    s = semantic(tr)
    answer = s.query({"from": ["via", ["cell", product.name], "order_lines"], "where": JUNE})
    assert set(answer) == {"rows", "explain", "version", "query", "terms"}
    assert "by_order_date" in _indexes_used(answer["explain"])
    assert sorted(v.oid for v in answer["rows"]) == sorted(june)
    assert all(isinstance(v, Version) and type(v.record) is OrderLine for v in answer["rows"])
    assert answer["version"] == s.version()
    assert answer["query"]["where"] == JUNE
    assert {"type_name": LINE, "name": "order"} in [{k: t[k] for k in ("type_name", "name")} for t in answer["terms"]]
    # A traversal through the maintained-key end yields its members (row 44), and through them the orders.
    orders = s.query({"from": ["via", ["cell", product.name], "order_lines", "order"], "of_type": ORDER,
                      "where": ["==", ["attr", "date"], ["const", date(2026, 6, 10)]]})
    assert [v.number for v in orders["rows"]] == [6]
    assert all(isinstance(v.entity, MutableObject) for v in orders["rows"])
    assert s.query({"from": ["via", ["cell", product.name], "order_lines"], "aggregate": ["count"]})["rows"] == \
        [{"count": 24}]
    # The same answer as a residual scan of the members.
    scanned = [v for v in s.navigate(product, "order_lines") if date(2026, 6, 1) <= v.order.date <= date(2026, 6, 30)]
    assert sorted(v.oid for v in scanned) == sorted(june)
    tr.abort()


def test_changing_an_order_date_rekeys_its_lines(db):
    def build(tr):
        s = semantic(tr)
        company, customer, product = _world(s)
        june_order = _order(s, company, customer, 1, day=date(2026, 6, 5))
        kept = _order(s, company, customer, 2, day=date(2026, 6, 6))
        for order in (june_order, kept):
            _line(s, order, product)
            _line(s, order, product)
        return product, june_order, kept
    product, june_order, kept = db.run(build)
    query = {"from": ["via", ["cell", product.name], "order_lines"], "where": JUNE}
    rows = db.run(lambda tr: semantic(tr).query(query)["rows"])
    assert len(rows) == 4
    db.run(lambda tr: semantic(tr).update(june_order, date=date(2026, 7, 1)))
    tr = db.new_transaction()
    s = semantic(tr)
    answer = s.query(query)
    assert {v.order.entity for v in answer["rows"]} == {kept}
    keys = sorted(entry.key for entry in s.get(product).order_lines.collection)
    assert keys == [date(2026, 6, 6)] * 2 + [date(2026, 7, 1)] * 2
    july = s.query({"from": ["via", ["cell", product.name], "order_lines"],
                    "where": ["==", ["attr", "order", "date"], ["const", date(2026, 7, 1)]]})
    assert {v.order.entity for v in july["rows"]} == {june_order} and len(july["rows"]) == 2
    tr.abort()


# -- maintained aggregates ----------------------------------------------------------------------------------

def test_order_total_and_customer_committed_are_maintained_by_deltas(db, monkeypatch):
    tr = db.new_transaction()
    s = semantic(tr)
    company, customer, product = _world(s)
    first = _order(s, company, customer, 1)
    second = _order(s, company, customer, 2)
    assert s.get(first).record.total == 0 and s.get(customer).record.committed == 0     # stored, not computed

    def no_whole_collection(*args, **kwargs):
        raise AssertionError("a maintained aggregate was recomputed over its collection")
    monkeypatch.setattr(expressions, "_aggregate", no_whole_collection)
    _line(s, first, product, "2", "9.50")
    _line(s, first, product, "1", "3")
    _line(s, second, product, "4", "1.25")
    s.update(second, state="cancelled")                              # leaves committed by its where
    s.update(first, number=11)                                       # a change that moves no aggregate
    assert s.get(first).record.total == Decimal("22.00")
    assert s.get(second).record.total == Decimal("5.00")
    assert s.get(customer).record.committed == Decimal("22.00")
    s.update(second, state="open")
    assert s.get(customer).record.committed == Decimal("27.00")
    s.unlink(customer, "orders", second)
    assert s.get(customer).record.committed == Decimal("22.00")
    s.link(second, "customer", customer)                             # complete again before the commit (F28)
    assert s.get(customer).record.committed == Decimal("27.00")
    monkeypatch.undo()
    # The maintained values equal their definition.
    assert s.get(first).record.total == sum(v.amount for v in s.navigate(first, "lines"))
    tr.commit()
    tr = db.new_transaction()
    assert semantic(tr).get(customer).committed == Decimal("27.00")
    tr.abort()


def test_direct_mode_concurrent_lines_on_one_order_conflict(db):
    def build(tr):
        s = semantic(tr)
        company, customer, product = _world(s)
        return _order(s, company, customer, 1), product
    order, product = db.run(build)
    first, second = db.new_transaction(), db.new_transaction()
    _line(semantic(first), order, product)
    _line(semantic(second), order, product)
    first.commit()
    with pytest.raises(ProtoConflictError):
        second.commit()
    tr = db.new_transaction()
    assert len(semantic(tr).navigate(order, "lines")) == 1
    tr.abort()


def test_concurrent_link_and_unlink_from_both_ends_of_an_inverse(db):
    """Two transactions move one order from customer A: the first through the customers' ``orders`` ends,
    the second through the order's ``customer`` end.  Events, never sleeps, order the commits; the second
    conflicts and both ends agree afterwards."""
    def build(tr):
        s = semantic(tr)
        company, a, product = _world(s)
        b = s.create("acme.sales.Customer", name="B", credit_limit=Decimal("1000"), company=company)
        c = s.create("acme.sales.Customer", name="C", credit_limit=Decimal("1000"), company=company)
        return _order(s, company, a, 1), a, b, c
    order, a, b, c = db.run(build)
    staged = [threading.Event(), threading.Event()]
    first_committed = threading.Event()
    outcome = {}

    def from_customers():
        tr = db.new_transaction()
        s = semantic(tr)
        s.unlink(a, "orders", order)
        s.link(b, "orders", order)
        staged[0].set()
        assert staged[1].wait(60)
        tr.commit()
        first_committed.set()

    def from_order():
        tr = db.new_transaction()
        s = semantic(tr)
        s.unlink(order, "customer", a)
        s.link(order, "customer", c)
        staged[1].set()
        assert first_committed.wait(60)
        try:
            tr.commit()
            outcome["second"] = "committed"
        except ProtoConflictError:
            outcome["second"] = "conflict"

    threads = [threading.Thread(target=from_customers), threading.Thread(target=from_order)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(90)
    assert not any(thread.is_alive() for thread in threads)
    assert first_committed.is_set() and outcome == {"second": "conflict"}
    tr = db.new_transaction()
    s = semantic(tr)
    assert s.navigate(order, "customer").entity == b
    assert [v.entity for v in s.navigate(b, "orders")] == [order]
    assert len(s.navigate(a, "orders")) == 0 and len(s.navigate(c, "orders")) == 0
    tr.abort()


# -- subtypes (R280, Review Focus 6) --------------------------------------------------------------------

def test_role_typed_by_supertype_accepts_a_subtype(db):
    db.run(lambda tr: tr.install_module("acme_rush"))

    def build(tr):
        s = semantic(tr)
        company, customer, product = _world(s)
        plain = _order(s, company, customer, 1)
        rush = _order(s, company, customer, 2, type_name="acme.sales.RushOrder", priority=1)
        _line(s, plain, product, "1", "10")
        _line(s, rush, product, "3", "5")
        return customer, plain, rush
    customer, plain, rush = db.run(build)
    assert re.fullmatch(r"acme\.sales\.RushOrder#[0-9a-f]{32}", rush.name)
    tr = db.new_transaction()
    s = semantic(tr)
    assert {v.entity for v in s.navigate(customer, "orders")} == {plain, rush}
    assert type(s.get(rush).record).__type_name__ == "acme.sales.RushOrder"
    assert [v.amount for v in s.navigate(rush, "lines")] == [Decimal(15)]
    assert s.get(rush).record.total == Decimal(15)
    assert s.get(customer).record.committed == Decimal(25)
    assert s.get(rush).priority == 1
    tr.abort()


# -- the query engine -------------------------------------------------------------------------------------

def test_query_select_aggregate_and_residual(db):
    def build(tr):
        s = semantic(tr)
        company, customer, product = _world(s)
        order = _order(s, company, customer, 1)
        _line(s, order, product, "2", "3")
        _line(s, order, product, "5", "1")
        return customer, order
    customer, order = db.run(build)
    tr = db.new_transaction()
    s = semantic(tr)
    lines = {"from": ["via", ["cell", order.name], "lines"]}
    stored = [">", ["attr", "quantity"], ["const", Decimal(1)]]          # a stored field: the planner's
    computed = ["<", ["attr", "amount"], ["const", Decimal(6)]]          # a computed attribute: a residual
    rows = s.query({**lines, "where": ["and", stored, computed], "select": ["quantity", "amount"]})
    assert rows["rows"] == [{"quantity": Decimal(5), "amount": Decimal(5)}]
    assert rows["explain"]["indexed"] == [stored] and rows["explain"]["residual"] == [computed]
    state = s.query({"from": ["via", ["cell", customer.name], "orders"],
                     "where": ["==", ["attr", "state"], ["const", "open"]]})      # a default: a residual (F16)
    assert [v.entity for v in state["rows"]] == [order] and state["explain"]["residual"]
    assert s.query({**lines, "aggregate": ["sum", ["attr", "amount"]]})["rows"] == [{"sum": Decimal(11)}]
    assert s.query({**lines, "aggregate": ["count"]})["rows"] == [{"count": 2}]
    orders = s.query({"from": ["via", ["cell", customer.name], "orders", "lines"], "select": ["price"]})
    assert orders["rows"] == [{"price": Decimal(3)}, {"price": Decimal(1)}]
    with pytest.raises(ProtoValidationError):
        s.query({"from": ["via", ["cell", order.name], "lines"], "where": ["frobnicate"]})
    with pytest.raises(ProtoValidationError, match="unknown"):
        s.query({**lines, "limit": 3})
    tr.abort()


# -- other end shapes (acme_keyed) ------------------------------------------------------------------------

def _keyed(db):
    db.run(lambda tr: tr.install_module("acme_keyed"))


def _keys(s, shelf) -> list:
    return sorted((entry.key if entry.has_defined_attr("key") else "<none>", s.get(entry.member).weight)
                  for entry in s.get(shelf).items.collection)


def test_a_maintained_key_through_an_entity_role_is_rekeyed_by_link_and_update(db):
    _keyed(db)
    tr = db.new_transaction()
    s = semantic(tr)
    shelf = s.create("acme.keyed.Shelf", label="top")
    acme, other = s.create("acme.keyed.Maker", name="acme"), s.create("acme.keyed.Maker", name="other")
    light = s.create("acme.keyed.Item", weight=1, shelf=shelf, maker=acme)
    heavy = s.create("acme.keyed.Item", weight=5, shelf=shelf)              # no maker yet: no key
    assert _keys(s, shelf) == [("<none>", 5), ("acme", 1)]
    assert [v.entity for v in s.navigate(shelf, "items")] in ([light, heavy], [heavy, light])
    s.link(heavy, "maker", other)                                            # the member's role changes
    assert _keys(s, shelf) == [("acme", 1), ("other", 5)]
    s.update(acme, name="zeta")                                              # the keyed attribute changes
    assert _keys(s, shelf) == [("other", 5), ("zeta", 1)]
    s.unlink(heavy, "maker", other)
    assert _keys(s, shelf) == [("<none>", 5), ("zeta", 1)]
    found = s.query({"from": ["via", ["cell", shelf.name], "items"],
                     "where": ["==", ["attr", "maker", "name"], ["const", "zeta"]]})
    assert [v.entity for v in found["rows"]] == [light] and found["explain"]["indexed"]
    assert s.get(shelf).record.weight == 6 and s.get(shelf).record.count == 2
    s.unlink(shelf, "items", light)                                          # leaves the end: entry and deltas
    assert _keys(s, shelf) == [("<none>", 5)]
    assert s.get(shelf).record.weight == 5 and s.get(shelf).record.count == 1
    s.update(heavy, weight=7)                                                # a member changes: a delta
    assert s.get(shelf).record.weight == 7
    tr.abort()


def test_a_member_attribute_index_and_a_refused_shape(db):
    _keyed(db)
    tr = db.new_transaction()
    s = semantic(tr)
    shelf = s.create("acme.keyed.Shelf")
    maker = s.create("acme.keyed.Maker", name="m")
    items = [s.create("acme.keyed.Item", weight=w, shelf=shelf, maker=maker) for w in range(30)]
    for slot, item in enumerate(items):
        s.relate("acme.keyed.Placement", shelf=shelf, item=item, slot=slot)
    collection = s.get(shelf).placements.collection
    assert collection.index_names() == ["by_slot"]
    found = s.query({"from": ["via", ["cell", shelf.name], "placements"],
                     "where": ["==", ["attr", "slot"], ["const", 3]], "select": ["slot"]})
    assert found["rows"] == [{"slot": 3}] and "by_slot" in _indexes_used(found["explain"])
    assert [v.item.entity for v in s.navigate(items[3], "placements")] == [items[3]]
    with pytest.raises(ProtoNotSupportedError, match="entity handles"):
        s.link(maker, "tagged", items[0])
    assert s.navigate(maker, "tagged").collection is None                   # nothing was written
    tr.abort()
