"""The agent operations (spec section 11; Task 7; preflight F18, F19, F23 and row 45).

Catches: a list or a description that loses the label, the meaning or the provenance; a term reported as
answerable when nothing in the catalog names it, or a substring match counted as a match (S7); a query that
runs a second engine (no ``explain`` naming the index), reports another version than the one it read, or
reads the current version when ``as_of`` names an older one; ``of_type`` that drops subtypes or keeps
supertypes; a ``personal`` or ``secret`` attribute that reaches the agent through a row, a ``select``, a
``where`` or an aggregate (directly, through a role path or through a computed attribute); raw mode that
leaks the catalog (labels, meanings, provenance, reserved roots); values that are not JSON-ready.
"""
from __future__ import annotations

import base64
import json
from datetime import date
from decimal import Decimal

import pytest

from proto_db.agent import operations
from proto_db.agent.operations import check_answerable, describe, list_entities, query, to_json
from proto_db.errors import ProtoValidationError
from proto_db.semantic import semantic
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from proto_db.values import MutableObject
from tests.unit.semantic_modules import MAPPING

pytestmark = [pytest.mark.timeout(120)]

ORDER, LINE, CUSTOMER = "acme.sales.Order", "acme.sales.OrderLine", "acme.sales.Customer"
JUNE = ["and", [">=", ["attr", "order", "date"], ["const", date(2026, 6, 1)]],
        ["<=", ["attr", "order", "date"], ["const", date(2026, 6, 30)]]]


@pytest.fixture
def space():
    space = ObjectSpace(MemoryStorage(), modules_mapping=MAPPING)
    try:
        yield space
    finally:
        space.close()


@pytest.fixture
def db(space):
    database = space.new_database("db")
    database.run(lambda tr: tr.install_module("acme_sales"))
    return database


def _load(db, *, rush: bool = False) -> dict:
    """A company, two customers, two products and four orders (two in June), every ``one`` role set."""
    def build(tr):
        s = semantic(tr)
        company = s.create("acme.core.Company", name="ACME")
        bob = s.create(CUSTOMER, name="Bob", credit_limit=Decimal("1000"), company=company)
        ann = s.create(CUSTOMER, name="Ann", credit_limit=Decimal("100"), company=company)
        widget = s.create("acme.catalog.Product", name="widget", list_price=Decimal("10"), company=company)
        gadget = s.create("acme.catalog.Product", name="gadget", list_price=Decimal("25"), company=company)
        orders = []
        plan = [(1, bob, date(2026, 6, 3), [(widget, "2", "10"), (gadget, "1", "25")]),
                (2, ann, date(2026, 6, 20), [(widget, "5", "9")]),
                (3, bob, date(2026, 7, 1), [(widget, "7", "10")]),
                (4, ann, date(2026, 5, 31), [(gadget, "1", "25")])]
        for number, customer, day, lines in plan:
            order = s.create(ORDER, number=number, date=day, company=company, customer=customer)
            for product, quantity, price in lines:
                s.relate(LINE, order=order, product=product, quantity=Decimal(quantity), price=Decimal(price))
            orders.append(order)
        rushed = None
        if rush:
            rushed = s.create("acme.sales.RushOrder", number=5, date=date(2026, 6, 9), company=company,
                              customer=bob, priority=1)
        return {"company": company, "bob": bob, "ann": ann, "widget": widget, "gadget": gadget, "orders": orders,
                "rush": rushed}
    return db.run(build)


def _index_names(explain) -> set:
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


def _json_ready(value) -> None:
    """``value`` round-trips through ``json`` unchanged (only JSON types inside)."""
    assert json.loads(json.dumps(value)) == value


# -- list_entities and describe -------------------------------------------------------------------------

def test_list_entities_gives_label_and_meaning(db):
    world = _load(db)
    listed = list_entities(db)
    _json_ready(listed)
    assert set(listed) == {"entities", "next"} and listed["next"] is None
    by_name = {e["type_name"]: e for e in listed["entities"]}
    assert set(by_name) == {"acme.core.Company", CUSTOMER, "acme.catalog.Product", ORDER, LINE}
    assert by_name[ORDER]["label"] == {"und": "Order"}
    assert by_name[ORDER]["meaning"].startswith("A customer's order")
    assert by_name[LINE]["label"] is None and by_name[LINE]["meaning"].startswith("A product included")
    # where an agent starts: the cells of each entity type (a relationship's atoms have none)
    assert by_name["acme.core.Company"]["cells"] == [world["company"].name]
    assert sorted(by_name[CUSTOMER]["cells"]) == sorted([world["bob"].name, world["ann"].name])
    assert sorted(by_name[ORDER]["cells"]) == sorted(o.name for o in world["orders"])
    assert by_name[LINE]["cells"] == []
    purchase = list_entities(db, "purchase")                                       # by synonym
    assert [e["type_name"] for e in purchase["entities"]] == [ORDER] and len(purchase["entities"][0]["cells"]) == 4
    assert list_entities(db, "advertising") == {"entities": [], "next": None}


def test_list_entities_pages_its_cells(db):
    world = _load(db)
    expected = sorted([world["company"].name, world["bob"].name, world["ann"].name, world["widget"].name,
                       world["gadget"].name, *(o.name for o in world["orders"])])
    seen, after, pages = [], None, 0
    while True:
        page = list_entities(db, limit=3, after=after)
        _json_ready(page)
        cells = [c for e in page["entities"] for c in e["cells"]]
        assert len(cells) <= 3
        seen += cells
        pages += 1
        after = page["next"]
        if after is None:
            break
        db.run(lambda tr: semantic(tr).create("acme.core.Company", name=f"later {pages}"))   # pages read one version
    assert sorted(seen) == expected and pages == 3
    raw, after = [], None
    while True:
        page = list_entities(db, limit=4, after=after, raw=True)
        raw += page["cells"]
        after = page["next"]
        if after is None:
            break
    assert len(raw) == len(expected) + 2 and set(expected) <= set(raw)               # the two later companies
    with pytest.raises(ProtoValidationError):
        list_entities(db, limit=0)
    with pytest.raises(ProtoValidationError):
        list_entities(db, after="not a token")


def test_describe_includes_provenance(db):
    db.run(lambda tr: tr.install_module("acme_delivery"))
    order = describe(db, ORDER)
    _json_ready(order)
    elements = {e["name"]: e for e in order["attributes"] + order.get("roles", [])}
    assert order["kind"] == "entity" and order["meaning"].startswith("A customer's order")
    assert elements["delivery_date"]["provenance"] == ["acme_delivery"]
    assert elements["number"]["provenance"] == ["acme_sales", "acme_delivery"]
    assert elements["number"]["label"] == {"und": "Order no."}
    customer = describe(db, CUSTOMER)
    name = next(e for e in customer["attributes"] if e["name"] == "name")
    assert name["sensitivity"] == "personal"                     # describe lists a personal attribute
    with pytest.raises(ProtoValidationError, match="advertising"):
        describe(db, "acme.ads.advertising")


# -- check_answerable -----------------------------------------------------------------------------------

def test_advertising_is_not_answerable(db):
    answer = check_answerable(db, ["advertising", "spend"])
    _json_ready(answer)
    assert answer["answerable"] is False
    assert sorted(answer["unmatched"]) == ["advertising", "spend"]
    assert answer["matches"] == {"advertising": [], "spend": []}


def test_answerable_terms_match_by_name_label_and_synonym_case_folded(db):
    answer = check_answerable(db, ["Credit_Limit", "PURCHASE ORDER", "customer", "price"])
    assert answer["answerable"] is True and answer["unmatched"] == []
    assert {"kind": "attribute", "type_name": CUSTOMER, "name": "credit_limit", "via": "name"} \
        in answer["matches"]["Credit_Limit"]
    assert {"kind": "entity", "type_name": ORDER, "name": None, "via": "synonym"} in answer["matches"]["PURCHASE ORDER"]
    assert {"kind": "entity", "type_name": CUSTOMER, "name": None, "via": "name"} in answer["matches"]["customer"]
    assert {"kind": "role", "type_name": ORDER, "name": "customer", "via": "name"} in answer["matches"]["customer"]
    assert {"kind": "attribute", "type_name": LINE, "name": "price", "via": "name"} in answer["matches"]["price"]
    # "price" is part of "list_price": a suggestion, never a match (S7)
    assert all(c["name"] != "list_price" for c in answer["matches"]["price"])
    near = check_answerable(db, ["limit"])
    assert near["answerable"] is False and near["unmatched"] == ["limit"]
    assert {"kind": "attribute", "type_name": CUSTOMER, "name": "credit_limit", "via": "name"} \
        in near["suggestions"]["limit"]


# -- query ----------------------------------------------------------------------------------------------

def test_product_sales_in_june(db):
    world = _load(db)

    def other_months(tr):               # enough widget lines outside June for the planner to prefer the index
        s = semantic(tr)
        for number, month in enumerate((1, 2, 3, 4, 8, 9, 10, 11, 12), start=10):
            order = s.create(ORDER, number=number, date=date(2026, month, 15), company=world["company"],
                             customer=world["bob"])
            for _ in range(2):
                s.relate(LINE, order=order, product=world["widget"], quantity=Decimal(1), price=Decimal(10))
    db.run(other_months)
    tree ={"from": ["via", ["cell", world["widget"].name], "order_lines"], "where": JUNE,
            "select": ["quantity", "price"]}
    answer = query(db, tree)
    _json_ready(answer)
    assert set(answer) == {"rows", "explain", "version", "query", "terms"}
    assert sorted(row["values"]["quantity"] for row in answer["rows"]) == ["2", "5"]
    assert "by_order_date" in _index_names(answer["explain"])
    tr = db.new_transaction()
    snapshot = db.snapshot_at(tr.snapshot_ref)
    tr.abort()
    assert answer["version"] == {"number": snapshot.version, "ref": base64.b64encode(snapshot.ref).decode("ascii")}
    assert answer["query"]["where"][1][2] == ["const", "2026-06-01"]
    assert {"type_name": LINE, "name": "quantity", "kind": "field", "certification": None,
            "sensitivity": "internal"} in answer["terms"]
    total = query(db, {**tree, "select": None, "aggregate": ["sum", ["attr", "quantity"]]})
    assert total["rows"] == [{"sum": "7"}]


def test_closest_to_limit(db):
    world = _load(db)
    answer = query(db, {"from": ["via", ["cell", world["company"].name], "customers"],
                        "select": ["name", "committed", "credit_limit"]})
    rows = answer["rows"]
    assert len(rows) == 2
    # the declared post-step (preflight F19): argmin(credit_limit - committed), computed by the client
    closest = min(rows, key=lambda r: Decimal(r["values"]["credit_limit"]) - Decimal(r["values"]["committed"]))
    assert closest["entity"] == {"cell": world["ann"].name}
    assert closest["values"] == {"committed": "70", "credit_limit": "100"}       # name is personal: omitted


def test_query_as_of_reads_that_version(db):
    world = _load(db)
    before = query(db, {"from": ["via", ["cell", world["company"].name], "orders"], "aggregate": ["count"]})
    db.run(lambda tr: semantic(tr).create(ORDER, number=9, date=date(2026, 6, 1), company=world["company"],
                                          customer=world["bob"]))
    now = query(db, {"from": ["via", ["cell", world["company"].name], "orders"], "aggregate": ["count"]})
    assert (before["rows"], now["rows"]) == ([{"count": 4}], [{"count": 5}])
    assert now["version"]["number"] == before["version"]["number"] + 1
    then = query(db, {"from": ["via", ["cell", world["company"].name], "orders"], "aggregate": ["count"],
                      "as_of": before["version"]["ref"]})
    assert then["rows"] == [{"count": 4}] and then["version"] == before["version"]
    with pytest.raises(ProtoValidationError):
        query(db, {"from": ["via", ["cell", world["company"].name], "orders"], "as_of": "not base64!"})


def test_query_over_orders_includes_subtypes_and_of_type_narrows(db):
    db.run(lambda tr: tr.install_module("acme_rush"))
    world = _load(db, rush=True)
    orders = ["via", ["cell", world["bob"].name], "orders"]
    every = query(db, {"from": orders, "select": ["number"]})
    assert sorted(r["values"]["number"] for r in every["rows"]) == [1, 3, 5]          # the RushOrder included
    urgent = query(db, {"from": orders, "where": ["==", ["attr", "priority"], ["const", 1]], "select": ["number"]})
    assert [r["values"]["number"] for r in urgent["rows"]] == [5]                     # false for plain orders
    rushed = query(db, {"from": orders, "of_type": "acme.sales.RushOrder"})
    assert [r["entity"] for r in rushed["rows"]] == [{"cell": world["rush"].name}]
    assert rushed["rows"][0]["type_name"] == "acme.sales.RushOrder"
    assert rushed["rows"][0]["values"]["priority"] == 1
    plain = query(db, {"from": orders, "of_type": ORDER})
    assert len(plain["rows"]) == 3


def test_entity_rows_are_json_values_of_the_catalogued_attributes(db):
    world = _load(db)
    answer = query(db, {"from": ["via", ["cell", world["bob"].name], "orders"], "where":
                        ["==", ["attr", "number"], ["const", 1]]})
    _json_ready(answer)
    [row] = answer["rows"]
    assert row["entity"] == {"cell": world["orders"][0].name} and row["type_name"] == ORDER
    assert row["values"]["number"] == 1 and row["values"]["date"] == "2026-06-03"
    assert row["values"]["state"] == "open" and row["values"]["total"] == "45"
    assert row["values"]["customer"] == {"cell": world["bob"].name}
    assert "lines" not in row["values"]                    # a many end is reached with "from", not listed


def test_personal_attributes_are_omitted(db):
    world = _load(db)
    customers = ["via", ["cell", world["company"].name], "customers"]
    rows = query(db, {"from": customers})["rows"]
    assert len(rows) == 2 and all("name" not in r["values"] for r in rows)
    assert all("credit_limit" in r["values"] for r in rows)
    selected = query(db, {"from": customers, "select": ["name", "credit_limit"]})
    assert all(set(r["values"]) == {"credit_limit"} for r in selected["rows"])
    assert {"type_name": CUSTOMER, "name": "name", "kind": "field", "certification": None,
            "sensitivity": "personal", "withheld": True,
            "withheld_because": {"reads": {"type_name": CUSTOMER, "name": "name", "sensitivity": "personal"}}} \
        in selected["terms"]
    through = query(db, {"from": ["via", ["cell", world["company"].name], "orders"], "select": ["customer.name"]})
    assert all(r["values"] == {} for r in through["rows"])                 # a path to a personal attribute
    for tree in ({"from": customers, "where": ["==", ["attr", "name"], ["const", "Ann"]]},
                 {"from": ["via", ["cell", world["company"].name], "orders"],
                  "where": ["==", ["attr", "customer", "name"], ["const", "Ann"]]},
                 {"from": customers, "aggregate": ["sum", ["attr", "name"]]},
                 {"from": ["root", "anything"], "where": ["==", ["attr", "name"], ["const", "Ann"]]}):
        with pytest.raises(ProtoValidationError, match=r"personal.*'name'|'name'.*personal"):
            query(db, tree)


def test_raw_mode_hides_the_catalog(db):
    world = _load(db)
    db.run(lambda tr: tr.set_root_object("customers", tr.new_list().append_last(world["bob"])))
    listed = list_entities(db, raw=True)
    _json_ready(listed)
    assert set(listed) == {"roots", "cells", "next"}                       # root and cell names (spec 11)
    assert listed["roots"] == ["customers"]                                 # no reserved root
    assert {world["bob"].name, world["ann"].name, world["company"].name} <= set(listed["cells"])
    assert all(isinstance(name, str) for name in listed["cells"])
    text = json.dumps([listed, describe(db, ORDER, raw=True)])
    for leaked in ("meaning", "label", "provenance", "certification", "A customer's order", "__semanticCatalog",
                   "synonyms"):
        assert leaked not in text
    order = describe(db, ORDER, raw=True)
    assert order["type_name"] == ORDER and order["cells"] == 4
    assert {"number", "date", "customer", "company", "lines", "total"} <= set(order["attributes"])
    answer = query(db, {"from": ["via", ["cell", world["widget"].name], "order_lines"], "where": JUNE,
                        "select": ["quantity"]}, raw=True)
    _json_ready(answer)
    assert sorted(r["values"]["quantity"] for r in answer["rows"]) == ["2", "5"]
    assert answer["terms"] == [] and answer["explain"]["indexed"] == []      # an entry's key needs the catalog
    customers = query(db, {"from": ["via", ["cell", world["company"].name], "customers"]}, raw=True)
    assert all("name" not in r["values"] for r in customers["rows"])               # still withheld
    with pytest.raises(ProtoValidationError, match="not available"):
        query(db, {"from": ["root", "__semanticCatalog"]}, raw=True)
    for tree in ({"from": ["via", ["cell", world["company"].name], "customers"],
                  "where": ["==", ["attr", "name"], ["const", "Ann"]]},
                 {"from": ["via", ["cell", world["company"].name], "orders"], "select": ["customer.name"],
                  "aggregate": ["sum", ["attr", "customer", "name"]]}):
        with pytest.raises(ProtoValidationError) as refused:
            query(db, tree, raw=True)
        message = str(refused.value)
        assert "not available" in message                                  # generic: no catalog type or sensitivity
        assert "personal" not in message and "acme.sales.Customer" not in message and "sensitiv" not in message


def test_raw_and_catalog_modes_run_one_engine(db):
    world = _load(db)
    trees = [{"from": ["via", ["cell", world["widget"].name], "order_lines"], "where": JUNE,
              "select": ["quantity", "price", "order.number"]},
             {"from": ["via", ["cell", world["orders"][0].name], "lines"],
              "where": ["==", ["attr", "quantity"], ["const", Decimal(2)]], "select": ["quantity", "price"]},
             {"from": ["via", ["cell", world["company"].name], "customers"], "select": ["credit_limit", "committed"]},
             {"from": ["via", ["cell", world["company"].name], "orders", "lines"],
              "aggregate": ["sum", ["*", ["attr", "quantity"], ["attr", "price"]]]},
             {"from": ["via", ["cell", world["company"].name], "orders"], "aggregate": ["count"]}]
    for tree in trees:
        catalog, raw = query(db, tree), query(db, tree, raw=True)
        # the same rows; an index may give a Set's members in another order than a scan does
        assert sorted(map(json.dumps, catalog["rows"])) == sorted(map(json.dumps, raw["rows"])), tree
        assert catalog["rows"] and catalog["version"] == raw["version"]
    # raw mode plans over a stored collection of records, as the catalog mode does
    catalog, raw = query(db, trees[1]), query(db, trees[1], raw=True)
    assert raw["explain"]["plan"] is not None and raw["explain"]["indexed"] != []
    assert raw["explain"]["plan"] == catalog["explain"]["plan"]
    assert raw["explain"]["indexed"] == catalog["explain"]["indexed"]


def test_a_version_bound_role_keeps_the_version_it_binds(db):
    world = _load(db)
    db.run(lambda tr: semantic(tr).update(world["widget"], list_price=Decimal("12")))
    for raw in (False, True):
        rows = query(db, {"from": ["via", ["cell", world["orders"][0].name], "lines"]}, raw=raw)["rows"]
        product = rows[0]["values"]["product"]
        assert product["entity"] == {"cell": world["widget"].name}
        assert product["version"]["type_name"] == "acme.catalog.Product"
        assert product["version"]["values"]["list_price"] == "10"               # the product as sold
        assert product["version"]["values"]["name"] == "widget"


def test_a_root_origin_select_agrees_with_the_default_selection(db):
    world = _load(db)
    db.run(lambda tr: tr.set_root_object("starts", tr.new_list().append_last(world["company"])
                                         .append_last(world["bob"])))
    default = query(db, {"from": ["root", "starts"]})["rows"]
    selected = query(db, {"from": ["root", "starts"], "select": ["name"]})["rows"]
    by_type = {row["type_name"]: row["values"] for row in default}
    assert by_type["acme.core.Company"]["name"] == "ACME" and "name" not in by_type[CUSTOMER]
    assert {row["type_name"]: row["values"] for row in selected} == {"acme.core.Company": {"name": "ACME"},
                                                                     CUSTOMER: {}}


def test_to_json():
    assert to_json(Decimal("10.50")) == "10.50"
    assert to_json(date(2026, 6, 1)) == "2026-06-01"
    assert to_json(MutableObject("acme.sales.Order#" + "0" * 32)) == {"cell": "acme.sales.Order#" + "0" * 32}
    assert to_json({"a": [1, True, None, "x"]}) == {"a": [1, True, None, "x"]}
    assert operations.from_json({"decimal": "10.50"}) == Decimal("10.50")
    assert operations.from_json({"date": "2026-06-01"}) == date(2026, 6, 1)
    assert operations.from_json({"cell": "c"}) == MutableObject("c")


# -- fix round 2: withheld paths never evaluated; paging; raw planning -------------------------------------

SECRET, PUBLIC = "Ann Secret", "Public Org Name"


@pytest.fixture
def leaky(space):
    from tests.unit.semantic_modules import acme_leaky
    database = space.new_database("leaky")
    database.run(lambda tr: tr.install_module("acme_leaky"))

    def build(tr):
        s = semantic(tr)
        org = s.create("leak.Org", name=PUBLIC)
        s.create("leak.Person", name=SECRET, org=org)
        return org
    org = database.run(build)
    acme_leaky.CALLS.clear()
    return database, org, acme_leaky.CALLS


@pytest.mark.parametrize("raw", [False, True])
def test_a_withheld_path_is_never_evaluated(leaky, raw):
    db, org, calls = leaky
    people = ["via", ["cell", org.name], "people"]
    for select in (["bad"], ["py"], ["name"], ["bad", "py", "name", "org"], None):
        tree = {"from": people} if select is None else {"from": people, "select": select}
        answer = query(db, tree, raw=raw)
        assert len(answer["rows"]) == 1
        assert not set(answer["rows"][0]["values"]) & {"bad", "py", "name"}, (select, answer["rows"])
        assert SECRET not in json.dumps(answer)
    assert calls == []                                   # the withheld Python-only computed never ran


def test_a_failing_selected_path_is_reported_without_its_value(leaky):
    db, org, _ = leaky
    with pytest.raises(ProtoValidationError) as refused:      # Org.oops is "name + 1" over a public str
        query(db, {"from": ["via", ["cell", org.name], "people"], "select": ["org.oops"]})
    message = str(refused.value)
    assert "'org.oops'" in message and PUBLIC not in message and SECRET not in message
    assert refused.value.__cause__ is None


def test_raw_roots_are_paged_like_cells(db):
    world = _load(db)

    def roots(tr):
        for name in ("r1", "r2", "r3"):
            tr.set_root_object(name, tr.new_list().append_last(world["bob"]))
    db.run(roots)
    seen_roots, seen_cells, after = [], [], None
    while True:
        page = list_entities(db, limit=2, after=after, raw=True)
        assert len(page["roots"]) + len(page["cells"]) <= 2
        seen_roots += page["roots"]
        seen_cells += page["cells"]
        after = page["next"]
        if after is None:
            break
    assert seen_roots == ["r1", "r2", "r3"] and len(seen_cells) == 9


def test_paging_does_not_sort_every_name_per_page(db, monkeypatch):
    world = _load(db)
    from proto_db.transactions import Snapshot

    def no_sort(self):
        raise AssertionError("a page re-sorted every cell name")
    monkeypatch.setattr(Snapshot, "mutable_names", property(no_sort))
    monkeypatch.setattr(Snapshot, "root_names", property(no_sort))
    pages, after = 0, None
    while True:
        page = list_entities(db, limit=2, after=after)
        pages += 1
        after = page["next"]
        if after is None:
            break
    assert pages == 5
    assert list_entities(db, limit=50, raw=True)["cells"]


def test_raw_planning_reads_the_kind_of_the_whole_collection(db):
    from proto_db.core.dbobject import DBObject

    def build(tr):
        target = tr.new_mutable("target", DBObject(x=1))
        items = tr.new_list().append_last(DBObject(x=1)).append_last(target)
        tr.new_mutable("holder", DBObject(items=items))
    db.run(build)
    answer = query(db, {"from": ["via", ["cell", "holder"], "items"], "where": ["==", ["attr", "x"], ["const", 1]]},
                   raw=True)
    assert len(answer["rows"]) == 2                       # the handle is followed, as a residual follows it
    assert answer["explain"]["indexed"] == []


# -- fix round 3: no value through an error; term sensitivity is the element's own ---------------------------

def _person_cell(db, org) -> str:
    [row] = query(db, {"from": ["via", ["cell", org.name], "people"], "select": ["org"]})["rows"]
    return row["entity"]["cell"]


def _never_discloses(db, tree, raw):
    """Run ``tree``: an answer, or a failure, that holds no personal value (nor a chained exception that does)."""
    try:
        answer = query(db, tree, raw=raw)
    except ProtoValidationError as error:
        assert SECRET not in str(error), (tree, str(error))
        assert error.__cause__ is None and error.__context__ is None, tree
        return error
    assert SECRET not in json.dumps(answer), tree
    return None


@pytest.mark.parametrize("raw", [False, True])
def test_an_evaluation_failure_never_carries_a_value_to_the_agent(leaky, raw):
    db, org, _ = leaky
    person = _person_cell(db, org)

    def everyone(tr):
        tr.set_root_object("everyone", tr.new_list().append_last(tr.get_mutable(person)))
    db.run(everyone)
    people = ["via", ["cell", org.name], "people"]
    to_org = ["via", ["cell", person], "org"]
    failing = {
        "where via +": ({"from": people, "where": ["==", ["+", ["via", ["cell", org.name], "people"], ["const", 1]],
                                                     ["const", 1]]}, "'where'"),
        "where root +": ({"from": people, "where": ["==", ["+", ["root", "everyone"], ["const", 1]], ["const", 1]]},
                         "'where'"),
        "sum via": ({"from": to_org, "aggregate": ["sum", ["via", ["self"], "people"]]}, "aggregate"),
        "where via truth": ({"from": people, "where": ["via", ["cell", person], "org"]}, "'where'"),
    }
    for label, (tree, part) in failing.items():
        error = _never_discloses(db, tree, raw)
        assert error is not None, label
        assert part in str(error) and "ProtoValidationError" in str(error), (label, str(error))
    for tree in ({"from": to_org, "where": [">", ["via", ["self"], "people"], ["const", 1]]},
                 {"from": ["root", "everyone"], "select": ["org"]}):
        _never_discloses(db, tree, raw)


def test_the_evaluator_describes_a_record_or_a_collection_by_kind(leaky):
    db, org, _ = leaky
    session = semantic(db.snapshot_at(db.history()[0]))
    tree = {"from": ["via", ["cell", org.name], "people"],
            "where": ["==", ["+", ["via", ["cell", org.name], "people"], ["const", 1]], ["const", 1]]}
    with pytest.raises(ProtoValidationError) as refused:
        session.query(tree)
    assert SECRET not in str(refused.value) and "a collection (tuple)" in str(refused.value)
    from proto_db.semantic.expressions import describe_operand
    record = db.snapshot_at(db.history()[0]).get_mutable(_person_cell(db, org))
    assert describe_operand(record) == "a record of type leak.Person"
    assert describe_operand((record,)) == "a collection (tuple)" and describe_operand([record]) == "a collection (list)"
    assert describe_operand(MutableObject("leak.Person#1")) == "an identity handle"
    assert describe_operand(Decimal("1.5")) == "a Decimal" and describe_operand("text") == "a str"
    assert describe_operand(7) == "an int" and describe_operand(None) == "None"


def test_a_term_carries_its_own_sensitivity_and_why_it_was_withheld(leaky):
    from proto_db.semantic.descriptors import SENSITIVITIES
    db, org, _ = leaky
    answer = query(db, {"from": ["via", ["cell", org.name], "people"], "select": ["bad", "py", "name", "org"]})
    terms = {term["name"]: term for term in answer["terms"] if term["type_name"] == "leak.Person"}
    assert all(term["sensitivity"] in SENSITIVITIES for term in answer["terms"]), answer["terms"]
    reads = {"reads": {"type_name": "leak.Person", "name": "name", "sensitivity": "personal"}}
    assert terms["name"]["sensitivity"] == "personal" and terms["name"]["withheld_because"] == reads
    assert terms["bad"]["sensitivity"] == "internal" and terms["bad"]["withheld"] is True
    assert terms["bad"]["withheld_because"] == reads
    assert terms["py"]["sensitivity"] == "internal" and terms["py"]["withheld_because"] == {"python_only": True}
    assert terms["org"]["sensitivity"] == "internal" and "withheld" not in terms["org"]
    assert "withheld_because" not in terms["org"]


@pytest.mark.parametrize("raw", [False, True])
def test_a_storage_failure_during_evaluation_propagates_unchanged(leaky, monkeypatch, raw):
    from proto_db.semantic import entities
    db, org, _ = leaky
    people = ["via", ["cell", org.name], "people"]

    def broken(*args, **kwargs):
        raise OSError("the object store is unreachable")
    monkeypatch.setattr(entities, "evaluate", broken)
    for tree in ({"from": people, "select": ["org"]}, {"from": people, "where": ["==", ["attr", "org"], ["const", 1]]},
                 {"from": people, "aggregate": ["sum", ["const", 1]]}):
        with pytest.raises(OSError) as failed:              # the kind is kept, the message is the agent's own
            query(db, tree, raw=raw)
        assert type(failed.value) is OSError and "unreachable" not in str(failed.value)
        assert "OSError" in str(failed.value)


# -- fix round 4: no stored value through any error of any agent operation -------------------------------------

#: Every stored scalar of the sentinel fixture holds this marker; no error, explain or terms may show it.
MARK = "SNTNL"


def _sentinels():
    count = iter(range(1000))
    return lambda: f"{MARK}{next(count):03d}"


@pytest.fixture
def sentinel_db(space):
    """A catalogued database (``acme_leaky``) whose every stored scalar is a unique sentinel string: the
    fields of its entities, the items of its root lists and sets, a root that is a scalar, the keys and values
    of a root dictionary, and the fields of an uncatalogued record cell."""
    from proto_db.core.dbobject import DBObject
    database = space.new_database("sentinel")
    database.run(lambda tr: tr.install_module("acme_leaky"))
    s = _sentinels()

    def build(tr):
        session = semantic(tr)
        org = session.create("leak.Org", name=s())
        person = session.create("leak.Person", name=s(), org=org)
        loose = DBObject(x=s(), note=s())
        tr.set_root_object("ssns", tr.new_list().append_last(s()).append_last(s()))
        tr.set_root_object("scalar", s())
        tr.set_root_object("mixed", tr.new_list().append_last(tr.get_mutable(person.name)).append_last(loose)
                           .append_last(s()))
        tr.set_root_object("nested", tr.new_list().append_last(tr.new_list().append_last(s())))
        tr.set_root_object("tags", tr.new_set().add(s()))
        tr.set_root_object("dict", tr.new_dictionary().set_at(s(), s()))
        tr.new_mutable("holder", DBObject(note=s(), items=tr.new_list().append_last(DBObject(x=s())),
                                          tags=tr.new_list().append_last(s())))
        return org.name, person.name
    org, person = database.run(build)
    return database, org, person


def _no_mark_in_error(error: BaseException, label) -> None:
    """Neither ``error`` nor any exception chained to it shows a sentinel."""
    seen = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        assert MARK not in str(error) and MARK not in repr(error.args), (label, type(error).__name__, str(error))
        error = error.__cause__ or error.__context__


def _sentinel_trees(org: str, person: str) -> list:
    froms = [["root", "ssns"], ["root", "scalar"], ["root", "mixed"], ["root", "nested"], ["root", "tags"],
             ["root", "dict"], ["via", ["root", "scalar"], "org"], ["via", ["root", "ssns"], "org"],
             ["via", ["root", "mixed"], "org"], ["via", ["root", "nested"], "x"], ["via", ["root", "dict"], "x"],
             ["via", ["cell", org], "people"], ["via", ["cell", org], "name"], ["via", ["cell", person], "org"],
             ["via", ["cell", person], "org", "name"], ["via", ["cell", "holder"], "note"],
             ["via", ["cell", "holder"], "items"], ["via", ["cell", "holder"], "tags"],
             ["via", ["cell", "holder"], "items", "x"]]
    wheres = [["attr", "x"], ["attr", "note"], ["not", ["attr", "note"]], ["and", ["attr", "x"], ["const", True]],
              ["==", ["+", ["attr", "x"], ["const", 1]], ["const", 1]],
              ["==", ["*", ["attr", "note"], ["attr", "x"]], ["const", 1]],
              ["<", ["attr", "x"], ["root", "ssns"]], [">", ["attr", "x"], ["const", 1]],
              ["==", ["count", ["root", "ssns"]], ["const", 1]], ["==", ["count", ["root", "scalar"]], ["const", 1]],
              ["==", ["sum", ["root", "mixed"], ["attr", "x"]], ["const", 1]],
              ["via", ["root", "scalar"], "org"], ["==", ["attr", "oops"], ["const", 1]],
              ["==", ["-", ["via", ["root", "ssns"], "x"], ["const", 1]], ["const", 0]],
              ["==", ["count", ["via", ["self"], "org"], ["attr", "oops"]], ["const", 1]]]
    aggregates = [["count"], ["sum", ["attr", "x"]], ["sum", ["attr", "note"]], ["sum", ["root", "scalar"]],
                  ["sum", ["count", ["root", "ssns"]]], ["sum", ["attr", "oops"]],
                  ["sum", ["via", ["root", "ssns"], "x"]], ["sum", ["+", ["attr", "x"], ["root", "dict"]]]]
    selects = [["x"], ["note"], ["oops"], ["org.oops"], ["org.name"], ["x.y"], ["items"], ["tags"]]
    trees = []
    for origin in froms:
        trees.append({"from": origin})
        trees += [{"from": origin, "where": where} for where in wheres]
        trees += [{"from": origin, "aggregate": aggregate} for aggregate in aggregates]
        trees += [{"from": origin, "select": select} for select in selects]
        trees.append({"from": origin, "of_type": "leak.Org", "where": wheres[4], "select": ["oops"]})
    return trees


@pytest.mark.parametrize("raw", [False, True])
def test_no_agent_operation_shows_a_stored_value_through_an_error_explain_or_terms(sentinel_db, raw):
    db, org, person = sentinel_db
    failures = 0
    for tree in _sentinel_trees(org, person):
        try:
            answer = query(db, tree, raw=raw)
        except Exception as error:      # noqa: BLE001 -- every failure, whatever its kind, is checked
            failures += 1
            _no_mark_in_error(error, tree)
            continue
        assert MARK not in json.dumps(answer["explain"]) and MARK not in json.dumps(answer["terms"]), tree
    assert failures > 100                               # the battery reaches the failing paths
    for type_name in ("leak.Org", "leak.Person", "DBObject", "no.Such", 5, None):
        try:
            describe(db, type_name, raw=raw)
        except Exception as error:      # noqa: BLE001
            _no_mark_in_error(error, type_name)
    for terms in (["name", "x", "note", "org"], ["zz"], [MARK.lower()], "x", [5]):
        try:
            check_answerable(db, terms, raw=raw)
        except Exception as error:      # noqa: BLE001
            _no_mark_in_error(error, terms)
    for filter_ in (None, "leak", "x", "holder"):
        after = None
        while True:
            page = list_entities(db, filter_, limit=1, after=after, raw=raw)
            after = page["next"]
            if after is None:
                break
    for bad in ({"limit": 0}, {"after": "bm90IGEgdG9rZW4="}, {"filter": 5}):
        try:
            list_entities(db, **bad, raw=raw)
        except Exception as error:      # noqa: BLE001
            _no_mark_in_error(error, bad)


@pytest.mark.parametrize("raw", [False, True])
def test_a_root_of_strings_fails_the_from_by_type_only(sentinel_db, raw):
    db, _, _ = sentinel_db
    for origin in (["root", "ssns"], ["via", ["root", "scalar"], "org"]):
        for tree in ({"from": origin}, {"from": origin, "aggregate": ["count"]}):
            with pytest.raises(ProtoValidationError) as refused:
                query(db, tree, raw=raw)
            message = str(refused.value)
            assert MARK not in message, message
            assert "the query's 'from'" in message and "ProtoValidationError" in message, message
            assert "a str" in message, message
            assert refused.value.__cause__ is None and refused.value.__context__ is None


def test_the_engine_describes_a_role_holding_a_scalar_by_its_type(sentinel_db):
    db, _, _ = sentinel_db
    session = semantic(db.snapshot_at(db.history()[0]))
    with pytest.raises(ProtoValidationError) as refused:
        session.query({"from": ["root", "ssns"]})
    message = str(refused.value)
    assert MARK not in message and message.endswith("not a str"), message


@pytest.mark.parametrize("raw", [False, True])
def test_a_scrubbed_failure_names_the_part_the_kind_and_the_operand_type(sentinel_db, raw):
    db, org, person = sentinel_db
    cases = [({"from": ["via", ["cell", "holder"], "items"], "where": ["==", ["+", ["attr", "x"], ["const", 1]],
                                                                       ["const", 1]]}, "the query's 'where'", "a str"),
             ({"from": ["via", ["cell", "holder"], "items"], "aggregate": ["sum", ["attr", "x"]]},
              "the query's aggregate", "a str"),
             ({"from": ["via", ["cell", "holder"], "items"], "where": ["attr", "x"]}, "the query's 'where'", "a str"),
             ({"from": ["via", ["cell", "holder"], "note"]}, "the query's 'from'", "a str")]
    for tree, part, operand in cases:
        with pytest.raises(ProtoValidationError) as refused:
            query(db, tree, raw=raw)
        message = str(refused.value)
        assert MARK not in message and part in message and operand in message, (tree, message)


@pytest.mark.parametrize("raw", [False, True])
def test_a_mistake_in_the_agents_own_input_is_still_explained(sentinel_db, raw):
    db, _, _ = sentinel_db
    with pytest.raises(ProtoValidationError, match="no.Such"):
        describe(db, "no.Such", raw=raw)
    with pytest.raises(ProtoValidationError, match="'from'"):
        query(db, {"from": ["attr", "x"]}, raw=raw)
    if not raw:
        with pytest.raises(ProtoValidationError, match="no.Such"):
            query(db, {"from": ["root", "ssns"], "of_type": "no.Such"})


def test_raw_planning_skips_the_element_scan_when_no_conjunct_could_plan(db, monkeypatch):
    from proto_db.core.dbobject import DBObject
    from proto_db.semantic import entities

    def build(tr):
        items = tr.new_list()
        for x in range(5):
            items = items.append_last(DBObject(x=x))
        tr.new_mutable("holder", DBObject(items=items))
    db.run(build)
    scans = []
    scan = entities._holds_records_only

    def counted(collection):
        scans.append(collection.count)
        return scan(collection)
    monkeypatch.setattr(entities, "_holds_records_only", counted)
    items = ["via", ["cell", "holder"], "items"]
    answer = query(db, {"from": items, "where": ["==", ["+", ["attr", "x"], ["const", 1]], ["const", 3]]}, raw=True)
    assert len(answer["rows"]) == 1 and answer["explain"]["indexed"] == []
    assert scans == []                                    # no conjunct could plan: the elements are not scanned
    answer = query(db, {"from": items, "where": ["==", ["attr", "x"], ["const", 2]]}, raw=True)
    assert len(answer["rows"]) == 1 and len(answer["explain"]["indexed"]) == 1
    assert scans == [5]                                   # a plannable conjunct: one scan of the whole collection


@pytest.mark.parametrize("raw", [False, True])
def test_a_member_that_is_a_stored_collection_is_a_row_without_values(sentinel_db, raw):
    db, _, _ = sentinel_db
    for root in ("nested", "dict"):
        [row] = query(db, {"from": ["root", root]}, raw=raw)["rows"]
        assert row["entity"] is None and row["values"] == {}, row
    from proto_db.collections.lists import List
    from proto_db.semantic.expressions import describe_operand
    assert describe_operand(List()) == "a collection (List)"


# -- fix round 5: storage failures keep their kind, never their message; the operator's log keeps it --------

def _corrupt_store(monkeypatch, marker: str):
    """A database whose cell ``holder`` holds a tuple field that the store answers malformed (its preview is
    ``marker``), reopened over the same storage so that every read decodes it again."""
    from proto_db.core.dbobject import DBObject
    storage = MemoryStorage()
    first = ObjectSpace(storage, modules_mapping=MAPPING)
    database = first.new_database("d")
    database.run(lambda tr: tr.install_module("acme_leaky"))
    database.run(lambda tr: tr.new_mutable("holder", DBObject(t=(marker,), n=1)))
    original = MemoryStorage.get_atom

    def get_atom(self, pointer):
        def damaged(value):
            if isinstance(value, dict):
                if set(value) == {"@tup"} and isinstance(value["@tup"], list) and marker in value["@tup"]:
                    return {"@tup": marker}
                return {key: damaged(item) for key, item in value.items()}
            if isinstance(value, list):
                return [damaged(item) for item in value]
            return value
        return damaged(original(self, pointer))
    monkeypatch.setattr(MemoryStorage, "get_atom", get_atom)
    second = ObjectSpace(storage, modules_mapping=MAPPING)
    return first, second, second.open_database("d")


def _logged(caplog, level: str, kind):
    """The ``proto_db.agent`` records at ``level`` that carry an original failure of ``kind``."""
    return [r for r in caplog.records if r.name == "proto_db.agent" and r.levelname == level
            and r.exc_info and isinstance(r.exc_info[1], kind)]


@pytest.mark.parametrize("raw", [False, True])
def test_a_corrupt_store_reaches_the_agent_by_its_kind_only(monkeypatch, caplog, raw):
    import logging
    from proto_db.errors import ProtoCorruptionError
    marker = f"{MARK}-corrupt-123-45-6789"
    first, second, db = _corrupt_store(monkeypatch, marker)
    try:
        calls = [lambda: query(db, {"from": ["via", ["cell", "holder"], "t"]}, raw=raw)]
        if raw:
            calls += [lambda: describe(db, "DBObject", raw=True), lambda: check_answerable(db, ["t"], raw=True)]
        for call in calls:
            caplog.clear()
            with caplog.at_level(logging.WARNING, logger="proto_db.agent"):
                with pytest.raises(ProtoCorruptionError) as failed:
                    call()
            message = str(failed.value)
            assert MARK not in message and "ProtoCorruptionError" in message, message
            assert failed.value.__cause__ is None and failed.value.__context__ is None
            [record] = _logged(caplog, "ERROR", ProtoCorruptionError)
            assert marker in str(record.exc_info[1])           # the operator keeps the diagnosis
    finally:
        second.close()
        first.close()


@pytest.mark.parametrize("raw", [False, True])
def test_a_lookup_failure_is_scrubbed_for_the_agent_and_logged_for_the_operator(leaky, monkeypatch, caplog, raw):
    import logging
    from proto_db.semantic import entities
    db, org, _ = leaky

    def buggy(*args, **kwargs):
        raise KeyError(MARK)
    monkeypatch.setattr(entities, "evaluate", buggy)
    with caplog.at_level(logging.WARNING, logger="proto_db.agent"):
        with pytest.raises(ProtoValidationError) as failed:
            query(db, {"from": ["via", ["cell", org.name], "people"], "select": ["org"]}, raw=raw)
    assert MARK not in str(failed.value) and "KeyError" in str(failed.value)
    [record] = _logged(caplog, "WARNING", KeyError)
    assert MARK in str(record.exc_info[1])


def test_raw_describe_of_cells_holding_non_records(sentinel_db):
    db, _, _ = sentinel_db

    def build(tr):
        tr.new_mutable("text", f"{MARK}-text")
        tr.new_mutable("items", tr.new_list().append_last(f"{MARK}-item"))
    db.run(build)
    for type_name in ("str", "proto_db.collections.List"):
        answer = describe(db, type_name, raw=True)
        assert answer["cells"] == 1 and answer["attributes"] == {} and MARK not in json.dumps(answer), answer


@pytest.mark.parametrize("raw", [False, True])
def test_a_malformed_after_token_is_explained(sentinel_db, raw):
    db, _, _ = sentinel_db
    ref = list_entities(db, limit=1, raw=raw)["next"]
    snapshot_ref = json.loads(base64.b64decode(ref))["ref"]
    for name in ("", "\ud800"):
        token = base64.b64encode(json.dumps({"ref": snapshot_ref, "kind": "cell", "after": name}).encode()).decode()
        with pytest.raises(ProtoValidationError, match="'next' token"):
            list_entities(db, raw=raw, after=token)


def test_a_version_bound_reference_to_a_collection_is_a_row_value(db):
    from proto_db.core.dbobject import DBObject

    def build(tr):
        target = tr.new_mutable("target", DBObject(x=1))
        bound = {"entity": target, "version": tr.new_list().append_last(f"{MARK}-bound")}
        tr.set_root_object("h", tr.new_list().append_last(DBObject(ref=bound)))
    db.run(build)
    for tree in ({"from": ["root", "h"]}, {"from": ["root", "h"], "select": ["ref"]}):
        [row] = query(db, tree, raw=True)["rows"]
        assert row["values"]["ref"]["entity"] == {"cell": "target"}, row
        assert row["values"]["ref"]["version"]["values"] == {} and MARK not in json.dumps(row), row
