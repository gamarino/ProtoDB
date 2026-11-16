#!/usr/bin/env python3
"""The semantic-model demo of proto_db: a mini ERP whose database describes itself (spec section 3).

Run from the repository root::

    python docs/kit/semantic/semantic_demo.py                 # runs straight through
    python docs/kit/semantic/semantic_demo.py --workdir DIR   # keeps the database in DIR

Step 1: the database describes itself.  Module ``acme_sales`` is installed; the catalog lists the entities
and says what an order is, what a line is, and what "price" means on a line (the agreed price, not the
current one).
Step 2: a module extends another.  ``acme_delivery`` adds the delivery date to the order and redefines the
label of the order number; the catalog says which module defined each field.
Step 3: relations navigable from both sides.  Orders with several lines are loaded.  From an order its
products are reached; from a product, its sales by date through its own index, with no global structure;
an invoice shows each product as it was when sold.

Steps 1 and 2 print the catalog through the semantic session (``semantic(tr).describe(...)``); the agent
interface (MCP) that will ask the same questions arrives with a later task, as do steps 4 to 6.

The demo checks what it shows.  Each "[check]" line is an assertion that held; a failed check stops the
demo with exit status 1.  It ends with "every check passed" only when all of them held.
"""
import argparse
import os
import shutil
import sys
import tempfile
from datetime import date
from decimal import Decimal

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, REPO)        # the canonical import path of the demo's modules is docs.kit.semantic (F41)

from proto_db import FileStorage, ObjectSpace  # noqa: E402
from proto_db.semantic import semantic  # noqa: E402

#: Registry module name -> import path of its code (the runtime's modules_mapping).
MAPPING = {"acme_sales": "docs.kit.semantic.acme_sales", "acme_delivery": "docs.kit.semantic.acme_delivery"}
ORDER, LINE = "acme.sales.Order", "acme.sales.OrderLine"
CHECKS = []


def check(condition: bool, what: str) -> None:
    """Assert ``condition`` and print it as a check that held."""
    if not condition:
        raise SystemExit(f"[FAILED] {what}")
    CHECKS.append(what)
    print(f"   [check] {what}")


def say(text: str = "") -> None:
    print(text, flush=True)


def title(text: str) -> None:
    say()
    say(text)
    say("=" * len(text))


def _text(value) -> str:
    """A label or help (a language map) as plain text."""
    if isinstance(value, dict):
        return value.get("und") or next(iter(value.values()), "")
    return "" if value is None else str(value)


def _element(described: dict, name: str) -> dict:
    return next(e for e in described.get("attributes", []) + described.get("roles", []) if e["name"] == name)


def _print_entity(described: dict) -> None:
    say(f"   {described['type_name']} ({described['kind']}): {described.get('meaning', '')}")
    for element in described.get("attributes", []) + described.get("roles", []):
        if "target" in element:
            what = f"role -> {element['target']} ({element['cardinality']})"
            if element.get("binding") == "version":
                what += ", bound by version"
        else:
            what = element["kind"] + (f" {element['value_type']}" if "value_type" in element else "")
            if "expression" in element:
                what += f" = {element['expression']['ast']}"
        label = f" \"{_text(element['label'])}\"" if "label" in element else ""
        meaning = f" -- {element['meaning']}" if "meaning" in element else ""
        say(f"     {element['name']}{label}: {what}; from {', '.join(element['provenance'])}{meaning}")


# -- step 1 ----------------------------------------------------------------------------------------------------

def step_1(db) -> None:
    db.run(lambda tr: tr.install_module("acme_sales"))
    tr = db.new_transaction()
    try:
        session = semantic(tr)
        say("list_entities():")
        listed = session.list_entities()
        for entity in listed:
            say(f"   {entity['type_name']}: {_text(entity['label']) or '-'}, {entity['meaning']}")
        check({e["type_name"] for e in listed} == {"acme.core.Company", "acme.sales.Customer", "acme.catalog.Product",
                                                   ORDER, LINE}, "the catalog lists the five entities of acme_sales")
        say("describe('acme.sales.Order'):")
        order = session.describe(ORDER)
        _print_entity(order)
        check(order["meaning"].startswith("A customer's order"), "an order says what it is")
        say("describe('acme.sales.OrderLine'):")
        line = session.describe(LINE)
        _print_entity(line)
        check(line["kind"] == "relationship" and {r["name"] for r in line["roles"]} == {"order", "product"},
              "a line is a relationship between an order and a product")
        price = _element(line, "price")
        check("agreed" in price["meaning"] and "not the product's current price" in price["meaning"],
              "'price' on a line means the agreed price, not the current one")
        check(_element(line, "product").get("binding") == "version",
              "a line's product is bound by version: the product as sold")
    finally:
        tr.abort()


# -- step 2 ----------------------------------------------------------------------------------------------------

def step_2(db) -> None:
    db.run(lambda tr: tr.install_module("acme_delivery"))
    tr = db.new_transaction()
    try:
        order = semantic(tr).describe(ORDER)
        say("describe('acme.sales.Order') after installing acme_delivery:")
        _print_entity(order)
        delivery = _element(order, "delivery_date")
        check(delivery["provenance"] == ["acme_delivery"], "acme_delivery defined delivery_date")
        number = _element(order, "number")
        check(number["provenance"] == ["acme_sales", "acme_delivery"] and _text(number["label"]) == "Order no.",
              "acme_sales defined number, and acme_delivery redefined its label to 'Order no.'")
        check(_element(order, "total")["provenance"] == ["acme_sales"], "acme_sales defined total, unmodified")
        check(set(tr.installed_modules()) >= {"acme_sales", "acme_delivery"}, "both modules are installed")
    finally:
        tr.abort()


# -- step 3 ----------------------------------------------------------------------------------------------------

PRODUCTS = [("widget", "10.00"), ("gadget", "25.00"), ("gizmo", "4.50")]
#: (order number, customer index, date, [(product index, quantity, agreed price)])
ORDERS = [(number, number % 2, date(2026, 1 + (number - 1) % 12, 5 + number % 20),
           [(number % 3, str(1 + number % 4), PRODUCTS[number % 3][1]),
            ((number + 1) % 3, "2", PRODUCTS[(number + 1) % 3][1]),
            (0, "1", "9.50")])
          for number in range(1, 25)]


def _load(tr) -> dict:
    """The company, two customers, three products and the orders with their lines; every ``one`` role set."""
    s = semantic(tr)
    company = s.create("acme.core.Company", name="ACME")
    customers = [s.create("acme.sales.Customer", name=name, credit_limit=Decimal("100000"), company=company)
                 for name in ("Northwind", "Contoso")]
    products = [s.create("acme.catalog.Product", name=name, list_price=Decimal(price), company=company)
                for name, price in PRODUCTS]
    orders = {}
    for number, customer, day, lines in ORDERS:
        order = s.create(ORDER, number=number, date=day, company=company, customer=customers[customer])
        for product, quantity, price in lines:
            s.relate(LINE, order=order, product=products[product], quantity=Decimal(quantity), price=Decimal(price))
        orders[number] = order
    return {"company": company, "customers": customers, "products": products, "orders": orders}


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


def _plan_line(plan) -> str:
    """The plan nodes of ``plan``, outermost first."""
    out, node = [], plan
    while isinstance(node, dict):
        index = node.get("index")
        out.append(node["plan"] + (f"({index['index']})" if isinstance(index, dict) else ""))
        node = (node.get("children") or [None])[0]
    return " <- ".join(out)


def step_3(db) -> None:
    world = db.run(_load)
    lines_loaded = sum(len(lines) for *_, lines in ORDERS)
    say(f"loaded {len(ORDERS)} orders with {lines_loaded} lines, for 2 customers and {len(PRODUCTS)} products")
    widget = world["products"][0]
    first = world["orders"][6]
    tr = db.new_transaction()
    try:
        s = semantic(tr)
        order = s.get(first)
        products = [line.product.name for line in order.lines]
        say(f"order {order.number} ({order.date}): its products, from the order: {products}")
        check(products == [PRODUCTS[p][0] for p, _, _ in ORDERS[5][3]], "the order reaches its products, in line order")
        check(order.customer.name == ("Northwind", "Contoso")[ORDERS[5][1]], "the order reaches its customer")
        check(order.total == sum(line.amount for line in order.lines),
              "the order's total is maintained: it equals the sum of its lines' amounts")

        june = ["and", [">=", ["attr", "order", "date"], ["const", date(2026, 6, 1)]],
                ["<=", ["attr", "order", "date"], ["const", date(2026, 6, 30)]]]
        tree = {"from": ["via", ["cell", widget.name], "order_lines"], "where": june}
        answer = s.query(tree)
        sold = sum((line.quantity for line in answer["rows"]), Decimal(0))
        say("the widget's sales in June, from the product (its own collection of lines, no global structure):")
        say(f"   query: from {tree['from']}, where order.date in June")
        say(f"   explain: {_plan_line(answer['explain']['plan'])}; on the index: {answer['explain']['indexed'] != []}")
        say(f"   version read: {answer['version']['number']}")
        for line in answer["rows"]:
            say(f"   order {line.order.number} of {line.order.date}: {line.quantity} at {line.price}")
        check("by_order_date" in _index_names(answer["explain"]),
              "the June sales are answered by the index by_order_date")
        expected = [(n, q) for n, _, day, lines in ORDERS if day.month == 6 for p, q, _ in lines if p == 0]
        check(sorted((line.order.number, line.quantity) for line in answer["rows"])
              == sorted((n, Decimal(q)) for n, q in expected),
              "the answer is the widget's June lines (members, not index entries)")
        say(f"   widget sold in June: {sold} units")
    finally:
        tr.abort()

    db.run(lambda tr: semantic(tr).update(widget, list_price=Decimal("12.00")))
    tr = db.new_transaction()
    try:
        s = semantic(tr)
        order = s.get(first)
        say(f"invoice of order {order.number}, after the widget's price changed to "
            f"{s.get(widget).list_price} (each line prints its product as sold):")
        as_sold = []
        for line in order.lines:
            product = line.product
            current = s.get(product.entity)
            say(f"   {product.name}: {line.quantity} x {line.price} = {line.amount}   "
                f"(list price when sold {product.list_price}, now {current.list_price})")
            as_sold.append((product.name, product.list_price, current.list_price))
        say(f"   total {order.total}")
        widgets = [(then, now) for name, then, now in as_sold if name == "widget"]
        check(widgets and all(then == Decimal("10.00") and now == Decimal("12.00") for then, now in widgets),
              "the invoice shows the widget as sold (10.00), and .entity reaches its current version (12.00)")
        customer = order.customer
        check(customer.committed == sum(o.total for o in customer.orders if o.state != "cancelled"),
              "the customer's committed amount is maintained over its orders")
    finally:
        tr.abort()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--workdir", help="directory for the demo's files (default: a temporary directory, removed)")
    args = parser.parse_args(argv)
    own = args.workdir is None
    workdir = tempfile.mkdtemp(prefix="semantic-demo-") if own else args.workdir
    os.makedirs(workdir, exist_ok=True)
    try:
        say("proto_db: the semantic-model demo")
        space = ObjectSpace(FileStorage(os.path.join(workdir, "space")), modules_mapping=MAPPING)
        try:
            db = space.new_database("acme")
            title("Step 1: the database describes itself")
            step_1(db)
            title("Step 2: a module extends another")
            step_2(db)
            title("Step 3: relations navigable from both sides")
            step_3(db)
        finally:
            space.close()
    finally:
        if own:
            shutil.rmtree(workdir, ignore_errors=True)
    say()
    say(f"every check passed ({len(CHECKS)} checks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
