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

Step 5: questions with verifiable answers.  "How much of the widget did we sell in June?", "Which customer
is closest to its limit?" and "How much did we spend on advertising?" (not answerable: the catalog names
neither term).  Each answer carries the query, its ``explain()`` and the version read.  The agent finds where
to start through the interface: ``list_entities`` gives the company's cell, and a query from the company
finds the widget among its products.

Steps 1, 2 and 5 ask through the agent interface: a scripted MCP client (ruling R266: no language model runs
in the demo) starts the server (``python -m proto_db.agent``) over stdio and prints each tool call and its
response.  They need the optional extra ``agent`` (the MCP SDK).  The server opens the database's files, so
the demo closes its own space while the agent asks.  Steps 4 and 6 arrive with later tasks.

The demo checks what it shows.  Each "[check]" line is an assertion that held; a failed check stops the
demo with exit status 1.  It ends with "every check passed" only when all of them held.
"""
import argparse
import asyncio
import json
import os
import shutil
import sys
import sysconfig
import tempfile
from datetime import date
from decimal import Decimal

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, REPO)        # the canonical import path of the demo's modules is docs.kit.semantic (F41)

from proto_db import FileStorage, ObjectSpace  # noqa: E402
from proto_db.semantic import semantic  # noqa: E402

DATABASE = "acme"

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


# -- the agent (a scripted MCP client) -------------------------------------------------------------------------

def _shown(answer) -> str:
    """A response as printed: its JSON, cut after a few hundred characters (the demo prints what it checks
    in full below)."""
    text = json.dumps(answer, ensure_ascii=False)
    return text if len(text) <= 360 else f"{text[:360]}... ({len(text)} characters)"


def ask(path: str, calls: list) -> list:
    """Start the MCP server on the database at ``path``, make the tool calls ``calls`` (``(tool, arguments)``
    pairs) in order, print each call and its response, and return the responses (parsed JSON)."""
    try:
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client
    except ImportError:
        raise SystemExit("[FAILED] the agent steps need the optional extra 'agent' (the MCP SDK): "
                         "pip install proto_db[agent]") from None
    flags = ["-X", "gil=1"] if sysconfig.get_config_var("Py_GIL_DISABLED") and sys._is_gil_enabled() else []
    mapping = [arg for name, target in MAPPING.items() for arg in ("--mapping", f"{name}={target}")]
    parameters = StdioServerParameters(command=sys.executable, cwd=REPO, env=dict(os.environ),
                                       args=[*flags, "-m", "proto_db.agent", "--path", path, "--database", DATABASE,
                                             *mapping])

    async def run() -> list:
        answers = []
        async with stdio_client(parameters) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                for tool, arguments in calls:
                    say(f"   agent -> {tool}({json.dumps(arguments, ensure_ascii=False)})")
                    result = await session.call_tool(tool, arguments)
                    text = result.content[0].text if result.content else ""
                    if result.isError:
                        raise SystemExit(f"[FAILED] the agent interface refused {tool}: {text}")
                    answers.append(json.loads(text))
                    say(f"   server <- {_shown(answers[-1])}")
        return answers
    return asyncio.run(run())


# -- step 1 ----------------------------------------------------------------------------------------------------

def step_1(path: str) -> None:
    listed, order, line = ask(path, [("list_entities", {}), ("describe", {"type_name": ORDER}),
                                     ("describe", {"type_name": LINE})])
    listed = listed["entities"]
    say("the entities:")
    for entity in listed:
        say(f"   {entity['type_name']}: {_text(entity['label']) or '-'}, {entity['meaning']}")
    check({e["type_name"] for e in listed} == {"acme.core.Company", "acme.sales.Customer", "acme.catalog.Product",
                                               ORDER, LINE}, "the catalog lists the five entities of acme_sales")
    say("what an order is:")
    _print_entity(order)
    check(order["meaning"].startswith("A customer's order"), "an order says what it is")
    say("what a line is:")
    _print_entity(line)
    check(line["kind"] == "relationship" and {r["name"] for r in line["roles"]} == {"order", "product"},
          "a line is a relationship between an order and a product")
    price = _element(line, "price")
    check("agreed" in price["meaning"] and "not the product's current price" in price["meaning"],
          "'price' on a line means the agreed price, not the current one")
    check(_element(line, "product").get("binding") == "version",
          "a line's product is bound by version: the product as sold")


# -- step 2 ----------------------------------------------------------------------------------------------------

def step_2(path: str) -> None:
    order, = ask(path, [("describe", {"type_name": ORDER})])
    say("an order, after installing acme_delivery (each element names the modules that defined it):")
    _print_entity(order)
    delivery = _element(order, "delivery_date")
    check(delivery["provenance"] == ["acme_delivery"], "acme_delivery defined delivery_date")
    number = _element(order, "number")
    check(number["provenance"] == ["acme_sales", "acme_delivery"] and _text(number["label"]) == "Order no.",
          "acme_sales defined number, and acme_delivery redefined its label to 'Order no.'")
    check(_element(order, "total")["provenance"] == ["acme_sales"], "acme_sales defined total, unmodified")


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


def step_3(db) -> dict:
    """Load the orders and show their relations; return what step 5 asks about: the cells of the widget,
    the company and the customers (by index), and the version read after the load."""
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
        return {"widget": widget.name, "company": world["company"].name,
                "customers": [c.name for c in world["customers"]], "version": s.version()["number"]}
    finally:
        tr.abort()


# -- step 5 ----------------------------------------------------------------------------------------------------

def _amount(lines) -> Decimal:
    return sum((Decimal(quantity) * Decimal(price) for _, quantity, price in lines), Decimal(0))


def step_5(path: str, world: dict) -> None:
    """The agent finds where to start through the interface (list_entities, then a query from the company
    to its products); ``world`` is the load's own record, used only to check the answers."""
    say("Where to start: the agent lists the companies, then finds the widget among the company's products.")
    companies, = ask(path, [("list_entities", {"filter": "acme.core.Company"})])
    [company] = next(e for e in companies["entities"] if e["type_name"] == "acme.core.Company")["cells"]
    found, = ask(path, [("query", {"tree": {"from": ["via", ["cell", company], "products"],
                                            "where": ["==", ["attr", "name"], ["const", "widget"]]}})])
    widget = found["rows"][0]["entity"]["cell"]
    check(company == world["company"] and widget == world["widget"],
          "the agent found the company and the widget through the interface, not from the script")
    say()
    june = ["and", [">=", ["attr", "order", "date"], ["const", {"date": "2026-06-01"}]],
            ["<=", ["attr", "order", "date"], ["const", {"date": "2026-06-30"}]]]
    sold_tree = {"from": ["via", ["cell", widget], "order_lines"], "where": june, "select": ["quantity", "price"]}
    limit_tree = {"from": ["via", ["cell", company], "customers"], "select": ["name", "committed", "credit_limit"]}
    say('Three questions.  For each, the agent first checks that the catalog names its terms.')
    terms_sold, sold, terms_limit, limits, terms_ads = ask(path, [
        ("check_answerable", {"terms": ["product", "quantity", "date"]}),
        ("query", {"tree": sold_tree}),
        ("check_answerable", {"terms": ["customer", "credit_limit", "committed"]}),
        ("query", {"tree": limit_tree}),
        ("check_answerable", {"terms": ["advertising", "spend"]})])

    say()
    say('"How much of the widget did we sell in June?"')
    check(terms_sold["answerable"], "the catalog names product, quantity and date")
    units = sum((Decimal(row["values"]["quantity"]) for row in sold["rows"]), Decimal(0))
    say(f"   query: {json.dumps(sold['query'])}")
    say(f"   explain: {_plan_line(sold['explain']['plan'])}; on the index: {sold['explain']['indexed'] != []}")
    say(f"   version read: {sold['version']['number']}")
    say(f"   answer: {units} units in {len(sold['rows'])} lines")
    expected = sum((Decimal(q) for _, _, day, lines in ORDERS if day.month == 6 for p, q, _ in lines if p == 0),
                   Decimal(0))
    check(units == expected, f"the answer ({units}) is the widget's June quantity, recomputed from the loaded orders")
    check("by_order_date" in _index_names(sold["explain"]), "the answer's explain names the index by_order_date")
    check(sold["version"]["number"] == world["version"], "the answer names the version it read")

    say()
    say('"Which customer is closest to its limit?"')
    check(terms_limit["answerable"], "the catalog names customer, credit limit and committed")
    say(f"   query: {json.dumps(limits['query'])}, then the declared post-step argmin(credit_limit - committed)")
    for row in limits["rows"]:
        say(f"   {row['entity']['cell']}: {row['values']}")
    closest = min(limits["rows"],
                  key=lambda row: Decimal(row["values"]["credit_limit"]) - Decimal(row["values"]["committed"]))
    left = Decimal(closest["values"]["credit_limit"]) - Decimal(closest["values"]["committed"])
    say(f"   answer: {closest['entity']['cell']}, {left} below its limit")
    committed = {}
    for _, customer, _, lines in ORDERS:
        committed[customer] = committed.get(customer, Decimal(0)) + _amount(lines)
    expected = world["customers"][min(committed, key=lambda c: Decimal("100000") - committed[c])]
    check(closest["entity"]["cell"] == expected, "the answer is the customer with the least credit left, "
          "recomputed from the loaded orders")
    withheld = [t for t in limits["terms"] if t.get("withheld")]
    check(all("name" not in row["values"] for row in limits["rows"])
          and [(t["name"], t["sensitivity"]) for t in withheld] == [("name", "personal")],
          "the customer's name is personal: the answer omits it and says so")

    say()
    say('"How much did we spend on advertising?"')
    check(not terms_ads["answerable"] and terms_ads["unmatched"] == ["advertising", "spend"],
          "the catalog names neither 'advertising' nor 'spend': the agent says it cannot answer, and runs no query")


def _space(workdir: str) -> ObjectSpace:
    return ObjectSpace(FileStorage(os.path.join(workdir, "space")), modules_mapping=MAPPING)


def _in_space(workdir: str, work):
    """Run ``work(db)`` in the demo's own space, closed afterwards (the agent's server opens the same files)."""
    space = _space(workdir)
    try:
        return work(space.open_database(DATABASE))
    finally:
        space.close()


def _install(name: str):
    def work(db):
        db.run(lambda tr: tr.install_module(name))
        tr = db.new_transaction()
        try:
            return list(tr.installed_modules())
        finally:
            tr.abort()
    return work


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--workdir", help="directory for the demo's files (default: a temporary directory, removed)")
    args = parser.parse_args(argv)
    own = args.workdir is None
    workdir = tempfile.mkdtemp(prefix="semantic-demo-") if own else args.workdir
    os.makedirs(workdir, exist_ok=True)
    try:
        say("proto_db: the semantic-model demo")
        path = os.path.join(workdir, "space")
        space = _space(workdir)
        try:
            space.new_database(DATABASE)
        finally:
            space.close()
        title("Step 1: the database describes itself")
        check("acme_sales" in _in_space(workdir, _install("acme_sales")), "acme_sales is installed")
        step_1(path)
        title("Step 2: a module extends another")
        check({"acme_sales", "acme_delivery"} <= set(_in_space(workdir, _install("acme_delivery"))),
              "both modules are installed")
        step_2(path)
        title("Step 3: relations navigable from both sides")
        world = _in_space(workdir, step_3)
        title("Step 5: questions with verifiable answers")
        step_5(path, world)
    finally:
        if own:
            shutil.rmtree(workdir, ignore_errors=True)
    say()
    say(f"every check passed ({len(CHECKS)} checks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
