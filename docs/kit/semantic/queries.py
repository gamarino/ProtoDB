"""The named query trees of the semantic-model kit (preflight F33).

One module defines every query tree the kit asks, so that the demo's step 5, the step-6 expected output
(``proto_db.agent.step6``, the protoBaseST handoff) and the ground truth of the agent experiment's questions
(``benchmarks/semantic/questions.json``: ``QUESTIONS`` and ``POST``) ask the same thing.  A tree is the agent
interface's JSON form (spec section 11); the cells it starts from are arguments, because a cell name carries a
random identity.  Import this module as ``docs.kit.semantic.queries`` (preflight F41).
"""
from datetime import date
from decimal import Decimal

#: Registry module name -> import path of its code (the runtime's ``modules_mapping``), the one canonical path
#: of the kit's modules (preflight F41).
MAPPING = {"acme_sales": "docs.kit.semantic.acme_sales", "acme_delivery": "docs.kit.semantic.acme_delivery"}

#: The demo's database name.
DATABASE = "acme"

COMPANY, WIDGET_NAME = "acme.core.Company", "widget"

#: The demo's products, by name.
PRODUCT_NAMES = (WIDGET_NAME, "gadget", "gizmo")


def between(path: tuple, first: str, last: str) -> list:
    """The predicate ``first <= path <= last`` over ISO dates, as the typed constants of the JSON form."""
    return ["and", [">=", ["attr", *path], ["const", {"date": first}]],
            ["<=", ["attr", *path], ["const", {"date": last}]]]


#: June 2026 for an order line (its order's date).
JUNE = between(("order", "date"), "2026-06-01", "2026-06-30")


def product_named(company: str, name: str) -> dict:
    """The product called ``name`` among the products of ``company`` (a cell name)."""
    return {"from": ["via", ["cell", company], "products"], "where": ["==", ["attr", "name"], ["const", name]]}


def widget_of(company: str) -> dict:
    """The widget among the products of ``company`` (a cell name)."""
    return product_named(company, WIDGET_NAME)


def sold_in_june(widget: str) -> dict:
    """The widget's order lines of June 2026, with their quantities and agreed prices."""
    return {"from": ["via", ["cell", widget], "order_lines"], "where": JUNE, "select": ["quantity", "price"]}


def customer_limits(company: str) -> dict:
    """The customers of ``company`` with what they committed and their credit limit."""
    return {"from": ["via", ["cell", company], "customers"], "select": ["name", "committed", "credit_limit"]}


#: The queries of the step-6 expected output, by name: each maps the cells it needs to its tree.  The
#: starting cells are found through ``list_entities`` (``starting_cells``), never handed over.
STEP6 = {
    "widget": lambda cells: widget_of(cells["company"]),
    "sold_in_june": lambda cells: sold_in_june(cells["widget"]),
    "customer_limits": lambda cells: customer_limits(cells["company"]),
}


def company_cell(listing: dict) -> str:
    """The company's cell in a ``list_entities`` answer (the demo has one company)."""
    [company] = next(e for e in listing["entities"] if e["type_name"] == COMPANY)["cells"]
    return company


def widget_cell(found: dict) -> str:
    """The widget's cell in the answer to :func:`widget_of`."""
    return found["rows"][0]["entity"]["cell"]


# -- the agent experiment's ground truth (Task 12; preflight F19, F33) ------------------------------------------

def _orders(cells: dict, **parts) -> dict:
    return {"from": ["via", ["cell", cells["company"]], "orders"], **parts}


def _lines(cells: dict, product: str, **parts) -> dict:
    return {"from": ["via", ["cell", cells[product]], "order_lines"], **parts}


def _order_number(number: int) -> list:
    return ["==", ["attr", "number"], ["const", number]]


#: The ground-truth queries of the experiment's questions, by name.  Each maps the starting cells (``company``
#: and one per product name, found through ``list_entities`` and :func:`product_named`) to its tree.  The
#: ground truth runs the tree in catalog mode; a question whose answer is not the tree's aggregate names a
#: post-step of :data:`POST` too.
QUESTIONS = {
    "widget_units_in_june": lambda cells: _lines(cells, "widget", where=JUNE, aggregate=["sum", ["attr", "quantity"]]),
    "customer_limits": lambda cells: customer_limits(cells["company"]),
    "order_count": lambda cells: _orders(cells, aggregate=["count"]),
    "orders_total": lambda cells: _orders(cells, aggregate=["sum", ["attr", "total"]]),
    "customer_count": lambda cells: {"from": ["via", ["cell", cells["company"]], "customers"],
                                     "aggregate": ["count"]},
    "credit_limits_total": lambda cells: {"from": ["via", ["cell", cells["company"]], "customers"],
                                          "aggregate": ["sum", ["attr", "credit_limit"]]},
    "widget_list_price": lambda cells: {**product_named(cells["company"], WIDGET_NAME), "select": ["list_price"]},
    "widget_agreed_prices": lambda cells: _lines(cells, "widget", select=["price"]),
    "gizmo_units": lambda cells: _lines(cells, "gizmo", aggregate=["sum", ["attr", "quantity"]]),
    "gadget_value_first_quarter": lambda cells: _lines(cells, "gadget",
                                                       where=between(("order", "date"), "2026-01-01", "2026-03-31"),
                                                       aggregate=["sum", ["attr", "amount"]]),
    "widget_value": lambda cells: _lines(cells, "widget", aggregate=["sum", ["attr", "amount"]]),
    "gizmo_line_count": lambda cells: _lines(cells, "gizmo", aggregate=["count"]),
    "orders_in_july": lambda cells: _orders(cells, where=between(("date",), "2026-07-01", "2026-07-31"),
                                            aggregate=["count"]),
    "order_7_total": lambda cells: _orders(cells, where=_order_number(7), select=["total"]),
    "order_15_date": lambda cells: _orders(cells, where=_order_number(15), select=["date"]),
    "order_12_line_count": lambda cells: {"from": ["via", ["cell", cells["company"]], "orders", "lines"],
                                          "where": ["==", ["attr", "order", "number"], ["const", 12]],
                                          "aggregate": ["count"]},
    "customers_over_half_their_limit": lambda cells: {
        "from": ["via", ["cell", cells["company"]], "customers"],
        "where": [">", ["*", ["attr", "committed"], ["const", 2]], ["attr", "credit_limit"]],
        "aggregate": ["count"]},
    "order_dates": lambda cells: _orders(cells, select=["number", "date"]),
    "orders_total_second_half": lambda cells: _orders(cells, where=between(("date",), "2026-07-01", "2026-12-31"),
                                                      aggregate=["sum", ["attr", "total"]]),
}


def _number(row: dict, name: str) -> Decimal:
    return Decimal(str(row["values"][name]))


def _extreme(rows: list, key, pick) -> dict:
    """The one row where ``key`` is ``pick`` (min or max); a tie has no single answer and is refused."""
    if not rows:
        raise ValueError("no rows")
    best = pick(key(row) for row in rows)
    found = [row for row in rows if key(row) == best]
    if len(found) != 1:
        raise ValueError(f"{len(found)} rows tie at {best}")
    return found[0]


def _only(rows: list) -> dict:
    if len(rows) != 1:
        raise ValueError(f"{len(rows)} rows, not one")
    return rows[0]


def _left(row: dict) -> Decimal:
    return _number(row, "credit_limit") - _number(row, "committed")


#: The declared client-side post-steps (preflight F19: the MVP tree has no ``min``, ``max`` or ordering).  Each
#: reads the rows of a query as the agent interface answers them (JSON form: decimals as strings, dates in ISO
#: 8601) and returns the answer: a number (``Decimal``), a cell name or an ISO date.  An argmin or argmax over
#: a tie, or ``only`` over anything but one row, raises ``ValueError``: such a question has no single answer.
POST = {
    "argmin(credit_limit - committed)": lambda rows: _extreme(rows, _left, min)["entity"]["cell"],
    "min(credit_limit - committed)": lambda rows: min(_left(row) for row in rows),
    "max(price)": lambda rows: max(_number(row, "price") for row in rows),
    "number of argmax(date)": lambda rows: Decimal(_extreme(rows, lambda r: date.fromisoformat(r["values"]["date"]),
                                                            max)["values"]["number"]),
    "only(list_price)": lambda rows: _number(_only(rows), "list_price"),
    "only(total)": lambda rows: _number(_only(rows), "total"),
    "only(date)": lambda rows: _only(rows)["values"]["date"],
}
