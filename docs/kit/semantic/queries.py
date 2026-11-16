"""The named query trees of the semantic-model kit (preflight F33).

One module defines every query tree the kit asks, so that the demo's step 5, the step-6 expected output
(``proto_db.agent.step6``, the protoBaseST handoff) and the ground truth of the questions ask the same thing.
A tree is the agent interface's JSON form (spec section 11); the cells it starts from are arguments, because
a cell name carries a random identity.  Import this module as ``docs.kit.semantic.queries`` (preflight F41).
"""

#: Registry module name -> import path of its code (the runtime's ``modules_mapping``), the one canonical path
#: of the kit's modules (preflight F41).
MAPPING = {"acme_sales": "docs.kit.semantic.acme_sales", "acme_delivery": "docs.kit.semantic.acme_delivery"}

#: The demo's database name.
DATABASE = "acme"

COMPANY, WIDGET_NAME = "acme.core.Company", "widget"

#: June 2026, as the typed constants of the JSON form.
JUNE = ["and", [">=", ["attr", "order", "date"], ["const", {"date": "2026-06-01"}]],
        ["<=", ["attr", "order", "date"], ["const", {"date": "2026-06-30"}]]]


def widget_of(company: str) -> dict:
    """The widget among the products of ``company`` (a cell name)."""
    return {"from": ["via", ["cell", company], "products"],
            "where": ["==", ["attr", "name"], ["const", WIDGET_NAME]]}


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
