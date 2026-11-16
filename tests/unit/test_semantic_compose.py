"""Composition of the semantic catalog with provenance (spec sections 4 and 5, R280-R282; Task 4).

Catches: the two extension spellings contributing differently; a redefinition that loses structural
fields it omits, or accepts a restated structural field that differs; provenance missing a module or in
the wrong order; inverse ends missing, created where the target declares them, or accepted when they
disagree; ``index_by`` through a role not recognised as a maintained key; a subtype that misses its
supertypes' elements, their provenance, ``inherited_from`` or their extensions by other modules; a
supertype's extension winning over the subtype's own redefinition; a diamond resolved against C3; a
subtype changing an inherited element's structure accepted (R282); ``describe`` returning live objects;
a composition that depends on the installation order.
"""
from __future__ import annotations

import itertools
from datetime import date
from decimal import Decimal

import pytest

from proto_db.catalog.model import canonical_form, check_model, from_atoms, to_atoms
from proto_db.errors import ProtoValidationError
from proto_db.registry import Registry
from proto_db.semantic.compose import (compose, compose_modules, describe, index_paths, is_a, linearization,
                                       module_contributions, subtypes)
from tests.unit.semantic_modules import MAPPING

pytestmark = [pytest.mark.timeout(120)]


def build(*names):
    return Registry.build(("base", "acme_sales") + names, mapping=MAPPING)


def element(entity: dict, name: str) -> dict:
    return next(e for e in entity.get("attributes", []) + entity.get("roles", []) + entity.get("intents", [])
                if e["name"] == name)


# -- module contributions -------------------------------------------------------------------------------

def test_module_contributions_of_the_demo():
    registry = build("acme_delivery")
    sales = module_contributions(registry, "acme_sales")
    assert sales["name"] == "acme_sales" and sales["version"] == "1" and "depends" not in sales
    assert [e["extends"] for e in sales["extensions"]] == [
        "acme.core.Company", "acme.sales.Customer", "acme.catalog.Product", "acme.sales.OrderLine", "acme.sales.Order"]
    delivery = module_contributions(registry, "acme_delivery")
    assert delivery == {"name": "acme_delivery", "version": "1", "depends": ["acme_sales"], "extensions": [{
        "module": "acme_delivery", "extends": "acme.sales.Order", "elements": [
            {"name": "delivery_date", "kind": "field", "value_type": "date", "meaning": "The committed delivery date"},
            {"name": "number", "kind": "field", "value_type": "int", "required": True,
             "label": {"und": "Order no."}}]}]}
    assert module_contributions(registry, "base") == {"name": "base", "version": "1"}
    with pytest.raises(ProtoValidationError, match="does not contain"):
        module_contributions(registry, "acme_rush")


def test_extension_subclass_equals_the_decorator():
    registry = build("acme_decorated", "acme_subclassed")
    decorated = module_contributions(registry, "acme_decorated")
    subclassed = module_contributions(registry, "acme_subclassed")
    assert {**decorated, "name": "x", "extensions": [{**e, "module": "x"} for e in decorated["extensions"]]} == \
           {**subclassed, "name": "x", "extensions": [{**e, "module": "x"} for e in subclassed["extensions"]]}
    assert decorated["extensions"][0]["elements"][1]["label"] == {"und": "Order no."}


def test_an_extension_of_a_relationship_states_its_kind():
    # Format spec: every Extension of a relationship type states kind relationship (kind is structural).
    entity = module_contributions(build(), "acme_sales")["extensions"][3]
    assert entity["extends"] == "acme.sales.OrderLine" and entity["entity"]["kind"] == "relationship"


# -- the fold and provenance (spec section 5) -------------------------------------------------------------

def test_delivery_redefines_the_number_label():
    order = compose(build("acme_delivery"))["entities"]["acme.sales.Order"]
    number = element(order, "number")
    assert number["label"] == {"und": "Order no."} and number["required"] is True
    assert number["meaning"] == "Sequential per company, assigned at creation"      # omitted, so unchanged
    assert number["provenance"] == ["acme_sales", "acme_delivery"]
    assert element(order, "delivery_date")["provenance"] == ["acme_delivery"]
    assert [a["name"] for a in order["attributes"]][-1] == "delivery_date"          # added after the others
    assert order["provenance"] == ["acme_sales", "acme_delivery"]


def test_restated_structural_field_must_be_equal():
    with pytest.raises(ProtoValidationError) as raised:
        compose(build("acme_restates"))
    message = str(raised.value)
    for part in ("'acme_restates'", "'acme.sales.Order'", "'number'", "'value_type'"):
        assert part in message


def test_cardinality_spellings_give_both_ends():
    entities = compose(build())["entities"]
    customer = element(entities["acme.sales.Order"], "customer")
    assert (customer["cardinality"], customer["inverse_cardinality"]) == ("one", "many")
    orders = element(entities["acme.sales.Customer"], "orders")
    assert (orders["cardinality"], orders["inverse_cardinality"]) == ("many", "one")
    # Inverse ends the targets do not declare are created there, with the declaring module.
    company = entities["acme.core.Company"]
    created = {e["name"]: e for e in company["attributes"] if "target" in e}
    assert created == {
        "customers": {"name": "customers", "target": "acme.sales.Customer", "inverse": "company", "cardinality": "many",
                      "inverse_cardinality": "one", "provenance": ["acme_sales"]},
        "products": {"name": "products", "target": "acme.catalog.Product", "inverse": "company", "cardinality": "many",
                     "inverse_cardinality": "one", "provenance": ["acme_sales"]},
        "orders": {"name": "orders", "target": "acme.sales.Order", "inverse": "company", "cardinality": "many",
                   "inverse_cardinality": "one", "provenance": ["acme_sales"]}}
    order_lines = element(entities["acme.catalog.Product"], "order_lines")
    assert (order_lines["target"], order_lines["inverse"], order_lines["cardinality"]) == (
        "acme.sales.OrderLine", "product", "many")


def test_relation_to_agrees_with_the_role():
    entities = compose(build())["entities"]
    lines = element(entities["acme.sales.Order"], "lines")
    order = element(entities["acme.sales.OrderLine"], "order")
    assert lines == {"name": "lines", "target": "acme.sales.OrderLine", "inverse": "order", "cardinality": "many",
                     "inverse_cardinality": "one", "ordered": True, "provenance": ["acme_sales"]}
    assert order["participant"] is True and order["ordered"] is True and order["inverse"] == "lines"
    assert [r["name"] for r in entities["acme.sales.OrderLine"]["roles"]] == ["order", "product"]
    disagree = Registry.build(("base", "acme_disagree"), mapping=MAPPING)
    with pytest.raises(ProtoValidationError, match="does not agree on ordered"):
        compose(disagree)


def test_index_by_through_a_role_is_catalogued_as_a_maintained_key():
    model = compose(build())
    product = element(model["entities"]["acme.sales.OrderLine"], "product")
    assert product["index_by"] == ["order.date"] and product["binding"] == "version"
    assert index_paths(model, "acme.sales.OrderLine", "product") == [
        {"path": "order.date", "index": "by_order_date", "maintained_key": True}]


def test_text_and_neutral_aggregates_are_catalogued_as_trees():
    entities = compose(build())["entities"]
    committed = element(entities["acme.sales.Customer"], "committed")
    assert committed["kind"] == "computed_stored" and committed["expression"] == {
        "ast": ["sum", "orders", ["attr", "total"], ["!=", ["attr", "state"], ["const", "cancelled"]]]}
    (constraint,) = entities["acme.sales.Customer"]["constraints"]
    assert constraint["scope"] == "aggregate" and constraint["name"].startswith("invariant_")
    confirm = element(entities["acme.sales.Order"], "confirm")
    assert confirm["requires"] == {"ast": ["==", ["attr", "state"], ["const", "open"]]}


def test_a_maintained_aggregate_over_a_python_only_computed_is_refused():
    with pytest.raises(ProtoValidationError, match="neutral all the way down"):
        compose(Registry.build(("base", "acme_lambda_total"), mapping=MAPPING))


# -- subclasses (R280-R282) ------------------------------------------------------------------------------

def test_subclass_composes_over_its_superclass():
    entities = compose(build("acme_rush"))["entities"]
    rush = entities["acme.sales.RushOrder"]
    assert rush["supertypes"] == ["acme.sales.Order"]
    assert rush["meaning"] == "An order delivered before the others" and rush["label"] == {"und": "Rush order"}
    assert rush["key"] == ["company", "number"] and "synonyms" not in rush       # key inherited, synonyms not
    order = entities["acme.sales.Order"]
    for name in ("number", "lines", "total", "add_line", "confirm"):
        inherited, original = element(rush, name), element(order, name)
        assert inherited["inherited_from"] == "acme.sales.Order"
        assert inherited["provenance"] == original["provenance"] == ["acme_sales"]
        assert {k: v for k, v in inherited.items() if k != "inherited_from"} == original
    priority = element(rush, "priority")
    assert priority == {"name": "priority", "kind": "field", "value_type": "int", "provenance": ["acme_rush"]}
    assert rush["provenance"] == ["acme_rush"]
    assert [a["name"] for a in rush["attributes"]][-1] == "priority"


def test_supertype_extension_reaches_the_subtype():
    rush = compose(build("acme_delivery", "acme_rush"))["entities"]["acme.sales.RushOrder"]
    delivery_date = element(rush, "delivery_date")
    assert delivery_date["inherited_from"] == "acme.sales.Order" and delivery_date["provenance"] == ["acme_delivery"]
    number = element(rush, "number")
    assert number["label"] == {"und": "Order no."} and number["provenance"] == ["acme_sales", "acme_delivery"]


def test_subclass_redefines_presentation_and_wins_over_supertype_extensions():
    # acme_zlate loads after acme_express (code-point order) and relabels Order.number; the subtype's
    # own chain still comes first (C3), as in resolution.
    entities = compose(build("acme_express", "acme_zlate"))["entities"]
    assert element(entities["acme.sales.Order"], "number")["label"] == {"und": "Late no."}
    number = element(entities["acme.sales.ExpressOrder"], "number")
    assert number["label"] == {"und": "Express no."} and number["inherited_from"] == "acme.sales.Order"
    assert number["provenance"] == ["acme_sales", "acme_zlate", "acme_express"]
    assert number["required"] is True                                       # the subtype omits it: unchanged
    confirm = element(entities["acme.sales.ExpressOrder"], "confirm")
    assert confirm["meaning"].startswith("Confirm the express order")
    assert confirm["requires"] == element(entities["acme.sales.Order"], "confirm")["requires"]


def test_multiple_semantic_bases_follow_c3():
    from tests.unit.semantic_modules.acme_diamond import D
    model = compose(Registry.build(("base", "acme_diamond"), mapping=MAPPING))
    order = ["acme.diamond.D", "acme.diamond.B", "acme.diamond.C", "acme.diamond.A"]
    assert linearization(model, "acme.diamond.D") == order
    assert [c.__type_name__ for c in D.__mro__[:4]] == order
    d = model["entities"]["acme.diamond.D"]
    assert d["supertypes"] == ["acme.diamond.B", "acme.diamond.C"] and d["key"] == ["x"]
    x = element(d, "x")
    assert x["label"] == {"und": "x of B"} and x["inherited_from"] == "acme.diamond.A"
    assert element(d, "y")["inherited_from"] == "acme.diamond.C" and "inherited_from" not in element(d, "z")
    assert subtypes(model, "acme.diamond.A") == frozenset(order)
    assert is_a(model, "acme.diamond.D", "acme.diamond.A") and not is_a(model, "acme.diamond.B", "acme.diamond.C")


@pytest.mark.parametrize("field", ["value_type", "cardinality", "target", "binding", "kind"])
def test_subclass_changing_an_inherited_element_is_refused(field):
    registry = build(f"r282_{field}")
    with pytest.raises(ProtoValidationError, match="R282") as raised:
        compose(registry)
    assert f"'{field}'" in str(raised.value) and f"'r282_{field}'" in str(raised.value)


def test_subtypes_and_is_a():
    model = compose(build("acme_rush", "acme_express"))
    assert subtypes(model, "acme.sales.Order") == {"acme.sales.Order", "acme.sales.RushOrder",
                                                   "acme.sales.ExpressOrder"}
    assert subtypes(model, "acme.sales.RushOrder") == {"acme.sales.RushOrder"}
    assert is_a(model, "acme.sales.RushOrder", "acme.sales.Order")
    assert not is_a(model, "acme.sales.Order", "acme.sales.RushOrder")
    with pytest.raises(ProtoValidationError, match="no type"):
        subtypes(model, "acme.sales.Nothing")


# -- describe and the model ------------------------------------------------------------------------------

def _neutral(value) -> bool:
    if isinstance(value, dict):
        return type(value) is dict and all(type(k) is str and _neutral(v) for k, v in value.items())
    if isinstance(value, list):
        return type(value) is list and all(_neutral(v) for v in value)
    return value is None or type(value) in (str, int, bool, Decimal, date)


def test_describe_returns_only_neutral_data():
    model = compose(build("acme_delivery", "acme_rush"))
    for type_name in model["entities"]:
        described = describe(model, type_name)
        assert _neutral(described), type_name
        assert described["kind"] == ("relationship" if type_name == "acme.sales.OrderLine" else "entity")
    described = describe(model, "acme.sales.Order")
    described["attributes"].clear()
    assert model["entities"]["acme.sales.Order"]["attributes"]                       # a copy, not the model
    with pytest.raises(ProtoValidationError, match="no type"):
        describe(model, "acme.sales.Nothing")


def test_the_composed_model_round_trips_through_the_records():
    model = compose(build("acme_delivery", "acme_rush", "acme_express"))
    check_model(model)
    assert canonical_form(from_atoms(to_atoms(model, None))) == canonical_form(model)
    # The stored contributions compose to the same entities (the reader's path, spec section 5).
    assert compose_modules(model["modules"]) == model


def test_composition_is_independent_of_install_order():
    names = ("acme_delivery", "acme_rush", "acme_express", "acme_zlate")
    forms = {canonical_form(compose(Registry.build(("base", "acme_sales") + order, mapping=MAPPING)))
             for order in itertools.permutations(names)}
    assert len(forms) == 1
    # And from the contributions in any order of the mapping.
    model = compose(build(*names))
    reversed_modules = dict(reversed(list(model["modules"].items())))
    assert canonical_form(compose_modules(reversed_modules)) == canonical_form(model)
