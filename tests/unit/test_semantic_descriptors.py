"""The semantic declarations in Python: descriptors, constraints and intents (spec sections 4, 6 and
10; Task 4).

Catches: a Python type catalogued as the wrong value kind; a stated field lost or an unstated one
written (defaults must be omitted, so a contribution equals its stored form); an unknown semantic
keyword accepted; a text expression stored as text, or a lambda stored as if neutral (R276, R277); a
relation spelling stored as anything but its two ends; an unnamed invariant whose name is not the
stable R279 hash; an intent whose signature, parameters or types are not the brief's.
"""
from __future__ import annotations

import datetime
import uuid
from decimal import Decimal

import pytest

from proto_db.errors import ProtoValidationError
from proto_db.semantic import (Aggregate, Certified, Computed, Enum, Field, Invariant, Relation, RelationTo, Role,
                               intent)
from proto_db.semantic.descriptors import invariant_name
from proto_db.values import MISSING

pytestmark = [pytest.mark.timeout(60)]


def named(descriptor, name="x"):
    descriptor.__set_name__(object, name)
    return descriptor.element()


@pytest.mark.parametrize("python_type, kind", [
    (int, "int"), (Decimal, "decimal"), (str, "str"), (bool, "bool"), (bytes, "bytes"), (float, "float"),
    (datetime.date, "date"), (datetime.time, "time"), (datetime.datetime, "datetime"),
    (datetime.timedelta, "timedelta"), (uuid.UUID, "uuid")])
def test_field_types_map_to_value_kinds(python_type, kind):
    assert named(Field(python_type)) == {"name": "x", "kind": "field", "value_type": kind}


def test_field_states_only_what_it_declares():
    assert named(Field(int, required=True, label="Number", meaning="Sequential", examples=[1001]), "number") == {
        "name": "number", "kind": "field", "value_type": "int", "required": True, "meaning": "Sequential",
        "label": {"und": "Number"}, "examples": [1001]}
    # Values at their default are not stated: a contribution equals its stored form.
    assert named(Field(int, required=False, indexed=False, sensitivity="internal", deprecated=False)) == {
        "name": "x", "kind": "field", "value_type": "int"}
    assert named(Field(str, default=None, unit="u", indexed=True, sensitivity="secret", ext={"k": 1},
                       label={"en": "X", "es": "X es"}, help="Help", group="g", order=2, synonyms=["y"],
                       caveats="c", certification=Certified("gamarino", "2026-11-14"), deprecated=True)) == {
        "name": "x", "kind": "field", "value_type": "str", "default": None, "unit": "u", "indexed": True,
        "sensitivity": "secret", "ext": {"k": 1}, "label": {"en": "X", "es": "X es"}, "help": {"und": "Help"},
        "group": "g", "order": 2, "synonyms": ["y"], "caveats": "c",
        "certification": "certified by gamarino on 2026-11-14", "deprecated": True}


def test_enum_gives_str_with_values():
    field = Field(Enum("open", "confirmed", "cancelled"), default="open")
    assert named(field, "state") == {"name": "state", "kind": "field", "value_type": "str",
                                     "values": ["open", "confirmed", "cancelled"], "default": "open"}
    assert field.resolve(None) == "open" and Field(int).default is MISSING
    with pytest.raises(ProtoValidationError):
        Enum()
    with pytest.raises(ProtoValidationError):
        Enum("a", "a")


@pytest.mark.parametrize("arguments, match", [
    ((object,), "value_type"), ((int,), "unknown semantic keywords"), ((int,), "sensitivity"),
    ((int,), "label"), ((int,), "required"), ((int,), "default")])
def test_refused_field_arguments(arguments, match):
    keywords = {"unknown semantic keywords": {"colour": "red"}, "sensitivity": {"sensitivity": "top"},
                "label": {"label": 3}, "required": {"required": 1}, "default": {"default": object()},
                "value_type": {}}[match]
    with pytest.raises(ProtoValidationError, match=match):
        Field(*arguments, **keywords)


def test_text_computed_is_neutral_and_lambda_computed_is_python_only():
    assert named(Computed("quantity * price"), "amount") == {
        "name": "amount", "kind": "computed", "expression": {"ast": ["*", ["attr", "quantity"], ["attr", "price"]]}}
    assert named(Computed(["+", ["attr", "a"], ["const", 1]])) == {
        "name": "x", "kind": "computed", "expression": {"ast": ["+", ["attr", "a"], ["const", 1]]}}
    lam = Computed(lambda self: self.quantity * self.price, unit="currency:order")
    assert named(lam, "amount") == {"name": "amount", "kind": "computed", "languages": ["python"],
                                    "unit": "currency:order"}
    with pytest.raises(ProtoValidationError, match="column"):
        Computed("quantity in (1, 2)")
    with pytest.raises(ProtoValidationError):
        Computed(42)


def test_aggregate_trees():
    assert named(Aggregate(over="lines", sum="amount", maintained=True, unit="currency:order"), "total") == {
        "name": "total", "kind": "computed_stored", "expression": {"ast": ["sum", "lines", ["attr", "amount"]]},
        "unit": "currency:order"}
    assert named(Aggregate(over="orders", count=True, where="state != 'cancelled'")) == {
        "name": "x", "kind": "computed",
        "expression": {"ast": ["count", "orders", ["!=", ["attr", "state"], ["const", "cancelled"]]]}}
    with pytest.raises(ProtoValidationError, match="exactly one"):
        Aggregate(over="lines")
    with pytest.raises(ProtoValidationError, match="exactly one"):
        Aggregate(over="lines", sum="amount", count=True)
    with pytest.raises(ProtoValidationError):
        Aggregate(over="lines", sum="amount", where=lambda member: True)


def test_cardinality_spellings_give_both_ends():
    ends = {spelling: (named(Relation("acme.x.T", cardinality=spelling, inverse="back"))["cardinality"],
                       named(Relation("acme.x.T", cardinality=spelling, inverse="back"))["inverse_cardinality"])
            for spelling in ("many-to-one", "one-to-many", "one-to-one", "many-to-many")}
    assert ends == {"many-to-one": ("one", "many"), "one-to-many": ("many", "one"), "one-to-one": ("one", "one"),
                    "many-to-many": ("many", "many")}
    for end in ("one", "many", "0..1", "1..*"):
        assert named(Relation("acme.x.T", cardinality=end)) == {"name": "x", "target": "acme.x.T",
                                                                 "cardinality": end}
    with pytest.raises(ProtoValidationError, match="relation spelling"):
        Relation("acme.x.T", cardinality="one", inverse="back")
    with pytest.raises(ProtoValidationError, match="cardinality"):
        Relation("acme.x.T", cardinality="several")
    with pytest.raises(ProtoValidationError, match="type name"):
        Relation("Order", cardinality="many-to-one")


def test_role_and_relation_to_are_the_two_ends_of_a_relationship_role():
    assert named(Role("acme.sales.Order", inverse="lines", ordered=True), "order") == {
        "name": "order", "target": "acme.sales.Order", "inverse": "lines", "cardinality": "one",
        "inverse_cardinality": "many", "participant": True, "ordered": True}
    assert named(Role("acme.catalog.Product", index_by=["order.date"], binding="version", on_delete="cascade"),
                 "product") == {"name": "product", "target": "acme.catalog.Product", "cardinality": "one",
                                "participant": True, "index_by": ["order.date"], "binding": "version",
                                "on_delete": "cascade"}
    # restrict is the default (format spec, "Omission"): stated or not, the same item (review M2).
    assert named(Role("acme.x.T", on_delete="restrict")) == named(Role("acme.x.T")) == {
        "name": "x", "target": "acme.x.T", "cardinality": "one", "participant": True}
    assert named(RelationTo("acme.sales.OrderLine", role="order", ordered=True), "lines") == {
        "name": "lines", "target": "acme.sales.OrderLine", "inverse": "order", "cardinality": "many",
        "inverse_cardinality": "one", "ordered": True}
    from docs.kit.semantic.acme_sales import OrderLine
    assert named(RelationTo(OrderLine, role="order"))["target"] == "acme.sales.OrderLine"
    with pytest.raises(ProtoValidationError, match="binding"):
        Role("acme.x.T", binding="copy")
    with pytest.raises(ProtoValidationError, match="on_delete"):
        Role("acme.x.T", on_delete="explode")
    with pytest.raises(ProtoValidationError, match="semantic class"):
        RelationTo(dict, role="x")


def test_a_descriptor_declares_one_attribute():
    field = Field(int)
    field.__set_name__(object, "a")
    with pytest.raises(ProtoValidationError, match="declared again"):
        field.__set_name__(object, "b")
    with pytest.raises(ProtoValidationError, match="no name"):
        Field(int).element()


def test_invariant_without_a_name_gets_the_hash_name():
    # R279: "invariant_" and the first 8 hex digits of the SHA-256 of the tree's canonical encoding: the
    # same in every process and every language, so the expected names are literals.
    invariant = Invariant("committed <= credit_limit", meaning="Within the credit limit")
    assert invariant.name == "invariant_9d9f212a"
    assert invariant_name(["<=", ["attr", "committed"], ["attr", "credit_limit"]]) == "invariant_9d9f212a"
    assert Invariant(["<=", ["attr", "committed"], ["attr", "credit_limit"]], meaning="m").name == "invariant_9d9f212a"
    assert Invariant("a < b", meaning="m").name == "invariant_e1b7ed08"
    assert Invariant("a <= b", meaning="m").name != Invariant("a < b", meaning="m").name
    named_one = Invariant("committed <= credit_limit", meaning="m", name="credit_limit_respected", enforced=False,
                          message="Over by {excess}")
    assert named_one.element("aggregate") == {
        "name": "credit_limit_respected", "scope": "aggregate", "enforced": False, "meaning": "m",
        "message": "Over by {excess}", "expression": {"ast": ["<=", ["attr", "committed"], ["attr", "credit_limit"]]}}
    assert named_one.reads() == {"committed", "credit_limit"}
    with pytest.raises(ProtoValidationError, match="meaning"):
        Invariant("a < b", meaning="")


def test_intent_keeps_the_briefs_signature():
    from proto_db.errors import ProtoUserError
    from proto_db.semantic import intent_context

    @intent(meaning="Add a product to the order", commutes_with=["add_line", "remove_line"],
            parameters={"product": "acme.catalog.Product", "quantity": "decimal", "price": "decimal"})
    def add_line(self, product, quantity, price):
        intent_context().relate("acme.sales.OrderLine", order=self, product=product, quantity=quantity, price=price)

    spec = add_line.__proto_intent__
    assert [name for name, _ in spec.parameters] == ["product", "quantity", "price"]
    assert spec.element("add_line") == {
        "name": "add_line", "meaning": "Add a product to the order",
        "parameters": [["product", "acme.catalog.Product"], ["quantity", "decimal"], ["price", "decimal"]],
        "commutes_with": ["add_line", "remove_line"]}
    with pytest.raises(ProtoUserError, match="only while an intent runs"):
        intent_context()

    @intent(meaning="Annotated", requires="state == 'open'")
    def annotated(self, count: int, note: "str", when: datetime.date, anything):
        return None

    assert annotated.__proto_intent__.element("annotated") == {
        "name": "annotated", "meaning": "Annotated",
        "parameters": [["count", "int"], ["note", "str"], ["when", "date"], ["anything", None]],
        "requires": {"ast": ["==", ["attr", "state"], ["const", "open"]]}}


def test_refused_intents():
    with pytest.raises(ProtoValidationError, match="meaning"):
        intent(meaning="")
    with pytest.raises(ProtoValidationError, match="lambda"):
        intent(meaning="m")(lambda self: None)

    def rest(self, *rest):
        return None
    with pytest.raises(ProtoValidationError, match="'rest'"):
        intent(meaning="m")(rest)

    def extra(self, **extra):
        return None
    with pytest.raises(ProtoValidationError, match="'extra'"):
        intent(meaning="m")(extra)

    def positional(self, x, /):
        return None
    with pytest.raises(ProtoValidationError, match="'x'"):
        intent(meaning="m")(positional)
    with pytest.raises(ProtoValidationError, match="does not have"):
        @intent(meaning="m", parameters={"other": "int"})
        def f(self, x):
            return None
    with pytest.raises(ProtoValidationError, match="parameters="):
        @intent(meaning="m")
        def g(self, x: "Decimal"):
            return None

    def args(self, *more):
        return None
    with pytest.raises(ProtoValidationError, match="'more'"):
        intent(meaning="m")(args)
