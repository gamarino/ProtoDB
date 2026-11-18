"""The registry module ``acme_sales`` of the semantic demo (spec section 3; the brief's blocks 1, 4, 6, 7
and 8, in English): a company, its customers with a credit limit, its products, and orders with lines.

It is imported as ``docs.kit.semantic.acme_sales`` (one canonical import path, preflight F41), through a
runtime's ``modules_mapping``.  Departures from the brief's text, each a ruling: ``total`` and
``amount`` are neutral expressions instead of lambdas (R276); the aggregate's ``where`` is
``state != 'cancelled'``, equivalent over the declared enumeration, since ``in`` is outside the MVP
subset; ``Customer`` declares ``__key__ = ("name",)`` (preflight F27); intents reach their effects
through ``intent_context()`` (R278).
"""
from datetime import date
from decimal import Decimal

from proto_db import DBObject
from proto_db.registry import module
from proto_db.semantic import (Aggregate, Computed, Enum, Field, Invariant, Relation, RelationTo, Relationship, Role,
                               intent, intent_context)

sales = module("acme_sales")


class Company(DBObject):
    __type_name__ = "acme.core.Company"
    __meaning__ = "The company that sells: it has customers, products and orders"
    __label__ = "Company"
    __key__ = ("name",)
    name = Field(str, required=True, label="Name")


class Customer(DBObject):
    __type_name__ = "acme.sales.Customer"
    __meaning__ = "A customer of the company"
    __label__ = "Customer"
    __key__ = ("name",)
    name = Field(str, required=True, label="Name", sensitivity="personal")
    credit_limit = Field(Decimal, unit="currency:company", meaning="The credit limit in force")
    company = Relation("acme.core.Company", cardinality="many-to-one", inverse="customers")
    orders = Relation("acme.sales.Order", cardinality="one-to-many", inverse="customer")
    committed = Aggregate(over="orders", where="state != 'cancelled'", sum="total", maintained=True,
                          unit="currency:company", meaning="The total of the customer's orders that are not cancelled")

    __constraints__ = [
        Invariant("committed <= credit_limit",
                  meaning="A customer cannot have open orders above its credit limit",
                  enforced=True,
                  message="The order exceeds the credit limit of {customer} by {excess}"),
    ]


class Product(DBObject):
    __type_name__ = "acme.catalog.Product"
    __meaning__ = "A product the company sells"
    __label__ = "Product"
    __key__ = ("name",)
    name = Field(str, required=True, label="Name")
    list_price = Field(Decimal, unit="currency:company", meaning="The current price of the product")
    company = Relation("acme.core.Company", cardinality="many-to-one", inverse="products")


class OrderLine(Relationship):
    __type_name__ = "acme.sales.OrderLine"
    __meaning__ = "A product included in an order, with the agreed terms"

    order = Role("acme.sales.Order", inverse="lines", ordered=True)
    product = Role("acme.catalog.Product", inverse="order_lines", index_by=["order.date"], binding="version")
    quantity = Field(Decimal, required=True, unit="unit:product")
    price = Field(Decimal, required=True, unit="currency:order",
                  meaning="The unit price agreed at the sale; not the product's current price")
    amount = Computed("quantity * price", unit="currency:order")


class Order(DBObject):
    __type_name__ = "acme.sales.Order"
    __meaning__ = "A customer's order to the company; open, confirmed or cancelled"
    __label__ = "Order"
    __synonyms__ = ["purchase order"]
    __key__ = ("company", "number")
    number = Field(int, required=True, label="Number", meaning="Sequential per company, assigned at creation")
    state = Field(Enum("open", "confirmed", "cancelled"), default="open")
    date = Field(date)
    company = Relation("acme.core.Company", cardinality="many-to-one", inverse="orders")
    customer = Relation("acme.sales.Customer", cardinality="many-to-one", inverse="orders")
    lines = RelationTo(OrderLine, role="order", ordered=True)
    total = Aggregate(over="lines", sum="amount", maintained=True, unit="currency:order")

    @intent(meaning="Add a product to the order with an agreed quantity and price",
            commutes_with=["add_line", "remove_line"],
            parameters={"product": "acme.catalog.Product", "quantity": "decimal", "price": "decimal"})
    def add_line(self, product, quantity, price):
        intent_context().relate("acme.sales.OrderLine", order=self, product=product,
                                quantity=quantity, price=price)

    @intent(meaning="Confirm the order; it accepts no new lines afterwards", requires="state == 'open'")
    def confirm(self):
        return self.with_(state="confirmed")
