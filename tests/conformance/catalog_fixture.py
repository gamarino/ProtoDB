"""The declarations of the catalog vector's model (``catalog_model.json``), in the brief's spelling
(``class Customer(DBObject)``, block 8): the central conformance test composes them and compares the
canonical form with ``vectors/catalog/canonical.msgpack`` (ruling R258).

The registry modules ``conformance.catalog_base`` and ``conformance.catalog_sales`` (preflight F10:
not ``conformance.base`` and ``conformance.sales``, which ``registry_fixture.py`` declares) are both
declared here, so each class names its module with ``__registry_module__``; :data:`MAPPING` maps both
names to this Python module.
"""
from datetime import date
from decimal import Decimal

from proto_db import DBObject
from proto_db.registry import module
from proto_db.semantic import (Aggregate, Computed, Enum, Extension, Field, Invariant, Relation, RelationTo,
                               Relationship, Role, intent, intent_context)

#: Registry module name -> the import path of the Python module that declares it.
MAPPING = {"conformance.catalog_base": __name__, "conformance.catalog_sales": __name__}

catalog_base = module("conformance.catalog_base")
catalog_sales = module("conformance.catalog_sales", depends=("conformance.catalog_base",))


class Customer(DBObject):
    __type_name__ = "conformance.sales.Customer"
    __registry_module__ = "conformance.catalog_base"
    __meaning__ = "A customer of the company"
    __label__ = "Customer"
    __key__ = ("name",)
    name = Field(str, required=True, label="Name", sensitivity="personal", ext={"ui.width": 40})
    credit_limit = Field(Decimal, unit="currency:company", meaning="The credit limit in force")


class Product(DBObject):
    __type_name__ = "conformance.sales.Product"
    __registry_module__ = "conformance.catalog_sales"
    __meaning__ = "A product the company sells"
    __label__ = "Product"
    __key__ = ("name",)
    name = Field(str, required=True)
    list_price = Field(Decimal, unit="currency:company", meaning="The current price of the product")


class OrderLine(Relationship):
    __type_name__ = "conformance.sales.OrderLine"
    __registry_module__ = "conformance.catalog_sales"
    __meaning__ = "A product included in an order, with the agreed terms"
    order = Role("conformance.sales.Order", inverse="lines", ordered=True)
    product = Role("conformance.sales.Product", index_by=["order.date"], binding="version")
    quantity = Field(Decimal, required=True, unit="unit:product")
    price = Field(Decimal, required=True, unit="currency:order",
                  meaning="The unit price agreed at the sale; not the product's current price",
                  caveats="Taxes are not included")
    amount = Computed("quantity * price")


class Order(DBObject):
    __type_name__ = "conformance.sales.Order"
    __registry_module__ = "conformance.catalog_sales"
    __meaning__ = "A customer's order; open, confirmed or cancelled"
    __label__ = "Order"
    __synonyms__ = ["purchase order"]
    __key__ = ("customer", "number")
    number = Field(int, required=True, label="Number", meaning="Sequential per customer, assigned at creation",
                   examples=[1001])
    state = Field(Enum("open", "confirmed", "cancelled"), default="open")
    date = Field(date, indexed=True)
    customer = Relation("conformance.sales.Customer", cardinality="many-to-one", inverse="orders")
    lines = RelationTo(OrderLine, role="order", ordered=True)
    total = Aggregate(over="lines", sum="amount", maintained=True, unit="currency:order")

    @intent(meaning="Add a product to the order with an agreed quantity and price", commutes_with=["add_line"],
            parameters={"product": "conformance.sales.Product", "quantity": "decimal", "price": "decimal"})
    def add_line(self, product, quantity, price):
        intent_context().relate("conformance.sales.OrderLine", order=self, product=product, quantity=quantity,
                                price=price)

    @intent(meaning="Confirm the order; it accepts no new lines afterwards", requires="state == 'open'")
    def confirm(self):
        return self.with_(state="confirmed")


class RushOrder(Order):
    __type_name__ = "conformance.sales.RushOrder"
    __registry_module__ = "conformance.catalog_sales"
    __meaning__ = "An order delivered before the others"
    __label__ = "Rush order"
    priority = Field(int)


class CustomerSales(Extension):
    __extends__ = "conformance.sales.Customer"
    __registry_module__ = "conformance.catalog_sales"
    credit_limit = Field(Decimal, label="Credit limit")
    orders = Relation("conformance.sales.Order", cardinality="one-to-many", inverse="customer")
    committed = Aggregate(over="orders", sum="total", where="state != 'cancelled'", maintained=True,
                          unit="currency:company")
    __constraints__ = [
        Invariant("committed <= credit_limit", name="credit_limit_respected",
                  meaning="A customer cannot have orders above its credit limit",
                  message="The order exceeds the credit limit of {entity} by {excess}"),
    ]
