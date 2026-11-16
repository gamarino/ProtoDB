"""The registry module ``acme_decorated`` (depends on ``acme_sales``): brief block 2 in the decorator
spelling, the equivalent of ``acme_delivery``'s ``Extension`` subclass."""
from datetime import date

from proto_db.registry import module
from proto_db.semantic import Field

decorated = module("acme_decorated", depends=("acme_sales",))


@decorated.extends("acme.sales.Order")
class OrderDelivery:
    delivery_date = Field(date, meaning="The committed delivery date")
    number = Field(int, required=True, label="Order no.")
