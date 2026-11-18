"""The registry module ``acme_subclassed`` (depends on ``acme_sales``): brief block 2 as an
``Extension`` subclass, for the comparison with ``acme_decorated``."""
from datetime import date

from proto_db.registry import module
from proto_db.semantic import Extension, Field

subclassed = module("acme_subclassed", depends=("acme_sales",))


class OrderDelivery(Extension):
    __extends__ = "acme.sales.Order"
    delivery_date = Field(date, meaning="The committed delivery date")
    number = Field(int, required=True, label="Order no.")
