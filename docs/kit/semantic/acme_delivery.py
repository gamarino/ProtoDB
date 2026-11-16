"""The registry module ``acme_delivery`` of the semantic demo (spec section 3; the brief's block 2, in
English): it depends on ``acme_sales``, adds the delivery date to the order and redefines the label of
the order number.  Imported as ``docs.kit.semantic.acme_delivery`` (preflight F41).
"""
from datetime import date

from proto_db.registry import module
from proto_db.semantic import Extension, Field

delivery = module("acme_delivery", depends=("acme_sales",))


class OrderDelivery(Extension):
    __extends__ = "acme.sales.Order"
    delivery_date = Field(date, meaning="The committed delivery date")
    number = Field(int, required=True, label="Order no.")      # redefines the presentation
