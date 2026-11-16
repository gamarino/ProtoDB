"""The registry module ``acme_restates`` (depends on ``acme_sales``): an extension that restates the
structural ``value_type`` of ``number`` with another value (refused at composition, spec section 5)."""
from proto_db.registry import module
from proto_db.semantic import Extension, Field

restates = module("acme_restates", depends=("acme_sales",))


class NumberAsText(Extension):
    __extends__ = "acme.sales.Order"
    number = Field(str, required=True, label="Order no.")
