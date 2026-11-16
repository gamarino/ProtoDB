"""The registry module ``acme_zlate`` (depends on ``acme_sales``; its name puts it after ``acme_express``
in the load order): it redefines the label of ``acme.sales.Order``'s ``number``."""
from proto_db.registry import module
from proto_db.semantic import Extension, Field

late = module("acme_zlate", depends=("acme_sales",))


class LateNumber(Extension):
    __extends__ = "acme.sales.Order"
    number = Field(int, label="Late no.")
