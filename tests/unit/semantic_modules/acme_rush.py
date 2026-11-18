"""The registry module ``acme_rush`` (depends on ``acme_sales``): ``RushOrder``, a subclass of the
demo's ``Order`` (R280; the plan's Review Focus 6).  The demo does not use it."""
from proto_db.registry import module
from proto_db.semantic import Field

from docs.kit.semantic.acme_sales import Order

rush = module("acme_rush", depends=("acme_sales",))


class RushOrder(Order):
    __type_name__ = "acme.sales.RushOrder"
    __meaning__ = "An order delivered before the others"
    __label__ = "Rush order"
    priority = Field(int)
