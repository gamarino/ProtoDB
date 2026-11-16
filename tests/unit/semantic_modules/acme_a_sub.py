"""The registry module ``acme_a_sub`` (depends on ``acme_sales``; it loads before ``acme_x_foo``):
``FooOrder``, a subclass of ``Order`` that declares ``foo``, which ``acme_x_foo`` adds to ``Order`` itself
(re-review finding 3: the module that defined the element on the type chain comes first in provenance)."""
from proto_db.registry import module
from proto_db.semantic import Field

from docs.kit.semantic.acme_sales import Order

a_sub = module("acme_a_sub", depends=("acme_sales",))


class FooOrder(Order):
    __type_name__ = "acme.sales.FooOrder"
    foo = Field(int, label="Foo of the subtype")
