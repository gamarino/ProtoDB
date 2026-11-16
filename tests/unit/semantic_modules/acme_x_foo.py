"""The registry module ``acme_x_foo`` (depends on ``acme_sales``; it loads after ``acme_a_sub``): adds
``foo`` to ``acme.sales.Order``."""
from proto_db.registry import module
from proto_db.semantic import Extension, Field

x_foo = module("acme_x_foo", depends=("acme_sales",))


class AddFoo(Extension):
    __extends__ = "acme.sales.Order"
    foo = Field(int, label="Foo of the order")
