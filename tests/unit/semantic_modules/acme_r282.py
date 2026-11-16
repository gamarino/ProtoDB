"""The registry modules ``r282_value_type``, ``r282_cardinality``, ``r282_target``, ``r282_binding`` and
``r282_kind`` (each depends on ``acme_sales``): subclasses of ``Order`` that change what an inherited
element may not change (R282).  Each is refused at composition."""
from proto_db.registry import module
from proto_db.semantic import Computed, Field, Relation

from docs.kit.semantic.acme_sales import Order

for _field in ("value_type", "cardinality", "target", "binding", "kind"):
    module(f"r282_{_field}", depends=("acme_sales",))


class TextNumberOrder(Order):
    __type_name__ = "acme.r282.TextNumberOrder"
    __registry_module__ = "r282_value_type"
    number = Field(str, required=True)


class ManyCustomersOrder(Order):
    __type_name__ = "acme.r282.ManyCustomersOrder"
    __registry_module__ = "r282_cardinality"
    customer = Relation("acme.sales.Customer", cardinality="many-to-many", inverse="orders")


class OtherCompanyOrder(Order):
    __type_name__ = "acme.r282.OtherCompanyOrder"
    __registry_module__ = "r282_target"
    customer = Relation("acme.core.Company", cardinality="many-to-one", inverse="orders")


class VersionLinesOrder(Order):
    __type_name__ = "acme.r282.VersionLinesOrder"
    __registry_module__ = "r282_binding"
    customer = Relation("acme.sales.Customer", cardinality="many-to-one", inverse="orders", binding="version")


class ComputedStateOrder(Order):
    __type_name__ = "acme.r282.ComputedStateOrder"
    __registry_module__ = "r282_kind"
    state = Computed("number")
