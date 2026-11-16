"""The registry modules ``acme_informative``, ``acme_far_invariant`` and ``acme_favourites`` (each depends on
``acme_sales``), for the constraint tests (spec section 10; Task 9):

- ``acme_informative`` adds to ``acme.sales.Customer`` an invariant declared ``enforced=False``: published for
  agents, never enforced;
- ``acme_far_invariant`` adds to ``acme.sales.Order`` an enforced invariant that reads another entity (its
  customer's credit limit): a change of that entity would not re-check it, so composition refuses it;
- ``acme_favourites`` adds to ``acme.sales.Customer`` a ``1..*`` end: every customer the commit touches holds at
  least one favourite product.
"""
from proto_db.registry import module
from proto_db.semantic import Extension, Invariant, Relation

for _name in ("acme_informative", "acme_far_invariant", "acme_favourites"):
    module(_name, depends=("acme_sales",))


class CustomerMargin(Extension):
    __extends__ = "acme.sales.Customer"
    __registry_module__ = "acme_informative"
    __constraints__ = [
        Invariant("committed <= credit_limit - 50", name="credit_margin", enforced=False,
                  meaning="A customer should keep a margin of 50 below its credit limit",
                  message="{customer} is {excess} into its margin"),
    ]


class OrderWithinCustomerLimit(Extension):
    __extends__ = "acme.sales.Order"
    __registry_module__ = "acme_far_invariant"
    __constraints__ = [
        Invariant("total <= customer.credit_limit", name="order_within_limit",
                  meaning="No single order exceeds its customer's credit limit"),
    ]


class CustomerFavourites(Extension):
    __extends__ = "acme.sales.Customer"
    __registry_module__ = "acme_favourites"
    favourites = Relation("acme.catalog.Product", cardinality="1..*")
