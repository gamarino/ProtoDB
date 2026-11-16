"""The registry module ``acme_credit_cap`` (depends on ``acme_sales``), for the final-review tests (I2): it
adds the enforced invariant ``credit_limit <= 500`` to ``acme.sales.Customer``.
"""
from proto_db.registry import module
from proto_db.semantic import Extension, Invariant

credit_cap = module("acme_credit_cap", depends=("acme_sales",))


class CreditCap(Extension):
    __extends__ = "acme.sales.Customer"
    __constraints__ = [Invariant("credit_limit <= 500", meaning="A customer's credit is capped at 500",
                                 enforced=True, message="The credit limit is over the cap")]
