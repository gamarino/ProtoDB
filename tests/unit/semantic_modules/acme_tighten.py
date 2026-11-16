"""The registry module ``acme_tighten`` (depends on ``acme_sales``), for the final-review tests (I3): it
redefines ``acme.sales.Customer.credit_limit`` as ``secret``, a later tightening of its sensitivity.
"""
from decimal import Decimal

from proto_db.registry import module
from proto_db.semantic import Extension, Field

tighten = module("acme_tighten", depends=("acme_sales",))


class TightenCredit(Extension):
    __extends__ = "acme.sales.Customer"

    credit_limit = Field(Decimal, sensitivity="secret")
