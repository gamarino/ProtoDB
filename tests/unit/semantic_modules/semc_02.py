"""The registry module ``semc_02`` (depends on ``acme_sales``), for the concurrent-install test of the
semantic catalog (preflight F38): it adds the field ``semc_02_note`` to ``acme.sales.Customer``."""
from proto_db.registry import module
from proto_db.semantic import Extension, Field

semc = module("semc_02", depends=("acme_sales",))


class Semc02Note(Extension):
    __extends__ = "acme.sales.Customer"
    semc_02_note = Field(str, meaning="A note added by semc_02")
