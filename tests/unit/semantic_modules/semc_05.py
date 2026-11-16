"""The registry module ``semc_05`` (depends on ``acme_sales``), for the concurrent-install test of the
semantic catalog (preflight F38): it adds the field ``semc_05_note`` to ``acme.sales.Order``."""
from proto_db.registry import module
from proto_db.semantic import Extension, Field

semc = module("semc_05", depends=("acme_sales",))


class Semc05Note(Extension):
    __extends__ = "acme.sales.Order"
    semc_05_note = Field(str, meaning="A note added by semc_05")
