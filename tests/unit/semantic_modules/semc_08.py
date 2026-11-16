"""The registry module ``semc_08`` (depends on ``acme_sales``), for the concurrent-install test of the
semantic catalog (preflight F38): it adds the field ``semc_08_note`` to ``acme.sales.Order``."""
from proto_db.registry import module
from proto_db.semantic import Extension, Field

semc = module("semc_08", depends=("acme_sales",))


class Semc08Note(Extension):
    __extends__ = "acme.sales.Order"
    semc_08_note = Field(str, meaning="A note added by semc_08")
