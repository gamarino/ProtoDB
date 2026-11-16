"""The registry module ``semc_01`` (depends on ``acme_sales``), for the concurrent-install test of the
semantic catalog (preflight F38): it adds the field ``semc_01_note`` to ``acme.core.Company``."""
from proto_db.registry import module
from proto_db.semantic import Extension, Field

semc = module("semc_01", depends=("acme_sales",))


class Semc01Note(Extension):
    __extends__ = "acme.core.Company"
    semc_01_note = Field(str, meaning="A note added by semc_01")
