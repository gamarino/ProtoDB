"""The registry module ``semc_06`` (depends on ``acme_sales``), for the concurrent-install test of the
semantic catalog (preflight F38): it adds the field ``semc_06_note`` to ``acme.core.Company``."""
from proto_db.registry import module
from proto_db.semantic import Extension, Field

semc = module("semc_06", depends=("acme_sales",))


class Semc06Note(Extension):
    __extends__ = "acme.core.Company"
    semc_06_note = Field(str, meaning="A note added by semc_06")
