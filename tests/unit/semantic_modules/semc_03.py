"""The registry module ``semc_03`` (depends on ``acme_sales``), for the concurrent-install test of the
semantic catalog (preflight F38): it adds the field ``semc_03_note`` to ``acme.catalog.Product``."""
from proto_db.registry import module
from proto_db.semantic import Extension, Field

semc = module("semc_03", depends=("acme_sales",))


class Semc03Note(Extension):
    __extends__ = "acme.catalog.Product"
    semc_03_note = Field(str, meaning="A note added by semc_03")
