"""The registry module ``semc_04`` (depends on ``acme_sales``), for the concurrent-install test of the
semantic catalog (preflight F38): it adds the field ``semc_04_note`` to ``acme.sales.OrderLine``."""
from proto_db.registry import module
from proto_db.semantic import Extension, Field

semc = module("semc_04", depends=("acme_sales",))


class Semc04Note(Extension):
    __extends__ = "acme.sales.OrderLine"
    semc_04_note = Field(str, meaning="A note added by semc_04")
