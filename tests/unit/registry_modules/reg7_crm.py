"""Task 7 fixture: a module whose dependency ``reg7_contacts`` depends on ``reg7_base``; it
introduces ``reg7.Lead``."""
from proto_db.registry import module

crm = module("reg7_crm", depends=("reg7_contacts",))


@crm.extends("reg7.Lead", parents=("reg7.Contact",))
class Lead:
    stage = "new"
