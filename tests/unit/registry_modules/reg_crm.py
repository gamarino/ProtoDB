"""Depends on ``reg_base`` and ``reg_contacts``."""
from proto_db.registry import module

crm = module("reg_crm", depends=("reg_base", "reg_contacts"))


@crm.extends("reg.Partner")
class Partner:
    stage = "new"

    def label(self):
        return "crm"
