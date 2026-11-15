"""Depends on ``reg_base``; gives ``reg.Partner`` the parent ``mail.Thread``."""
from proto_db.registry import module

contacts = module("reg_contacts", depends=("reg_base",))


@contacts.extends("reg.Partner", parents=("mail.Thread",))
class Partner:
    def label(self):
        return "contacts"


@contacts.extends("reg.Contact", parents=("reg.Partner",))
class Contact:
    kind = "contact"
