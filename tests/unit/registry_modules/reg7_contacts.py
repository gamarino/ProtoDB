"""Task 7 fixture: a module that depends on ``reg7_base``, extends ``reg7.Partner`` again and
introduces ``reg7.Contact``."""
from proto_db.registry import module

contacts = module("reg7_contacts", depends=("reg7_base",))


@contacts.extends("reg7.Partner")
class Partner:
    channel = "email"


@contacts.extends("reg7.Contact", parents=("reg7.Partner",))
class Contact:
    role = "contact"
