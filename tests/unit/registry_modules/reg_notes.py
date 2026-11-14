"""Depends on ``reg_base`` only, as ``reg_contacts`` does: the two tie in the topological order."""
from proto_db.registry import module

notes = module("reg_notes", depends=("reg_base",))


@notes.extends("reg.Partner", parents=("mail.Activity", "mail.Thread"))
class Partner:
    note = None
