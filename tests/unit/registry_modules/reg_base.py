"""The base fixture module: no dependencies."""
from proto_db.registry import module

base = module("reg_base")


@base.extends("reg.Partner")
class Partner:
    """A partner: one data default, one mutable default and one method."""

    kind = "partner"
    tags = ["a", ["b"]]
    settings = {"lang": "en"}

    def label(self):
        return "base"


@base.extends("mail.Thread")
class Thread:
    def post(self, message):
        return message
