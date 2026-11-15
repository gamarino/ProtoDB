"""Task 6 fixture: a registry name that defines ``label``, which a DBObject subclass defines in Python."""
from proto_db.registry import module

shadow = module("reg6_shadow")


@shadow.extends("reg6.Shadowed")
class Shadowed:
    note = "registry"

    def label(self):
        return "registry label"
