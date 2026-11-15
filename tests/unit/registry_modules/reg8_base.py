"""Task 8 fixture: a module without dependencies that extends ``reg8.Thing`` with a data default
and a method."""
from proto_db.registry import module

base = module("reg8_base")


@base.extends("reg8.Thing")
class Thing:
    kind = "thing"

    def greet(self):
        return "base"
