"""Task 8 fixture: a module without dependencies that extends ``reg8.Thing`` differently from
``reg8_base`` (for a second database of one space)."""
from proto_db.registry import module

other = module("reg8_other")


@other.extends("reg8.Thing")
class Thing:
    def greet(self):
        return "other"
