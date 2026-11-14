"""Task 6 fixture: a module that a second registry contains and the first does not."""
from proto_db.registry import module

other = module("reg6_other")


@other.extends("reg6.Opp")
class Opp:
    def other_super(self):
        return self._super().stage
