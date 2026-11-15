"""Tie-break fixture (ruling R254): the module ``Regtb_upper`` (capital R, code point 82) sorts
before every ``regtb_*`` name (small r, code point 114) by code point, although a
case-insensitive or locale collation would place it among them."""
from proto_db.registry import module

handle = module("Regtb_upper")


@handle.extends("regtb.Item")
class Item:
    tag = "upper"

    def who(self):
        following = self._super().who
        return "upper>" + (following() if following is not None else "end")
