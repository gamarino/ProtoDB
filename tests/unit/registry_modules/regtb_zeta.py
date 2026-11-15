"""Tie-break fixture (ruling R253): the dependency of ``regtb_first`` (see that module)."""
from proto_db.registry import module

handle = module("regtb_zeta")


@handle.extends("regtb.Item")
class Item:
    tag = "zeta"

    def who(self):
        following = self._super().who
        return "zeta>" + (following() if following is not None else "end")
