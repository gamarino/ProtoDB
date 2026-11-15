"""Tie-break fixture (ruling R253): ``regtb_first`` sorts before ``regtb_zeta`` by name but
depends on it, so the dependency wins and ``regtb_zeta`` loads first."""
from proto_db.registry import module

handle = module("regtb_first", depends=("regtb_zeta",))


@handle.extends("regtb.Item")
class Item:
    tag = "first"

    def who(self):
        following = self._super().who
        return "first>" + (following() if following is not None else "end")
