"""Tie-break fixture (ruling R253): an independent module whose name sorts before ``regtb_omega``.
Both extend ``regtb.Item`` with the same method and data default, and each declares its own
parent, so the load order shows in the chain, the result and the parents union."""
from proto_db.registry import module

handle = module("regtb_alpha")


@handle.extends("regtb.Item", parents=("regtb.FromAlpha",))
class Item:
    tag = "alpha"

    def who(self):
        following = self._super().who
        return "alpha>" + (following() if following is not None else "end")


@handle.extends("regtb.FromAlpha")
class FromAlpha:
    alpha_only = True
