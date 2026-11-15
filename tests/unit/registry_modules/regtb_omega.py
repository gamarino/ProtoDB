"""Tie-break fixture (ruling R253): an independent module whose name sorts after ``regtb_alpha``
(see that module)."""
from proto_db.registry import module

handle = module("regtb_omega")


@handle.extends("regtb.Item", parents=("regtb.FromOmega",))
class Item:
    tag = "omega"

    def who(self):
        following = self._super().who
        return "omega>" + (following() if following is not None else "end")


@handle.extends("regtb.FromOmega")
class FromOmega:
    omega_only = True
