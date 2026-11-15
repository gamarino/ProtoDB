"""Tie-break fixture (ruling R255): the module ``aaa_regtb`` sorts before ``base`` by name, yet
``base`` stays first in the load order."""
from proto_db.registry import module

handle = module("aaa_regtb")


@handle.extends("regtb.Item")
class Item:
    tag = "aaa"

    def who(self):
        following = self._super().who
        return "aaa>" + (following() if following is not None else "end")
