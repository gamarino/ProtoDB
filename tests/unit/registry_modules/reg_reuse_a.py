"""Defines a method that ``reg_reuse_b`` reuses."""
from proto_db.registry import module

reuse_a = module("reg_reuse_a")


@reuse_a.extends("reg.Reuse")
class Reuse:
    def shared(self):
        return "a"
