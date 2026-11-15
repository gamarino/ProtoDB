"""Uses the function object of ``reg_reuse_a``'s method in a definition of its own."""
from proto_db.registry import module

from .reg_reuse_a import Reuse as _ReuseA

reuse_b = module("reg_reuse_b", depends=("reg_reuse_a",))


@reuse_b.extends("reg.Other")
class Other:
    again = _ReuseA.shared
