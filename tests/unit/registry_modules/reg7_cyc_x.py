"""Task 7 fixture: ``reg7.X`` has the parent ``reg7.Y``; with ``reg7_cyc_y`` installed too, the
registry-name parents form a cycle, though each module alone is valid."""
from proto_db.registry import module

x = module("reg7_cyc_x")


@x.extends("reg7.X", parents=("reg7.Y",))
class X:
    side = "x"
