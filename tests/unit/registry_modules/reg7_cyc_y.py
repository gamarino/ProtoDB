"""Task 7 fixture: ``reg7.Y`` has the parent ``reg7.X`` (see ``reg7_cyc_x``)."""
from proto_db.registry import module

y = module("reg7_cyc_y")


@y.extends("reg7.Y", parents=("reg7.X",))
class Y:
    side = "y"
