"""Two registry names that are each other's parent."""
from proto_db.registry import module

name_cycle = module("reg_name_cycle")


@name_cycle.extends("cyc.A", parents=("cyc.B",))
class A:
    pass


@name_cycle.extends("cyc.B", parents=("cyc.A",))
class B:
    pass
