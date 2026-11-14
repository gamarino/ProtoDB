"""Half of a dependency cycle (with ``reg_cycle_b``)."""
from proto_db.registry import module

module("reg_cycle_a", depends=("reg_cycle_b",))
