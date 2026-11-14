"""Half of a dependency cycle (with ``reg_cycle_a``)."""
from proto_db.registry import module

module("reg_cycle_b", depends=("reg_cycle_a",))
