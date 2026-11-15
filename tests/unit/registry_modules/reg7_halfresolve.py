"""Final review C1 fixture: a registry module whose import-time code resolves a registry name
through a database that lists it, between its two extensions.  That lookup is refused, and caches
nothing.  Inert when ``reg7_driver.half`` is ``None``."""
from proto_db.errors import ProtoDBError
from proto_db.registry import module
from tests.unit.registry_modules import reg7_driver

handle = module("reg7_halfresolve")


@handle.extends("reg7.HalfResolve")
class Early:
    early = "early"


if reg7_driver.half is not None:
    with reg7_driver.half.new_transaction() as tr:
        try:
            reg7_driver.half_seen.append(tr.new_object(parents=("reg7.HalfResolve",)).early)
        except ProtoDBError as e:
            reg7_driver.half_seen.append(e)


@handle.extends("reg7.HalfResolve")
class Late:
    late = "late"
