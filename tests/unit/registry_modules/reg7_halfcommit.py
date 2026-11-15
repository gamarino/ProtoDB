"""Final review C1 fixture: a registry module whose import-time code commits through a database
that lists it, between its two extensions.  The commit's prebuild must not build (and cache) the
registry of that list from the first extension only.  Inert when ``reg7_driver.half`` is ``None``."""
from proto_db.registry import module
from tests.unit.registry_modules import reg7_driver

handle = module("reg7_halfcommit")


@handle.extends("reg7.Half")
class Early:
    early = "early"


if reg7_driver.half is not None:
    reg7_driver.half.run(lambda tr: tr.set_root_object("seeded", 1))
    reg7_driver.half_seen.append("committed")


@handle.extends("reg7.Half")
class Late:
    late = "late"
