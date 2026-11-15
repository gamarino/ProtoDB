"""Task 7 fix round 2 fixture (review N-3): import-time code that installs the module being
imported, before its second extension is declared.  Inert when ``reg7_driver.selfinstall`` is
``None``."""
from proto_db.errors import ProtoDBError
from proto_db.registry import module
from tests.unit.registry_modules import reg7_driver

handle = module("reg7_selfinstall")


@handle.extends("reg7.SelfFirst")
class SelfFirst:
    v = 1


if reg7_driver.selfinstall is not None:
    try:
        reg7_driver.selfinstall.install_module("reg7_selfinstall")
    except ProtoDBError as e:
        reg7_driver.selfinstall_errors.append(e)


@handle.extends("reg7.SelfSecond")
class SelfSecond:
    v = 2
