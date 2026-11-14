"""Task 7 fix round 2 fixture (review N-2): import-time code that uses a transaction another
thread is committing.  The commit imports this module (outside the root lock) while this thread is
inside its import: it must not hold the transaction's lock meanwhile, so the read here fails fast
(the transaction is committing) instead of blocking forever.  Inert when
``reg7_driver.committing`` is ``None``."""
from proto_db.errors import ProtoDBError
from proto_db.registry import module
from tests.unit.registry_modules import reg7_driver

handle = module("reg7_peekcommit")


@handle.extends("reg7.PeekCommit")
class PeekCommit:
    v = 1


if reg7_driver.committing is not None:
    reg7_driver.importing.set()
    reg7_driver.go.wait(10)                 # the commit has reached its lock step
    try:
        reg7_driver.seen.append(reg7_driver.committing.installed_modules())
    except ProtoDBError as e:
        reg7_driver.seen.append(e)
