"""Task 7 fix round fixture (review I-1): a registry module whose import-time code commits.

Importing it while another thread's commit holds the root lock and wants to import it too was a
permanent deadlock: that commit waited for the import lock, and this import waited for the root
lock.  Driven by ``reg7_driver``; inert when ``reg7_driver.db`` is ``None``."""
from proto_db.registry import module
from tests.unit.registry_modules import reg7_driver

handle = module("reg7_importcommit")


@handle.extends("reg7.ImportCommit")
class ImportCommit:
    v = 1


if reg7_driver.db is not None:
    reg7_driver.importing.set()             # the importing thread is inside this import now
    reg7_driver.go.wait(10)                 # the other commit takes the root lock first
    reg7_driver.db.run(lambda tr: tr.set_root_object("from_import", 1))
