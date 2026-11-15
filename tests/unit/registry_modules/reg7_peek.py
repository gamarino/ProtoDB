"""Task 7 fix round fixture (review M-5): import-time code that reads the installing transaction
from another thread; it would block if ``install_module`` held the transaction's lock while
importing.  Inert when ``reg7_driver.tr`` is ``None``."""
import threading

from proto_db.registry import module
from tests.unit.registry_modules import reg7_driver

handle = module("reg7_peek")


@handle.extends("reg7.Peek")
class Peek:
    v = 1


if reg7_driver.tr is not None:
    reader = threading.Thread(target=lambda: reg7_driver.peeked.append(reg7_driver.tr.installed_modules()),
                              daemon=True)
    reader.start()
    reader.join(5)
