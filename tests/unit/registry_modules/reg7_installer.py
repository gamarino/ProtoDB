"""Task 7 fix round helper: run as a separate process, install the module ``argv[2]`` into the
database ``db`` of the file-storage space at ``argv[1]`` (the other process of the I-1 scenario)."""
import sys

from proto_db.storage.file import FileStorage
from proto_db.transactions import ObjectSpace

if __name__ == "__main__":
    path, name = sys.argv[1], sys.argv[2]
    space = ObjectSpace(FileStorage(path), modules_mapping={name: f"tests.unit.registry_modules.{name}"})
    try:
        space.open_database("db").run(lambda tr: tr.install_module(name))
    finally:
        space.close()
