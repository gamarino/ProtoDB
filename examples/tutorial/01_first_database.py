#!/usr/bin/env python3
"""1. Install and first database

Generated from docs/tutorial/01-first-database.md by ``python tests/docs/test_markdown_tutorials.py --write-scripts``:
the code below is the chapter's code blocks, verbatim and in order. After the blocks of each output the chapter
shows, ``_expect`` compares what they printed with it, so the script checks every figure the chapter shows,
exits with status 1 on the first difference, and prints "01_first_database: OK" at the end. Edit the chapter, not this file.

Run it from anywhere: python examples/tutorial/01_first_database.py
"""
import os as _os
import sys as _sys

_HERE = _os.path.dirname(_os.path.abspath(__file__))
_REPO = _os.path.normpath(_os.path.join(_HERE, *[_os.pardir] * 2))
_sys.path[:0] = [_REPO, _os.path.join(_REPO, "examples", "tutorial")]


class _Tee:
    """Standard output that also keeps what was printed since the last check."""

    def __init__(self, stream):
        self.stream, self.kept = stream, []

    def write(self, text):
        self.kept.append(text)
        return self.stream.write(text)

    def flush(self):
        self.stream.flush()


_sys.stdout = _Tee(_sys.stdout)


def _expect(shown):
    """Exit with status 1 unless the code since the last check printed ``shown`` (the chapter's Output)."""
    printed = "".join(_sys.stdout.kept)
    _sys.stdout.kept.clear()
    def normal(text):
        return "\n".join(line.rstrip() for line in text.strip().splitlines())
    if normal(printed) != normal(shown):
        _sys.stderr.write(f"--- the chapter shows ---\n{normal(shown)}\n--- the code printed ---\n{normal(printed)}\n")
        _sys.exit(1)


def _sample():
    """The chapter shows what the code since the last check printed as a sample of one run: not compared."""
    _sys.stdout.kept.clear()


# -- 01-first-database.md, line 31 ----------------------------------------
from proto_db import MemoryStorage, ObjectSpace

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")

with db.new_transaction() as tr:              # commits when the block ends, aborts if it raises
    tr.set_root_object("company", tr.new_object(name="ACME", currency="ARS"))
    tr.set_root_object("logins", tr.new_list().append_last("ann").append_last("bob"))

tr = db.new_transaction()
company = tr.get_root_object("company")
print(company.name, company.currency, list(tr.get_root_object("logins")))
tr.abort()
_expect("""
ACME ARS ['ann', 'bob']
""")

# -- 01-first-database.md, line 71 ----------------------------------------
from proto_db import ProtoUserError

tr = db.new_transaction()
print("active:", tr.is_active)
tr.set_root_object("company", tr.get_root_object("company").with_(currency="USD"))
tr.commit()
print("active after commit:", tr.is_active, "| version", tr.committed_version)
try:
    tr.set_root_object("company", None)
except ProtoUserError as error:
    print("a finished transaction refuses work:", type(error).__name__)

try:
    with db.new_transaction() as tr:
        tr.set_root_object("company", tr.get_root_object("company").with_(currency="EUR"))
        raise ValueError("the user cancelled")
except ValueError:
    pass
tr = db.new_transaction()
print("currency:", tr.get_root_object("company").currency)
tr.abort()
_expect("""
active: True
active after commit: False | version 2
a finished transaction refuses work: ProtoUserError
currency: USD
""")

# -- 01-first-database.md, line 113 ----------------------------------------
def add_login(login):
    def work(tr):
        logins = tr.get_root_object("logins")
        tr.set_root_object("logins", logins.append_last(login))
        return len(logins) + 1
    return work

print("logins now:", db.run(add_login("cy")))
_expect("""
logins now: 3
""")

# -- 01-first-database.md, line 135 ----------------------------------------
print([entry.version for entry in db.history()])
first = db.snapshot_at(db.history()[-2])          # version 1, the first commit
print(first.get_root_object("company").currency, list(first.get_root_object("logins")))
_expect("""
[3, 2, 1, 0]
ARS ['ann', 'bob']
""")

# -- 01-first-database.md, line 155 ----------------------------------------
from proto_db import FileStorage

space.close()

disk = ObjectSpace(FileStorage("erp-space"))
erp = disk.new_database("erp")
with erp.new_transaction() as tr:
    tr.set_root_object("company", tr.new_object(name="ACME", currency="ARS"))
disk.close()

reopened = ObjectSpace(FileStorage("erp-space"))
print(reopened.list_databases())
tr = reopened.open_database("erp").new_transaction()
print(tr.get_root_object("company").name)
tr.abort()
reopened.close()
_expect("""
['erp']
ACME
""")

print("01_first_database: OK")
