#!/usr/bin/env python3
"""7. Durability and operations

Generated from docs/tutorial/07-durability-and-operations.md by ``python tests/docs/test_markdown_tutorials.py --write-scripts``:
the code below is the chapter's code blocks, verbatim and in order. After the blocks of each output the chapter
shows, ``_expect`` compares what they printed with it, so the script checks every figure the chapter shows,
exits with status 1 on the first difference, and prints "07_durability_and_operations: OK" at the end. Edit the chapter, not this file.

Run it from anywhere: python examples/tutorial/07_durability_and_operations.py
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


# -- 07-durability-and-operations.md, line 30 ----------------------------------------
import os
import signal
import subprocess
import sys
import time

from proto_db import FileStorage, ObjectSpace

WRITER = """
import sys
from proto_db import FileStorage, ObjectSpace
space = ObjectSpace(FileStorage("crash-space"))
db = space.new_database("erp")
i = 0
while True:
    with db.new_transaction() as tr:              # the count and the list must always agree
        tr.set_root_object("closed", i)
        tr.set_root_object("log", tr.new_list().append_last(i))
    if i == 20:
        print("committing", flush=True)
    i += 1
"""
env = dict(os.environ, PYTHONPATH=os.pathsep.join(sys.path))
writer = subprocess.Popen([sys.executable, "-c", WRITER], stdout=subprocess.PIPE, text=True, env=env)
assert writer.stdout.readline() == "committing\n"     # at least 21 commits are published
time.sleep(0.2)
writer.send_signal(signal.SIGKILL)                    # no clean-up, no close: a crash
writer.wait()

space = ObjectSpace(FileStorage("crash-space"))
tr = space.open_database("erp").new_transaction()
closed, log = tr.get_root_object("closed"), list(tr.get_root_object("log"))
print("killed by", signal.Signals(-writer.returncode).name, "| the two roots agree:", log == [closed],
      "| at least 21 commits:", closed >= 20)
tr.abort()
space.close()
_expect("""
killed by SIGKILL | the two roots agree: True | at least 21 commits: True
""")

# -- 07-durability-and-operations.md, line 87 ----------------------------------------
from proto_db.tools.verify import verify

import erp_app

space = ObjectSpace(FileStorage("erp-space"))
db = space.new_database("erp")
erp_app.seed(db, opportunities=300)
erp_app.advance(db, 7, "won", "bob")
print("databases:", space.list_databases(), "| versions:", [entry.version for entry in db.history()])
space.close()

report, failures = verify("erp-space")
print("root record version:", report["root"]["version"], "| collections:", report["collections"],
      "| failures:", failures)
print("roots:", [name for name, _ in report["databases"]["erp"]["roots"]])
_expect("""
databases: ['erp'] | versions: [2, 1, 0]
root record version: 7 | collections: 13 | failures: []
roots: ['groups', 'opportunities', 'persons']
""")

# -- 07-durability-and-operations.md, line 122 ----------------------------------------
import shutil

shutil.copytree("erp-space", "erp-backup")
report, failures = verify("erp-backup")
restored = ObjectSpace(FileStorage("erp-backup"))
db = restored.open_database("erp")
tr = db.new_transaction()
print("verified:", not failures, "| deal 7:", tr.get_root_object("opportunities").get_at(7).stage,
      "| versions:", len(db.history()))
tr.abort()
restored.close()
_expect("""
verified: True | deal 7: won | versions: 3
""")

# -- 07-durability-and-operations.md, line 151 ----------------------------------------
from proto_db import ProtoCorruptionError

shutil.copytree("erp-backup", "erp-damaged")
with open(os.path.join("erp-damaged", "space.root"), "r+b") as f:
    f.seek(10)
    byte = f.read(1)
    f.seek(10)
    f.write(bytes([byte[0] ^ 0xFF]))                  # flip one byte of the root record
try:
    FileStorage("erp-damaged")
except ProtoCorruptionError as error:
    print("refused:", type(error).__name__)
_expect("""
refused: ProtoCorruptionError
""")

print("07_durability_and_operations: OK")
