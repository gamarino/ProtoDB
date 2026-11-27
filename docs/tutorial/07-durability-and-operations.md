# 7. Durability and operations

This chapter is for the person who runs the ERP: what a returned commit survives, what a crash leaves, how to
look inside a space, how to take and check a backup, and what the storage format 7 of 2.0.0rc5 means for an
upgrade.

## What a returned commit survives

A commit writes its records, makes them durable, and only then publishes the new root. `FileStorage(path,
durability=...)` decides what "durable" means:

| Mode | The commit waits for | Survives a process kill | Survives a power loss |
|---|---|---|---|
| `fsync` (default) | data `fsync`, then root and directory `fsync` | yes | yes |
| `flush` | data handed to the operating system, root `fsync` | yes | no |
| `none` | nothing | no guarantee | no |

Use `fsync` for data you cannot afford to lose; the other modes are for tests and repeatable bulk loads. After
a power loss in `flush` or `none` mode, opening the space can fail with `ProtoCorruptionError`; it never opens
a damaged database as if it were sound. What `fsync` costs is a property of the device: the
[operations tutorial](../source/tutorials/operations.rst) measures it on the machine that builds the docs, and
[performance](../source/performance.rst) has the measured rates by mode and by number of writers.

## A crash leaves the last published commit

The chapter works in a temporary directory of its own, removed at its end. To look at the files a space is
made of afterwards, run it with `PROTO_DB_TUTORIAL_KEEP=1` in the environment: the directory is then kept,
and `workdir.name` is its path.

The block starts a second Python process that commits in a loop, two roots that must always agree, kills it
with `SIGKILL` in the middle of its work, and opens what it left (POSIX only: `SIGKILL` does not exist on
Windows):

```python
import os
import signal
import subprocess
import sys
import tempfile
import time

from proto_db import FileStorage, ObjectSpace

keep = bool(os.environ.get("PROTO_DB_TUTORIAL_KEEP"))   # set it to keep the files for inspection
workdir = tempfile.TemporaryDirectory(prefix="proto_db-tutorial-", delete=not keep)
home = os.getcwd()
os.chdir(workdir.name)                                # its files stay out of yours

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
```

Output:

```text
killed by SIGKILL | the two roots agree: True | at least 21 commits: True
```

The writer was killed between two arbitrary instructions, perhaps half way through writing a record. Opening
the space read the last root that was published, a complete commit; anything written after it is ignored.
There is no recovery step to run and no log to replay.

## Looking inside

A space is a directory of plain files: `space.root` (the published root, replaced atomically), write-once
`<uuid>.seg` segments, and `space.lock`, which only carries the advisory lock between processes. Every record
carries a CRC32. `proto_db.tools.verify` (also `python -m proto_db.tools.verify <directory>`) decodes every
atom reachable from the root, every version of every database, checks the canonical encoding, validates every
collection and index, and never writes:

```python
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
```

Output:

```text
databases: ['erp'] | versions: [2, 1, 0]
root record version: 7 | collections: 13 | failures: []
roots: ['groups', 'opportunities', 'persons']
```

Run `verify` on a restored backup, and on a space another implementation wrote, before trusting it. A record
of an application type the process cannot import is read as an opaque atom and listed under
`opaque_types`: not a failure, but usually a missing import.

## Backup and restore

Nothing in a space directory is rewritten in place: segments are append-only and the root is replaced
atomically. A backup of a **closed** space is a copy of the directory, and it has the whole history:

```python
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
```

Output:

```text
verified: True | deal 7: won | versions: 3
```

An **open** space can be copied too, if `space.root` is copied *first* and the segments after it: data is
always written before the root that refers to it, so the copy is the state a crash would have left. The
[operations tutorial](../source/tutorials/operations.rst) runs that procedure against a live writer; it is a
consequence of the design, not a backup API. A cluster node's directory is not a backup of the cluster.

## Damage is loud

A damaged file never looks like an empty database:

```python
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

os.chdir(home)
if not keep:
    workdir.cleanup()                                 # removes only the directory this chapter created
```

Output:

```text
refused: ProtoCorruptionError
```

A damaged record is found when it is read, by its checksum, and raises `ProtoCorruptionError` naming its
offset. There is no repair tool: restore the backup.

## Upgrading to 2.0.0rc5: format 7

The precondition model of chapter 5 changes what a writer must do at commit, so 2.0.0rc5 marks the spaces it
writes. The `root record version: 7` printed above is that mark:

* **Opening an older space needs no migration.** A space written by earlier 2.0 code (root versions 2 to 6)
  opens as it is: the content of collections and roots did not change. Its first commit by 2.0.0rc5 writes a
  version-7 root.
* **The upgrade is one-way.** Once a space holds a version-7 root, software without the precondition model
  can neither read nor write it. A writer that predates the model would apply "the last writer wins" and break
  the guarantee for every other writer, so it is refused rather than tolerated.
* **Back up first.** Copy the closed space before its first commit under 2.0.0rc5 if you may need to go back.
* **Every node of a cluster** must run the same protocol (version 16); nodes of older protocols are refused.
* **protoBaseST** (the Smalltalk implementation of the format) cannot open a version-7 space until it ports
  the model.

The CHANGELOG lists every API change of the release (`replace=` removed from `set_root_object`, blind writes
compared, `root_names()` recorded as a read, the intents mode removed, and the rest).

Depth: [operations](../source/tutorials/operations.rst), [durability and recovery](../source/durability.rst),
[the storage format](../source/format-spec.rst), [compatibility in the concurrency
model](../source/concurrency-model.rst).

Next: [8. Extensible models and the runtime registry](08-extensible-models.md)
