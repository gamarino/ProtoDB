# 1. Install and first database

This chapter installs `proto_db`, opens a space in memory and on disk, and follows one transaction from start
to finish. By the end you will have the first records of the tutorial's application: a small ERP with
users, groups, persons and opportunities. It is a model only, with no ERP features.

## Install

`proto_db` needs Python 3.12 or later. The tutorial's scripts are in a clone of the repository
(`examples/tutorial/`), not in the installed package. Until 2.0.0 is final the published versions are release
candidates, so `pip` needs `--pre`:

```text
pip install --pre proto_db
```

`import proto_db` imports no optional package and starts no thread. The extras (`vectors` for chapter 9,
`parquet`, `s3`, `gcs`, `hnsw`) are listed in the [README](../../README.md#installation).

## Four nouns

* An **object space** (`ObjectSpace`) owns one storage: memory, a directory of files, or a cluster.
* A **database** (`Database`) is a named set of **roots** inside the space. A root is a name bound to a
  value: a record, a collection, a number.
* A **transaction** (`Transaction`) is a snapshot of one database plus the changes it wants to make. It is
  an ordinary object: it is not tied to a thread, and several can be open at once.
* A **version** is what a commit produces. Nothing is overwritten: a commit publishes a new version of the
  database that shares everything unchanged with the previous one.

## A first transaction

```python
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
```

Output:

```text
ACME ARS ['ann', 'bob']
```

`tr.new_object(...)` makes a record (a `DBObject`) and `tr.new_list()` an empty list. A record is immutable,
and so is the list: `append_last` returned a new list, which is the one stored. Chapter 2 explains why.

## The life of a transaction

A transaction goes through four steps, and the `with` block above hid three of them:

1. **Begin.** `db.new_transaction()` takes the newest committed version as its snapshot, its *base*.
   Everything the transaction reads comes from that base, whatever other transactions commit meanwhile.
2. **Read and write.** `get_root_object` reads the base; `set_root_object` records a change. Nothing is
   visible to anyone else yet.
3. **Commit** (`tr.commit()`) checks that what the transaction relied on still holds (chapter 5) and
   publishes a new version. **Abort** (`tr.abort()`) drops the changes. A transaction that only read
   should be aborted: it has nothing to publish.
4. **Done.** A committed or aborted transaction accepts no more operations.

The same steps written out, with an exception that aborts the second transaction:

```python
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
```

Output:

```text
active: True
active after commit: False | version 2
a finished transaction refuses work: ProtoUserError
currency: USD
```

The exception inside the `with` block aborted its transaction, so the euro never reached the database.

## `Database.run`: the transaction as a function

Most application code writes a transaction as a function of the transaction and hands it to `run`. It opens a
transaction, calls the function, commits, and returns what the function returned. When the commit meets a
conflict (another transaction changed what this one relied on, chapter 5), `run` calls the function again on
a fresh snapshot.

```python
def add_login(login):
    def work(tr):
        logins = tr.get_root_object("logins")
        tr.set_root_object("logins", logins.append_last(login))
        return len(logins) + 1
    return work

print("logins now:", db.run(add_login("cy")))
```

Output:

```text
logins now: 3
```

## History comes with it

Because a commit never overwrites, every version stays readable. `Database.history()` lists them, newest
first, and `Database.snapshot_at` opens one, read-only:

```python
print([entry.version for entry in db.history()])
first = db.snapshot_at(db.history()[-2])          # version 1, the first commit
print(first.get_root_object("company").currency, list(first.get_root_object("logins")))
```

Output:

```text
[3, 2, 1, 0]
ARS ['ann', 'bob']
```

Version 0 is the empty database that `new_database` created. Chapter 6 builds on this.

## On disk

`FileStorage` keeps a space in a directory. The code is the same; only the storage changes. Close a space when
you are done: closing flushes and releases the directory's lock.

```python
import os
import tempfile

from proto_db import FileStorage

workdir = tempfile.TemporaryDirectory(prefix="proto_db-tutorial-")   # this chapter's own directory
home = os.getcwd()
os.chdir(workdir.name)                                # its files stay out of yours

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

os.chdir(home)
workdir.cleanup()                                     # removes only the directory this chapter created
```

Output:

```text
['erp']
ACME
```

The block works in a temporary directory of its own and removes it at the end, after closing the space, so
nothing is left behind; an application passes the path where its space lives. A commit that returned has
reached the disk: by default `FileStorage` calls `fsync` before it publishes the new root. Chapter 7 shows the
cheaper modes and what each one risks.

## What you have

A space, a database, records and lists, and the life of a transaction. The next chapter looks at the values
themselves: why they never change, and what that buys.

Depth: [getting started](../source/tutorials/getting-started.rst) and the [concepts](../source/concepts.rst).

Next: [2. Objects and immutable collections](02-values-and-collections.md)
