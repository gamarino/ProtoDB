Operations: durability, inspection and backup
=============================================

This tutorial is for the person who runs a ProtoDB application: what a returned commit survives, how to
look inside a space without a special tool, how to take a backup and check it, and what the database does
when a file is damaged. Every code block runs in the test suite.

Durability modes
----------------

``FileStorage(path, durability=...)`` decides what a commit that has returned survives. The commit order
is always the same: write the records, make them durable, then publish the new root. The mode changes only
what "make them durable" does.

=========== =========================================== ======================== ==========================
Mode        What the commit waits for                   Survives a process kill  Survives power loss
=========== =========================================== ======================== ==========================
``fsync``   data ``fsync``, then root ``fsync`` and     yes                      yes
            directory ``fsync``
``flush``   data handed to the operating system, root   yes                      no
            ``fsync``
``none``    nothing                                     no guarantee             no
=========== =========================================== ======================== ==========================

After a power loss in ``flush`` or ``none`` mode, acknowledged commits can be lost, and the published
root can name records that never reached the disk. Opening such a space checks the record at the root
and can then fail with ``ProtoCorruptionError``; it never opens an empty or half-written database as if
it were sound.

``fsync`` is the default and the only mode to use for data you cannot afford to lose. The other two exist
for tests, benchmarks and bulk loads that can be repeated. How much ``fsync`` costs is a property of the
storage device and the file system, so measure it where you will run. The next block does, on the machine
that builds this page.

.. code-block:: python

   import time

   from proto_db import FileStorage, ObjectSpace

   def commit_rate(mode, commits=20):
       space = ObjectSpace(FileStorage(f"space-{mode}", durability=mode))
       db = space.new_database("d")
       start = time.perf_counter()
       for i in range(commits):
           with db.new_transaction() as tr:
               tr.set_root_object("n", i)
       elapsed = time.perf_counter() - start
       space.close()
       return commits / elapsed

   rates = {mode: commit_rate(mode) for mode in ("fsync", "flush", "none")}
   for mode, rate in rates.items():
       print(f"{mode:6} {rate:8.0f} commits/s (single thread, 20 commits)")

.. code-block:: text
   :caption: Sample output (varies with the machine and its load)

   fsync       651 commits/s (single thread, 20 commits)
   flush      1049 commits/s (single thread, 20 commits)
   none       1230 commits/s (single thread, 20 commits)

The ``fsync`` rate is set by the storage device, not by ProtoDB, and the sample above is only one machine's
answer (on it the three modes are close, which is not what a device with a slow flush gives). Concurrent committers share the device cost through *group commit*: commits that arrive together
are published together, so the rate per commit improves with the number of writers. The measured rates by
mode and by writer count are in :doc:`../performance`.

Looking inside a space
----------------------

ProtoDB 2.0 does not ship an ``inspect`` command-line tool. What you can see without one:

.. code-block:: python

   space = ObjectSpace(FileStorage("library"))
   db = space.new_database("books")
   for title in ("Dune", "Solaris", "Hyperion"):
       with db.new_transaction() as tr:
           books = tr.get_root_object("books") or tr.new_list()
           tr.set_root_object("books", books.append_last(title), replace=True)

   print("databases:", space.list_databases())
   for entry in db.history():
       print(entry.version, entry.root_names)
   print("timestamps are UTC:", all(e.committed_at.utcoffset().total_seconds() == 0 for e in db.history()))
   print("caches:", sorted(space.atom_cache_stats()), "|", sorted(space.query_cache_stats()))
   space.close()

.. code-block:: text
   :caption: Output

   databases: ['books']
   3 ('books',)
   2 ('books',)
   1 ('books',)
   0 ()
   timestamps are UTC: True
   caches: ['capacity', 'entries', 'hits', 'identity_map', 'loads', 'misses', 'read_unloads', 'saved', 'unloads'] | ['admissions', 'bytes', 'capacity', 'entries', 'evictions', 'hits', 'misses', 'refused', 'uncacheable']

``history()`` lists every version (each also has ``committed_at``), a ``Snapshot`` opens any of them, and
the two cache counters tell you whether the working set fits (see :doc:`cad`). For the bytes themselves,
a space directory is plain files:

.. code-block:: python

   import os

   from proto_db.storage.format import RECORD_HEADER_SIZE, SEGMENT_HEADER_SIZE, decode_segment_header, \
       read_record, record_size

   def scan_segment(path):
       """Count the records of one segment by kind, checking every checksum; return (counts, bytes_read)."""
       counts, offset = {}, SEGMENT_HEADER_SIZE
       with open(path, "rb") as f:
           data = f.read()
       decode_segment_header(data)                           # raises ProtoCorruptionError if it is not a segment
       while offset < len(data):
           kind, _payload = read_record(data, offset)        # verifies the header and the payload checksums
           counts[kind.name] = counts.get(kind.name, 0) + 1
           offset += record_size(data[offset:offset + RECORD_HEADER_SIZE])
       return counts, offset

   print(sorted(name if name.endswith((".lock", ".root")) else "<uuid>.seg" for name in os.listdir("library")))
   for name in sorted(os.listdir("library")):
       if name.endswith(".seg"):
           print("segment:", scan_segment(os.path.join("library", name)))

.. code-block:: text
   :caption: Output

   ['<uuid>.seg', 'space.lock', 'space.root']
   segment: ({'ATOM': 37}, 4278)

The layout is a ``space.root`` file (the published root, replaced atomically), one or more write-once
``<uuid>.seg`` segments, and a ``space.lock`` file that only carries the advisory lock. The byte-level
specification is in :doc:`../format-spec`. Because every record carries a CRC32, ``scan_segment`` is also a
full integrity check of a segment, which is what you want after a restore.

Backup and restore
------------------

Everything is in the directory and nothing in it is ever rewritten in place: segments are append-only
(and written by one process only), and the root is replaced atomically. A backup is therefore a copy of
the directory.

.. code-block:: python

   import shutil

   shutil.copytree("library", "library-backup")
   restored = ObjectSpace(FileStorage("library-backup"))
   tr = restored.open_database("books").new_transaction()
   print(list(tr.get_root_object("books")), [e.version for e in restored.open_database("books").history()])
   tr.abort()
   restored.close()

.. code-block:: text
   :caption: Output

   ['Dune', 'Solaris', 'Hyperion'] [3, 2, 1, 0]

The restored copy has the whole history, not only the last state.

Copying a space that is **open** is also possible, if you copy in the right order. Three rules of the
storage make it work: data is always written before the root that refers to it, a segment is only ever
appended to, and ``FileStorage`` never deletes a segment. So a copy of ``space.root`` taken *first*, followed
by the segments, holds every record that root refers to, and possibly a torn record at the end of a
segment that a later commit was writing. That is exactly the state a crash leaves, which opening a space
handles by ignoring everything after the published root.

.. code-block:: python

   import threading

   space = ObjectSpace(FileStorage("busy"))
   busy = space.new_database("d")
   stop = threading.Event()

   def writer():
       i = 0
       while not stop.is_set():
           with busy.new_transaction() as tr:               # two roots that must always agree
               tr.set_root_object("n", i)
               tr.set_root_object("l", tr.new_list().append_last(i))
           i += 1

   thread = threading.Thread(target=writer)
   thread.start()
   consistent = 0
   for k in range(5):
       time.sleep(0.05)
       target = f"busy-copy-{k}"
       os.makedirs(target)
       shutil.copy2("busy/space.root", target)               # the root first ...
       for name in os.listdir("busy"):
           if name.endswith(".seg"):
               shutil.copy2(os.path.join("busy", name), target)      # ... then the segments
       copy = ObjectSpace(FileStorage(target))
       tr = copy.open_database("d").new_transaction()
       consistent += list(tr.get_root_object("l")) == [tr.get_root_object("n")]
       tr.abort()
       copy.close()
   stop.set()
   thread.join()
   space.close()
   print(consistent, "of 5 live copies opened as one consistent commit")

.. code-block:: text
   :caption: Output

   5 of 5 live copies opened as one consistent commit

This is a consequence of the design, not a supported backup API: there is no ``backup()`` call, and the
product test suite does not run this procedure (the crash tests cover the states it relies on).
Rules that keep a backup sound:

* Prefer to copy a **closed** space, or use the root-first order above and verify the copy.
* Verify what you copied. ``scan_segment`` above checks every record, and opening the copy checks the root
  and its pointer.
* A backup of a cluster node is not a backup of the cluster: see :doc:`../coordinator-protocol` for what a
  node's directory holds and what the object store holds.

Damage is loud
--------------

A damaged file never looks like an empty database. Flip one byte in the root and open the space:

.. code-block:: python

   from proto_db import ProtoCorruptionError

   shutil.copytree("library-backup", "library-damaged")
   with open(os.path.join("library-damaged", "space.root"), "r+b") as f:
       f.seek(10)
       byte = f.read(1)
       f.seek(10)
       f.write(bytes([byte[0] ^ 0xFF]))
   try:
       FileStorage("library-damaged")
   except ProtoCorruptionError as error:
       print("refused:", type(error).__name__)

.. code-block:: text
   :caption: Output

   refused: ProtoCorruptionError

The same holds for a damaged record: it is found when it is read, by its checksum, and raises
``ProtoCorruptionError`` naming the offset; a torn tail left by a crash is never followed by later appends
and is ignored. There is no repair tool: restore the backup. Version history helps here too, because
older versions of data live in older records that a damage to newer ones has not touched, but only a
backup makes that a guarantee.

If an I/O error occurs while writing, the storage instance is marked *failed*: later writes raise
``ProtoUnexpectedError`` and reads keep working, so that no later root can reference a record whose write
was lost. Recovery is to open a new instance on the directory.

Cache health
------------

The decoded-atom cache is the one setting worth watching. ``atom_cache_entries`` (default 50,000 atoms)
bounds how many decoded atoms a space keeps. When the set of atoms your reads touch does not fit,
reads reload what was evicted. ``atom_cache_stats()`` tells the two cases apart: compare two snapshots, and
``read_unloads`` divided by ``loads`` near 0 means it fits, near 1 means it thrashes. :doc:`cad` shows
the measurement on a real structure.

When another system is the better choice
----------------------------------------

* ProtoDB has no online backup, no point-in-time restore tooling and no replication to a standby other than
  the cluster tier. A database that must offer those today (streaming replicas, ``pg_basebackup``-style
  hot backups, managed backups) is better served by PostgreSQL or a managed service.
* ``fsync`` per commit means one commit costs one device flush. For many tiny independent writes per
  second from a single writer, a log-structured system tuned for that, or a batch API, does better.
