CAD: reusing what earlier transactions read
===========================================

A computer-aided design model is a deep, mostly unchanging graph: an assembly holds subassemblies, which
hold parts, which hold features, which hold geometry. An engineer opens it, reads most of it, edits one
feature, and reads it again, many times a day. A database that re-reads the whole graph from storage for
every transaction wastes almost all of its work. ProtoDB does not have to: a pointer names one content
forever (P1), so what a transaction decoded can be reused by every later transaction, thread and reader of
the space with nothing to invalidate. This tutorial builds such a model, shows the cache counters, and then
shows where the benefit stops. Every code block runs in the test suite.

The model
---------

A gearbox with 4 subassemblies of 10 parts, each part with 5 features and each feature with a 20-number
geometry. Each level is a record holding a ``List`` of the next level, so a reader loads a level only when
it walks into it.

.. code-block:: python

   import time

   from proto_db import FileStorage, ObjectSpace

   def build(tr):
       subassemblies = tr.new_list()
       for s in range(4):
           parts = tr.new_list()
           for p in range(10):
               features = tr.new_list()
               for f in range(5):
                   geometry = tuple(float(s + p + f + i) for i in range(20))
                   features = features.append_last(tr.new_object(kind="hole" if f % 2 else "pad", geometry=geometry))
               parts = parts.append_last(tr.new_object(name=f"part {s}.{p}", features=features))
           subassemblies = subassemblies.append_last(tr.new_object(name=f"sub {s}", parts=parts))
       return tr.new_object(name="gearbox", subassemblies=subassemblies)

   def measure(tr):
       """Walk the whole model and add up the geometry: a stand-in for any full read."""
       total, features = 0.0, 0
       for sub in tr.get_root_object("assembly").subassemblies:
           for part in sub.parts:
               for feature in part.features:
                   total += sum(feature.geometry)
                   features += 1
       return features, total

   writer = ObjectSpace(FileStorage("gearbox-space"))
   with writer.new_database("cad").new_transaction() as tr:
       tr.set_root_object("assembly", build(tr))
   writer.close()

Cold, then warm
---------------

A reader opens the space from disk. Its first transaction has to decode every atom it touches; later
transactions find them already decoded. ``atom_cache_stats()`` counts the atoms read from storage
(``loads``) so we do not have to guess.

.. code-block:: python

   reader = ObjectSpace(FileStorage("gearbox-space"))
   cad = reader.open_database("cad")
   timings = {}

   def transaction(label):
       before = reader.atom_cache_stats()["loads"]
       start = time.perf_counter()
       tr = cad.new_transaction()
       result = measure(tr)
       tr.abort()
       timings[label] = (time.perf_counter() - start) * 1000
       print(f"{label:30} {result[0]} features, atoms loaded from storage: "
             f"{reader.atom_cache_stats()['loads'] - before}")

   transaction("transaction 1 (cold)")
   transaction("transaction 2 (warm)")
   transaction("transaction 3 (warm)")
   assert timings["transaction 2 (warm)"] < timings["transaction 1 (cold)"]

.. code-block:: text
   :caption: Output

   transaction 1 (cold)           200 features, atoms loaded from storage: 540
   transaction 2 (warm)           200 features, atoms loaded from storage: 0
   transaction 3 (warm)           200 features, atoms loaded from storage: 0

.. code-block:: python

   for label, ms in timings.items():
       print(f"{label:30} {ms:7.1f} ms")

.. code-block:: text
   :caption: Sample output (timings vary with the machine)

   transaction 1 (cold)             27.3 ms
   transaction 2 (warm)              0.3 ms
   transaction 3 (warm)              0.2 ms

The first walk loaded the model from disk; the next two loaded nothing, and took a small fraction of the
time. The timing is whatever your machine gives (the sample is one run, with the model in the operating
system's file cache; a cold disk makes the first transaction slower, not the others). The load counts are
exact and repeatable, and they are what the tutorial test checks.

An edit decodes only the changed path
-------------------------------------

Now another process edits one feature. We use a second ``ObjectSpace`` on the same directory to play that
process (in a cluster it would be another node, as the :doc:`cluster` tutorial shows). A version of a
feature is a new record; its part, subassembly and assembly are new records too, because their content
changed (they point at new children); everything else is shared with the old version.

.. code-block:: python

   editor = ObjectSpace(FileStorage("gearbox-space"))
   with editor.open_database("cad").new_transaction() as tr:
       assembly = tr.get_root_object("assembly")
       sub = assembly.subassemblies.get_at(2)
       part = sub.parts.get_at(7)
       feature = part.features.get_at(3)
       moved = feature.with_(geometry=tuple(g + 1 for g in feature.geometry))
       part = part.with_(features=part.features.set_at(3, moved))
       sub = sub.with_(parts=sub.parts.set_at(7, part))
       tr.set_root_object("assembly", assembly.with_(subassemblies=assembly.subassemblies.set_at(2, sub)))
   editor.close()

   transaction("reader, after the edit")
   transaction("reader, again")

.. code-block:: text
   :caption: Output

   reader, after the edit         200 features, atoms loaded from storage: 19
   reader, again                  200 features, atoms loaded from storage: 0

The reader's next transaction saw the new version and loaded only the atoms that differ: the new feature,
and the nodes on the way up from it. The other 500-odd atoms came from the reader's own cache, still valid,
because a pointer never changes its content. No message told the reader what had changed and nothing was
invalidated.

The same reuse works across threads of one process, since the cache belongs to the space and not to a
transaction. ``ObjectSpace`` is meant to be shared: open one per process and give every thread its own
transactions.

The limit: the working set must fit
-----------------------------------

The cache holds ``atom_cache_entries`` decoded atoms (default 50,000). If the atoms your transactions touch
do not fit, the oldest are evicted and reloaded on the next pass, and the benefit disappears (nothing breaks;
reads are just as slow as with no cache). The model above has 540 atoms (the cold run loaded 540), and each pass below loads each of them once:
an atom evicted while the walk is still inside it is not loaded again, because a walk keeps what it
still needs from a node when it first reads it. Give a reader a cache of only 50:

.. code-block:: python

   small = ObjectSpace(FileStorage("gearbox-space"), atom_cache_entries=50)
   cad_small = small.open_database("cad")
   for number in (1, 2, 3):
       before = small.atom_cache_stats()
       tr = cad_small.new_transaction()
       measure(tr)
       tr.abort()
       after = small.atom_cache_stats()
       loads = after["loads"] - before["loads"]
       evicted = after["read_unloads"] - before["read_unloads"]
       print(f"pass {number}: loads {loads}, evicted by those loads {evicted}, ratio {evicted / loads:.2f}")
   small.close()

.. code-block:: text
   :caption: Output

   pass 1: loads 540, evicted by those loads 490, ratio 0.91
   pass 2: loads 540, evicted by those loads 540, ratio 1.00
   pass 3: loads 540, evicted by those loads 540, ratio 1.00

This is the thrash signal. ``read_unloads`` divided by ``loads`` over an interval close to 0 means the set
fits; close to 1 means every load pushes another atom out, so every pass reloads what it needs. The first
pass over a set that was never loaded also approaches 1 when it is larger than the cache; judge a second
pass, as here, where the ratio of 1.00 says the model does not fit in 50 atoms.

How to size it
~~~~~~~~~~~~~~

* A point read walks one root-to-leaf path of an AVL tree, which has a height of at most about
  ``1.44 * log2(n)``, and then reads the record; a query touches the paths it needs plus every record it
  returns. Count what your *concurrent transactions together* touch repeatedly, and use the counters of this
  page to check the count rather than the formula.
* Each resident atom costs its decoded state plus about 0.4 KB of bookkeeping, measured on CPython 3.14t
  (decision D53, the residency amendment of Task 8.3m). 50,000 atoms is therefore on the order of tens of
  megabytes plus the state itself.
* Raise ``atom_cache_entries`` until the ratio on your real workload is near 0, and stop there. A
  larger cache costs memory, never correctness.

When this does not help, or another system is better
----------------------------------------------------

* If the model does not fit in the cache (a very large assembly read end to end), every pass reloads it,
  and ProtoDB is no faster than any database reading from disk.
* The benefit is largest where the same data is read repeatedly through new transactions, as in the
  examples above. A workload that reads each record once gains nothing from a cache of any kind.
* Other systems have caches too. What differs is invalidation: a cache over mutable data must learn
  when something changed, and across nodes that costs messages or staleness. Immutable stores share the property that
  cached data never goes stale (Datomic's peers cache immutable segments; this is general knowledge, not
  verified here), so it is a consequence of immutability, not something only ProtoDB has. The cluster-wide measurement, by workload (the ``cluster_cache`` profile: reads across transactions on two
  nodes under concurrent writes on a third, with the cache on and off, the decodes per read and the query-result
  hit rate), is in :doc:`../performance`; this page makes no performance claim of its own.
* A CAD kernel with its own in-memory representation will edit geometry faster than any database. ProtoDB's
  part here is the persistent, versioned, shareable store of the model and its history, not the modelling
  operations.

.. code-block:: python

   reader.close()
