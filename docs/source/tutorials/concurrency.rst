Concurrency and retries
=======================

Many threads, and many processes on a shared filesystem or in a cluster, can write the same database. This
tutorial shows what a transaction sees, what a conflict is, how ``Database.run`` retries, which writes
merge instead of conflicting, and one mistake the model allows. Every code block runs in the test suite,
and the threaded blocks run in both GIL modes.

A transaction sees one snapshot
-------------------------------

.. code-block:: python

   import threading

   from proto_db import MemoryStorage, ObjectSpace, ProtoConflictError

   space = ObjectSpace(MemoryStorage())
   db = space.new_database("counters")
   with db.new_transaction() as tr:
       tr.set_root_object("hits", 0)

   reader = db.new_transaction()
   with db.new_transaction() as tr:
       tr.set_root_object("hits", 41)
   print("reader still sees", reader.get_root_object("hits"))
   reader.abort()
   print("a new transaction sees", db.new_transaction().get_root_object("hits"))

.. code-block:: text
   :caption: Output

   reader still sees 0
   a new transaction sees 41

The reader took its snapshot when it started. Uncommitted work is invisible to everyone else, and a commit
never changes what an open transaction reads, so a long report is never disturbed by writers.

A conflict
----------

Two transactions read the same value and each writes an increment. If both were applied, one increment
would be lost. ProtoDB records what each transaction **read** (its read set) and, at commit, refuses a
transaction that read something that has changed since:

.. code-block:: python

   first, second = db.new_transaction(), db.new_transaction()
   first.set_root_object("hits", first.get_root_object("hits") + 1)
   second.set_root_object("hits", second.get_root_object("hits") + 1)
   first.commit()
   try:
       second.commit()
   except ProtoConflictError as error:
       print("second commit refused:", type(error).__name__)
   print("hits =", db.new_transaction().get_root_object("hits"))

.. code-block:: text
   :caption: Output

   second commit refused: ProtoConflictError
   hits = 42

Nothing of the second transaction was published. The rule is the same for every root, every
``MutableObject`` cell and the *absence* of a root: reading "there is no root called x" and then creating
it conflicts with a concurrent creator.

Retrying with ``run``
---------------------

A conflict is not an error in your program: it means "someone else got there first, try again on the new
state". ``Database.run(fn)`` does exactly that. It calls ``fn(tr)`` in a fresh transaction, commits, and on
a conflict aborts and calls ``fn`` again in a new transaction, with a bounded exponential back-off with
jitter between attempts.

.. code-block:: python

   def increment(tr):
       tr.set_root_object("hits", tr.get_root_object("hits") + 1)

   def worker():
       for _ in range(25):
           db.run(increment, retries=1000)

   threads = [threading.Thread(target=worker) for _ in range(4)]
   for t in threads:
       t.start()
   for t in threads:
       t.join()
   print("hits =", db.new_transaction().get_root_object("hits"), "(42 + 4 x 25)")

.. code-block:: text
   :caption: Output

   hits = 142 (42 + 4 x 25)

Four threads, one hot counter, no lost update. Three rules for the function you give ``run``:

* It may run **several times**, so everything it does outside the database (sending an email, calling an
  API) can happen several times. Do those things after ``run`` returns.
* It must do its reading **inside** the function, through the ``tr`` it receives. A value read before
  ``run`` belongs to an older snapshot.
* ``run`` re-raises the conflict after ``retries`` attempts (default 10). A hot key with many writers needs
  more retries or, better, a design where writers do not read what they only add to (next section).

Writes that merge instead of conflicting
----------------------------------------

Retrying is the safe default, but it serialises writers of the same root. Some collections avoid that when
a transaction **writes a root it never read**: a ``Dictionary``, ``HashDictionary``, ``Set``,
``CountedSet`` or ``RepeatedKeysDictionary`` built from empty in the transaction records the operations
you applied (per key, or per element), and the commit replays them onto whatever the root holds by then.
Writers of different keys do not conflict, and writers of the same key are applied in commit order.

.. code-block:: python

   def add_events(thread_number):
       for i in range(25):
           with db.new_transaction() as tr:
               # Never read "events": its operations are merged at commit.
               tr.set_root_object("events", tr.new_dictionary().set_at(f"t{thread_number}-{i}", i))

   threads = [threading.Thread(target=add_events, args=(n,)) for n in range(4)]
   for t in threads:
       t.start()
   for t in threads:
       t.join()
   print("events:", len(db.new_transaction().get_root_object("events")), "keys, no conflict, no retry")

.. code-block:: text
   :caption: Output

   events: 100 keys, no conflict, no retry

The merge applies the *intent* of each transaction (set this key to this value), never a guess about what a
value means: there is no "add the two numbers" for an integer, because ProtoDB cannot know that you meant a
counter. For counting, use a ``CountedSet`` (``add`` and ``remove`` are counter operations that merge), or
read-modify-write under ``run``.

A ``CountedSet`` merges its counts, so concurrent writers can count without retrying:

.. code-block:: python

   first, second = db.new_transaction(), db.new_transaction()
   first.set_root_object("visits", first.new_counted_set().add("home").add("home"))
   second.set_root_object("visits", second.new_counted_set().add("home").add("about"))
   first.commit()
   second.commit()                                       # no conflict: neither read "visits"
   visits = db.new_transaction().get_root_object("visits")
   print(visits.count_of("home"), visits.count_of("about"), visits.total_count)

.. code-block:: text
   :caption: Output

   3 1 4

Reading the root first makes the write a read-modify-write again:

.. code-block:: python

   first, second = db.new_transaction(), db.new_transaction()
   for tr, key in ((first, "a"), (second, "b")):
       events = tr.get_root_object("events")                 # a read: it enters the read set
       tr.set_root_object("events", events.set_at(key, 1))
   first.commit()
   try:
       second.commit()
   except ProtoConflictError:
       print("conflict: the second transaction read what the first changed")

.. code-block:: text
   :caption: Output

   conflict: the second transaction read what the first changed

Choose per root. If a writer must base its write on the current content ("add this order only if the
total is under the limit") it has to read, and conflicts are the correct outcome. If it only adds, do not
read.

One mistake the model allows
----------------------------

A transaction commits only for what *it* read and wrote. That is what makes transactions explicit objects
you can hand between threads, and it means the database cannot see a read-modify-write that you split
across two transactions:

.. code-block:: python

   read_tx = db.new_transaction()
   seen = read_tx.get_root_object("hits")
   read_tx.abort()

   with db.new_transaction() as tr:                   # another writer, meanwhile
       tr.set_root_object("hits", tr.get_root_object("hits") + 1000)

   with db.new_transaction() as tr:                   # writes a value computed from the stale read
       tr.set_root_object("hits", seen + 1)
   print("hits =", db.new_transaction().get_root_object("hits"), "(the +1000 was overwritten)")

.. code-block:: text
   :caption: Output

   hits = 143 (the +1000 was overwritten)

No exception was raised, because the second transaction read nothing. The fix is the rule of the section
above: read and write in the same transaction. The same applies to reusing an object that came from a
transaction whose commit failed. ProtoDB documents these cases instead of policing them with expensive
checks.

Outcome unknown
---------------

On storages that can fail after the data reached the medium (a cloud store, a cluster), a commit can end
with ``ProtoOutcomeUnknownError``: it may or may not have been applied. ``run`` never retries it, because a
retry could apply the work twice. Resolve it by reading: make each commit write a token (a unique id you
generate) and, after an unknown outcome, look for the token in the database. The :doc:`cluster` tutorial
uses ``sync`` and a barrier commit for the related question of what a reader sees.

.. code-block:: python

   space.close()

Limits
------

* ``MemoryStorage`` and ``FileStorage`` serialise the *publication* of each commit under a lock, so the
  commit rate of one database has a ceiling set by that critical section (and, with ``FileStorage``, by
  the ``fsync``). Many threads help with building and serialising the work, not with the publication.
  :doc:`../performance` has the measured rates by durability mode and by number of writers.
* Under heavy contention on one root, optimistic retry wastes work: every loser repeats it. If you have
  a single hot counter, a ``CountedSet`` or an arrangement where each writer owns its own key is faster
  than any retry policy.
* A relational database with row locks can be the better choice when many writers update the *same*
  rows for long transactions and you want the waiting done by the server.
