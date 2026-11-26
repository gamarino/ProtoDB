Concurrency and retries
=======================

Many threads can write the same database, and so can several processes on one host (the file lock between
processes is only as reliable as the operating system's advisory locks; on a network file system it is only
as reliable as that file system's, so do not rely on it there) or in a cluster. This tutorial shows what a
transaction sees, what a conflict is and what it names, how ``Database.run`` retries, which concurrent writes
compose instead of conflicting, the patterns that keep conflicts narrow ("check and add", "check a
reference", identifiers without a counter, lock cells), the one deliberate way out of the guarantee, and one
mistake the model allows. The rules behind every result are on the :doc:`../concurrency-model` page. Every
code block runs in the test suite, and the threaded blocks run in both GIL modes.

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

The reader took its snapshot, its **base**, when it started. Uncommitted work is invisible to everyone else,
and a commit never changes what an open transaction reads, so a long report is never disturbed by writers.

A conflict, and what it names
-----------------------------

Two transactions read the same value and each writes an increment. If both were applied, one increment
would be lost. Each read is a **precondition**: the transaction declares "my work is valid only while this
still holds what I saw". At commit every precondition is checked against the newest committed state, and a
transaction whose preconditions no longer hold is refused with ``ProtoConflictError``:

.. code-block:: python

   first, second = db.new_transaction(), db.new_transaction()
   first.set_root_object("hits", first.get_root_object("hits") + 1)
   second.set_root_object("hits", second.get_root_object("hits") + 1)
   first.commit()
   try:
       second.commit()
   except ProtoConflictError as error:
       print("second commit refused:", error)
       print("unit:", error.unit.kind.name, error.unit.root, "of database", error.database)
   print("hits =", db.new_transaction().get_root_object("hits"))

.. code-block:: text
   :caption: Output

   second commit refused: Root 'hits' of database 'counters' was changed by a concurrent commit (unit ROOT)
   unit: ROOT hits of database counters
   hits = 42

Nothing of the second transaction was published. The error names the **conflict unit** that failed: here a
root, because an integer is an object root and an object root is one unit. Reading "there is no root called
x" is a precondition too, so reading an absent root and then creating it conflicts with a concurrent creator.

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

Four threads, one hot counter, no lost update. Because ``run`` may call your function **several times**,
everything the function does outside the database (sending an email, calling an API, appending to a Python
list) can happen several times. The next block forces one conflict by committing a concurrent change from
inside the first attempt:

.. code-block:: python

   attempts, outbox = [], []

   def place_order(tr):
       attempts.append(len(attempts) + 1)
       hits = tr.get_root_object("hits")
       if len(attempts) == 1:                    # another writer commits while this attempt runs
           with db.new_transaction() as other:
               other.set_root_object("hits", other.get_root_object("hits") + 1)
       tr.set_root_object("hits", hits + 1)
       return "order placed"

   message = db.run(place_order)
   outbox.append(message)                        # the external effect: after run returned, once
   print("attempts:", attempts, "messages sent:", outbox)
   print("hits =", db.new_transaction().get_root_object("hits"))

.. code-block:: text
   :caption: Output

   attempts: [1, 2] messages sent: ['order placed']
   hits = 144

Three rules for the function you give ``run``:

* It may run several times, so put external effects **after** ``run`` returns, using what it returned.
* It must do its reading **inside** the function, through the ``tr`` it receives. A value read before
  ``run`` belongs to an older snapshot, and nothing at commit checks it.
* ``run`` re-raises the conflict after ``retries`` attempts (default 10). A hot unit with many writers needs
  more retries or, better, a design where writers touch different units (the sections below).

Writes you did not read are checked too
---------------------------------------

A write is a precondition as well: writing a root, or a mutable cell, without reading it first still
compares the unit with the value it had in the transaction's base. There is no "last writer wins":

.. code-block:: python

   with db.new_transaction() as tr:
       tr.set_root_object("motto", "first")
       tr.new_mutable("status", "open")

   first, second = db.new_transaction(), db.new_transaction()
   first.set_root_object("motto", "from first")         # neither transaction read "motto"
   second.set_root_object("motto", "from second")
   first.set_mutable("status", "closed")                 # nor the cell
   second.set_mutable("status", "archived")
   first.commit()
   try:
       second.commit()
   except ProtoConflictError as error:
       print(error.unit.kind.name, error.unit.root or error.unit.key)
   tr = db.new_transaction()
   print(tr.get_root_object("motto"), tr.get_mutable("status"))
   tr.abort()

.. code-block:: text
   :caption: Output

   ROOT motto
   from first closed

A blind write that the next commit silently overwrote is the classic lost update; here it is a conflict that
``run`` retries on the new state. Two transactions that write *different* roots or cells never conflict.

Collections: the unit is the key
--------------------------------

A ``Dictionary`` or ``HashDictionary`` root is not one unit. ``get_root_object`` returns a collection
**linked** to your transaction: it records what you read and wrote **per key**. At commit, the keys you
touched must still hold what your transaction saw, and your changes (your **intentions**) are applied to
whatever the root holds by then. Writers of different keys do not conflict; two writers of the same key do.

.. code-block:: python

   with db.new_transaction() as tr:
       tr.set_root_object("events", tr.new_dictionary())

   def add_events(thread_number):
       for i in range(25):
           with db.new_transaction() as tr:
               # Only the key written is recorded: other threads' keys never conflict.
               tr.set_root_object("events", tr.get_root_object("events").set_at(f"t{thread_number}-{i}", i))

   threads = [threading.Thread(target=add_events, args=(n,)) for n in range(4)]
   for t in threads:
       t.start()
   for t in threads:
       t.join()
   print("events:", len(db.new_transaction().get_root_object("events")), "keys, no conflict, no retry")

.. code-block:: text
   :caption: Output

   events: 100 keys, no conflict, no retry

The commit applies the *intention* of each transaction (set this key to this value), never a guess about
what a value means: there is no "add the two numbers" for an integer, because ``proto_db`` cannot know that you
meant a counter. Two rules make this work. The collection must be the one ``get_root_object`` returned (or a
version derived from it), assigned back to the **same** root with ``set_root_object``; a collection built
separately (``tr.new_dictionary()``, ``Dictionary.from_items``, one taken from another root or another
transaction) replaces the root, compared against its base value like an integer. And a version you derive
but do not assign publishes nothing.

The following helper runs two transactions on the same snapshot, commits the first and reports what happens
to the second. It is used for the rest of this tutorial:

.. code-block:: python

   def race(first, second):
       """Run ``first(tr)`` and ``second(tr)`` on one snapshot; commit first, then second."""
       a, b = db.new_transaction(), db.new_transaction()
       first(a)
       second(b)
       a.commit()
       try:
           b.commit()
           return "both committed"
       except ProtoConflictError as error:
           unit = error.unit
           shown = "" if unit.kind.name == "RANGE" or unit.key is None else f" {unit.key!r}"
           index = f" of index {unit.index!r}" if unit.index else ""
           return f"conflict on {unit.kind.name}{shown}{index}"

   def set_event(key, value):
       return lambda tr: tr.set_root_object("events", tr.get_root_object("events").set_at(key, value))

   print("different keys:", race(set_event("a", 1), set_event("b", 2)))
   print("the same key:  ", race(set_event("c", 1), set_event("c", 2)))

   def count_then_set(key):
       def work(tr):
           events = tr.get_root_object("events")
           tr.set_root_object("events", events.set_at(key, len(events)))    # len(): the whole dictionary
       return work

   print("after len():   ", race(count_then_set("d"), count_then_set("e")))

.. code-block:: text
   :caption: Output

   different keys: both committed
   the same key:   conflict on KEY 'c'
   after len():    conflict on WHOLE

Two transactions that insert the same new key conflict even if they write the same value: each declared
"the key is absent". Reading the whole dictionary (its length, an iteration, a query without an index)
records the **whole collection**, which makes the write a read-modify-write of all of it. Choose per
operation: if a writer must base its write on the current content ("add this order only if the total is
under the limit") it has to read it, and conflicts are the correct outcome; if it only adds, read only the
keys it needs.

The other collections follow the same idea, each with the units its operations have:

.. code-block:: python

   with db.new_transaction() as tr:
       tr.set_root_object("tags", tr.new_set().add("red").add("blue"))
       tr.set_root_object("visits", tr.new_counted_set())
       tr.set_root_object("log", tr.new_list().append_last("start"))

   def on_root(name, change):
       return lambda tr: tr.set_root_object(name, change(tr.get_root_object(name)))

   print("set, add and add:       ", race(on_root("tags", lambda s: s.add("green")),
                                           on_root("tags", lambda s: s.add("green"))))
   print("set, add and remove:    ", race(on_root("tags", lambda s: s.add("green")),
                                           on_root("tags", lambda s: s.remove("red"))))
   print("set, remove and remove: ", race(on_root("tags", lambda s: s.remove("blue")),
                                           on_root("tags", lambda s: s.remove("blue"))))
   print("counted set, two adds:  ", race(on_root("visits", lambda c: c.add("home").add("home")),
                                           on_root("visits", lambda c: c.add("home").add("about"))))
   print("list, two appends:      ", race(on_root("log", lambda l: l.append_last("a")),
                                           on_root("log", lambda l: l.append_last("b"))))
   print("list, set one position: ", race(on_root("log", lambda l: l.set_at(0, "begin")),
                                           on_root("log", lambda l: l.set_at(0, "BEGIN"))))
   print("list, insert at front:  ", race(on_root("log", lambda l: l.append_last("y")),
                                           on_root("log", lambda l: l.append_first("x"))))
   tr = db.new_transaction()
   visits = tr.get_root_object("visits")
   print(sorted(tr.get_root_object("tags")), visits.count_of("home"), visits.count_of("about"),
         list(tr.get_root_object("log")))
   tr.abort()

.. code-block:: text
   :caption: Output

   set, add and add:        both committed
   set, add and remove:     both committed
   set, remove and remove:  conflict on ELEMENT 'blue'
   counted set, two adds:   both committed
   list, two appends:       both committed
   list, set one position:  conflict on POSITION 0
   list, insert at front:   conflict on WHOLE
   ['green'] 3 1 ['begin', 'a', 'b', 'y']

A ``Set`` ``add`` has no precondition (any serial order leaves the element present); a ``remove`` declares
that the element is still present. A ``CountedSet`` composes its counts, so concurrent writers count without
retrying, and so does a ``RepeatedKeysDictionary``. A ``List`` appends in commit order without a
precondition; ``set_at`` of an existing position declares that position; an insertion, a removal or an
``append_first`` shifts every later position, so it takes the whole list (``x`` was refused above, and
a retry would insert it in front of the new content). A collection kept in an order of
your choosing, with concurrent insertions, is a ``Dictionary`` whose key is the ordering key (see
:doc:`../concurrency-model`).

Check and add
-------------

"Register this e-mail unless it is taken" reads before it writes, and *how* it reads decides what it
conflicts with. Four ways, from the narrowest to the widest:

.. code-block:: python

   import uuid

   from proto_db import EqualityIndex, F, ProtoValidationError, from_collection

   with db.new_transaction() as tr:
       tr.set_root_object("by_email", tr.new_dictionary())                    # e-mail -> user id
       users = tr.new_dictionary().add_index(EqualityIndex("email", "email"))  # user id -> record
       for i in range(50):                     # enough users for the planner to prefer the index
           users = users.set_at(f"seed{i}", tr.new_object(email=f"user{i}@seed"))
       tr.set_root_object("users", users)
       tr.set_root_object("accounts", tr.new_dictionary()
                          .add_index(EqualityIndex("email", "email", unique=True)))

   def by_key(email):                        # the e-mail is the key: has() declares that key
       def work(tr):
           index = tr.get_root_object("by_email")
           if not index.has(email):
               tr.set_root_object("by_email", index.set_at(email, uuid.uuid4().hex))
       return work

   def by_index(email):                      # an equality-index lookup declares that key's group
       def work(tr):
           users = tr.get_root_object("users")
           query = from_collection(users).where(F.email == email)
           assert query.explain()["plan"] == "IndexLookup"
           if query.count() == 0:
               tr.set_root_object("users", users.set_at(uuid.uuid4().hex, tr.new_object(email=email)))
       return work

   def by_unique_index(email):               # no read at all: the unique index imposes the check
       def work(tr):
           accounts = tr.get_root_object("accounts")
           tr.set_root_object("accounts", accounts.set_at(uuid.uuid4().hex, tr.new_object(email=email)))
       return work

   def by_traversal(email):                  # a scan of the values declares the whole collection
       def work(tr):
           users = tr.get_root_object("users")
           if all(user.email != email for user in users.values()):
               tr.set_root_object("users", users.set_at(uuid.uuid4().hex, tr.new_object(email=email)))
       return work

   for n, (label, way) in enumerate((("by key", by_key), ("by index lookup", by_index),
                                     ("by unique index", by_unique_index), ("by traversal", by_traversal))):
       print(f"{label:16} same e-mail: {race(way(f'ann{n}@x'), way(f'ann{n}@x')):48} "
             f"other e-mails: {race(way(f'bob{n}@x'), way(f'cid{n}@x'))}")

.. code-block:: text
   :caption: Output

   by key           same e-mail: conflict on KEY 'ann0@x'                         other e-mails: both committed
   by index lookup  same e-mail: conflict on INDEX_GROUP 'ann1@x' of index 'email' other e-mails: both committed
   by unique index  same e-mail: conflict on INDEX_GROUP 'ann2@x' of index 'email' other e-mails: both committed
   by traversal     same e-mail: conflict on WHOLE                                other e-mails: conflict on WHOLE

The table on the :doc:`../concurrency-model` page sums it up: a check by key or element conflicts on that key;
a check through an equality index conflicts on that index key's group; a declared unique index conflicts
with a concurrent duplicate even if the application never reads; a traversal, or a query that no equality
index answers, conflicts with any change of the collection.

What a query declares follows the plan that runs, not the indexes that exist. The ``assert`` in ``by_index``
is there because the collection was seeded first: on a nearly empty collection a scan is the cheaper plan
(:doc:`indexing-querying`), and a scan declares the whole collection, so the "same e-mail" race would
conflict on ``WHOLE`` and so would the "other e-mails" one. ``explain()`` tells you which unit a query will
declare.

With a unique index, a duplicate against the transaction's own view is refused at once, and a duplicate
committed concurrently is a conflict whose retry sees the duplicate and is refused:

.. code-block:: python

   tr = db.new_transaction()
   accounts = tr.get_root_object("accounts").set_at("u1", tr.new_object(email="eve@x"))
   try:
       accounts.set_at("u2", tr.new_object(email="eve@x"))
   except ProtoValidationError as error:
       print("own view:", type(error).__name__)
   tr.abort()

   tries = []

   def register_eve(tr):
       tries.append(1)
       if len(tries) == 1:                           # someone else registers eve@x meanwhile
           other = db.new_transaction()
           by_unique_index("eve@x")(other)
           other.commit()
       by_unique_index("eve@x")(tr)

   try:
       db.run(register_eve)
   except ProtoValidationError as error:
       print(f"after {len(tries)} attempts:", type(error).__name__)

.. code-block:: text
   :caption: Output

   own view: ProtoValidationError
   after 2 attempts: ProtoValidationError

Check a reference
-----------------

The same reasoning applies to a reference. An order that names a customer is valid only while the customer
exists, so the transaction that writes the order reads the customer, and that read is the precondition that
makes a concurrent removal of the customer a conflict:

.. code-block:: python

   with db.new_transaction() as tr:
       tr.set_root_object("customers", tr.new_dictionary().set_at("acme", tr.new_object(name="ACME")))
       tr.set_root_object("orders", tr.new_dictionary())

   def remove_customer(tr):
       tr.set_root_object("customers", tr.get_root_object("customers").remove_at("acme"))

   def order_for(customer, check):
       def work(tr):
           if check and not tr.get_root_object("customers").has(customer):
               raise ProtoValidationError(f"no customer {customer!r}")
           orders = tr.get_root_object("orders")
           tr.set_root_object("orders", orders.set_at(uuid.uuid4().hex, tr.new_object(customer=customer)))
       return work

   print("unchecked:", race(remove_customer, order_for("acme", check=False)))
   with db.new_transaction() as tr:
       tr.set_root_object("customers", tr.get_root_object("customers").set_at("acme", tr.new_object(name="ACME")))
   print("checked:  ", race(remove_customer, order_for("acme", check=True)))

.. code-block:: text
   :caption: Output

   unchecked: both committed
   checked:   conflict on KEY 'acme'

The unchecked order committed and now names a customer that no longer exists; the checked one conflicted
on the customer's key, and its retry would see the removal and refuse the order.

Identifiers without a shared counter
------------------------------------

The orders above are keyed by ``uuid.uuid4().hex``: every writer makes its own identifier, and two writers
never touch the same key. The obvious alternative, a counter cell that every writer increments, is a single
unit that every writer reads and writes, so concurrent writers conflict on it and all but one redo their work:

.. code-block:: python

   with db.new_transaction() as tr:
       tr.new_mutable("next_order", 1)

   def order_numbered(tr):
       number = tr.get_mutable("next_order")
       tr.set_mutable("next_order", number + 1)
       tr.set_root_object("orders", tr.get_root_object("orders").set_at(number, tr.new_object(n=number)))

   def order_uuid(tr):
       tr.set_root_object("orders", tr.get_root_object("orders").set_at(uuid.uuid4().hex, tr.new_object()))

   print("counter:", race(order_numbered, order_numbered))
   print("uuid:   ", race(order_uuid, order_uuid))

.. code-block:: text
   :caption: Output

   counter: conflict on KEY 1
   uuid:    both committed

Both numbered orders took number 1, so the second commit was refused on the key it would have overwritten;
it also read the counter cell, which the first commit changed. Either way one of the two redoes its work,
and with N concurrent writers N - 1 of them do.

A ULID (a time-ordered 128-bit identifier, from a third-party package) works the same way and keeps the
keys in creation order. ``proto_db`` has no sequence that hands out numbers outside the transactions: a
number visible to everyone before the commit would either conflict like the counter or leave gaps on every
failed attempt.

Reading outside the guarantee: a ``Snapshot``
---------------------------------------------

Sometimes a transaction must not be refused because the data it read changed: a summary that is fine if it
was true a moment ago. Reading that data from a ``Snapshot`` records nothing, so a concurrent change does
not make the commit conflict. It is the deliberate escape from the guarantee, the analogue of a relational
database's ``READ COMMITTED``, and the value you write may then be stale:

.. code-block:: python

   def summary(use_snapshot):
       def work(tr):
           source = db.snapshot_at(tr.snapshot_ref) if use_snapshot else tr
           events = source.get_root_object("events")
           tr.set_root_object("summary", f"{len(events)} events")
       return work

   print("through the transaction:", race(set_event("z", 0), summary(use_snapshot=False)))
   print("through a snapshot:     ", race(set_event("y", 0), summary(use_snapshot=True)))
   tr = db.new_transaction()
   print(tr.get_root_object("summary"), "stored;", len(tr.get_root_object("events")), "events now")
   tr.abort()

.. code-block:: text
   :caption: Output

   through the transaction: conflict on WHOLE
   through a snapshot:      both committed
   105 events stored; 106 events now

The same holds for ``Transaction.snapshot_mutable``, ``Database.history()`` and objects brought from another
transaction: they are read outside the transaction's guarantee, on purpose.

Lock cells
----------

The core has no pessimistic lock: a read never blocks or reserves anything. When work outside the database
must not run twice at once (one invoice run per month), a lock is a mutable cell with an agreed name: taking
it is creating the cell, which fails if it exists, and releasing it is removing it. The cell holds an
**owner** and an **expiry**, because a process that crashed while holding it never removes it:

.. code-block:: python

   def take_lock(name, owner, now, ttl):
       """Create the lock cell, or take over an expired one; False if someone else holds it."""
       def work(tr):
           held = tr.get_mutable(name)
           if held is None:
               tr.new_mutable(name, {"owner": owner, "expires": now + ttl})
           elif held["expires"] <= now:                 # the holder crashed or stalled: take it over
               tr.set_mutable(name, {"owner": owner, "expires": now + ttl})
           else:
               return False
           return True
       return db.run(work)

   def release_lock(name, owner):
       def work(tr):
           if (tr.get_mutable(name) or {}).get("owner") == owner:
               tr.remove_mutable(name)
       db.run(work)

   print("A takes it at t=0:   ", take_lock("lock:invoices", "A", now=0, ttl=60))
   print("B tries at t=30:     ", take_lock("lock:invoices", "B", now=30, ttl=60))
   print("B tries at t=90:     ", take_lock("lock:invoices", "B", now=90, ttl=60))   # A never released it
   release_lock("lock:invoices", "A")                                             # too late: not A's any more
   print("holder:", db.new_transaction().get_mutable("lock:invoices")["owner"])

.. code-block:: text
   :caption: Output

   A takes it at t=0:    True
   B tries at t=30:      False
   B tries at t=90:      True
   holder: B

An expiry decides only when someone may take over, and clocks drift, so it is not what keeps the data safe.
What does is the precondition: a holder that writes its results reads the lock cell in the same transaction,
so if the lock was taken over meanwhile, that commit conflicts on the cell instead of publishing work done
without the lock:

.. code-block:: python

   def invoice_run(owner):
       def work(tr):
           if (tr.get_mutable("lock:invoices") or {}).get("owner") != owner:
               raise ProtoValidationError(f"{owner} does not hold the lock")
           tr.set_root_object("invoiced", owner)
       return work

   def take_over(owner, now, ttl=60):
       def work(tr):
           if tr.get_mutable("lock:invoices")["expires"] <= now:
               tr.set_mutable("lock:invoices", {"owner": owner, "expires": now + ttl})
       return work

   release_lock("lock:invoices", "B")
   take_lock("lock:invoices", "A", now=200, ttl=60)
   print("A writes, lock held:       ", race(take_over("B", now=230), invoice_run("A")))
   print("A writes, lock taken over: ", race(take_over("B", now=300), invoice_run("A")))
   print("results written by:", db.new_transaction().get_root_object("invoiced"))

.. code-block:: text
   :caption: Output

   A writes, lock held:        both committed
   A writes, lock taken over:  conflict on CELL 'lock:invoices'
   results written by: A

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

   hits = 145 (the +1000 was overwritten)

No exception was raised: the last transaction started after the +1000 commit, so the value it overwrote is
the one in its base, and nothing changed before its commit. The fix is the rule of the ``run`` section: read
and write in the same transaction. The same applies to reusing an object that came from another
transaction, or from one whose commit failed: a collection's link to its transaction ends with that
transaction, so in a later one it is a plain value that replaces the root. ``proto_db`` documents these cases
instead of policing them with expensive checks.

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
  :doc:`../performance` has the measured rates by durability mode and by number of writers (measured before
  the precondition model; see that page).
* Under heavy contention on one unit, optimistic retry wastes work: every loser repeats it. If you have
  a single hot counter, a ``CountedSet`` or an arrangement where each writer owns its own key is faster
  than any retry policy.
* A transaction that traverses or range-queries a collection and then writes collides with any concurrent
  change inside what it read, and a long transaction over a heavily written unit can exhaust its retries:
  nothing makes the retries fair.
* A relational database with row locks can be the better choice when many writers update the *same*
  rows for long transactions and you want the waiting done by the server.
