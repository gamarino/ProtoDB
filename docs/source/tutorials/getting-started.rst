Getting started
===============

This tutorial creates a space, commits a few transactions, reads the data back, reopens it from disk and
walks the history of the database. Every code block on the page runs in the test suite
(``tests/docs/test_tutorials.py``), and every ``Output`` block shows what its code printed.

The four nouns
--------------

* An **object space** (``ObjectSpace``) owns one storage. The storage is where bytes live: memory, a
  directory of files, or a cluster.
* A **database** (``Database``) is a named collection of *roots* inside the space. A root is a name
  (``"people"``) bound to a value: a collection, a record, a number.
* A **transaction** (``Transaction``) is a snapshot of one database plus a set of pending writes. It is an
  ordinary object: it is not tied to a thread, and you can have several open at once.
* A **version** is what a commit produces. Nothing is ever overwritten: a commit publishes a new
  database version that shares everything unchanged with the one before it.

A first transaction
-------------------

.. code-block:: python

   from proto_db import MemoryStorage, ObjectSpace

   space = ObjectSpace(MemoryStorage())
   db = space.new_database("example")

   # The block commits when it ends and aborts if it raises.
   with db.new_transaction() as tr:
       numbers = tr.new_list().append_last(1).append_last(2).append_last(3)
       ada = tr.new_object(name="Ada Lovelace", born=1815)
       tr.set_root_object("numbers", numbers)
       tr.set_root_object("ada", ada)

   tr = db.new_transaction()
   print(list(tr.get_root_object("numbers")))
   print(tr.get_root_object("ada").name)
   tr.abort()

.. code-block:: text
   :caption: Output

   [1, 2, 3]
   Ada Lovelace

``tr.new_list()`` and ``tr.new_object(...)`` create values that belong to this transaction. ``append_last``
did not change ``numbers``: it returned a new list. That is the rule for every collection in ``proto_db``, and
the next section shows what it buys you.

Values do not change
--------------------

.. code-block:: python

   with db.new_transaction() as tr:
       numbers = tr.get_root_object("numbers")
       longer = numbers.append_last(4)
       print(list(numbers), list(longer))
       tr.set_root_object("numbers", longer)

   try:
       ada = db.new_transaction().get_root_object("ada")
       ada.born = 1816
   except Exception as error:
       print(type(error).__name__ + ":", error)

.. code-block:: text
   :caption: Output

   [1, 2, 3] [1, 2, 3, 4]
   ProtoUserError: DBObject is immutable; use with_(born=...) to create a new version

The old list is intact after the append, and a record refuses assignment. To "change" a record you make a
new one with ``with_``:

.. code-block:: python

   with db.new_transaction() as tr:
       ada = tr.get_root_object("ada")
       tr.set_root_object("ada", ada.with_(field="mathematics"))

   tr = db.new_transaction()
   ada = tr.get_root_object("ada")
   print(ada.name, ada.field, ada.born)
   tr.abort()

.. code-block:: text
   :caption: Output

   Ada Lovelace mathematics 1815

History is free
---------------

Because a commit never overwrites, the previous versions are still there. ``Database.history()`` lists
them, newest first, and ``Database.snapshot_at`` opens any of them read-only:

.. code-block:: python

   print([entry.version for entry in db.history()])

   first = db.history()[-2]              # the version that held the first commit
   snapshot = db.snapshot_at(first)
   print(snapshot.version, list(snapshot.get_root_object("numbers")))
   print(snapshot.get_root_object("ada").has_defined_attr("field"))

.. code-block:: text
   :caption: Output

   [3, 2, 1, 0]
   1 [1, 2, 3]
   False

Version 0 is the empty database created by ``new_database``. Version 1 is the first commit, and a snapshot
of it still shows the list without the ``4`` and the record without ``field``. No audit table was
written: the history is the data. The :doc:`ERP tutorial <erp>` builds on this.

Persisting to disk
------------------

``FileStorage`` stores a space in a directory. The same code works against it; only the storage changes.
Close the space when you are done: it releases the directory lock and flushes.

.. code-block:: python

   from proto_db import FileStorage

   space.close()

   disk = ObjectSpace(FileStorage("library-space"))
   library = disk.new_database("library")
   with library.new_transaction() as tr:
       tr.set_root_object("title", "Dune")
   disk.close()

   reopened = ObjectSpace(FileStorage("library-space"))
   print(reopened.list_databases())
   tr = reopened.open_database("library").new_transaction()
   print(tr.get_root_object("title"))
   tr.abort()
   reopened.close()

.. code-block:: text
   :caption: Output

   ['library']
   Dune

A commit that returns has reached the disk: ``FileStorage`` calls ``fsync`` before it publishes the new
root. :doc:`operations` shows the two cheaper modes and what each one risks.

What to read next
-----------------

* :doc:`modelling-data` for records, collections, values against ``MutableObject`` cells and versioned
  entities.
* :doc:`indexing-querying` for indexes and the query planner.
* :doc:`concurrency` for conflicts, ``Database.run`` and merges.
* :doc:`../concepts` for the ideas behind these calls.
