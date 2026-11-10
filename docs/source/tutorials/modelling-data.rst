Modelling data
==============

ProtoDB stores immutable values. Most of modelling is deciding, for each thing in your domain, whether it
should be a *value* (a fact that never changes) or an *entity* (something with an identity whose state
changes over time). This tutorial shows the building blocks, the two choices side by side, and a versioned
sales order built with a plain ``previous`` attribute. Every code block runs in the test suite.

Records and what they may hold
------------------------------

A record is a ``DBObject``: named attributes, immutable. The values it can hold are the canonical types of
the value model (:doc:`../concepts`): ``None``, ``bool``, ``int``, ``float``, ``Decimal``, ``str``,
``bytes``, ``date``, ``time``, ``datetime``, ``timedelta``, ``UUID``, ``tuple``, ``list``, ``dict``, other
records and collections. Anything else is refused when you build the record, not when you commit:

.. code-block:: python

   from proto_db import MemoryStorage, ObjectSpace, ProtoValidationError

   space = ObjectSpace(MemoryStorage())
   db = space.new_database("shop")

   with db.new_transaction() as tr:
       try:
           tr.new_object(callback=lambda x: x)
       except ProtoValidationError as error:
           print("refused:", error)
       try:
           tr.new_object(tags={"a", "b"})          # a set is not a canonical value; use new_set()
       except ProtoValidationError as error:
           print("refused:", error)
       line = tr.new_object(sku="A-1", quantity=2, options=["gift", "express"])
       print(line.sku, line.options)
       try:
           line.options.append("x")
       except ProtoValidationError as error:
           print("a stored list is frozen:", type(error).__name__)

.. code-block:: text
   :caption: Output

   refused: Unsupported value type: function
   refused: Unsupported value type: set
   A-1 ['gift', 'express']
   a stored list is frozen: ProtoValidationError

A ``list`` or ``dict`` you store becomes a frozen copy. Mutating it would change a value that other
versions share, so it raises. Use ``.copy()`` to get a mutable copy, or ``with_`` to make a new record.

The collections
---------------

Each collection is persistent: a change returns a new collection that shares its unchanged parts with the
old one.

=========================== ===================================================================================
Collection                  Use it for
=========================== ===================================================================================
``List``                    ordered items, positional access (``get_at``, ``insert_at``, ``slice``)
``Dictionary``              a map ordered by key, ordered iteration and range scans (``set_at``, ``get_at``)
``HashDictionary``          a map whose keys are any canonical value, including tuples
``Set``                     membership, ``union`` / ``intersection`` / ``difference``
``CountedSet``              a multiset: ``count_of``
``RepeatedKeysDictionary``  a multimap: several records under one key
=========================== ===================================================================================

.. code-block:: python

   with db.new_transaction() as tr:
       by_sku = tr.new_dictionary().set_at("B-2", 20).set_at("A-1", 10)
       print(list(by_sku.keys()))                   # a Dictionary iterates in key order

       grid = tr.new_hash_dictionary().set_at((0, 1), "tuple keys are fine")
       print(grid.get_at((0, 1)))

       tags = tr.new_counted_set().add("red").add("red").add("blue")
       print(tags.count_of("red"), tags.count_of("blue"), tags.total_count)

       owners = tr.new_repeated_keys_dictionary()
       owners = owners.set_at("A-1", tr.new_object(who="ann")).set_at("A-1", tr.new_object(who="bob"))
       print(sorted(r.who for r in owners.get_at("A-1")))   # several records under one key

.. code-block:: text
   :caption: Output

   ['A-1', 'B-2']
   tuple keys are fine
   2 1 3
   ['ann', 'bob']

The order of the records inside one key of a ``RepeatedKeysDictionary`` is not part of its contract, which
is why the example sorts them.

Values and entities
-------------------

Take a customer and an order. What should the order hold about the customer?

*   Two things are true at once. The order must keep **the address it was sold to**: if the customer moves
    next year, last year's invoice still shows the old address. That is a *value*: copy it into the order.
*   The order also belongs to **the customer**, whose name may be corrected. Every order should show the
    corrected name. That is an *entity*: it has an identity and a current state.

ProtoDB gives you both and leaves the choice to you. An immutable record is a value. A ``MutableObject`` is
an entity: a named cell in the database whose value changes by commit, referred to by a small handle you
can store in any record.

.. code-block:: python

   with db.new_transaction() as tr:
       acme = tr.new_mutable("customer:acme", tr.new_object(name="ACME Ltd", address="1 Main St"))
       customer = acme.get(tr)
       order = tr.new_object(number="SO-1",
                             customer=acme,                       # the entity: a handle
                             ship_to=customer.address)            # the value: a copy
       tr.set_root_object("order", order)

   # The customer moves and the company renames itself: one commit changes the cell.
   with db.new_transaction() as tr:
       acme = tr.get_root_object("order").customer
       acme.set(tr, acme.get(tr).with_(name="ACME Corp", address="9 Side Ave"))

   tr = db.new_transaction()
   order = tr.get_root_object("order")
   print(order.customer.get(tr).name, "|", order.ship_to)
   tr.abort()

.. code-block:: text
   :caption: Output

   ACME Corp | 1 Main St

The order shows the current name through the handle, and the address it was sold to through the copy.
Neither is a trick: it is what each choice means.

Entities keep their time travel. A snapshot of an earlier version reads the cell as it was then:

.. code-block:: python

   oldest = db.history()[-2]                     # the first commit: the order and the original cell
   snapshot = db.snapshot_at(oldest)
   print(snapshot.get_mutable("customer:acme").name)
   print(db.new_transaction().get_mutable("customer:acme").name)

.. code-block:: text
   :caption: Output

   ACME Ltd
   ACME Corp

A ``MutableObject`` is also where the usual concurrency rules apply: reading it enters the read set, so two
transactions that read and change the same cell conflict (:doc:`concurrency`). Its handle compares and
indexes by name, so an index on a handle-valued attribute groups by entity: "the orders of this customer" is
an index lookup on the customer's identity, however often the customer's own record changes.

.. code-block:: python

   from proto_db import EqualityIndex, F, MutableObject, from_collection

   with db.new_transaction() as tr:
       customers = [tr.new_mutable(f"customer:{i}", tr.new_object(name=f"customer {i}")) for i in range(5)]
       sales = tr.new_dictionary().add_index(EqualityIndex("by_customer", "customer"))
       for n in range(500):
           sales = sales.set_at(n, tr.new_object(number=n, customer=customers[n % 5]))
       tr.set_root_object("sales", sales)

   tr = db.new_transaction()
   query = from_collection(tr.get_root_object("sales")).where(F.customer == MutableObject("customer:3"))
   print(query.explain()["plan"], query.count())
   tr.abort()

.. code-block:: text
   :caption: Output

   IndexLookup 100

Which to choose
~~~~~~~~~~~~~~~

* Use a **value** when the thing is a fact at a moment (an invoice line, a price list, a sale), or when you
  want history of *what it was* to be part of the data. The cost of an edit is a new record, and every
  record that points at the old one keeps pointing at it.
* Use an **entity** when other records must see its *current* state and you do not need the old states in
  the record graph (the history is still reachable through snapshots). Edits are cheap and do not touch the
  records that hold the handle.
* At the all-entity extreme the model resembles a relational one and keeps time travel; at the all-value
  extreme it is a persistent data structure. Most applications sit between, and the choice is per
  relationship, not per database.

A versioned sales order
-----------------------

A sales order moves through states: draft, confirmed, invoiced. Business rules often need every state it
went through. With immutable records the pattern is one attribute: each new version of the order points at
the one it replaced.

.. code-block:: python

   def revise(order, **changes):
       """The next version of ``order``: a new record that remembers the one before it."""
       return order.with_(previous=order, **changes)

   with db.new_transaction() as tr:
       orders = tr.new_dictionary().add_index(EqualityIndex("by_status", "status"))
       for number in ("SO-1", "SO-2", "SO-3"):
           orders = orders.set_at(number, tr.new_object(number=number, status="draft", total=100, previous=None))
       tr.set_root_object("orders", orders)

   with db.new_transaction() as tr:
       orders = tr.get_root_object("orders")
       so1 = orders.get_at("SO-1")
       so1 = revise(so1, status="confirmed")
       so1 = revise(so1, status="invoiced", total=110)
       tr.set_root_object("orders", orders.set_at("SO-1", so1))

   def history_of(order):
       """The statuses of an order, oldest first, by walking ``previous``."""
       chain = []
       while order is not None:
           chain.append((order.status, order.total))
           order = order.previous
       return list(reversed(chain))

   tr = db.new_transaction()
   orders = tr.get_root_object("orders")
   print(history_of(orders.get_at("SO-1")))
   print(history_of(orders.get_at("SO-2")))
   tr.abort()

.. code-block:: text
   :caption: Output

   [('draft', 100), ('confirmed', 100), ('invoiced', 110)]
   [('draft', 100)]

The collection holds only the *current* version of each order, so the indexes hold only current versions
too. An old version leaves the index when it is replaced, and the question "which orders are still in
draft?" needs no extra condition:

.. code-block:: python

   tr = db.new_transaction()
   orders = tr.get_root_object("orders")
   print(sorted(o.number for o in from_collection(orders).where(F.status == "draft").to_list()))
   print(sorted(o.number for o in from_collection(orders).where(F.status == "confirmed").to_list()))
   tr.abort()

.. code-block:: text
   :caption: Output

   ['SO-2', 'SO-3']
   []

SO-1 passed through "draft" and "confirmed", but it is neither now, and a query for either does not see it.
Its history is reachable from the current version and costs nothing to the queries that ignore it. The
:doc:`ERP tutorial <erp>` goes further. Why the cost of a query does not depend on the length of a version
chain is a property of the structure, explained in the next sections: the collection and its indexes hold only
the current version of each order, so a query never meets the superseded ones.

The relational contrast
~~~~~~~~~~~~~~~~~~~~~~~

The same requirement in a relational database is usually met with a versioned table: one row per version,
with the validity interval in columns. SQLite, from the standard library, is enough to show the cost.

.. code-block:: python

   import sqlite3

   sql = sqlite3.connect(":memory:")
   sql.executescript("""
       CREATE TABLE sales_order_version (
           order_no TEXT, version INTEGER, status TEXT, total INTEGER,
           valid_to INTEGER,                      -- NULL while the version is current
           PRIMARY KEY (order_no, version));
       INSERT INTO sales_order_version VALUES
           ('SO-1', 1, 'draft',     100, 2),
           ('SO-1', 2, 'confirmed', 100, 3),
           ('SO-1', 3, 'invoiced',  110, NULL),
           ('SO-2', 1, 'draft',     100, NULL),
           ('SO-3', 1, 'draft',     100, NULL);
   """)
   naive = "SELECT order_no FROM sales_order_version WHERE status = 'draft' ORDER BY order_no"
   careful = naive.replace("WHERE status = 'draft'", "WHERE status = 'draft' AND valid_to IS NULL")
   print("without the version condition:", [row[0] for row in sql.execute(naive)])
   print("with the version condition:   ", [row[0] for row in sql.execute(careful)])
   sql.close()

.. code-block:: text
   :caption: Output

   without the version condition: ['SO-1', 'SO-2', 'SO-3']
   with the version condition:    ['SO-2', 'SO-3']

The first query is a plausible query that returns an order as draft after it was invoiced. Every query,
view, report and index over that table must remember ``valid_to IS NULL`` (or an equivalent), including
the ones other people write later, and the table grows with every revision whether the query asks for
history or not. That is a modelling cost, not a defect of SQL: a mutable table has one state and the
versions are an add-on. In ProtoDB the version chain is the model and "current" is what the collection
holds.

Does a long history slow the current queries down?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The design says no: superseded versions are not in the collection or its indexes, so a query for current
orders does the same work whether each order has one version or a thousand behind it. The argument is
structural, and the tutorial test checks the part that is exact (the index holds one entry per order, whatever
the history). The timing evidence is a benchmark result, not something this page measures:

.. note::

   TODO(9c): add the measured query cost against history depth from the 9c CARRY results
   (``benchmarks/results``, history-depth profile) when it lands. It is ProtoDB measured against itself, not a
   comparison. Until then no timing figure is claimed here.

.. code-block:: python

   for round_number in range(200):                          # 200 more versions behind SO-2, one commit each
       with db.new_transaction() as tr:
           orders = tr.get_root_object("orders")
           tr.set_root_object("orders", orders.set_at("SO-2", revise(orders.get_at("SO-2"), total=100 + round_number)))

   tr = db.new_transaction()
   orders = tr.get_root_object("orders")
   print(len(orders), "orders;", orders.get_index("by_status").explain()["entries"],
         "entries in the status index; the chain behind SO-2 has", len(history_of(orders.get_at("SO-2"))), "versions")
   tr.abort()

.. code-block:: text
   :caption: Output

   3 orders; 3 entries in the status index; the chain behind SO-2 has 201 versions

What this pattern does not do
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* A chain built entirely inside **one** transaction is saved recursively, and the default Python recursion
  limit stops it at a depth that was measured at between 150 (works) and 200 (``RecursionError``) unsaved
  versions. This is a current defect (a save that recurses once per link), reported for a product fix.
  Revising an order once per transaction, the normal case, is not affected, because each earlier version
  is already saved.
* ``previous`` is **not** part of the database module: it is an ordinary attribute you set. Nothing checks
  that you set it, and a version that forgets it simply has no history chain.
* The chain says *what the order was*. It does not say *when* or *who* unless you store them in the version
  (``changed_by=...``, ``changed_at=...``). The database-wide history (:meth:`Database.history`) records
  when each commit happened, not who made it.
* History is never rewritten and there is no way to delete a version: a record that must disappear for
  legal reasons needs encryption with an external key, or an application rule deciding what is shown. See
  :doc:`../objects-across-transactions`.
* A relational database does some things better here: ad-hoc analytics over *all* versions of every row,
  for example, are a plain ``GROUP BY`` there and need a walk of the chains here.

.. code-block:: python

   space.close()
