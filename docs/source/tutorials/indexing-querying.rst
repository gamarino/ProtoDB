Indexing and querying
=====================

An index answers "which elements have this key?" without looking at every element. In ProtoDB every
collection can carry indexes, there are none by default, and one planner, shared by all the query front
ends, decides whether to use them. This tutorial builds a small collection of people, adds indexes,
watches the planner use them and checks that an index never changes an answer. Every code block runs in the
test suite.

The data
--------

.. code-block:: python

   from proto_db import F, MemoryStorage, ObjectSpace, from_collection

   space = ObjectSpace(MemoryStorage())
   db = space.new_database("people")
   CITIES = ["Lima", "Oslo", "Paris", "Rome"]

   with db.new_transaction() as tr:
       people = tr.new_dictionary()
       for i in range(2000):
           person = tr.new_object(name=f"person{i:04d}", city=CITIES[i % 4], age=18 + i % 60,
                                  address={"zip": str(1000 + i % 50)})
           people = people.set_at(i, person)
       tr.set_root_object("people", people)

   def plan_of(query):
       """The node names of a query's plan, from the root down."""
       names = []
       node = query.explain()
       while node:
           names.append(node["plan"])
           node = node["children"][0] if node.get("children") else None
       return " <- ".join(names)

   tr = db.new_transaction()
   people = tr.get_root_object("people")
   query = from_collection(people).where(F.city == "Oslo")
   print(plan_of(query), "->", query.count(), "rows")
   print(people.index_names())
   tr.abort()

.. code-block:: text
   :caption: Output

   Filter <- CollectionScan -> 500 rows
   []

``from_collection(...).where(...)`` builds a query; ``explain()`` describes the plan it will run, and
``count()`` or ``to_list()`` runs it. With no index, the only access path is a scan of every element, and
the predicate is a ``Filter`` over it. Nothing is wrong: for a small collection that is the cheapest plan.

Adding indexes
--------------

An index is part of the collection. ``add_index`` builds it over the elements the collection holds now and
returns a new version of the collection; from then on every change to the collection updates the index in
the same step, so the two can never disagree.

.. code-block:: python

   from proto_db import EqualityIndex, RangeIndex

   with db.new_transaction() as tr:
       people = tr.get_root_object("people")
       people = (people.add_index(EqualityIndex("by_city", "city"))
                       .add_index(RangeIndex("by_age", "age"))
                       .add_index(EqualityIndex("by_zip", "address.zip")))
       tr.set_root_object("people", people)

   tr = db.new_transaction()
   people = tr.get_root_object("people")
   print(people.index_names())
   for label, query in [
       ("city == Oslo", from_collection(people).where(F.city == "Oslo")),
       ("city in (Lima, Rome)", from_collection(people).where(F.city.in_(["Lima", "Rome"]))),
       ("zip == 1010", from_collection(people).where(F.address.zip == "1010")),
       ("30 <= age <= 35", from_collection(people).where(F.age.between(30, 35))),
       ("age 30-35, by age, first 5", from_collection(people).where(F.age.between(30, 35)).order_by(F.age).take(5)),
   ]:
       print(f"{label:28} {plan_of(query):32} {query.count()} rows")
   tr.abort()

.. code-block:: text
   :caption: Output

   ['by_age', 'by_city', 'by_zip']
   city == Oslo                 IndexLookup                      500 rows
   city in (Lima, Rome)         IndexLookup                      1000 rows
   zip == 1010                  IndexLookup                      40 rows
   30 <= age <= 35              IndexRange                       204 rows
   age 30-35, by age, first 5   Limit <- Sort <- IndexRange      5 rows

Notes on what you see:

* The key of an index is a *path*: ``"city"``, or ``"address.zip"`` to reach inside a nested value. A
  record with no such attribute is filed under a ``MISSING`` marker, so ``F.x.is_none()`` and a scan agree.
* An ``EqualityIndex`` answers ``==``, ``in``, ``!=`` and the presence tests. A ``RangeIndex`` also answers
  ``<``, ``<=``, ``>``, ``>=`` and the ``between`` family, and it yields elements in key order, which the
  ``order_by`` above uses.
* An index is chosen because the planner estimated it cheaper than the alternatives, never because it
  exists. On the one-element collection of the previous section a scan was cheaper, and the planner took it.

Several conditions
------------------

When a query has several conditions the planner picks the cheapest access path among the indexes that can
answer one of them, and keeps every other condition as a residual ``Filter``. It never drops a condition.

.. code-block:: python

   tr = db.new_transaction()
   people = tr.get_root_object("people")
   query = from_collection(people).where((F.city == "Oslo") & (F.age > 70))
   print(plan_of(query), "->", query.count(), "rows")
   scan = from_collection(people.remove_index("by_city").remove_index("by_age")
                          .remove_index("by_zip")).where((F.city == "Oslo") & (F.age > 70))
   print(plan_of(scan), "->", scan.count(), "rows")
   same = sorted(p.name for p in query.to_list()) == sorted(p.name for p in scan.to_list())
   print("same rows with and without indexes:", same)
   tr.abort()

.. code-block:: text
   :caption: Output

   Filter <- IndexRange -> 66 rows
   Filter <- CollectionScan -> 66 rows
   same rows with and without indexes: True

The planner used the age index and filtered the city on the survivors. The last line is the invariant that
holds the whole design together: for every collection, every index configuration and every predicate, the
indexed answer is the same multiset of elements as a full scan. The test suite checks it with generated
data (``tests/property/test_index_equals_scan.py``), and you can check one index yourself:

.. code-block:: python

   tr = db.new_transaction()
   people = tr.get_root_object("people")
   people.get_index("by_city").validate(people.elements())      # raises ProtoCorruptionError on a mismatch
   print(people.get_index("by_city").explain())
   tr.abort()

.. code-block:: text
   :caption: Output

   {'index': 'by_city', 'kind': 'EqualityIndex', 'key': 'city', 'entries': 2000, 'keys': 4, 'unorderable': 0}

Indexes follow every change
---------------------------

.. code-block:: python

   before = db.new_transaction()                                # a snapshot taken before the change

   with db.new_transaction() as tr:
       people = tr.get_root_object("people")
       person = people.get_at(0)                                # city Lima
       people = people.set_at(0, person.with_(city="Oslo"))     # moves to Oslo
       people = people.remove_at(1)                             # an Oslo person leaves the collection
       tr.set_root_object("people", people)

   def counts(tr):
       people = tr.get_root_object("people")
       return (len(people), from_collection(people).where(F.city == "Oslo").count(),
               from_collection(people).where(F.city == "Lima").count())

   after = db.new_transaction()
   print("after: ", counts(after))
   print("before:", counts(before))
   before.abort()
   after.abort()

.. code-block:: text
   :caption: Output

   after:  (1999, 500, 499)
   before: (2000, 500, 500)

Person 0 left the Lima group and entered the Oslo one, and person 1 (also Oslo) is gone, so Oslo is
500 + 1 - 1. The transaction opened before the commit still sees its own snapshot, indexes included.

A key that is computed
----------------------

A key can also be a named function of the element, registered once per process. Only the *name* is stored
in the database, never the function, so the function must be registered again when the database is opened
in another process, and it must be pure: the same element always gives the same key.

.. code-block:: python

   from proto_db import KeySpec, register_extractor

   register_extractor("name_length", lambda person: len(person.name), version="1")

   with db.new_transaction() as tr:
       people = tr.get_root_object("people")
       people = people.add_index(RangeIndex("by_name_length", KeySpec.extractor("name_length")))
       tr.set_root_object("people", people)

   tr = db.new_transaction()
   people = tr.get_root_object("people")
   query = from_collection(people).where(F["@name_length"] == 10)
   print(plan_of(query), query.count())
   tr.abort()

.. code-block:: text
   :caption: Output

   IndexLookup 1999

Every name is ``person`` plus four digits, which is ten characters, so all the remaining 1999 people have
that key. If an extractor's meaning changes, register it under a new name or a new ``version``: indexes and
paged queries remember both.

Queries that cannot use an index
--------------------------------

Some predicates cannot be translated into terms an index understands, notably Python callables. They still
work, and they are visible:

.. code-block:: python

   tr = db.new_transaction()
   people = tr.get_root_object("people")
   query = from_collection(people).where(lambda person: person.age % 7 == 0)
   print(plan_of(query), query.count())
   tr.abort()

.. code-block:: text
   :caption: Output

   PythonFilter <- CollectionScan 300

``PythonFilter`` is the planner telling you that this predicate runs element by element in Python. Prefer
``F`` expressions where you can: they use an index when one fits and they are what a pager token can
describe.

Grouping, joining, paging
-------------------------

The same front end groups and aggregates:

.. code-block:: python

   from proto_db import Agg

   tr = db.new_transaction()
   people = tr.get_root_object("people")
   summary = (from_collection(people)
              .group_by(F.city, {"city": Agg.key(), "n": Agg.count(), "oldest": Agg.max(F.age)})
              .order_by(F.city))
   for row in summary.to_list():
       print(row)
   tr.abort()

.. code-block:: text
   :caption: Output

   {'city': 'Lima', 'n': 499, 'oldest': 74}
   {'city': 'Oslo', 'n': 500, 'oldest': 75}
   {'city': 'Paris', 'n': 500, 'oldest': 76}
   {'city': 'Rome', 'n': 500, 'oldest': 77}

A stable report over a long result is a *pager*: it reads one snapshot, so the pages never repeat a row or
skip one, whatever is committed meanwhile. The :doc:`ERP tutorial <erp>` shows it under concurrent writes.

.. code-block:: python

   with db.new_transaction() as tr:
       pager = tr.pager("people", where=[["city", "==", "Oslo"]], order_by=["age"], page_size=200)
       print(pager.rows, pager.pages, len(pager.page(0)), len(pager.page(2)))

.. code-block:: text
   :caption: Output

   500 3 200 100

.. code-block:: python

   space.close()

When another system is the better choice
----------------------------------------

* ProtoDB has equality, range and vector indexes. It has no full-text, spatial or composite (multi-column)
  index, and no SQL. A workload built on ad-hoc SQL, joins across many tables or a mature optimiser belongs
  in a relational database. A custom index (:doc:`custom-index`) can add a new kind, but it is code you
  write and prove.
* The planner's cost model is simple. When it picks a path you would not, ``explain()`` says why in numbers,
  and a differently shaped index usually fixes it.
* Every index adds work to every write, as in any database, and here also a new version of the index
  structure per commit. Index what you query.
* For measured costs and for the workloads where SQLite or rqlite are the better choice, see
  :doc:`../performance`.
