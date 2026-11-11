The ERP example
===============

This tutorial models the core of a small ERP: users, persons, and sales opportunities that move through
stages. It is deliberately small (model only, no ERP features), because its purpose is to show three
questions that an application normally needs extra machinery to answer, and that here need none:

1. *"What did the pipeline look like last Tuesday?"* An "as of" query.
2. *"Who changed this opportunity, and what did it look like before?"* An audit, with no audit table.
3. *"Give me the report page by page while other people keep working."* A stable paginated report.

Each section starts with the question, then shows the code, and ends by tying the answer back to the
property of the database that made it free. Every code block runs in the test suite.

The model
---------

* A **user** is an entity: its name may be corrected and every record that refers to it should show the
  current name. It is a ``MutableObject`` cell (see :doc:`modelling-data`).
* A **person** (a contact) is a plain record in a ``Dictionary``.
* An **opportunity** is a value: each change produces a new version that points at the previous one
  (``previous``) and records who made it (``changed_by``). The collection holds the current versions.

.. code-block:: python

   import datetime as dt
   import os
   import time

   from proto_db import (EqualityIndex, F, FileStorage, MutableObject, ObjectSpace, RangeIndex,
                         from_collection)

   def directory_bytes(path):
       return sum(os.path.getsize(os.path.join(path, name)) for name in os.listdir(path))

   STAGES = ["lead", "qualified", "proposal", "won", "lost"]
   space = ObjectSpace(FileStorage("erp-space"))
   db = space.new_database("erp")

   with db.new_transaction() as tr:
       logins = {"ann": "Ann Lee", "bob": "Bob Ruiz", "cy": "Cy Park"}
       users = {login: tr.new_mutable(f"user:{login}", tr.new_object(login=login, name=name))
                for login, name in logins.items()}
       persons = tr.new_dictionary().add_index(EqualityIndex("by_company", "company"))
       for i in range(200):
           persons = persons.set_at(i, tr.new_object(name=f"person {i}", company=f"company {i % 20}"))
       opportunities = (tr.new_dictionary()
                        .add_index(EqualityIndex("by_stage", "stage"))
                        .add_index(RangeIndex("by_amount", "amount"))
                        .add_index(EqualityIndex("by_owner", "owner")))        # keyed by the user's identity
       for n in range(1000):
           opportunities = opportunities.set_at(n, tr.new_object(
               number=n, title=f"opportunity {n}", stage=STAGES[n % 3], amount=1000 + (n * 37) % 9000,
               owner=users[list(logins)[n % 3]], contact=n % 200, changed_by="import", previous=None))
       tr.set_root_object("persons", persons)
       tr.set_root_object("opportunities", opportunities)

   def pipeline(source):
       """Opportunities and total amount per stage, in one query."""
       from proto_db import Agg
       rows = (from_collection(source).group_by(F.stage, {"stage": Agg.key(), "n": Agg.count(), "amount": Agg.sum(F.amount)})
               .order_by(F.stage).to_list())
       return [(r["stage"], r["n"], r["amount"]) for r in rows]

   tr = db.new_transaction()
   print(pipeline(tr.get_root_object("opportunities")))
   tr.abort()

.. code-block:: text
   :caption: Output

   [('lead', 334, 1808821), ('proposal', 333, 1795500), ('qualified', 333, 1783179)]

Question 1: the pipeline as it was
----------------------------------

Some people work the pipeline during the week. We note the time before and after, so that the questions
that follow have something to refer to.

.. code-block:: python

   def advance(number, stage, who, **changes):
       """Move an opportunity to a new stage: a new version that remembers the old one and who made it."""
       def work(tr):
           opportunities = tr.get_root_object("opportunities")
           current = opportunities.get_at(number)
           revised = current.with_(previous=current, stage=stage, changed_by=who, **changes)
           tr.set_root_object("opportunities", opportunities.set_at(number, revised))
       db.run(work)
       time.sleep(0.01)                           # commits get distinct timestamps on a fast machine

   before_week = dt.datetime.now(dt.timezone.utc)
   time.sleep(0.02)
   size_before = directory_bytes("erp-space")
   advance(3, "qualified", "ann")
   advance(3, "proposal", "ann", amount=7500)
   advance(3, "won", "bob")
   advance(4, "lost", "cy")
   mid_week = dt.datetime.now(dt.timezone.utc)
   time.sleep(0.02)
   advance(6, "qualified", "ann")
   size_after = directory_bytes("erp-space")

   def as_of(moment):
       """The database as it was at ``moment``: the newest committed version not later than it."""
       for entry in db.history():                 # newest first
           if entry.committed_at <= moment:
               return db.snapshot_at(entry)
       raise LookupError("the database did not exist yet")

   now = db.new_transaction()
   print("now:       ", pipeline(now.get_root_object("opportunities")))
   print("mid-week:  ", pipeline(as_of(mid_week).get_root_object("opportunities")))
   print("before:    ", pipeline(as_of(before_week).get_root_object("opportunities")))
   now.abort()

.. code-block:: text
   :caption: Output

   now:        [('lead', 332, 1806488), ('lost', 1, 1148), ('proposal', 333, 1795500), ('qualified', 333, 1783253), ('won', 1, 7500)]
   mid-week:   [('lead', 333, 1807710), ('lost', 1, 1148), ('proposal', 333, 1795500), ('qualified', 332, 1782031), ('won', 1, 7500)]
   before:     [('lead', 334, 1808821), ('proposal', 333, 1795500), ('qualified', 333, 1783179)]

The same function, ``pipeline``, ran against three versions of the data: the current transaction and two
snapshots found by time. No query was rewritten and no history table was consulted.

*Why it is free.* A commit never overwrites (P1), so every past version of the database is still there and
is a complete database. ``snapshot_at`` opens one; a snapshot reads the same way a transaction does.

Question 2: who changed it, and what did it look like
-----------------------------------------------------

.. code-block:: python

   def audit(opportunity):
       """Every version of an opportunity, oldest first: (stage, amount, changed_by)."""
       trail = []
       while opportunity is not None:
           trail.append((opportunity.stage, opportunity.amount, opportunity.changed_by))
           opportunity = opportunity.previous
       return list(reversed(trail))

   tr = db.new_transaction()
   opportunities = tr.get_root_object("opportunities")
   for row in audit(opportunities.get_at(3)):
       print(row)
   tr.abort()

.. code-block:: text
   :caption: Output

   ('lead', 1111, 'import')
   ('qualified', 1111, 'ann')
   ('proposal', 7500, 'ann')
   ('won', 7500, 'bob')

There is no audit table and no trigger: the chain of versions is the audit trail. Opportunity 3 was imported
as a lead, qualified by Ann, given an amount by Ann, and won by Bob. Because the owner is a user *entity*,
the question "which of Ann's opportunities were touched this week?" can use the owner's identity, and
the user's name shown is the current one even in old versions:

.. code-block:: python

   with db.new_transaction() as tr:
       ann = MutableObject("user:ann")
       ann.set(tr, ann.get(tr).with_(name="Ann Lee-Smith"))         # a correction, committed once

   tr = db.new_transaction()
   opportunities = tr.get_root_object("opportunities")
   query = from_collection(opportunities).where(F.owner == MutableObject("user:ann"))
   touched = [o for o in query.to_list() if o.changed_by != "import"]
   print(query.explain()["plan"], len(touched), "of Ann's", query.count(), "opportunities changed this week")
   first = audit(opportunities.get_at(3))[0]
   print(opportunities.get_at(3).owner.get(tr).name, "| first version by", first[2])
   print("Ann's name as of before the week:", as_of(before_week).get_mutable("user:ann").name)
   tr.abort()

.. code-block:: text
   :caption: Output

   IndexLookup 2 of Ann's 334 opportunities changed this week
   Ann Lee-Smith | first version by import
   Ann's name as of before the week: Ann Lee

*Why it is free.* A version points at the version it replaced, and nothing can change what it points at (P1).
The cost is the one the pattern states: *who* and *when* exist only if you store them in the version.
``changed_by`` is stored here by the ``advance`` function, and the commit time is in ``db.history()``.

What does history cost? Each edit writes a new opportunity, the path of tree nodes above it in the
collection and in each index that it touches, and a new database version. The block below measured the
growth of the space directory over the five edits above.

.. code-block:: python

   print(f"{size_after - size_before:,} bytes for 5 edits of a {size_before:,}-byte space"
         f" ({(size_after - size_before) // 5:,} bytes per edit)")

.. code-block:: text
   :caption: Sample output (the byte counts vary slightly from run to run)

   72,381 bytes for 5 edits of a 1,170,396-byte space (14,476 bytes per edit)

That is path copying: an edit costs about the height of the trees, not the size of the collection. It is
also permanent. Nothing is ever deleted, so a space that sees many edits grows with them. Plan disk for
the history you keep.

Question 3: a stable report while people keep working
-----------------------------------------------------

A report of the open opportunities, most valuable first, 250 rows per page, while a colleague keeps closing
deals. With offset paging (a new query for every page, "skip 250, take 250") edits between page requests move
rows across page boundaries, and the report loses some rows (or repeats them, if rows are added). First,
the usual way:

.. code-block:: python

   def top_open(source):
       return (from_collection(source).where(F.stage != "won").order_by(F.amount, descending=True)
               .then_by(F.number))

   with db.new_transaction() as tr:
       expected = [o.number for o in top_open(tr.get_root_object("opportunities")).to_list()]   # the truth at the start

   def offset_page(k, size=250):
       with db.new_transaction() as tr:               # a fresh snapshot per page, as a web request would
           return [o.number for o in top_open(tr.get_root_object("opportunities")).skip(k * size).take(size).to_list()]

   seen = offset_page(0)
   for number in seen[:40]:                           # another user wins the 40 best deals meanwhile
       advance(number, "won", "cy")
   seen += offset_page(1)
   print(len(expected), "rows were open when the report started; the offset report has", len(seen),
         "rows from its first two pages and misses", len(set(expected[:500]) - set(seen)), "of the first 500")

.. code-block:: text
   :caption: Output

   999 rows were open when the report started; the offset report has 500 rows from its first two pages and misses 40 of the first 500

Now the same report with a *pager*. It is bound to one snapshot when it is made, so every page comes from the
same version of the data:

.. code-block:: python

   with db.new_transaction() as tr:
       pager = tr.pager("opportunities", where=[["stage", "!=", "won"]], order_by=[("amount", True), "number"],
                        page_size=250)
       report = [o.number for o in pager.page(0)]
       token = pager.token()                          # a portable reference to this snapshot and query
       rows_at_start = pager.rows

   for number in report[:40]:                         # the 40 best deals of this report are won meanwhile
       advance(number, "won", "cy")

   resumed = db.resume_pager(token)                   # later, in any process
   for k in range(1, resumed.pages):
       report += [o.number for o in resumed.page(k)]
   with db.new_transaction() as tr:
       open_now = from_collection(tr.get_root_object("opportunities")).where(F.stage != "won").count()
   print(len(report), "rows in the report,", len(set(report)), "distinct,", rows_at_start, "rows at the start;",
         open_now, "open right now")

.. code-block:: text
   :caption: Output

   959 rows in the report, 959 distinct, 959 rows at the start; 919 open right now

The report has exactly the rows that were open when it started: none repeated, none skipped, and the
deals won during the report are still in it, as they were at its start. The token can be stored and used from
another process.

*Why it is free.* A snapshot is stable (P1), so pagination is stable, and readers never block writers
or the reverse (P4). There is no cursor to keep open on a server, no ``REPEATABLE READ`` transaction held
for the length of the report, and no temporary table.

Reusing what earlier transactions read
--------------------------------------

The three reports above ran in different transactions over the same data. A transaction does not start
from nothing: the space keeps the atoms that earlier transactions decoded, by storage pointer, and a
pointer's content never changes, so nothing needs invalidating. Reopen the space and watch the counters:

.. code-block:: python

   space.close()
   space = ObjectSpace(FileStorage("erp-space"))
   db = space.open_database("erp")

   def loads_for(label, work):
       before = space.atom_cache_stats()["loads"]
       work()
       print(f"{label:34} atoms loaded from disk: {space.atom_cache_stats()['loads'] - before}")

   def report():
       with db.new_transaction() as tr:
           return pipeline(tr.get_root_object("opportunities"))

   loads_for("first pipeline report (cold)", report)
   loads_for("same report, new transaction", report)
   loads_for("a related query, new transaction", lambda: from_collection(
       db.new_transaction().get_root_object("opportunities")).where(F.stage == "lost").count())
   space.close()

.. code-block:: text
   :caption: Output

   first pipeline report (cold)       atoms loaded from disk: 2012
   same report, new transaction       atoms loaded from disk: 0
   a related query, new transaction   atoms loaded from disk: 6

The second and third transactions decode only what the earlier ones did not. :doc:`cad` explores this
with a deep object graph, and states its limit: the benefit needs the working set to fit in the cache.

What this example does not give you
-----------------------------------

* The time travel here is per commit and per database, not per row: "as of" finds a whole database
  version. A question like "show me this opportunity's changes between two dates" walks its chain.
* Nothing in the database says *who* made a commit. If you need that, store it, as ``changed_by`` does.
* Nothing is deleted. If a person's data must be erased, history cannot do it; encryption with a key
  outside the database, or an application rule that decides what is shown, can. There is no access
  policy in the database either: show data through methods that apply your rules.
* An ERP built on a relational database has decades of tooling around reports, reporting engines and
  access control. This example shows what ``proto_db`` changes in the model, not that it replaces that tooling.
