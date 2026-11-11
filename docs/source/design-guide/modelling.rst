Modelling choices: values, entities and versioned chains
========================================================

*Values against* ``MutableObject`` *entities; the versioned-entity pattern against relational versioned
tables. Decision D35; the maintainer's runtime decisions of 2026-11-08 (recorded in the ledger); principles P1
and P2.*

What it gives the ERP
---------------------

The ERP makes two different modelling choices, on purpose:

* **an opportunity is a value.** Each change is a new version that points at the one it replaced and records
  who made it. "Who changed opportunity 3, and what did it look like before?" is a walk of its chain: in the
  :doc:`tutorial </tutorials/erp>`, imported as a lead, qualified by Ann, given an amount by Ann, won by Bob.
  No audit table, no trigger;
* **a user is an entity**, a ``MutableObject`` cell. When Ann's name is corrected to "Ann Lee-Smith", every
  opportunity she owns shows the new name, old versions included, because they hold a *handle* on Ann, not a
  copy of her record. Her old name is still there for an "as of" read.

A **group** of users would be modelled the same way: a record whose members are user handles. Renaming a user
then changes nothing in any group, and a group that must remember who was in it at a given moment is a value
with its own version chain. (Groups are not in the tutorial yet; this paragraph is a design sketch, not
executed code.)

And the question "which opportunities are in proposal now?" needs no condition about versions at all.

The problem
-----------

An application must keep the history of some things (an order's states, an opportunity's stages) and the
current state of others (a user's name). It must also query "current" cheaply, whatever the length of the
history behind it.

**The symptom, as reported.** The maintainer's experience with an Odoo deployment: queries degraded as history
accumulated, because every search had to filter out the rows that were no longer current. This is a report,
not a measurement made in this project, and this chapter claims no timing for it.

**The symptom, executed.** The :doc:`modelling tutorial </tutorials/modelling-data>` builds the relational
version of a versioned sales order in SQLite: one row per version, a ``valid_to`` column that is ``NULL`` while
the version is current. The plausible query ``WHERE status = 'draft'`` returns ``['SO-1', 'SO-2', 'SO-3']``,
although SO-1 was invoiced; only the query that remembers ``AND valid_to IS NULL`` returns ``['SO-2',
'SO-3']`` (the tutorial's executed output).

Why the obvious solution fails
------------------------------

The obvious relational design is the **versioned table**: versions as rows, validity as columns. It works, and
it puts the burden in the wrong place:

* every query, view, report and index over the table must carry the version condition, including the ones
  other people write later, and a query that forgets it returns old rows *silently*;
* the table and its indexes grow with every revision, whether a query asks for history or not, so current
  queries pay for the history unless every index also encodes "current".

The obvious *ProtoDB* mistake is the opposite one: making everything a value, or everything an entity, by
habit. All values means that editing a user forces a new version of every record that should follow the user.
All entities gives a near-relational model: it keeps time travel through snapshots, but loses the per-record
history that a value chain gives for free.

The alternatives and their costs
--------------------------------

=====================  ===================================  ============================================
Choice                 What an edit costs                   What a reader sees
=====================  ===================================  ============================================
value (a record)       a new record, plus the path of tree  that exact version, forever; the history of
                       nodes above it in the collection     *what it was* is part of the data
                       and in each index it touches
entity (a cell)        a new value for the cell; the        the entity's state in the version being
                       records holding its handle do not    read; old states through snapshots
                       change
versioned table        a new row, plus the update of the    whatever the query's version condition says
(relational)           previous row's validity              (all versions, if it says nothing)
=====================  ===================================  ============================================

The edit cost of a value was measured in the ERP tutorial: 72,381 bytes for five edits of a
1,170,396-byte space, about 14,476 bytes per edit (sample output; the byte counts vary slightly from run to
run). That is path copying: an edit costs about the height of the trees, not the size of the collection. It is
also permanent.

The choice, and the invariant that makes it correct
---------------------------------------------------

**Mutability is a modelling decision, made per relationship** (maintainer, 2026-11-08). The programmer picks a
value or an entity for each use. At the all-entity extreme the system is near-relational and keeps time
travel; at the all-value extreme it is a persistent data structure. Most applications sit between.

**The versioned-entity pattern is a pattern, not a feature.** Each new version holds a ``previous`` attribute
that points at the version it replaced. It is an ordinary attribute set by the programmer; the base module does
not provide it (maintainer's decision, 2026-11-08), and nothing checks that it is set.

**The invariant: a collection holds only current versions.** Replacing an entry with its new version removes
the old version from the collection *and from every index of the collection*, in the same commit. The old
version is reachable only through the new version's ``previous``. Therefore:

* a query for "current" needs no version condition, because nothing else is in the collection;
* the index holds one entry per current record, however long the chains behind them. The modelling tutorial
  checks this exactly: after 200 more versions behind SO-2, the collection has 3 orders and its status index 3
  entries, while SO-2's chain has 201 versions;
* a query's cost depends on the collection and its indexes, which do not contain the history, so it does not
  grow with the depth of the chains. This last step is a structural argument. The timing measurement against
  history depth is the ``history_depth`` benchmark of :doc:`/performance`: 35.78 decodes per uncached query at
  every depth from 1 to 1,000 versions, and a latency that does not move.

By P1, the chain cannot be wrong: a version points at the exact version it replaced, and nothing can change
what that pointer returns.

Its cost, stated honestly
-------------------------

* **Every edit of a value is permanent and costs disk** (about 14 KB per edit in the ERP sample above, path
  copies included). Plan disk for the history you keep.
* **"Who" and "when" exist only if you store them.** The chain says *what* a record was; ``changed_by`` is
  stored by the ERP's own ``advance`` function, and ``Database.history()`` records when commits happened, not
  who made them.
* **A chain built inside one transaction is saved recursively**: the default Python recursion limit stops it
  between 150 versions (works) and 200 (``RecursionError``), measured in the modelling tutorial. One revision
  per transaction, the normal case, is not affected.
* **Analytics over all versions is harder here.** A relational ``GROUP BY`` over every version of every row is
  one statement; here it is a walk of every chain.
* **"As of" is per database version, not per row.** "This opportunity's changes between two dates" walks its
  chain.

What was tried and rejected
---------------------------

* **The version chain as part of the base module.** Rejected by the maintainer: it is a documented pattern,
  and the programmer decides where it applies.
* **Class extents** (every instance of a class reachable from the class): not provided; a list or set held in
  an attribute serves when an application needs one (maintainer, runtime decisions, 2026-11-08).
* **Excision of old versions** to keep the history small: rejected as a principle. History is never rewritten;
  whether something is current is decided by the application, as with Odoo's ``active`` field (see
  :doc:`referential-integrity` and :doc:`/objects-across-transactions`).

Back to the ERP
---------------

"Which opportunities are in proposal?" is a plain index lookup over the current versions, with no version
condition to forget. "Who moved opportunity 3 to won, and what was its amount before?" is a walk of its chain.
"Show Ann's current name on her old opportunities" works because the owner is an entity handle, not a copy.
Each answer comes from a modelling choice made once, per relationship, rather than from extra machinery: no
audit table, no trigger, no validity column.
