Objects across transactions
===========================

Objects are values. A committed object belongs to no transaction: it can be read, shared and built on by any
transaction, thread or process. This page explains what that allows, why references cannot dangle, how to
model things that change, and what is deliberately left outside the database. The principles are P1 and P2;
the decisions D18, D35 and D53. The tutorials :doc:`modelling data <tutorials/modelling-data>` and :doc:`the
CAD example <tutorials/cad>` use it on running code.

Committed objects are shared values
-----------------------------------

Everything in a committed version is immutable and addressed by a pointer that names one content forever. So:

* a transaction may use an object read by an earlier transaction, and may store a version derived from it;
* the decoded form of an object is cached by pointer for the whole space (D53), so a later transaction that
  reads the same object does not decode it again, and nothing needs invalidating because the content at a
  pointer cannot change;
* two transactions that read the same object see the same object, not two copies that may diverge.

The benefit and its limit (the working set must fit ``atom_cache_entries``) are shown with measured counters in
the CAD tutorial.

Referential integrity by construction
-------------------------------------

A reference in ProtoDB is a pointer to an atom that is already persisted. Two properties follow, without a
single constraint declared or checked:

1. **A new reference can only point to something that exists.** Atoms are saved children first: when a
   record is saved, everything it refers to is saved before it. A reference to a record is therefore always to
   a record that is in the storage.
2. **An old reference can never become wrong or vanish.** The referenced atom is immutable (P1) and nothing
   is ever deleted, so the pointer keeps returning exactly what it returned when the reference was made.

There are no foreign-key constraints and none are needed. There is nothing to "cascade" and nothing that
``DELETE`` can orphan. What the database cannot know is whether a referenced object is still *current* in
some collection: that is an application question, answered by the module's own rule (a status field, a
presence in a collection), and is not integrity.

Contrast this with a mutable system. There a row is changed in place and may be deleted, so the referencing
rows need a declared constraint that restricts, nulls or cascades the delete (Odoo's ``ondelete`` on a
many-to-one field is one such declaration) and that every code path must respect; records that must stop
being used are hidden with an ``active`` flag instead of deleted. ProtoDB moves the same two ideas to the
application: whether something is current is a method of your model, and nothing that was ever referenced
can disappear.

Values and entities
-------------------

The programmer chooses, per use, between a **value** (an immutable record: a fact at a moment) and an
**entity** (a ``MutableObject`` cell: an identity whose state changes by commit):

* A record that points at a *value* points at that exact version forever. Changing the value makes a new
  record, and every record that should follow it must be changed too.
* A record that holds a *handle* on an entity follows the entity's current state, and the entity's own history
  is reachable through snapshots. Editing the entity does not touch the records that hold its handle.
  Indexes on a handle-valued attribute group by the entity's identity.

At the all-entity extreme the model resembles a relational one and keeps time travel; at the all-value extreme
it is a persistent data structure. Mutability is a modelling decision, not a detail. Immutable sales orders
keep every intermediate state; mutable order lines change the order only when added or removed. The
:doc:`modelling tutorial <tutorials/modelling-data>` builds this.

The versioned-entity pattern
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To keep the history of a record chain its versions: each new version holds a ``previous`` attribute that
points at the version it replaced. It is a documented **pattern**, not part of the base module: ``previous`` is
an ordinary attribute set by the programmer. The collection holds only the current version, so indexes and
queries over "current" need no extra condition, and the history of one entity is a walk of the chain. A
relational design needs a versioned table with a validity interval and a version-aware condition in every
query that must see only current rows, including future ones; a query that forgets it quietly returns old
rows (the tutorial runs the contrast with SQLite).

Not in the mechanism
--------------------

Three things are deliberately left outside the database, because policy changes over time and a database that
keeps everything forever must not encode it:

No excision
   History is never rewritten. There is no operation that removes a version, and no compaction that drops
   data. If data must become unreadable, use encryption with a key held outside the database (destroying the
   key), or an application rule that decides what is shown. Whether an object is current is decided by methods
   of your model, as with a flag in a mutable system.

No access policy
   The database does not know users, roles or ownership. Access is enforced by the application, through the
   methods it exposes. A token that names a snapshot or a pager (``Database.snapshot_at``, ``Pager.token``)
   is a bearer reference to a version of the database, not a credential; do not hand one to a party that may
   not read every version.

No erasure
   A deletion in the model (removing a key from a dictionary) makes a new version without the key. The
   earlier versions still hold it. Plan disk for the history you keep.

What the model allows and does not protect
------------------------------------------

Because a transaction commits only for what it read and wrote (P2), two mistakes are possible. Both are
documented instead of being policed with expensive checks:

* a read-modify-write split across two transactions, where the second transaction wrote a value it computed
  from a read it did not make, silently overwrites an update committed in between;
* reusing objects of a transaction whose commit failed: they are valid values, but they were never published,
  and anything built from them may refer to state no version of the database ever held.

The :doc:`concurrency tutorial <tutorials/concurrency>` runs the first one.
