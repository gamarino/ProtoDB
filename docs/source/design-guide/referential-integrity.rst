Referential integrity by construction
=====================================

*Ruling R85; decisions D34 (children-first saves) and D35 (handles); principle P1. The reference page is*
:doc:`/objects-across-transactions`.

What it gives the ERP
---------------------

An opportunity of the ERP refers to its owner and to earlier versions of itself. A report run today, and an
"as of" report of last year, can follow those references without ever meeting a reference to nothing. The ERP
declares no foreign key, runs no check at commit, and has no rule saying what to do with an opportunity when
something it refers to is "deleted". Nothing it refers to by pointer can be.

The chapter also explains the case where the ERP *can* look something up and find nothing, because the
difference is the point of the chapter.

The problem
-----------

In a database that updates in place, a row can be changed or deleted while other rows refer to it. Integrity
then has to be *declared and enforced*: a foreign-key constraint that restricts, nulls or cascades the delete
(Odoo's ``ondelete`` on a many-to-one field is one such declaration), respected by every code path, and an
``active`` flag for records that must stop being used but cannot be deleted.

**Measured symptom: none.** This chapter has no figure. The problem is a class of defects (dangling
references, cascades that remove more than intended, code paths that bypass a constraint), not a cost, and
this project did not measure a relational system's behaviour here. The claim made is structural, and the
reasoning below is its evidence.

Why the obvious solution fails
------------------------------

The obvious move for a new database is to offer the same thing: foreign-key-like constraints, checked at
commit. In ProtoDB it would be both costly and pointless:

* **costly**, because every reference written would need a check at commit, inside the critical section of a
  cluster commit, which is the one place the design keeps free of storage reads;
* **pointless for references by pointer**, because, as the next section shows, such a reference cannot dangle;
* **policy in the wrong place.** Restrict, cascade or null is a business rule. A database that keeps every
  version forever must not encode a policy that will change (the maintainer's principle of 2026-11-08: no
  access policy and no erasure in the database mechanism).

The choice, and the invariant that makes it correct
---------------------------------------------------

**Integrity follows from two rules the system already has** (ruling R85):

1. **A new reference can only point to something already persisted.** A commit saves the atoms it wrote
   *children first* (D34, step 2): when a record is saved, everything it refers to is saved before it. A
   reference stored in a record is therefore always a pointer to a record already in the storage. In a
   cluster the same holds per node, with one more rule on top: the holder makes its own records durable
   locally before any of them leaves the node (D46).
2. **An old reference can never become wrong or vanish.** The referenced atom is immutable and nothing is ever
   deleted (P1): no compaction drops a data version, and local copies are released only once the object store
   holds them in full. A pointer keeps returning exactly what it returned when the reference was made.

There is nothing to cascade and nothing that a delete can orphan. Removing a key from a ``Dictionary`` makes a
new version without the key; every record that pointed at the removed value still points at it, and still
reads it.

References by pointer, and lookups by name
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The guarantee covers **references by pointer**: an attribute whose value is an atom (a record, a collection,
an earlier version). It does not cover **lookups by name**, and the ERP uses both:

* ``opportunity.previous`` is a pointer. It cannot dangle;
* ``opportunity.owner`` is a ``MutableObject`` handle, which holds only the cell's *name*. ``owner.get(tr)``
  looks the cell up in the transaction's version, and returns ``None`` if the cell has been removed
  (``remove_mutable``);
* ``opportunity.contact`` holds a person's *key* in the ``persons`` dictionary (``contact=n % 200`` in the
  tutorial). Looking it up returns nothing if the key was removed.

A lookup by name asks "*what is there under this name, in the version I am reading?*", and the answer can
legitimately be "nothing". Nothing checks a name when it is written either: ``MutableObject("user:zoe")``
creates a handle, not a cell. That is the nature of a name, not an integrity failure: a version in which the
name resolved still resolves it when read "as of" that version. Whether a referenced thing is still *current*
is an application question, answered by the application's own rule (a status field, presence in a
collection), not by the database.

Its cost, stated honestly
-------------------------

* **Nothing is ever deleted, so disk grows with history.** There is no erasure. Data that must become
  unreadable needs encryption with a key kept outside the database (destroying the key), or an application
  rule that decides what is shown.
* **"Is it current?" is the application's job.** The database will not tell the ERP that a person was removed
  from the persons dictionary while an opportunity still names her.
* **Lookups by name can return nothing**, and the application must handle that case wherever it uses a key or
  a handle.

What was tried and rejected
---------------------------

* **Foreign-key constraints:** not offered, for the reasons above.
* **Excision** (removing a version, for example for legal reasons): rejected as a principle. History is never
  rewritten (maintainer, 2026-11-08).
* **Access policy in the database:** rejected for the same reason as constraints; policy changes over time.
  Access goes through the application's methods, and a pager or snapshot token is a bearer reference, not a
  credential (:doc:`/objects-across-transactions`).

Back to the ERP
---------------

Every report, current or "as of", can follow every pointer it meets: an earlier version of an opportunity is
always there and always the same. Where the ERP refers to something *by name* (the owner's cell, the contact's
key), the lookup answers for the version being read, and "is this person still a contact?" is decided by the
ERP's own rules. The ERP gets integrity without declaring it, and keeps the business rules where they can
change: in the application.
