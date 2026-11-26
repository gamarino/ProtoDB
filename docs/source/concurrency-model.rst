The concurrency and isolation model
===================================

This page states precisely what a transaction sees, what a commit checks, which concurrent writes compose
and which conflict, and what the model leaves to you. It is the reference for the **precondition model**
of ``proto_db`` 2.0.0rc5: decision D73 (which amends D9, D26, D33, D34 and D35), with D11 and D52; the
principles are P1, P2, P4 and P7. The normative text is the specification
``docs/superpowers/specs/2026-11-24-precondition-concurrency-model-design.md``; its rule numbers (P-A to P-G,
R1 to R70) are cited here. The :doc:`concurrency tutorial <tutorials/concurrency>` runs the main statements
of this page.

.. contents::
   :local:
   :depth: 1

Snapshot isolation
------------------

A transaction reads the storage root once, when it starts, and every read goes to that snapshot, its
**base**. A commit by anyone else never changes what an open transaction reads, and uncommitted work does not
exist for anyone but its transaction. Readers never block writers or one another, and snapshot reads never
contend with writers (P4). A ``Snapshot`` (``Database.snapshot_at``) is the same view of any past version,
read-only.

Snapshot isolation alone is not enough: two transactions that read the same balance and each write a
withdrawal both see a consistent snapshot, and both commits would leave a state no serial order produces.
What a commit checks is the subject of the rest of this page.

Why the model changed
---------------------

The obvious design, and the one ``proto_db`` used until 2.0.0rc4, takes the **named root** as the unit of
conflict: reading a root puts the token of the whole root in the transaction's read set, and the commit
refuses the transaction if any root it read changed. It is simple and correct for object roots. It fails in
three ways for the rest:

* **Collections conflict as a whole.** A transaction that reads a dictionary root to change one key holds
  the whole dictionary in its read set, so two transactions that touch different keys conflict, and a hot
  collection serializes every writer.
* **A merge was bolted on, conditionally.** To let writers of different keys coexist, a collection written
  *without being read* was merged by replaying its operation log (D33), but only when the log was
  "complete", a runtime state (``log_is_complete``) the caller could not see. The same ``set_root_object``
  call merged or replaced depending on how the value had been built.
* **Blind writes lost updates.** A root or a mutable cell written without a prior read was not compared with
  anything: the last writer won.

Three mechanisms (the read set per root, the op-log merge and, for the semantic layer, an intent replay)
coexisted without a common definition of conflict. The precondition model replaces all three with one.

Preconditions and intentions
----------------------------

Every operation of a transaction records, as data, what it saw of its base (a **precondition**) and what
change it wants to make (an **intention**). At commit, every precondition is validated against the current
state and, if they all hold, the intentions are reapplied on that state. If any fails, the whole transaction
fails with ``ProtoConflictError`` and nothing is published.

Definitions
   *Base*: the database version in force when the transaction opened; it never changes during the
   transaction. *Current*: the version the commit is built on (the last one published or, inside a
   group-commit round, the one the previous commit of the round produced). *Conflict unit*: the portion of
   state a precondition protects: a dictionary key, a set element, a list position, a mutable cell, a root,
   a whole collection. *Absent* is a value of every unit: creating is going from absent to a value.

The rules every precondition follows
   * **Exact comparison** (R6). A unit's value is compared by exact token
     (:func:`proto_db.values.exact_token`): plain values by type and exact payload, so ``1`` becoming ``1.0``
     or ``Decimal("1")`` becoming ``Decimal("1.00")`` is a change; atoms by storage pointer, never loaded.
   * **ABA by value** (R7). If the current value equals the expected one, the precondition holds even if
     there were intermediate changes. There are no version numbers.
   * **Fixed base** (R8). The expected value is always the base's, whenever the transaction touched the unit.
   * **One record per unit** (R9). The first operation that observes a unit records its precondition; an
     operation on a unit the transaction already wrote reads its own value and adds none.
   * **Writing the base value is a write** (R10): it keeps its precondition.
   * **Reading is declaring** (P-E). What a transaction read is taken as necessary for its logic and must
     still be the same at publication. Do not read what your logic does not need.
   * **Coarser when in doubt** (P-A). An operation whose fine unit is not proven safe takes a coarser one (in
     the end, the whole collection). An extra conflict is acceptable; a result that no serial execution would
     produce is not.

The guarantee
   Committed transactions are **serializable in commit order**: every committed transaction read what a
   serial execution of all of them, in commit order, would have shown it, and the final state is that
   execution's. Control is optimistic: reading neither blocks nor reserves anything, and the only point of
   exclusion is the publication of the space root (P-D). A transaction that writes nothing neither validates
   nor publishes: it sees a prefix of the commit order and never fails with a conflict (R23).

   The guarantee is tested, not only argued: ``tests/concurrency/test_serializability.py`` runs threads of
   randomly generated transactions over the closed set of operations below, re-runs the committed ones
   serially in commit order on an in-memory model, and requires every read and the final state to match. Its
   docstring lists the classes of defect it catches (a missing precondition, an intention reapplied wrongly,
   a commit order that does not match the published versions).

Every conflict names its unit: ``ProtoConflictError.unit`` is a :class:`proto_db.preconditions.Unit` (its
``kind``, ``root``, ``key`` and ``index``), and ``ProtoConflictError.database`` names the database (R36).
``Database.run`` retries a conflict; ``ProtoOutcomeUnknownError`` is never retried.

Roots, collections and cells
----------------------------

Which unit a root has depends on its value (R1):

* An **object root** holds any value other than a collection: an integer, a string, a record (``DBObject``),
  a tuple. The unit is the **whole value** (R2): two transactions that change different attributes of the
  same record conflict. There is no per-attribute merge.
* A **collection root** holds a ``Dictionary``, ``HashDictionary``, ``Set``, ``CountedSet``,
  ``RepeatedKeysDictionary`` or ``List``. Its unit depends on the operation (the tables below).
* A **mutable cell** (``MutableObject``, D35) is its own unit, by name, in a namespace of its own: a root and
  a cell of the same name are unrelated, and different cells never conflict (R3).

**One level only** (R5). Fine grain applies only to the collection that is the direct value of a root. A
collection inside another collection, inside a record or inside a cell has no units of its own: changing it
is writing the unit that contains it.

**A subclass is fine-grained only if it changes nothing the model relies on.** Each collection class lists the
operations its units depend on; a subclass that overrides any of them is treated as an object root (its
override could do anything the engine cannot see).

Linked collections
~~~~~~~~~~~~~~~~~~

``get_root_object`` on a collection root returns a **linked** copy: the transaction's own instance, with the
base collection's content and storage pointer and, as runtime state, a link to the transaction and the root
(R40, R41). Every version a mutator derives from it inherits the link (R42).

* **Writes are recorded per version** (R43). Each version carries the record of the intentions that produced
  it. You may derive two branches from one base and assign one; only the assigned branch's intentions count.
* **Reads are recorded per transaction** (R44), whatever branch they were made on. A derived version you do
  not assign publishes nothing, but its reads stay recorded (R45).
* **Assignment is explicit** (R45, R46). A collection's changes are published only when the version is
  assigned back to the **same root of the same transaction** with ``set_root_object``.
* **The link ends with the transaction** (R47), committed or not. A collection reused in a later
  transaction, one built separately (``tr.new_dictionary()``, ``from_items``, ``from_iterable``), one taken
  from another root, another transaction or a ``Snapshot``, is **not linked**: assigning it replaces the
  root, compared against the root's base value like an object root (R11).
* **A linked collection stored elsewhere depends on its whole source root.** Persisting a linked version
  other than by assigning it to its own root -- writing it (or a version derived from it) to another root
  or a mutable cell, holding it in an object attribute, or as a value or element of another collection, at
  any depth -- publishes its whole content, so the transaction records the whole source root, exactly as
  reading all of it does: any concurrent change of that root is a conflict. This also holds when the same
  version is assigned to its own root as well: that root is then published as is, without reapplying.

Writes
------

The list is closed (P-B): an operation not listed takes the whole collection as its unit. "Whole collection"
means the precondition "the root's current value is its base value"; when it holds, the transaction's value
is published as is, without reapplying.

Roots and cells
~~~~~~~~~~~~~~~

==============================================  ==========================================  ===========================
Operation                                       Precondition                                Intention
==============================================  ==========================================  ===========================
``set_root_object(n, v)``, ``v`` not linked     root ``n`` holds its base value, absent      root ``n`` holds ``v``
                                                included
``remove_root_object(n)``                       root ``n`` holds its base value, absent      root ``n`` becomes absent
                                                included
``new_mutable(n, v)``                           cell ``n`` is absent                         cell ``n`` holds ``v``
``set_mutable(n, v)``                           cell ``n`` holds its base value              cell ``n`` holds ``v``
``remove_mutable(n)``                           cell ``n`` holds its base value              cell ``n`` becomes absent
==============================================  ==========================================  ===========================

A write without a prior read is compared too: a blind ``set_mutable`` on a cell another transaction changed
is a conflict, and so is removing a cell another transaction already removed. Assigning a new collection to
an existing root replaces it, compared against the base value.

Collections
~~~~~~~~~~~

=========================================  ===============================================  ==========================
Operation                                  Precondition                                     Intention
=========================================  ===============================================  ==========================
Dictionary, HashDictionary ``set_at(k)``   key ``k`` holds its base value, absent included  ``k`` holds the value
Dictionary, HashDictionary ``remove_at``   key ``k`` holds its base value, absent included  ``k`` becomes absent
Set ``add(e)``                             none                                             ``e`` present
Set ``remove(e)``                          ``e`` is still present (or still absent, when    ``e`` absent
                                           absent in the base)
Set ``union`` / ``difference``             one ``add`` / ``remove`` per element
Set ``intersection``                       whole collection
CountedSet ``add`` / ``remove``            none: counts compose (D26)                       one adjustment per element
RepeatedKeysDictionary record operations   none: counts compose per key (D26)               one adjustment per key
List ``append_last``, ``extend``           none                                             appended, in commit order
List ``set_at(i)`` of a base position      position ``i`` exists and holds its base value   ``i`` holds the value
List ``set_at(i)``, ``i`` past the base    whole collection
length, or negative
List ``insert_at``, ``remove_at``,         whole collection
``append_first`` and every other
structural operation
``add_index``, ``remove_index``            whole collection (see `Indexes`_ for the
                                           catch-up that keeps it from failing)
any other mutator                          whole collection
=========================================  ===============================================  ==========================

Notes on the table:

* Two transactions that insert the same new dictionary key conflict even if they write the same value.
* ``Set.add`` has no precondition because every serial order leaves the element present. ``remove`` of an
  element absent in the base still declares "absent": without it, a remove that found nothing would coexist
  with someone else's add, and the result would match no serial order.
* **Counters compose** (D26, kept). Concurrent increments and decrements of one ``CountedSet`` element, or of
  one ``RepeatedKeysDictionary`` key, are composed per element as the transform
  ``c -> max(c - removes, 0) + adds``, which equals replaying the transaction's operations after the
  concurrent ones.
* **Indexed counters do not compose on the element they remove** (D73 amendment of D26). On a collection
  that carries an index, a removal whose reapplication would take out a member the preparation could not key
  records a declared precondition instead: ``CountedSet.remove(e)`` records the count of ``e``
  (``COUNT``), ``RepeatedKeysDictionary.remove_at`` (alias ``remove_key``) and ``remove_record`` record the
  records of the key (``RECORDS``), and an indexed ``Set.remove`` records the element from the base even after the
  transaction's own write of it. Two such transactions on the same element or key conflict; other elements
  still compose. Reason: under the root lock no extractor may run (P-C), and a concurrent commit may have
  replaced the stored member by an equal one of another exact form (``10`` by ``10.0``), whose index key was
  never computed.
* **List positions do not move** (R14-R16). Indices are not translated: an insertion shifts every later
  position, so a structural operation makes the whole list the unit for that transaction. Anchoring an
  insertion to its neighbours was evaluated and discarded as incorrect.
* **A position the transaction appended is read through the length** (D73 amendment of R9 and R16). A
  ``get_at`` or ``set_at`` of a position at or past the base length (one of the transaction's own appends) or
  a negative position records the whole list, because the position names "my append" only if the length is
  what the transaction assumes. The folding R9 suggests was found non-serializable by the randomized checker
  (base ``[1, 2, 3]``; A appends 7 and sets position 3 to 8, B appends 9 and commits first; folding gives
  ``[1, 2, 3, 9, 8]``, which neither serial order gives).
* A position out of range records the whole list before raising: the error reveals the length.

Reads
-----

A read records a precondition without an intention (P-E). The list is closed: a read not listed records the
whole collection (P-B), and the model's serializability depends on no read path being left unrecorded.

The transaction
~~~~~~~~~~~~~~~

==========================================  ===================================================================
Read                                        Precondition recorded
==========================================  ===================================================================
``get_root_object(n)``, root absent         ``n`` is still absent
``get_root_object(n)``, object root         ``n`` holds its base value
``get_root_object(n)``, collection root     ``n`` is still present and of the same class; content not recorded
``root_names()``                            the set of root names did not change
``get_mutable(n)``                          cell ``n`` holds its base value, absent included
``mutable_names()``                         the set of cell names did not change
==========================================  ===================================================================

``root_names()`` and ``mutable_names()`` conflict with a concurrent creation or removal of a root or a cell,
never with a change of a value. If a root that a transaction used with fine-grained operations is absent at
commit, or now holds another class, the transaction conflicts (R12).

A linked collection
~~~~~~~~~~~~~~~~~~~

=====================================================================  ==============================================
Read                                                                   Precondition recorded
=====================================================================  ==============================================
Dictionary, HashDictionary ``get_at(k)`` (alias ``get``), ``has(k)``,  key ``k`` holds its base value, absent
``k in d``                                                             included
Set ``has(e)``, ``e in s``                                             presence or absence of ``e`` unchanged
CountedSet ``has(e)``, ``count_of(e)``                                 count of ``e`` unchanged
RepeatedKeysDictionary ``get_at(k)`` (alias ``get``), ``has(k)``       the records of ``k`` unchanged
List ``get_at(i)``, ``i`` a base position                              position ``i`` exists and holds its base value
query answered by an equality-index lookup of key ``x`` (``==``,       the group of ``x`` in that index unchanged
``in``), alone or under filter, sort, limit, union, intersection
query answered by a ``RangeIndex`` range scan (``<``, ``<=``, ``>``,   no index key inside the scanned range changed
``>=``, the ``between`` family)                                        (a range invariant)
Dictionary ``keys(after=k)``                                           no key from ``k`` to the last key returned
                                                                       changed (to the end when exhausted)
planner window over a Dictionary (``take`` / ``skip``)                 no key from the **first** key to the window's
                                                                       last changed; a window reaching the end: whole
``index_names()``                                                      the set of index definitions unchanged
``get_index(name)``                                                    whole collection, and the index definitions
``len``, ``count``, iteration, ``elements``, ``items``, ``keys()``,    whole collection
``values``, ``slice``, ``head``, ``tail``, ``content_equals``
``key_at_position(i)``, ``position_of(k)``                             whole collection
a scan, an unindexed query, ``!=``, ``?!N``, an ordered index scan,    whole collection
a vector search, ``explain()`` of a plan that scans, a pager
=====================================================================  ==============================================

Rules for reads:

* **A read on a modified version** (R18) records what it reveals about the base: ``len`` after an ``add``
  reveals the base count, so it records the whole collection. Reading a unit the transaction wrote records
  nothing (R9).
* **What is reachable from a read value is covered** (R20). Values are immutable, so navigating the attributes
  of a record obtained by a recorded read needs no further record. The exception is a ``MutableObject``:
  resolving one is a ``get_mutable`` and is recorded.
* **Ranges** (R67, R68). A range is data: the root, the source (the Dictionary's own keys or a named
  ``RangeIndex``) and two bounds compared by ``order_key``, so validating it runs no user code. A range read
  conflicts only with a change inside its range. A window by **offset** depends on every key before it, so it
  records the range from the collection's first key (D73 amendment of R68). ``keys(after=k)`` includes ``k``
  itself in its range: a concurrent change of ``k`` conflicts although ``k`` was not returned (stricter than
  needed, safe).
* **The upper layers read through these operations** (R22): the query planner, the query cache (a result
  served from the cache records reads that cover what the query that produced it recorded), the pagers and
  the semantic layer. A plan node the cache does not know records the whole collection of every scan under it.
* **What a query declares follows the plan that runs**, not the indexes that exist: on a small collection the
  planner may prefer a scan (D30), and a scan declares the whole collection. ``explain()`` shows the plan.
* **Positional access** (R17). ``Dictionary.key_at_position(i)`` and ``position_of(k)`` run in O(log n) from
  the node counts, and record the whole collection. ``position_of`` of an absent key raises
  ``ProtoValidationError`` (D73), as ``List.index`` does for a missing value.

Reads outside the guarantee
~~~~~~~~~~~~~~~~~~~~~~~~~~~

These paths record nothing, on purpose, and what they return is outside the transaction's serializability
guarantee (R21):

* a ``Snapshot`` from ``Database.snapshot_at`` (also of the transaction's own ``snapshot_ref``),
  ``Database.history()`` and its entries;
* ``Transaction.snapshot_mutable``;
* ``Transaction.load`` of a pointer (an engine path);
* objects brought from another transaction, or from a ``Snapshot``.

Reading from a ``Snapshot`` is the deliberate escape, the analogue of a relational ``READ COMMITTED``: a
transaction that computes a summary from a snapshot is not refused when the source changes, and the summary
it writes may be stale. The tutorial shows both behaviours side by side.

Indexes
-------

An index is data derived from the collection's elements (``EqualityIndex``, ``RangeIndex``, ``VectorIndex``).
Before this model, a commit that merged a write into an indexed collection ran the extractors with the root
held. That violates P-C ("with the space root held, only engine code runs"), and it is gone.

* **Precomputed keys** (R24, R49). In a preparation phase, before the commit takes the space root, the key of
  every element a transaction adds, removes or replaces is computed with the extractors of the base
  collection's indexes. The keys travel with the intentions as data; under the root, the indexes are updated
  from those keys alone.
* **Stable definitions** (R25). Every transaction with intentions on an indexed collection declares that the
  set of index definitions is unchanged: an index added concurrently was not covered by its keys.
* **Pure extractors** (R27). A key must depend only on its element. An extractor that reads through a
  transaction (a root, a cell, ``load``) fails with ``ProtoUserError``.
* **Equality-group invariant** (R28). A lookup by an ``EqualityIndex`` (or a ``RangeIndex`` used for ``==``)
  declares that the group of that key is unchanged: changes to other keys do not conflict with it.
* **Unique index** (R29). ``EqualityIndex(name, key, unique=True)`` holds at most one element per key (the
  attribute is persisted; MISSING and ``None`` never count as duplicates; ``1`` and ``1.0`` are one key). A
  duplicate against the transaction's own view raises ``ProtoValidationError`` at the write. A duplicate
  committed concurrently is ``ProtoConflictError`` on that key's group, checked on the final reapplied state;
  the retry sees the duplicate and fails with the validation error.
* **Vector indexes** (R30). The exact and HNSW backends are maintained from the precomputed vector. An
  IVF-Flat index cannot be (its lists depend on the whole collection), so every write to a collection that
  carries one takes the whole collection.
* **Custom indexes.** An ``Index`` subclass is maintained from precomputed keys only if it opts in
  (``_keyed_reapplication = True`` and the ``with_added_key`` / ``with_removed_key`` hooks,
  :doc:`indexing-framework`); otherwise a write to its collection takes the whole collection.
* **Creating an index catches up** (R69). ``add_index`` and ``remove_index`` take the whole collection, so on
  a busy collection they would never commit. A transaction whose only operations on a collection are index
  definition changes, and that read nothing else of it, does not fail because the collection changed: in the
  pre-pass the engine diffs the base against the newest root, applies the changed elements to the index under
  construction (extractors run there, outside the lock) and takes the newer collection as the expected value,
  at most ``INDEX_CATCH_UP_ROUNDS = 3`` times. Under the lock the whole-collection precondition compares
  against the last expected value; if it changed again, ``ProtoConflictError`` and ``Database.run`` retries.
  On a unique index, a concurrent duplicate found during the catch-up is a ``ProtoConflictError``, whose retry
  fails with ``ProtoValidationError`` (D73 amendment of R29).

Patterns
--------

Check and add
~~~~~~~~~~~~~

"Add this only if it is not there yet" reads before it writes, and how it reads decides what it conflicts
with:

==============================================  ===================================================================
How existence is checked                        Resulting conflict unit
==============================================  ===================================================================
by key or element (``get_at(k)``, ``has(e)``)   that key or that element
by an equality index on the field               the group of that index key
with a declared unique index                    imposed by the engine, even if the application does not read first
by a traversal or a query without an equality   the whole collection
index
==============================================  ===================================================================

Check a reference
~~~~~~~~~~~~~~~~~

The same reasoning applies to a reference. A record that names another (an order naming a customer key, a
handle of a cell) is valid only while the target exists, so the transaction that writes the reference reads
the target (``customers.has(k)``, ``get_mutable(name)``): that read makes a concurrent removal of the target a
conflict. Without it the order commits and names a customer that no longer exists.

Identifiers without a shared counter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A counter cell that every writer increments is a single unit every writer reads and writes: a hot key, where
of N concurrent writers one commits per attempt. Generate identifiers in the transaction instead, without a
shared state: a UUID (``uuid.uuid4()``), or a ULID when creation order matters. A sequence outside the
transactions is out of scope: a number visible before the commit would either conflict like the counter or
leave gaps on every failed attempt.

A collection ordered by a user criterion
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A ``List`` does not serve concurrent ordered insertions: each insertion takes the whole list. Model the case
with a ``Dictionary`` whose key is the ordering key, computed in the transaction; the order is ``order_key``'s,
and repeated orderings use a composite key ``(ordering key, tie-breaker)``, a UUID for instance. Inserts of
different keys compose; ``key_at_position`` and ``position_of`` give positional access, at the price of a
whole-collection read.

Retries and external effects
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``Database.run(fn, retries=10, backoff=(0.001, 0.2))`` runs ``fn(tr)`` in a fresh transaction and retries on
``ProtoConflictError`` with exponential back-off and jitter. Any other exception aborts and propagates, and
``ProtoOutcomeUnknownError`` is never retried: the commit may have been applied. Because ``fn`` may run
several times, its external effects (a message sent, an API called) belong **after** ``run`` returns.

Lock cells
~~~~~~~~~~

Pessimistic locks are not part of the core (P-D), and nothing in the core prevents them. A lock is a cell
with an agreed name: taking it is ``new_mutable`` in a short transaction, which conflicts if someone created
it first; releasing it is ``remove_mutable``. The cell must carry an **owner** and an **expiry**, because an
owner that crashed never removes its cell; a taker may replace an expired cell. The expiry is a liveness
device only (clocks drift): the safety comes from the holder reading the lock cell in the transaction that
publishes its results, so a takeover in between makes that commit conflict on the cell.

The commit protocol
-------------------

The structure is the one of D34 (exact tokens, nothing failing after a successful publish, unknown outcomes
reported as such) with a different validation, a preparation phase and a pre-pass:

1. **Close.** The transaction stops being active. Without intentions it ends without publishing (R23).
2. **Preparation, without the space root.** One record per unit (R9); the index keys of the affected
   elements and the new indexes (R24, R26); the catch-up of index definitions (R69); every atom reachable
   from the written values is saved (the storage is append-only, so unpublished records are harmless).
3. **Pre-pass, without the space root.** Read the last published root, R0; validate every precondition
   against it, raising ``ProtoConflictError`` without ever taking the lock; reapply the intentions on R0 and
   keep the resulting maps of roots and cells as the **candidate** (R64).
4. **Publication, with the space root held** (the root lock; in a cluster, the write permission). Read the
   current root R1. If R1 is R0 by pointer, the new database version is built on the candidate: no
   validation, no reapplication, no storage read in the tests (R31, R39). Otherwise the commit validates the
   **delta**: a structural diff of the persistent trees from R0 to R1, which skips every pair of subtrees with
   the same pointer, fails if any changed unit is covered by a recorded precondition or range (R65); then it
   reapplies the intentions on R1. All preconditions are checked before any intention is applied (R33, P-F).
   Delta validation costs O(changes since R0 × log n) for the diff, plus a scan of each recorded range over
   the changed keys of its root, and the reapplication costs O(intentions × log n); neither depends on the
   size of the read set. Comparing an atom key (its order key or token) may load a stub under the lock: I/O
   bounded by the diff, never user code.
5. **Publish** the root by compare-and-swap of the epoch, after ``flush``; the outcome rules are below.

Details:

* **Group commit** (R32, D52). A commit that finds a round running queues, and the thread that finds none
  running publishes everything queued in one round, each commit built on the root the previous one produced.
  The candidate serves any entry whose current root is still its R0 by pointer (D73: this includes a later
  entry after a first entry that published nothing); every other entry diffs R0 against the round's current
  root.
* **Cluster** (R38). Validation and reapplication run on the node that builds the commit, under its write
  permission, with the same rules. When R0 cannot be read outside the permission (a record not uploaded yet
  while a peer is silent), the commit runs without a candidate and validates fully against the permission's
  base. Every node must validate the same way, hence protocol version 16 (:doc:`coordinator-protocol`).
* **Escalation** (R66). A transaction records at most ``PRECONDITION_ESCALATION_LIMIT = 1024`` fine
  preconditions and ranges per collection root; the next one replaces them all with a whole-collection
  precondition. The limit bounds the memory of a read set, as PostgreSQL bounds its predicate locks.
* **Reserved roots** (R37). The roots of installed modules and of the semantic catalog keep their own merges
  (add-if-absent under the root lock, :doc:`design-guide/registry`), outside this model.
* **Outcome.** If publishing fails, the outcome may be unknown. The storage's ``publish_outcome`` hook, or for
  a storage whose root lock excludes every writer a re-read of the root, resolves it where that is provable;
  otherwise the caller gets ``ProtoOutcomeUnknownError``, an honest "I do not know", never a guess (P5). After
  a successful publish nothing can fail the commit: releasing the lock is best effort and a failure there is
  logged.

What the model leaves to you
----------------------------

A transaction commits for what it read and wrote and nothing else (P2). That is what makes it an explicit
object, and it means three things are possible and not policed:

* a read-modify-write split across two transactions (read in one, write the result in another) is not a
  conflict, because the second transaction's base already holds whatever it overwrites;
* an object taken from a transaction whose commit failed can be reused in another; its content is what it
  was, and it is on you to know that it was never published;
* a read through a path outside the guarantee (the list above) is not validated: that is its purpose.

Known limits
------------

These follow from the model. None of them can produce a result that no serial order produces; each costs
conflicts, retries or throughput.

* **Hot units.** Of N concurrent transactions on the same unit, one commits per attempt and the others redo
  their work. Commutative operations (``Set.add``, unindexed counters, ``List.append_last``) do not have this
  limit.
* **Traversals.** A transaction that traverses or range-queries a collection and then writes collides with any
  concurrent change in what it read: the whole collection for a traversal, the range for a range query.
* **No fairness.** A long transaction that reads a heavily modified unit can exhaust its retries; nothing
  orders the retries of competing transactions.
* **Write throughput.** Validation and reapplication of one space run on a single thread at a time;
  commit volume does not scale with cores. There is one ordering point per space.
* **Escalation coarsens.** Past 1024 fine preconditions or ranges on one root, a transaction holds the whole
  collection, and conflicts with any change of it.
* **Coarse reads by design.** ``len``/``count`` record the whole collection, not a count unit; ``explain()``
  of a plan that scans records the whole collection (it shows the row count); ``key_at_position`` and
  ``position_of`` record the whole collection; an offset window records from the first key; ``keys(after=k)``
  also covers ``k``. Positions a transaction appended, and negative positions, take the whole list.
* **Plans decide units.** The same query declares a group or the whole collection depending on whether the
  planner chose the index.
* **Indexed collections are coarser than plain ones.** Indexed counters conflict on the removed element or
  key (above); on an indexed ``Set`` or ``CountedSet`` a ``has`` or ``count_of`` also conflicts with a
  concurrent swap of the stored member for an equal one of another exact form; an IVF-Flat index, a custom
  index that does not opt in, and an index stored in format 3 make every write take the whole collection.
* **Catch-up limits.** Index creation catches up at most 3 rounds, and only when the transaction did nothing
  else with the collection (no read, no other write); after that it conflicts and ``run`` retries. A catch-up
  over a diff the engine cannot express element by element (the diff reports the whole collection) rebuilds
  the new index from the newer collection: O(n) extractor calls, outside the lock.
* **Cost under the lock has fallbacks.** A changed root the engine cannot diff (absent on one side, of
  another class, a ``List`` that shrank, a set indexed on one side only) has every recorded unit of it
  checked, with the escalation limit as the bound. The candidate path reads nothing only while the atoms the pre-pass loaded stay in the
  atom cache; an eviction in between reloads them under the lock.
* **A pre-pass per commit.** A commit does its validation and reapplication twice when the root changed
  between the pre-pass and the lock (once on R0, once on R1); the extra work is outside the lock.
* **One-way format upgrade.** See below: software without this model cannot open a space this code wrote.

Out of scope: pessimistic locks in the core (build them on cells, above), stable per-element identity in
lists (it would change the persisted format), per-attribute merge of a record (R2), invariants over vector
queries (they record the whole collection), and parallel publication.

Compatibility
-------------

* **Format 7, one-way** (R50, R52, R63). Every root this code publishes is a version-7 root record (feature
  ``preconditions``), whatever it holds; segments keep the lowest sufficient version, so no record byte
  changes. A writer that predates the model would apply "the last writer wins" and break the guarantee for
  every other writer, so it must not write here: once a space holds a version-7 root, software without this
  model can neither read nor write it. Opening a space written by earlier 2.0 code (root versions 2 to 6)
  needs no migration: the content of collections and roots is unchanged, and the first commit marks it. protoBaseST
  cannot open a version-7 space until it ports the model (R62). See :doc:`format-spec`.
* **Protocol 16** (R51). Every node of a cluster validates the same way; nodes of older protocols are refused.
* **The intents mode is removed** (R53-R55). ``new_transaction(intents=True)`` and ``run(fn, intents=True)``
  validated per record attribute and replayed operations, computed attributes included, with the root held:
  per-attribute merge contradicts R2, and code under the root contradicts P-C. The semantic session has one
  path, whose unit is the cell. The catalog's ``Intent`` and ``IntentRecord`` types are kept; a leftover
  ``__intentLog`` root is still readable and is dropped by the next commit to that base.
* **API changes**: the ``CHANGELOG`` lists every one (``replace=`` removed, blind writes compared, removing a
  removed cell conflicts, ``root_names``/``mutable_names`` recorded, positional access, ``unique=``,
  impure extractors refused).

A note on storage compaction
----------------------------

``proto_db`` never deletes a version (D32, :doc:`objects-across-transactions`), and the model relies on it:
validation, the pre-pass's diff and the index catch-up read the **base** of every open transaction, and a
precondition's expected value names atoms of the base by pointer. Any future storage compaction must keep the
base of every open transaction readable, or end those transactions with a conflict first.

Free-threading
--------------

Collections, indexes and records are immutable and need no locks. Shared mutable state is guarded by locks:
the storage write buffer and segment writer, the atom and record caches, the publication of a lazy load, the
transaction's own bookkeeping (its read log has its own lock, so reads recorded from several branches never
race), and the space's root reference. ``ObjectSpace`` and ``Database`` are shared freely between threads. A
``Transaction`` is meant for one thread at a time; its internal lock serialises its methods and the semantic
operations on it (create, update, link, unlink and relate of :mod:`proto_db.semantic`, and the reads of a
semantic session over it), so a misuse from several threads sees each call happen entirely before or after
the others: a commit or an abort from another thread waits for a semantic operation in progress, and an
operation that waited for a commit fails before its first step. Iterating the members of a ``many`` end is
many calls, each one atomic, not one. Ending a transaction from inside one of its semantic operations on the
same thread (from a computed attribute, for example) is refused with ``ProtoUserError``. A group-commit leader
builds other threads' transactions under the root lock without taking their locks, so it adds no wait to that
order. The test suite runs on free-threaded CPython 3.14t and with the GIL.

Interrupts and signal handlers on Python 3.14
---------------------------------------------

On CPython 3.14, including the free-threaded build, an exception raised by a signal handler, such as
``KeyboardInterrupt`` from Ctrl-C, can arrive just after a ``with lock:`` statement has taken its lock and
before its protected block starts. ``__exit__`` then never runs, and the lock stays held for good. Up to
3.13 that window did not exist for a C-implemented lock (measured: no leak in 3,000 interrupts on 3.12 and
3.13; more than half of 3,000 on 3.14). It is a property of the interpreter, not of ``proto_db``, and code
in Python cannot close it.

``proto_db`` takes such locks while it loads, caches and saves objects. If the interrupt lands inside one,
the effect is a hang, not damage:

* a lock held by the interrupted thread blocks every other thread that needs it, and a non-reentrant
  lock blocks even that thread;
* the stored data is not at risk, because a commit is published by one root write, so an interrupted
  commit is either published whole or not at all.

The rule on 3.14: after a ``KeyboardInterrupt``, or any exception from a signal handler, that may have
interrupted ``proto_db`` code, do not keep using the ``ObjectSpace`` in that process; let the process end,
and open the space again in a new one. A long-running service that must survive Ctrl-C should handle the
signal by setting a flag that its threads check between transactions, rather than letting the exception
unwind through them. Python 3.12 and 3.13 are not affected. The upstream issue is CPython gh-148874: in
3.14 ``with`` calls ``__enter__`` as an ordinary call, so the pending signal check at the end of that call
runs outside the protected range. The fix (gh-150911) is in 3.15; as of November 2026 it has not been
backported to 3.14.
