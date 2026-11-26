# protoBase: specification of the precondition concurrency model

Source: the maintainer's specification (claude.ai artifact
`HiVd1i4RdPRgorsebiV2Pg`, revision 23, 2026-11-24), translated from Spanish.
The rule numbers (P-A..P-G, R1..R61, A1..A5) are the original ones.

## 1. Summary

The refactor replaces conflict detection per named root with a single model of
preconditions and intentions over fine-grained units: a dictionary key, a set
element, a list position, a mutable cell, or the whole object. It applies to
proto_db (Python, repository gamarino/protoBase) and then to protoBaseST
(Smalltalk, gamarino/protoBaseST).

Each operation of a transaction records, as data, what it saw of its base (the
precondition) and what change it wants to make (the intention). At commit, with
the space root held, the engine validates every precondition against the
current state and, if they all hold, reapplies the intentions on that state. If
any fails, the whole transaction fails with ProtoConflictError.

The resulting guarantee is serializability in commit order, with optimistic
control: reading neither blocks nor reserves anything.

### Assumed decisions, to be confirmed by the author

These five were not answered explicitly in the design discussion. The
specification takes the option shown; changing any of them affects only the
section cited.

| # | Topic | Option taken | Section |
| --- | --- | --- | --- |
| A1 | `intents=True` mode | Resolved by the author: the mode is removed | 10 |
| A2 | Pre-pass outside the lock | Included | 8 |
| A3 | Unique equality index | Included | 7 |
| A4 | Collection ordered by a user criterion | Dictionary with positional access; no new type | 5 |
| A5 | `append_last` on lists | No precondition | 5 |

### Instructions for the implementer

The numbered rules R<n> are normative. When this specification contradicts the
existing code or documents, this specification prevails.

If a rule turns out to be impossible or ambiguous during implementation, stop
and ask. Do not resolve it by analogy with the previous behavior.

Two areas of the code were not reviewed when this was written: the cluster
coordinator (`proto_db/cluster/`) and the vector index
(`proto_db/indexing/vector*.py`). The rules that touch them are marked "to be
verified".

## 2. Current state and what is replaced

Today the conflict unit is a database's named root, and the mutable cell. The
behavior is described in `docs/source/concurrency-model.rst` and in decisions
D9, D33, D34 and D35 of `docs/decisions.md`.

| Aspect | Current behavior | Where |
| --- | --- | --- |
| Reading a root | `get_root_object` records the token of the whole root in `_reads` | `Transaction.get_root_object` in `proto_db/transactions.py` |
| Reading a mutable | `get_mutable` records the cell's token in `_mreads` | `Transaction.get_mutable` |
| Validation | Under the root lock, each read token is compared with the current root | `Transaction._build_root`, `_check_mutables` |
| Root written without reading | Rebased by op-log if it is ConcurrentOptimized with a complete log; otherwise the last writer wins | `Transaction._merged`, `rebase_onto` in `proto_db/collections/base.py` |
| Mutable written without reading | The last writer wins; only the cell's existence is checked | `set_mutable`, `_mexpected` |
| Publication | Epoch CAS of the space root, with group commit or cluster rounds | `_commit_root`, `_standalone_round`, `_commit_under_lease` |
| Smalltalk | The same model, ported | `src/protobase/transactions.st`, `docs/deviations.md` |

### Problems that motivate the change

Reading a collection root in order to modify it puts the whole root in the read
set. Two transactions that touch different keys of the same dictionary
conflict.

Op-log merging applies only to collections created empty in the transaction
and written without reading. The same `set_root_object` call merges or replaces
depending on a runtime state (`log_is_complete`).

Blind writes to roots and mutables do not compare against the base: updates can
be lost.

Three mechanisms coexist (read set per root, op-log rebase, intent replay)
without a common definition of conflict.

### Decisions replaced

D9 (conflict implies retry; merge only for unread roots): the retry is kept;
the conditional merge disappears.

D33 (merge only values with a complete log): replaced entirely.
`log_is_complete` and the `replace=True` parameter disappear.

D34 (commit protocol and exact tokens): exact tokens and the handling of an
unknown outcome are kept; the validation and merge step changes.

D35 (mutables): the separate namespace is kept; blind writes now compare
against the base.

Add a new decision to `docs/decisions.md` that records this model and marks the
four above as amended, and rewrite `docs/source/concurrency-model.rst`.

## 3. Principles

They are normative and prevail over any particular rule.

**P-A. Correctness takes priority over concurrency.** When in doubt, an
operation takes the coarser conflict unit. An extra conflict is acceptable; a
result that no serial execution would produce is not.

**P-B. Closed set of operations.** Only the operations listed in sections 5
and 6 get fine-grained treatment. Every unlisted write has the whole root as
its unit. Every unlisted read records the whole collection as an invariant.

**P-C. With the space root held, only engine code runs.** Preconditions and
intentions are canonical values. They are evaluated with comparisons defined by
the format (`exact_token`, `order_key`), identical in Python and Smalltalk. No
extractor, key function, computed attribute or user callback runs in that
section.

**P-D. The core is optimistic.** Reading does not block, wait or reserve. The
only point of exclusion is the publication of the space root. Pessimistic
locks stay outside the core (section 13).

**P-E. Reading is declaring.** What a transaction read is considered necessary
for its logic and must still be the same at publication. What is not relevant
is not read.

**P-F. All or nothing.** All preconditions of all roots and mutables are
validated before any intention is applied. A failure publishes nothing.

**P-G. No change to the persisted format.** Preconditions, intentions and links
to the transaction are runtime state. The only persisted additions are the
protocol version mark (section 10) and an index's uniqueness attribute
(section 7).

## 4. Model

### Definitions

**Base.** The DatabaseRoot in force when the transaction opens. It does not
change during the transaction.

**Current.** The DatabaseRoot on which the commit is built: the last one
published or, within a group commit round, the one produced by the previous
commit of the round.

**Conflict unit.** The portion of state a precondition protects.

**Precondition.** A datum (unit, expected value). It holds if the unit's value
in the current state equals the expected one.

**Intention.** A datum describing the change to apply on the current state. It
belongs to the closed set of section 5.

**Absent.** A possible value of any unit. Creating is going from absent to a
value; deleting is the reverse.

### General rules

**R1. Classification of a root.** A root is a collection if its value is an
instance of Dictionary, HashDictionary, Set, CountedSet, RepeatedKeysDictionary
or List. In any other case it is an object.

**R2. Object root.** The unit is the whole object. There is no per-attribute
merge: two transactions that modify different attributes of the same object
conflict.

**R3. Mutable.** The unit is the cell, identified by its name, in its own
namespace. Different cells never conflict.

**R4. Collection root.** The unit depends on the operation (sections 5 and 6).

**R5. One level only.** Fine grain applies only to the collection that is the
direct value of a root. A collection contained in another collection, in an
object or in a mutable has no units of its own: modifying it is writing the
unit that contains it.

**R6. Equality.** Every precondition comparison uses the current exact token
(`proto_db.values.exact_token`): atoms by storage pointer, plain values by type
and exact content. An atom that is the direct value of the unit is compared by
pointer without loading it.

**R7. ABA by value.** If the current value equals the expected one, the
precondition holds even if there were intermediate changes. No version numbers
are added.

**R8. Fixed base.** The expected value of a unit is always the base's,
regardless of when the transaction touched it.

**R9. Collapse per unit.** Several operations of the transaction on the same
unit produce a single record: the precondition set by the first operation that
observed the base, and the final intention. An operation on a unit the
transaction already wrote reads its own value and adds no precondition.

**R10. Writing the base value.** A write that leaves the unit with the same
value as the base is still a write: it keeps its precondition.

**R11. Linked collection.** Fine-grained treatment requires that the value
written to a root derive, through recorded operations, from the base collection
of that same root (section 9). If it does not derive from it (a collection
created separately, built with `from_items` or `from_iterable`, obtained from
another root, from another transaction or from a Snapshot), the write is
treated as that of an object root.

**R12. Class change.** If the transaction recorded fine-grained operations on a
root and in the current state that root is absent or of another class, there is
a conflict.

**R13. Reading one's own view.** Inside the transaction, reads see its own
writes and never those of other transactions.

## 5. Write operations

The list is closed (P-B). "Whole collection" means: the current value of the
root equals the base value of the root (R6); if it holds, the transaction's
value is published as is, without reapplying.

### Roots and mutables

| Operation | Precondition | Intention |
| --- | --- | --- |
| `set_root_object(n, v)` with v not linked (R11) | `current[n] == base[n]`, absent included | Root n holds v |
| `remove_root_object(n)` | `current[n] == base[n]` | Root n becomes absent |
| `new_mutable(n, v)` | Cell n is absent | The cell holds v |
| `set_mutable(n, v)` | `current[n] == base[n]` | The cell holds v |
| `remove_mutable(n)` | `current[n] == base[n]` | The cell becomes absent |

Changes from today: a blind `set_mutable` on a cell another transaction
modified is a conflict (before, the last writer won). Deleting a cell another
transaction already deleted is a conflict (before, it was a no-op). Assigning a
new collection to an existing root compares against the base value and replaces
it (before, it was merged).

### Dictionary and HashDictionary

| Operation | Precondition | Intention |
| --- | --- | --- |
| `set_at(k, v)` | `current[k] == base[k]`, absent included | k holds v |
| `remove_at(k)` | `current[k] == base[k]`, absent included | k becomes absent |

Two transactions that insert the same new key conflict even if they write the
same value.

### Set

| Operation | Precondition | Intention |
| --- | --- | --- |
| `add(e)` | None | e becomes present |
| `remove(e)`, with e present in the base | e is still present | e becomes absent |
| `remove(e)`, with e absent in the base | e is still absent | None |
| `union(others)` | As one `add` per element | |
| `difference(others)` | As one `remove` per element | |
| `intersection(others)` | Whole collection | |

`add(e)` when another transaction removed e leaves e present. The third row
refines what was discussed: without that precondition, a remove that found
nothing would coexist with someone else's add and the result would not
correspond to any serial execution.

### CountedSet and RepeatedKeysDictionary

They keep D26's counter composition: concurrent increments and decrements are
composed per element, without a precondition. The current implementation
(`_net_ops`, `counter_net_effect`) is reused as the intention.

### List

| Operation | Precondition | Intention |
| --- | --- | --- |
| `append_last(v)`, `extend(vs)` | None | Append at the end, in commit order |
| `set_at(i, v)` | i exists in the current state and `current[i] == base[i]` | Position i holds v |
| `insert_at`, `remove_at`, `append_first` and every other structural operation | Whole collection | |

**R14.** As soon as a transaction records a structural operation on a list,
the whole list becomes the unit for that transaction. Indices are not
translated.

**R15.** `set_at(i)` uses base indices. A negative index is normalized against
the base length, which constitutes a read of the length (section 6).

**R16.** Insertions are not relocated. Anchoring the insertion to its neighbors
was evaluated and discarded as incorrect.

### Collection ordered by a user criterion

A list does not serve concurrent ordered insertions: each insertion takes the
whole list. The case is modeled with a Dictionary whose key is the ordering
key, computed by the user inside their transaction. The order is that of
`order_key`, defined by the format. For repeated keys a composite key
(ordering key, tie-breaker) is used.

**R17.** Add positional access to Dictionary: `key_at_position(i)` and
`position_of(k)`, implemented with the `count` field that KeyNode nodes already
persist. They are reads (section 6).

### Indexes

| Operation | Precondition |
| --- | --- |
| `add_index`, `remove_index` | Whole collection |

### Any other write

A mutating operation of a collection that does not appear in this section has
the whole collection as its unit (P-B).

## 6. Read operations

A read records a precondition without an intention (P-E). The list is closed:
a read not listed here records the whole collection (P-B). The model's
serializability depends on no read path being left unrecorded.

### Transaction

| Read | Precondition recorded |
| --- | --- |
| `get_root_object(n)`, root absent | n is still absent |
| `get_root_object(n)`, object root | `current[n] == base[n]` |
| `get_root_object(n)`, collection root | n is still present and of the same class. The content is not recorded |
| `root_names()` | The set of root names did not change |
| `get_mutable(n)` | `current[n] == base[n]`, absent included |
| `mutable_names()` | The set of cell names did not change |

`root_names()` and `mutable_names()` do not enter the read set today; they will
be recorded.

### Linked collection

| Read | Precondition recorded |
| --- | --- |
| Dictionary / HashDictionary: `get_at(k)`, `get(k)`, `has(k)` | `current[k] == base[k]`, absent included |
| Set: `has(e)` | The presence or absence of e did not change |
| CountedSet: `has(e)`, `count_of(e)` | The count of e did not change |
| RepeatedKeysDictionary: `get_at(k)`, `has(k)` | The set of records of k did not change |
| List: `get_at(i)` | i exists in the current state and `current[i] == base[i]` |
| Lookup by equality index with key x | The group of x in that index did not change (section 7) |
| `count`, `len`, iteration, `elements`, `items`, `keys`, `values`, `slice`, `head`, `tail`, `content_equals` | Whole collection |
| `key_at_position(i)`, `position_of(k)` | Whole collection |
| Query by range or vector index, query without an index, `as_query_plan`, paginators | Whole collection |
| `index_names`, `get_index` | The set of index definitions did not change |

### Rules

**R18. A read on a modified version.** A read made on a version the
transaction already modified records what it reveals about the base. Reading
`count` after an `add` reveals the base count: it records the whole collection.
Reading a unit the transaction wrote records nothing (R9).

**R19. A list with a structural operation.** By R14 the list is already the
whole unit; its later reads add nothing.

**R20. What is reachable from a read value is covered.** Values are immutable:
navigating the attributes of an object obtained with a recorded read needs no
further record. The exception is MutableObject: resolving one is a
`get_mutable` and is recorded.

**R21. Unrecorded reads.** The only reads that record nothing are those through
paths explicitly outside the transaction: `snapshot_mutable` (documented that
way today), a Snapshot from `Database.snapshot_at`, `Database.history()`, and
objects brought from another transaction. They are outside the guarantee and
the documentation must say so.

**R22. Upper layers.** The query module (`proto_db/query/`), the query cache,
the paginators and the semantic layer (`proto_db/semantic/`) read through the
operations of this section. Every point where they access tree nodes or
indexes directly today must be audited. A result served from the query cache
records the same as the query that produced it.

**R23. A transaction without writes.** It neither publishes nor validates: it
sees a prefix of the commit order and never fails with a conflict.

## 7. Indexes

An index is data derived from the collection's elements. The existing classes
are EqualityIndex, RangeIndex and VectorIndex (`proto_db/indexing/`). Today,
when a commit rebases a write on an indexed collection, the extractors run
inside the commit; that violates P-C and is removed.

**R24. Precomputed keys.** In the preparation phase, for each element the
transaction adds, removes or replaces in an indexed collection, its key in each
index of the base collection is computed with the extractor. The keys travel
in the intention as data. Under the root lock the engine adds and removes
entries with those keys, without invoking extractors.

**R25. Stable definitions.** Every transaction with intentions on an indexed
collection carries the precondition "the set of index definitions of the
current state equals the base's". If another transaction added or removed an
index, the precomputed keys do not cover it: conflict.

**R26. Definition changes.** `add_index` and `remove_index` have the whole
collection as their unit. The index is built in the preparation phase.

**R27. Pure extractors.** An element's key must depend only on the element. An
extractor cannot resolve a MutableObject, read a computed attribute that
depends on other state, or consult the transaction. During extraction, any read
through the transaction is rejected with ProtoUserError.

**R28. Invariant per equality-index key.** A lookup by EqualityIndex with key x
records the precondition "the group of elements with key x in that index is the
same in the current state as in the base". The comparison is on data: by
pointer if the group is an atom, or by the sequence of its elements' tokens.
What shape the group has in KeyGroupIndex is to be verified.

**R29. Unique index.** EqualityIndex accepts a persisted `unique` attribute. In
a collection with a unique index, every intention that adds or replaces an
element with key x automatically carries the precondition "the group of x in
the current state is empty, or contains only the element being replaced".
Inside the transaction, a violation against its own view is an immediate
ProtoValidationError; against a concurrent commit it is ProtoConflictError, and
the retry will see the duplicate and fail with the validation error.

**R30. Vector index.** It must be updatable under the lock from the
precomputed vector, without user code. To be verified in `vector.py` and
`vector_backends.py`. If that is not possible for some backend, writes to a
collection with that index take the whole collection.

### The "check and add" pattern

The documentation must explain it with this table:

| How existence is checked | Resulting conflict unit |
| --- | --- |
| By key or element (`get_at(k)`, `has(e)`) | That key or that element |
| By an equality index on the field | The group of that index key |
| With a declared unique index | Imposed by the engine, even if the application does not read first |
| By a traversal or a query without an equality index | The whole collection |

## 8. Commit protocol

The current structure of `Transaction._commit`, `_commit_root` and
`_build_root` is kept. The validation and the construction of the new root
change, and the pre-pass is added.

### Phases

1. **Close.** The transaction stops being active. If it has no intentions, it
   ends without publishing (R23).
2. **Preparation, without the space root:**
    1. Collapse the record per unit (R9).
    2. Compute the index keys of the affected elements (R24) and build the new
       indexes (R26).
    3. Persist every atom reachable from the written values.
3. **Pre-pass, without the space root:**
    1. Read the last published root, R0.
    2. Validate every precondition against R0. If any fails:
       ProtoConflictError, without having taken the lock.
    3. Reapply the intentions on R0, persist the new nodes and keep the result
       as the candidate.
4. **Publication, with the space root held** (root lock, or lease or permit in
   a cluster):
    1. Read the current root, R1.
    2. If R1 is the same root as R0 (same pointer), publish the candidate.
    3. Otherwise, validate every precondition against R1; if any fails,
       ProtoConflictError. If they hold, reapply the intentions on R1, persist
       and publish.
5. **After.** Mark the records of the written collections as consumed
   (section 9).

### Rules

**R31. A single pre-pass.** There is no iteration outside the lock. If the root
changed between the pre-pass and the publication, the work is repeated once
under the lock.

**R32. Group commit.** Within a round each commit is built on the result of the
previous one. The pre-pass candidate only serves the first commit of the round,
and only if R1 is R0.

**R33. Validation order.** First all preconditions of roots, collections,
indexes and mutables; then all intentions (P-F).

**R34. Reapplication.** An intention is applied on the current collection with
the same tree operations the transaction uses, plus the precomputed index
keys. A root with a whole-collection precondition is not reapplied: the
transaction's value is published.

**R35. Mutables.** The intentions are applied on the current state's map of
mutables, not on the base's.

**R36. Error.** ProtoConflictError names the unit that failed: base, root or
cell, unit class and key, element or position. `Database.run` retries as
today. ProtoOutcomeUnknownError does not change.

**R37. Reserved roots.** The roots of installed modules and of the semantic
catalog keep their own merge (`_merged_registry`, `_merged_semantic_catalog`).

**R38. Cluster.** Validation and reapplication happen on the node that builds
the commit within the round, with the same rules. To be verified against
`proto_db/cluster/coordinator.py` and `_commit_under_lease`.

**R39. Cost under the lock.** Bounded by O(operations × log n) comparisons and
path copies. The storage reads needed to walk the current tree are done, as
far as possible, in the pre-pass, which leaves the nodes in cache.

## 9. Collection binding and recording

This is the largest code change. Today collections are immutable atoms that do
not know which transaction they belong to; their reads leave no trace and
their op-log (`ConcurrentOptimized._op_log`) is complete only if the
collection was born empty in the transaction.

### Required design

**R40. Link.** `get_root_object(n)` on a collection root returns a linked
instance: it carries, as runtime state, the reference to the transaction, the
root's name and the pointer of the base collection.

**R41. Own instance.** The linked instance belongs to the transaction. The atom
cache (`_AtomCache`) can share one atom between transactions, so the link is
never written on the cached atom: a new instance is created with the same
persisted fields and the same pointer.

**R42. Inheritance.** Every version derived by a mutator inherits the link. Use
the existing mechanism of runtime fields (`_runtime_fields`, `_with`).

**R43. Writes: record per version.** The record of intentions travels in each
version, immutable, as `_op_log` does today. It cannot be shared state: the
user can derive two branches from the same base and assign only one. On
`set_root_object(n, v)`, the record that counts is v's.

**R44. Reads: record per transaction.** Reads are noted in the transaction,
under its lock, regardless of the branch they were made from. Recording too
much is safe.

**R45. Explicit assignment.** A collection's changes are published only if the
transaction assigns the final version with `set_root_object`. A derived and
unassigned version is discarded; its reads stay recorded.

**R46. Validity of the link.** v is linked for `set_root_object(n, v)` only if
its link names this transaction, active, and root n. In any other case R11
applies.

**R47. End of the link.** When the transaction ends, successfully or not, its
links stop being valid. A collection reused in another transaction is not
linked. This replaces the current "consumed log" rule.

**R48. List.** It is not ConcurrentOptimized today. It must carry a record for
`append_last`, `extend` and `set_at`, and the structural-operation mark (R14).

**R49. Indexes during reapplication.** The current IndexedCollection hooks
(`on_add`, `on_remove`, `on_replace`) invoke extractors. Reapplication needs a
variant of `Index.with_added`, `with_removed` and `with_replaced` that receives
the already computed key.

### What is retired

- `log_is_complete` and the `_log_complete` attribute.
- `rebase_onto` in its current form. It is replaced by precondition validation
  followed by the application of intentions (R34). The per-key net-effect logic
  (`net_effect`, `counter_net_effect`) is reused for R9.
- `Transaction._merged`, `_replaced` and the `replace` parameter of
  `set_root_object`.
- `_mexpected` and the distinction between blind and read writes on mutables.

## 10. Compatibility

### Version mark

Each process does the validation when it publishes. A writer running the
previous code on the same storage would keep applying "the last writer wins"
and would break the guarantee for everyone.

**R50.** A space operated with this model is marked so that the previous
software cannot write to it. Use the existing format-version mechanism
(`FORMAT_VERSION`, `WRITE_FORMAT_VERSION`, `FEATURE_VERSIONS` in
`proto_db/storage/format.py`): define a new version the previous code does not
read, and write it in the root record. The exact form is to be verified
against that module and `docs/source/format-spec.rst`.

**R51.** If the nodes of a cluster must agree on the validation rules, also
raise `PROTOCOL_VERSION`. To be verified.

**R52.** Opening a space written by the previous software with the new
software must work without data migration: the persisted content of
collections and roots does not change.

### API-visible changes

| Change | Effect for the user |
| --- | --- |
| `replace=True` is removed from `set_root_object` | Assigning an unlinked collection always replaces, comparing against the base |
| A new collection assigned to an existing root is no longer merged | Code that relied on merging blind writes must get the root and modify it |
| A blind `set_mutable` compares against the base | Conflicts appear where the last writer used to win |
| `remove_mutable` of a cell deleted by someone else | A conflict instead of a no-op |
| `root_names()` and `mutable_names()` record a read | Conflicts appear on concurrent creation or deletion of roots or cells |
| Reading a collection root and modifying one key | No longer collides with changes to other keys |
| `Dictionary.key_at_position`, `position_of` | New |
| `EqualityIndex(unique=True)` | New |
| Index extractors that read the transaction | ProtoUserError |

Record everything in `CHANGELOG.md`. The current version is 2.0.0rc4; since
these are incompatible behavior changes, they must go in before the final
2.0.0 or in a major version.

### The `intents=True` mode is removed

The intents mode (`proto_db/semantic/intents.py`) validates per cell attribute
and its replay runs computed attributes with the root held. It contradicts R2
and P-C, and the design audit discarded it.

**R53. Removal.** Remove the `intents` parameter from
`Database.new_transaction` and from Transaction, the intent log and its replay
in `_build_root`, the fine-grained reads per (cell, attribute), the
restrictions on raw reads and writes of cells touched by the log, and the
mode's tests and documentation. The semantic session is left with a single
path: its operations read and write cells with the operations of sections 5
and 6, and the unit is the cell (R3).

**R54. Scope of the removal.** "Intent" is also a catalog concept
(`proto_db.catalog.Intent`, `IntentRecord`, with type ids in
`proto_db/storage/format.py`). Only the replay mechanism at commit is removed.
Before deleting, list every use of the mode and of the catalog types and report
which ones depend on the replay. No catalog type is deleted and no type id is
reused without the author's decision.

**R55. Existing data.** A space containing a `__intentLog` root, or catalog
records written in intents mode, must keep opening and reading. A leftover
`__intentLog` root is discarded on the next commit to that base, as the
current code already does with the log of a previous commit.

protoBaseST has no intents mode, so this part needs no port.

## 11. protoBaseST

protoBaseST replicates proto_db's model and takes it as the reference. The
refactor is done first in Python and then ported.

**R56. Same rules.** Sections 3 to 9 apply unchanged. P-C is what makes it
possible for both implementations to decide alike: everything evaluated under
the lock is data and format comparisons.

**R57. Files affected.** `src/protobase/transactions.st` (read sets,
`_merged:onto:`, commit), `collections.st` (record, `rebaseOnto:`,
`isMergeable`), `indexes.st` (precomputed keys, unique index), `avl.st`
(positional access), `round.st` (group commit) and `cluster/` (R38).

**R58. Deviations that are kept.** PBRepeatedKeysDictionary stays read-only.
Execution of hnsw plans stays unsupported. Update `docs/deviations.md`: retire
the entries on merging blind writes and on the extractor that runs inside the
commit, which no longer apply.

**R59. Version mark.** protoBaseST must write and require the same mark (R50).
A protoBaseST version without this model must not be able to write to a
marked space.

**R60. Shared conformance vectors.** Add to `tests/conformance/` of both
repositories a set of concurrency scenarios as data. Each scenario has: base
state, operations of transaction A, operations of transaction B, commit order,
and expected result (final state, or the unit in conflict). Both
implementations run them and must agree.

**R61. Interoperability.** Extend `tests/interop/concurrency.sh` with crossed
cases: one transaction in Python and another in Smalltalk on the same space,
for each unit class.

## 12. Verification

The acceptance criterion is that everything below passes on CPython with the
GIL and on the free-threaded 3.14t variant, in standalone mode, with group
commit and in a cluster.

### Required tests

1. A pair of tests per row of the tables in sections 5 and 6. For each
   operation: one case where a concurrent change in another unit produces no
   conflict and the result contains both changes, and one case where a
   concurrent change in the same unit produces ProtoConflictError naming that
   unit.
2. Serializability checker. A randomized test with a fixed seed: several
   threads run randomly generated transactions on a small state, using only
   the closed set of operations. The commit order and each transaction's
   program are recorded. Then they are re-run serially, in commit order, on an
   in-memory model. The final state and every value each committed
   transaction read must match. Build on `tests/concurrency/harness.py`.
3. No user code under the lock. A test extractor that detects being invoked
   with the root held; it must never happen, including during a reapplication
   on an indexed collection.
4. Impure extractor. An extractor that reads the transaction fails with
   ProtoUserError.
5. Complete reads. For each path of R22 (queries, paginators, query cache,
   semantic layer): one transaction reads through that path, another modifies
   what was read, and the first fails at commit.
6. Reads on modified versions (R18) and divergent branches (R43): derive two
   branches, assign one, and check that only the intentions of the assigned
   one are published.
7. Unique index. Two transactions insert different keys with the same indexed
   value: only one commits.
8. Pre-pass. With the root unchanged, the phase under the lock does no storage
   reads and does not reapply. With the root changed, it validates and
   reapplies once.
9. Version mark. A writer that does not declare the new version is rejected.
10. Conformance and interoperability vectors (R60, R61).

### Existing tests to rewrite

They encode the behavior being replaced: the tests of merging blind writes and
of `replace=True` in `tests/unit/test_transactions.py`, the blind-write tests
in `tests/unit/test_mutable_object.py`, the collection contract based on
`rebase_model`, and the tests in `tests/concurrency/` that expect "the last
writer wins". Every test modified or deleted must be listed in the commit
message with the rule that motivates it. No test is deleted without a
replacement that covers the new rule.

### Documentation

Rewrite `docs/source/concurrency-model.rst` and the concurrency tutorial. Add
the new decision and the amendments to `docs/decisions.md`. Update
`docs/source/format-spec.rst` for R50 and R29.

## 13. Out of scope, known limits and order of work

### Out of scope

- Pessimistic locks. They are not part of the core (P-D). They can be built on
  top without modifying it: taking a lock is creating a mutable cell with an
  agreed name in a short transaction (it fails if it exists, by the absence
  precondition), and releasing it is deleting it. The core must include
  nothing that assumes them or prevents them.
- Stable per-element identity in lists, which would allow concurrent
  insertions: it changes the persisted format.
- Range invariants ("nothing changed between a and b") and vector-query
  invariants: they record the whole collection.
- Per-attribute merge of an object (R2).
- Changes to the catalog's Intent types (R54).
- Parallel publication. There is still a single ordering point per space.

### Known limits

They must appear in the model's documentation.

- Hot keys. Of N concurrent transactions on the same unit, one commits per
  attempt; the others redo their work. Commutative operations (set `add`,
  counters, `append_last`) do not have this limit.
- Traversals. A transaction that traverses or range-queries a collection and
  then writes collides with any concurrent change in it.
- No fairness. A long transaction that reads a heavily modified unit can
  exhaust its retries.
- Write throughput. Validation and reapplication run on a single thread per
  space; commit volume does not scale with cores.

### Suggested order of implementation

Each step leaves the suite green before moving on to the next.

1. Retire the intents mode (R53 to R55), which simplifies the commit path.
   Then mutables: compare against the base on every write (section 5).
2. Object roots: compare against the base; recording of `root_names()` and
   `mutable_names()`; removal of `replace` and `_merged`.
3. Collection binding and recording of reads and writes for Dictionary and
   HashDictionary (section 9), with validation and reapplication in
   `_build_root`.
4. Set, CountedSet and RepeatedKeysDictionary.
5. List.
6. Indexes: precomputed keys, pure extractors, equality-key invariant, unique
   index (section 7).
7. Audit of read paths in queries, paginators, cache and semantic layer (R22).
8. Serializability checker (section 12, item 2). It is best to have it from
   step 3 and keep extending it.
9. Pre-pass (section 8).
10. Cluster (R38) and version mark (R50, R51).
11. Documentation and decisions.
12. Port to protoBaseST, conformance vectors and interoperability
    (section 11).

## 14. Amendments approved by the author (2026-11-24)

The author confirmed A1-A5 and accepted every resolution of the
pre-implementation review (`2026-11-24-precondition-concurrency-model-review.md`,
items B1-B8, N1-N5 and its correction list) and of the PostgreSQL comparison
(M1-M4 below). Where an amendment and an earlier rule differ, the amendment
prevails. Rule numbers continue from R61.

### Scope

**R62. protoBaseST.** Section 11 (R56-R61) is deferred: protoBaseST is not
changed in this work. A proposal document presents the model to the
protoBaseST community (the FAST group) for feedback. R59 and R61 wait for that
port; the conformance scenarios of R60 are written in proto_db only.

### Version mark (B1)

**R63.** A root record written by this model has format version 7, the new
feature `preconditions` in `FEATURE_VERSIONS`. Every root this code publishes
is version 7, whatever its content. The upgrade is one-way: once a space holds
a version-7 root, software without this model can neither read nor write it.
R52 holds (this code reads versions 2-7 without migration). `PROTOCOL_VERSION`
rises to 16 (R51): every node of a cluster must apply the same validation.
protoBaseST cannot open a version-7 space until its port.

### Commit protocol (B2, M1)

**R64. Candidate.** The pre-pass candidate is the pair of reapplied maps
(objects and mutables). The DatabaseRoot and the SpaceRoot are always built
under the space root; in a cluster they carry the permission's stamp. The
module-import loop (`_IMPORT_ROUNDS`) is kept: it re-runs engine code, not
user code.

**R65. Delta validation.** Under the space root, validation does not re-check
every precondition. The pre-pass proved every precondition against R0, so a
precondition holds against R1 if and only if its unit did not change between
R0 and R1. The engine computes the changes from R0 to R1 by a structural diff
of the persistent trees that skips every pair of subtrees with the same
pointer, and fails if any changed unit is covered by a recorded precondition
or by a recorded range (R68). The cost under the lock is
O(changes since R0 × log n), independent of the size of the read set. Within a
group commit round, entries after the first diff R0 against the round's
current root. When there was no pre-pass (R0 unavailable), validation is the
full check against R1.

**R66. Escalation.** A transaction records at most
`PRECONDITION_ESCALATION_LIMIT = 1024` fine preconditions per collection root.
The next one replaces them all with a whole-collection precondition (P-A). The
limit bounds the memory of a read set, as PostgreSQL bounds its predicate
locks.

### Ranges (M4)

**R67.** Range invariants are in scope (they leave section 13's out-of-scope
list). A range is (collection root, source, low, high, low inclusive, high
inclusive), where the source is the Dictionary's own keys or a named
RangeIndex, and the bounds follow `order_key`.

**R68.** The reads that record a range instead of the whole collection:
a query plan answered by a RangeIndex range scan (the range of index keys it
scanned), and a window read of Dictionary keys (`keys(after=...)` and the
planner's ordered window): the range from its start to the last key it
returned, or to the end when it was exhausted. A range precondition fails when
any key (Dictionary) or index group key (RangeIndex) inside the range differs
between base and current; with R65 that is any changed key of the diff inside
the range. Unbounded and unordered reads still record the whole collection.

### Liveness of index creation (M3)

**R69. Catch-up.** A transaction whose only operations on a collection are
`add_index` or `remove_index`, and that recorded no other read of it, does not
fail because the collection changed. In the pre-pass, if the collection in R0
differs from the base, the engine computes the elements changed from the base
to R0 by diff, applies them to the index under construction (extractors run
here, outside the lock), and takes R0's collection as the expected value; it
repeats with the newer root at most `INDEX_CATCH_UP_ROUNDS = 3` times. Under
the lock the whole-collection precondition compares against the last expected
value; if it fails, ProtoConflictError, and `Database.run` retries.

### Documentation additions (M2 and the comparison)

**R70.** The model's documentation must also cover: identifiers generated
without a shared counter (UUID or ULID; a counter cell is a hot key, and a
non-transactional sequence is out of scope); the "verify reference" pattern
beside "verify and add"; reading from a `Snapshot` as the deliberate escape
from the guarantee (the analogue of READ COMMITTED); the rule that a function
given to `Database.run` may run several times, so external effects go after
the commit; lock cells (section 13) carrying an owner and an expiry, since a
crashed owner never deletes its cell; and that any future storage compaction
must keep the base of every open transaction readable.

## 15. Amendments recorded during implementation (2026-11-25)

The implementation (plan `docs/superpowers/plans/2026-11-24-precondition-concurrency-model.md`) met six
points where a rule's text was silent or, applied literally, contradicted a principle. Each was decided by
a controller ruling on the principle that prevails (section 3: principles prevail over any particular rule)
and is recorded in decision D73 of `docs/decisions.md`. They are reported to the author as amendments; where
one differs from an earlier rule, it prevails.

**A-1. List positions appended by the transaction (amends R9 and R16 for List).** A `get_at(i)` or
`set_at(i, v)` with `i` at or past the base length (a position the transaction appended), like a negative
`i`, records the whole list: the position names "my append" only if the length is what the transaction
assumes, which is a read of the length. The fold of R9 is not serializable: base `[1, 2, 3]`; A appends 7
and sets position 3 to 8; B appends 9 and commits first; the fold publishes `[1, 2, 3, 9, 8]`, while the
serial orders give `[1, 2, 3, 8, 7]` and `[1, 2, 3, 8, 9]`. The randomized checker found it. P-A.

**A-2. Offset windows (amends R68).** A planner window by offset (`take`/`skip` over a Dictionary) records
the range from the collection's first key to the window's last key: the positions depend on every earlier
key.

**A-3. Indexed D26 types (amends section 5, CountedSet and RepeatedKeysDictionary, for indexed
collections).** On a collection that carries an index, a removal whose reapplication would take out a
member the preparation could not key records a declared fine unit at the write: RECORDS of the key for a
RepeatedKeysDictionary `remove_at`/`remove_key`/`remove_record`, COUNT of the element (token: the exact
member and its count) for a CountedSet `remove`, and ELEMENT from the base for an indexed `Set.remove`, also
after the transaction's own write of that element. A concurrent commit may have replaced the stored member
by an equal one of another exact form (`10` by `10.0`) whose key was never computed, and P-C forbids
computing it under the lock. Consequence: indexed counters conflict on the same element or key;
unindexed ones keep D26 composition.

**A-4. `position_of` of an absent key (completes R17).** `Dictionary.position_of(k)` raises
`ProtoValidationError` when `k` is absent, as `List.index` does for a missing value.

**A-5. Unique index under catch-up (completes R29 for R69).** A concurrent duplicate that the `add_index`
catch-up finds on a unique index is `ProtoConflictError` on the whole collection; `Database.run` retries,
and the retry fails with `ProtoValidationError`, as R29 prescribes for a concurrent duplicate. The
catch-up applies every removal before any addition, so a key swap is not a spurious duplicate.

**A-6. Group-commit candidate (refines R32).** The pre-pass candidate is used whenever the current root is
still R0 by pointer, including a later entry of a round after a first entry that published nothing:
pointer identity of R1 and R0 is the exact condition R32 protects.
