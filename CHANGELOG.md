# Changelog

All notable changes to `proto_db` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

**The precondition concurrency model** (decision D73; specification
`docs/superpowers/specs/2026-11-24-precondition-concurrency-model-design.md`; reference page
`docs/source/concurrency-model.rst`; tutorial `docs/source/tutorials/concurrency.rst`). **Behaviour-incompatible**:
it goes in before 2.0.0 final, in 2.0.0rc5. Conflict detection per named root, the op-log merge of unread
collections and the intents mode are replaced by one model: every operation records what it saw of its base (a
precondition over a fine unit: a key, an element, a count, a position, a cell, a root, a whole collection, an index
group or a key range) and what it changes (an intention); at commit every precondition must hold against the
current state, then the intentions are reapplied on it. Committed transactions are serializable in commit order.

- **API-visible changes** (spec section 10):

  | Change | Effect for the user |
  | --- | --- |
  | `replace=True` is removed from `set_root_object` | Assigning an unlinked collection always replaces, comparing against the base (`TypeError` for the keyword) |
  | A new collection assigned to an existing root is no longer merged | Code that relied on merging blind writes must get the root and modify it |
  | A blind `set_mutable` compares against the base | Conflicts appear where the last writer used to win |
  | `remove_mutable` of a cell deleted by someone else | A conflict instead of a no-op |
  | `root_names()` and `mutable_names()` record a read | Conflicts appear on concurrent creation or deletion of roots or cells |
  | Reading a collection root and modifying one key | No longer collides with changes to other keys |
  | `Dictionary.key_at_position(i)`, `Dictionary.position_of(k)` | New: positional access in O(log n); both record the whole collection; `position_of` of an absent key raises `ProtoValidationError` |
  | `EqualityIndex(name, key, unique=True)` | New: at most one element per key; own-view duplicate `ProtoValidationError`, concurrent duplicate `ProtoConflictError` (the retry then fails with `ProtoValidationError`) |
  | Index extractors that read the transaction | `ProtoUserError` |

- **Every conflict names its unit**: `ProtoConflictError.unit` (a `proto_db.preconditions.Unit`: kind, root, key,
  index) and `ProtoConflictError.database`. `Database.run` retries as before.
- **Blind writes of roots are compared too**: `set_root_object` of a value (an integer, a record, an unlinked
  collection) and `remove_root_object` compare the root with its base value, absent included; two creators of
  one absent root conflict; `remove_root_object` of a root absent in the base conflicts with a concurrent
  creator.
- **Linked collections**: `get_root_object` on a collection root returns the transaction's own linked copy;
  its versions record their reads and intentions, and only a version assigned back to the same root of the same
  transaction publishes them. A linked collection stored anywhere else (another root, a mutable cell, an object
  attribute, inside another collection) depends on its whole source root: any concurrent change of that root is
  a conflict. Reading a List root through a transaction now loads its root atom, as it already did for the other
  collections.
- **Collections**: Dictionary and HashDictionary keys, Set elements (`add` has no precondition), CountedSet and
  RepeatedKeysDictionary counts (composed, D26), List appends in commit order and `set_at` of base positions.
  Structural List operations, and reads or writes of positions the transaction appended or negative positions,
  take the whole list. On an indexed collection, removals of CountedSet elements and RepeatedKeysDictionary keys
  record their element or key, so indexed counters conflict on the same element or key.
- **Ranges** (R67, R68): a query answered by a `RangeIndex` range scan and `Dictionary.keys(after=k)` record a
  key range instead of the whole collection; a planner window records the range from the first key; unbounded and
  unordered reads still record the whole collection. Equality-index lookups record the group of their key.
- **Indexes under the model**: index keys are computed before the commit takes the space root and carried as
  data; no extractor runs with the root held. IVF-Flat vector indexes, custom indexes that do not opt in
  (`_keyed_reapplication`) and indexes stored in format 3 make every write take the whole collection.
- **Index creation catches up** (R69): a transaction that only adds or removes an index, and read nothing else
  of the collection, is no longer refused because the collection changed meanwhile: the pre-pass applies the
  changes to the new index, at most 3 rounds (`INDEX_CATCH_UP_ROUNDS`), then a conflict that `run` retries.
- **Commit protocol**: a preparation phase and a pre-pass against the last published root run before the space
  root is taken; under it, an unchanged root (by pointer) publishes the pre-pass candidate, otherwise only the
  changes since the pre-pass are validated (delta validation, R65) and the intentions reapplied once.
- **Escalation** (R66): more than 1024 fine preconditions or ranges on one collection root
  (`PRECONDITION_ESCALATION_LIMIT`) become a whole-collection precondition.
- **Format 7, one-way** (feature `preconditions`): every root this code publishes is version 7; segments keep the
  lowest sufficient version (no record byte changes). Software without this model can neither read nor write a
  space that holds a version-7 root; spaces of root versions 2-6 open without migration and are marked by their
  next commit. protoBaseST cannot open a version-7 space until it ports the model.
- **Cluster protocol 16**: every node validates the same way; validation and reapplication run on the building
  node under its write permission.
- **The intents mode is removed** (R53-R55): `new_transaction(intents=...)` and `run(fn, intents=...)` raise
  `TypeError`. The catalog types `Intent` and `IntentRecord` are kept; a leftover `__intentLog` root still opens
  and is dropped by the next commit to that base.
- **Known limits** (documented in the concurrency model): hot units, traversals, no fairness among retries,
  single-threaded publication per space, escalation, catch-up rounds, coarser indexed collections, the one-way
  format upgrade. No performance figure of `docs/source/performance.rst` was re-measured under this model.

**The semantic model (MVP)**: a database that describes itself (decisions D63-D72; design
`docs/superpowers/specs/2026-11-15-semantic-model-mvp-design.md`; demo `docs/kit/semantic/README.md`). Additive:
code that uses no semantic layer keeps today's API, bytes and behaviour.

- **Format 6, feature `catalog`** (format spec, "Semantic catalog (format 6)"): eleven built-in types, aliases 24
  to 34, for the roots `__semanticCatalog` (the composed catalog, written when a module is installed and verified
  on use) and `__intentLog` (readable; no longer written, since the intents mode is removed, above). A record
  of one of them is version 6; data that uses none of them keeps version 4 or
  5, byte for byte. A reader of format 5 refuses a format-6 segment or root (`ProtoNotSupportedError`), never
  misreads it. Catalog and expression vectors join the conformance vectors.
- **Cluster protocol 15**, raised with format 6: a node of protocol 14 does not know the catalog's aliases and
  would write a version-4 header over a segment opened by a catalog record. Every node of a cluster runs the same
  protocol version.
- **`proto_db.semantic`**: entities declared as classes (`Field`, `Enum`, `Computed`, `Aggregate`, `Relation`,
  `RelationTo`, `Role`, `Relationship`, `Extension`, `Invariant`, `intent`, `Certified`) in registry modules;
  composition with provenance, the same catalog in any installation order; a neutral expression language;
  relations with inverses navigable from both ends, relationships with data, roles bound by version, maintained
  keys and aggregates, `index_by`; the semantic session `semantic(transaction or snapshot)` with `describe`,
  `query` and atomic operations. A query term naming an attribute the members' type does not declare is refused
  before anything is read, never read as missing; a row the catalog does not type, and every row over stored
  structure alone, is read by what it stores (an absent attribute is refused).
- **Reserved class attributes**: a `DBObject` subclass whose own body defines `__meaning__`, `__label__`,
  `__help__`, `__synonyms__`, `__key__`, `__certification__`, `__constraints__` or `__validity__` is a semantic
  class and needs a registry module; rc4 accepted such a class as a plain one, and now its creation is refused
  ("declares no registry module"). Python reserves `__*__` names; no other name changes meaning.
- **Semantic writes read the catalog**: a transaction's first semantic write enters
  `__currentModules`, `__catalog` and `__semanticCatalog` in its read set, so a concurrent install makes its
  commit conflict and `Database.run` retries it against the new catalog (its writes were checked against the
  catalog of its snapshot). A transaction that only reads through the session is unaffected.
- **Catalog evolution over existing entities**: a maintained aggregate a later module adds to a type with
  entities reads as computed from the relation's current members, and the first write that moves it stores it,
  seeded by that computation. A `required` field or an enforced invariant a later module adds is **not**
  checked against the entities that exist at install: it binds the operations after the install as everywhere
  (a `required` field at creation, an invariant when an operation changes its operands). Migrations are
  designed, not built.
- **Constraints**: role cardinality and enforced invariants are checked on the combined result; a violation
  raises the new `ProtoConstraintError` (a `ProtoValidationError`, never retried), which carries the rule's name,
  meaning and rendered message (`describe()`). An enforced invariant may read only its own entity.
- **A semantic object outside a registry**: an attribute it does not hold resolves through the registry of the
  database it was read from; with none current, `getattr(o, "x", None)` raises `ProtoNotSupportedError` instead
  of returning `None` (its own fields read normally).
- **`proto_db.agent`**: the agent interface, operations `list_entities`, `describe`, `check_answerable` and
  `query` over one version, JSON-ready. Personal and secret attributes are listed by `describe` and never
  returned by `query`, nor disclosed through an error or tested by a `where`, also through a path that crosses
  a record the catalog does not describe (on the safe side: an uncatalogued attribute sharing the name of a
  catalogued personal one is withheld too). A role or a cell written outside the semantic layer that holds
  another type than the catalog declares refuses every agent query that reaches it (`ProtoCorruptionError`,
  content-free), never serves or tests what it holds. A raw mode exposes the stored structure only (the agent
  experiment's control group: it sees the stored type and attribute names, not their meaning); it withholds
  what the stored catalog marks personal or secret without needing the modules' code, and a catalog it cannot
  decode or check makes it refuse, never serve everything. Sensitivity is today's policy: a query with `as_of`
  withholds what the version read or the current version withholds. Agents never write.
- **Agent queries refuse the two silent empty answers** (spec section 11, R283): in both modes, a comparison
  the agent wrote between present values of different kinds (a date with a text, a number with a text) and a
  `root` or `cell` origin absent from the version read raise `ProtoValidationError` (naming the kinds, or the
  origin) where the expression language answers false or no members. Int and decimal stay comparable; MISSING and
  nil keep their meaning; origins are checked on the query before evaluation. Constraints, derived fields (also
  those read during an agent query), maintained aggregates, the session's own `query` and the conformance vectors
  are unchanged.
  `python -m proto_db.agent --path P --database D [--raw] [--log FILE]` serves the operations as MCP tools
  over stdio; the operator's log (the original messages the agent is not told) goes to `--log FILE` or
  nowhere, never to the server's stderr, which the MCP client collects. `python -m proto_db.agent.step6`
  writes the expected output of the protoBaseST step.
- **New optional extra `agent`**: `pip install proto_db[agent]` installs the MCP SDK, pinned
  `mcp>=1.30.0,<2` (the tested 1.x line). It is imported only when the server starts; `import proto_db.agent`
  needs nothing beyond the core.
- New public read accessors: `iter_root_names(after=)` and `iter_mutable_names(after=)` on `Snapshot` and
  `HistoryEntry`, and `Dictionary.keys(after=)`: name-ordered cursors that never sort every name.
- The demo (`docs/kit/semantic/semantic_demo.py`: six steps, one command, checking itself), the concurrency
  experiment (`benchmarks/semantic/credit_experiment.py`, in `docs/source/performance.rst`) and the agent
  experiment's harness and questions (`benchmarks/semantic/agent_experiment.py`; results pending,
  `docs/kit/semantic/agent-experiment.md`).

### Fixed (defects of 2.0.0rc4; candidates for a backport)

- **Concurrent installs in one space**: an older registry rebind landing after a newer one left the database
  bound to an older module list, so the next transaction failed with a registry mismatch. The database's binding
  now moves only forward in the order of its versions, so concurrent installs never move it back.
- **Publish-then-rebind window**: a transaction that started between an install's publish and its rebind got a
  registry mismatch, depending on timing. The space adopts the list being committed under the root lock before
  the publish, and a transaction whose snapshot lists an adopted list binds to it (design guide, "The runtime
  registry").

## [2.0.0rc4] - 2026-11-15

- A read on a cluster owner after its term lease expired, but before the lease was released, no longer asks its
  peers with a timeout of 0 s: only a read inside a commit is capped, at what remains of the commit's write
  permission and of any lease the node holds (on any node, also one that holds no term lease); every other read waits the full `fetch_timeout`.
  A read of a record whose segment the object store does not hold yet, while some peer did not answer the page
  request in time, now raises `ProtoUnexpectedError` (the record is unavailable) instead of `ProtoCorruptionError`;
  `ProtoCorruptionError` stays for a record that every peer answered it lacks (each peer counted once, however
  often its reply arrives; a peer that answered corrupt bytes, one with no storage attached, one whose space
  closed while it served the request, or one still attaching its storage (for a commit query), counts as not
  heard, and a corrupt answer no longer ends the page request: the members not heard yet are asked again while
  the wait lasts), or
  whose stored copy is wrong. The cases of a member that cannot tell travel as a new optional reply field,
  `unknown`, on `fetched` and `commit_info` (part of protocol 14). A
  commit whose base read fails with `ProtoUnexpectedError` after its write permission expired, nothing having
  been sent and its local tier not failed, raises `ProtoConflictError`, which `Database.run` retries. A failed
  read of the base a round must first re-commit no longer ends the permission with an unknown outcome, since
  nothing was sent (D46 amendment, rulings R212, R230-R233, R234L-R236L, R240L-R242L and R243L-R245L).
- A cluster owner whose lease renewal was refused by a majority no longer renews again once per round trip
  until its lease ends (GitHub issue #1). When a majority refused the term for good (it promised or granted a
  higher term), the owner stops renewing, refuses its queued permission requests with `ending`, lets an
  outstanding permission end, and releases its lease (new stat `terms_fenced`); after any other renewal that
  fails, including one a silent member leaves unanswered, it waits for the refusals' `remaining`, at least
  `request_timeout / 4`, before renewing again (D50 amendment, rulings R149 and R149a).
- **The runtime registry** (format 5; decision D62; tutorial `docs/source/tutorials/extensible-models.rst`, design
  chapter `docs/source/design-guide/registry.rst`). Additive: a `DBObject` without parents, in a database that
  never installs a module, keeps today's API, bytes and behaviour, with one breaking change for code that
  stored a field named `parents` (below).
  - `DBObject` gains parents (`DBObject(parents=(...))`, `o.parents`, `o.with_parents(...)`): other `DBObject`s or
    registry names (non-empty `str`), persisted as the atom envelope key `"p"`, the format-5 feature `parents`; an
    object without parents writes the format-4 record byte for byte. `parents` and `with_parents` are no longer
    field names. A record written before that holds a field of either name still loads, keeps the field
    readable (`o["parents"]`) and `with_` carries it over unchanged: it is never refused (ruling R157).
  - **Breaking change: the keyword `parents=` now sets resolution parents.** `DBObject(parents=...)` and
    `tr.new_object(parents=...)` used to store a field named `parents`; they now set the object's parents
    (ruling R158). A tuple or list of `DBObject`s and non-empty strings is accepted silently: the new object then
    inherits every attribute it does not set from those objects (`kid.surname` returns the mother's), has no
    field `parents` (`has_defined_attr("parents")` is False), and an index on `parents` finds nothing. Any other
    value (a single string or object, a dict, a list holding numbers) is refused with `ProtoValidationError`, as
    are `o.with_(parents=...)` and the keyword `with_parents=` in the constructor and in `with_`. **Migration:**
    before upgrading, rename such a field in the code that writes it (for example to `parent_records`); records
    already stored keep their field and load unchanged (R157), and `o["parents"]` reads it.
  - Resolution (`proto_db.resolution`): `o.x` and `o["x"]` read own attributes, then the C3 linearization of the
    parents, registry names included (their extension chains, newest module first); a miss is still `None`
    (R150), and `"x" in o` tells an inherited or explicit `None` from a miss. `o["oid"]`, `o["with_"]` and the
    other API names raise `ProtoAttributeNameError` (new, exported from `proto_db`), a `ProtoValidationError`
    that is also a `KeyError`, so a mapping-first consumer such as a Django template (`{{ obj.oid }}` tries
    `obj["oid"]` first) falls back to attribute access as it did when `DBObject` was not subscriptable. `DBObject` now defines `__getitem__`, `__contains__`,
    `__setitem__` and `__delitem__` (both raise `ProtoUserError`), and sets `__iter__ = None` (it stays
    non-iterable). A mixin listed after `DBObject` in a subclass's bases no longer provides any of these, and
    `%`-formatting with a `DBObject` as the mapping (`"%(n)s" % o`) now formats instead of raising `TypeError`.
    `has_defined_attr`, index keys and query paths read own attributes only, as before.
  - Modules (`proto_db.registry`): `module(name, depends=...)` and `@handle.extends(registry_name, parents=...)`
    declare extensions whose plain functions are methods and whose other attributes are canonical data
    defaults; a property, staticmethod, classmethod, callable instance, lambda, nested class or a default holding
    a database object is refused at import (`ProtoValidationError`). `self._super()` reaches the next definition
    with the same `self` (a miss is `None`; refused outside a registry method and in generators, allowed in
    coroutine methods). A method wrapped by a `functools.wraps` decorator is indexed by the function it wraps, so
    one decorator may wrap many methods and `_super()` works in the wrapped body. A `DBObject` subclass whose
    Python member hides a definition of a registry name its objects reach is refused (`ProtoValidationError`) at
    the first resolution of such an object through that registry, including `name in o` and `o[name]` of the
    member; plain attribute access of the member involves no resolution. A registry name no module
    extends is an empty node; `Registry.unknown_names` lists such
    names. Registry module names are process-global (one name, one Python module per process). A lookup made by a
    module's import-time code through the registry of a database that lists that module raises
    `ModulesBeingImported` (a `ProtoUserError`) and caches nothing; an import-time commit still succeeds.
  - `Transaction.install_module`, `installed_modules`, `registry`, `catalog_object` and `set_catalog_object`
    (`installed_modules` and `catalog_object` never enter the read set; `get_root_object` of a registry root
    does, so a concurrent install makes that transaction conflict);
    the roots `__currentModules` and `__catalog` (format-5 feature `registry`, built-in type aliases 22 and 23),
    merged add-if-absent at commit, so concurrent installs never conflict with each other. Installing is
    permanent. The module `base` (`proto_db.modules.base`, with `read_data_file`) is installed first,
    implicitly, and is the last parent of every registry name. A persisted list that does not start with `base`
    (another writer's) is read as if `base` preceded it, and the next install writes it with `base` first;
    `format-spec.rst` states it normatively (writers SHOULD put `base` first, readers MUST treat it as first). Catalog updates follow normal conflict rules
    (the first updater wins) and reach only objects created after them.
  - The load order is a function of the set of installed modules (rulings R253-R257): `base` first, then each
    module after the modules it depends on, ties broken by module name in code-point order (no locale). The
    persisted list's order is the install history only, so two databases that installed the same modules in
    different orders, or whose concurrent installs committed in either order, compose the same registry (same
    chains, parents unions, linearizations and results); `format-spec.rst` states it normatively (MUST). The
    process cache shares one registry per set of modules, and a space's fallback treats two orders of one set as
    one list.
  - `ObjectSpace(modules_mapping=..., modules_allowed=...)` and `ObjectSpace.reload_registry(database_name)`,
    which invalidates every registry of that database cached in the space (later snapshots and installing
    transactions build fresh too). When
    another process installs a module, a space bound to the older list raises `ProtoValidationError` at the
    next registry lookup inside one of its transactions on the new list (no automatic rebuild; a snapshot
    resolves with its own version's list and never mismatches).
    A loaded object resolved outside any block keeps using the space's bound list until a transaction of the
    space has seen the new list, and then raises the same mismatch. A listed module that cannot be imported
    raises `ProtoNotSupportedError` at the first lookup that needs it, and is remembered until
    `reload_registry`; a failed reload is not remembered, so the next lookup tries the import once more. As-of
    reads (`with snapshot:`) resolve with the modules of their version. A transaction or reader of a database that
    lists no modules resolves with no registry (a lookup that reaches a registry name raises "needs a
    registry"), never through another database's modules; the space's single-list fallback applies only when
    no reader of the space is current.
  - Format: `FORMAT_VERSION` 5 (`FEATURE_VERSIONS`: `parents`, `registry`); a space that uses neither keeps
    writing version 4, and a reader of format 4 refuses version-5 segments and roots with
    `ProtoNotSupportedError`. The cluster `PROTOCOL_VERSION` is 14, and an acceptor log that holds a root of
    format 5 has log format 3. `remove_root_object` refuses `__` roots, which only the registry writes. The
    conformance vectors gained `parents/` and `registry/`; `format-spec.rst` publishes the additions, and states
    that symbol references were measured and deferred (`docs/superpowers/2026-11-14-symbol-measurement.md`).
- `proto_db.__version__` reports the package version; in 2.0.0rc3 it still said `2.0.0.dev0`.
- `CloudStorage.publish_root` reports a write refused by its precondition as `ProtoConflictError`, which
  `Database.run` retries, when the object store declares that its refusals are definitive
  (`refusals_are_definitive`: nothing below it re-sends a conditional write). `GCSObjectStore` declares it with any
  client (its writes pass `retry=None`); `S3ObjectStore` declares it with the client it builds, and with a
  client passed in only when that client is configured for one attempt in total. Before, such a refusal was reported as `ProtoOutcomeUnknownError` when
  another instance had published more than `root_history` roots before the read-back, although nothing of
  ours could have been applied (seen as an intermittent CI failure of two instances sharing a counter). Other
  stores keep the unknown outcome, and the read-back still runs first, so a write that did land is reported as
  applied (D43 amendment, ruling R147a).
- A cluster commit that started while the node was ending a write permission kept for a retry (D50 "held
  retries": its hold timer expired, or `Database.run` returned instead of retrying) no longer fails with
  `ProtoUserError("Node ... already holds a write permission")`, a misuse error that `Database.run` does not
  retry. The commit now waits until that permission is finished and then asks for a new one; the wait counts
  against the permit timeout and ends, like the permit's own wait, with `ProtoTimeoutError` (ruling R148).
  `ClusterCoordinator.acquire_timeout` exposes the default permit timeout.
- `S3ObjectStore` builds its client with one attempt in total (`retries={"total_max_attempts": 1}`). botocore
  reads the former `max_attempts=1` as one retry, so a conditional `PutObject` could be sent twice.
- Documented a Python 3.14 hazard (concurrency model, "Interrupts and signal handlers on Python 3.14"): an
  exception from a signal handler, such as `KeyboardInterrupt`, can leave a `proto_db` lock held, so a
  process interrupted inside `proto_db` code should end rather than keep using the space.
- Tests: pytest-timeout ends a run that overruns by stopping the process (`timeout_method = "thread"`) rather
  than raising in the main thread, which on 3.14 could leak a lock and left test threads running into the
  next tests (CI-314); `test_pager_stability` stops its threads and bounds its writers.
- Tests: the two pipelined-permit tests that bounded the abandoned terms
  (`test_permits_overlap_the_predecessors_durability`, `test_p5_a_proposal_base_is_never_re_proposed`) no
  longer count an abandonment that timing explains: an owner timer (R82), or rule O5 after a send-reserve
  refusal (R121) of that very permission, which a permit that waited in the owner's queue gets under CPU load.
  Every other abandonment still fails them; plant `o5_none_unrefused` checks it.
- Tests: the P1 power-loss schedules of `test_cluster_pipelined_permits` no longer report an unavailable read
  as lost history. Under CPU load a read after the power cut failed with `ProtoUnexpectedError` ("store
  unreachable") while two members held the record in their local files: the peer fetch timed out
  (`fetch_timeout`, 75 ms there) and the schedule keeps the object store in outage. Reads failing with exactly
  that type are now retried for up to 15 s (scaled in stress mode); a record that no member holds still fails
  them (plant `p1_catch_up`). Every read check also compares the length of `history()` with the commits
  acknowledged before it (plant `history_truncated`). A schedule that did not form is rebuilt in a fresh
  cluster: A's round ended before it proposed; A's term ended after the term's first root, or k opened a new
  term (`False` cases: the voters then hold an older term's root, the `True` schedule, which the planted V2 rule
  loses; the product's V2' refuses k+1 there); B's permission was not issued on A's proposal; B's round sent
  nothing a voter may accept; or B, whose permission on k had run out, was acknowledged on another base. The
  records-nowhere scenario is rebuilt when n2's term ended right after its commit, when A's proposal never
  reached n2, or when B committed under a later permission, and it now runs with a 1.8 s lease, so that the
  owner's delayed permit still arrives valid. Every run that reaches the power cut still goes through the reads
  and their checks, a rebuilt one included (plants `p1_catch_up_missed`, `p1_conflict_after_root_stage`,
  `nowhere_false_ack`). A schedule that never forms raises a setup error, which the strict xfails of the planted
  cases no longer accept: they expect the read check's failure only (plant `p1_setup_never_forms`). The `False`
  cases wait until every voter holds the term's first root, the premise of their claim. No product change.
- Tests: the two compaction checks of `test_owner_locks` held on ext4, where a freed inode number is
  reused; they now keep the original file open and compare with `os.path.samestat`.

## [2.0.0rc3] - 2026-11-13

The first release candidate published on PyPI.
- Python 3.12 is the minimum (D61): on 3.11, comparing deeply nested tuples counts against the recursion
  limit, so a value near `MAX_VALUE_DEPTH` could raise a bare `RecursionError`. `pip` refuses to
  install it on 3.11 (`requires-python >=3.12`).
- Test suite: process-wide object counts replaced by per-test counts, a stress mode for the cluster tests
  (`PROTO_STRESS`, safety asserts only), and fixes to tests that failed in a clean export or under load.
- CI: a test matrix over 3.12, 3.13, 3.14 and 3.14t, and a `sphinx-build -W` documentation job.
- Benchmarks: a 10^7-object inventory and the periodic cluster stress task, with result files under
  `benchmarks/results/`.

## [2.0.0] - unreleased

`proto_db` 2.0 is a complete rebuild of the 0.1 code base under the design principles of
`docs/design-principles.md`: everything stored is immutable, a transaction is an explicit object, and
every safety argument holds under arbitrary delay and loss. There are no 0.1 data or API to preserve.
The design is `docs/superpowers/specs/2026-11-01-protodb-v2-design.md`; every decision is in
`docs/decisions.md` (D1-D62).

### Added
- **One value model** with a total order across types (`1 == 1.0 == Decimal(1)`, `True != 1`), used by
  collections, indexes and queries identically (D2, D3); frozen `list` and `dict` values (D25).
- **Atoms** with stable identity (`oid`), lazy thread-safe loading and msgpack serialisation (D10, D18, D21).
- **Collections**, all persistent AVL trees and all indexable: `List`, `Dictionary` (key-ordered),
  `HashDictionary`, `RepeatedKeysDictionary`, `Set`, `CountedSet`, and `DBObject` records (D8, D20-D27).
- **Index framework**: `Index` base class with a documented hook contract, `KeySpec` (paths, the element,
  registered extractors), `EqualityIndex`, `RangeIndex` and `VectorIndex` (exact, IVF-Flat, optional HNSW),
  every mutator of every collection keeping its indexes current, and a reusable index contract
  (`tests/contracts/index_contract.py`) for new kinds (D22, D29, D30, D39).
- **Query layer**: expression terms, physical plans, one cost-based planner that never drops a predicate, a
  LINQ front end (`from_collection`, `F`, `Agg`), explicit `PythonFilter` nodes, `explain()`, a query result
  cache keyed by pointers, and `Pager`s with portable tokens over snapshots (D28, D36-D38, D56).
- **Transactions**: snapshot isolation with read-set validation (absent roots included), `Database.run` with
  bounded exponential back-off and jitter, per-key and per-element merge of unread collection roots,
  `MutableObject` cells, group commit, `Database.history()` and read-only `Snapshot`s of any past version,
  `ProtoOutcomeUnknownError` for commits whose outcome cannot be proved (D9, D11, D32-D35, D52).
- **Storage v2**: `MemoryStorage`; `FileStorage` with append-only CRC-checked segments, atomic root
  publication and `fsync` per commit by default (`durability="flush"` and `"none"` are explicit opt-outs);
  `CloudStorage` over S3 or Google Cloud Storage with data strictly before root and a conditional root write;
  `TieredStorage` (D4, D6, D19, D43, D46, D51).
- **Cluster**: several writer processes through a peer coordinator (majority lease, write permissions, commit
  by majority acknowledgement, bounded replication, learners, fencing), with the object store written after
  the commit point (D44-D46, D50, D54, D55). `ObjectSpace.sync()` is a durability barrier; a barrier commit is
  the linearizable read path (it adds a permanent database version per read).
- **Decoded-atom cache** shared by the transactions and threads of a space, by pointer, bounded on loaded
  atoms, with counters (`ObjectSpace.atom_cache_stats()`) that expose a thrashing working set (D53).
- **Runtime services**: `parallel_scan` and `parallel_filter` (real speedup on free-threaded CPython),
  `FSM`; **interop**: `to_arrow`, `to_parquet`, `scan_parquet` (D40-D42).
- **Free-threading**: tested on CPython 3.14t with the GIL disabled and with the GIL.
- **Performance suite** (`python -m benchmarks`): self-verifying benchmarks, with results by workload
  profile in `docs/source/performance.rst` (D49).
- **Documentation**: a rewritten Sphinx site (concepts, design pages, ten executable tutorials, API
  reference) and a test that runs every tutorial code block (`tests/docs/test_tutorials.py`).
- A single exception hierarchy rooted at `ProtoDBError` (D12), and a static check that forbids swallowed
  exceptions and test-aware code in `proto_db/` (D16).

### Changed
- The public API is the set of names in `proto_db.__all__`, resolved lazily: `import proto_db` imports no
  submodule or optional dependency and starts no thread (D47).
- Dictionaries are key-ordered AVL trees (O(log n)), not a binary search over a positional list (D8).
- A conflict raises `ProtoConflictError` and the caller retries; merging applies only to roots the transaction
  did not read and only by replaying recorded operations, never by inferring intent from value types (D9, D33).
- Strings are stored inline; the global `Literal` catalog is gone (D7).
- Python floor 3.12 (D61; was 3.11 under D14); CI targets 3.12-3.14 and 3.14t. `six` and `wrapt` are no longer
  dependencies (D14).
- The package is split into subpackages: `core`, `storage`, `collections`, `indexing`, `query`, `cluster`,
  `runtime`, `interop` (D15; the spec's `indexes` became `indexing`).

- Storage format 4 (D58): exact tokens are canonical encodings, datetimes in an IANA zone are stored as their
  UTC instant, map keys follow an explicit conflict rule, and the class of a built-in subclass is not persisted
  (a `str` or `int` `Enum` is stored and read back as its value). Formats 2 and 3 are still read. Migration
  cost: a key-group or vector index written by format 3 is rebuilt from its collection at the collection's
  first change, inside that write transaction (an ivf_flat index retrains, an hnsw graph is rebuilt). A
  format-3 map keyed by a zone datetime or by repeated NaNs is read as written but refused when written again
  until the application converts those keys.
- Polyglot freeze (D59, Task 10.P): a record of a type this process has no class for (another language's
  type) loads as an `OpaqueAtom` that keeps its fields readable and is never rewritten, instead of raising
  `ProtoCorruptionError`; root names starting with `__` are reserved for the format (`ProtoValidationError`);
  the attribute-name errors explain the accessor rule. `python -m proto_db.tools.verify <dir>` decodes and
  validates a whole file-storage space and dumps it in the conformance notation. The format specification
  gains "Format stability" (the freeze point, the additive `@sym` and registry), "Atoms of unknown types",
  the accessor rule and "A minimal external writer"; `tests/conformance/vectors.sha256` pins the vectors.
  After the pre-tag review: a reference to an opaque atom may name its type differently from the record's
  `t` (an alias only the owner knows) and keeps the name it was referenced by; writers write the lowest
  sufficient format version (`WRITE_FORMAT_VERSION`, separate from the highest read, `FORMAT_VERSION`); the
  verifier also reports non-canonical encodings and exits 2 only for an unreadable root; a field named like a
  metaclass attribute (`mro`) is readable; a `date` or `timedelta` subclass is stored from its base fields,
  never its `__reduce__`.
- Format-3 data misfiled by built-in subclasses: format 3 wrote a `(str, Enum)` member (on every Python
  version) or a subclass overriding `__str__`, `__float__` or `__bytes__` through that conversion
  (`"Color.RED"`) but filed it by its base value (`"red"`). Collections holding such values cannot find them,
  and a key-ordered Dictionary also loses other keys. `collection.validate()` now detects it
  (`ProtoMisfiledError`, a `ProtoCorruptionError`) and `tx.set_root_object(name, collection.rebuilt())`
  repairs it (entries refiled, indexes rebuilt; one key with two different values is refused, never dropped).
- IVF-Flat training no longer depends on the members' oids (R128, 012db7f): a bulk build of the same vectors
  with the same `seed` gives the same centroids and the same answers, in any order (an index grown by inserts
  still trains on the members present at each retrain threshold, which depends on the insertion order). An `ivf_flat` index built before 012db7f
  keeps its persisted centroids until it retrains; at its next retrain (or rebuild) it gets different
  centroids than that same data gave before, and stable ones from then on. No format change.

### Deprecated
- The 0.1 exception names `ProtoBaseException`, `ProtoValidationException`, `ProtoUserException`,
  `ProtoCorruptionException`, `ProtoNotSupportedException`, `ProtoNotAuthorizedException`,
  `ProtoUnexpectedException` and `ProtoLockingException`: aliases of the `Proto*Error` classes, removed in 2.1
  (D12).

### Removed
- The 0.1 implementation (`common`, `db_access`, the 0.1 storages, collections, queries, LINQ, vectors,
  `parallel`, `fsm`, `arrow_bridge`, `hybrid_executor`, `exceptions`) and its test suite `proto_db/tests/`
  (D47, D48; dropped tests: `docs/legacy/dropped-tests.md`). UDP voting is gone from the cloud storages (D5).

### Fixed
- Saving a chain of unsaved versions built in one transaction (the `previous` pattern) no longer raises
  `RecursionError` past about 150 links; 10,000 versions save in one commit, and the verifier describes them
  (R128).
- Nested values: `proto_db` no longer writes values it cannot read back (B12, D60). The codec, `canonical()`,
  the map-key, order-key, hash and equality helpers and the verifier walked values by recursion, so a value
  past about 330 nested lists could be written but raised a bare `RecursionError` when read in a fresh
  process (`verify` exited 1), and the limit moved with the Python stack in use. They now go on with an
  explicit stack past 12 levels, and the format has a normative limit, `MAX_VALUE_DEPTH` = 1022 MessagePack
  containers per value (format spec, "Value nesting depth"): a writer refuses a deeper value with
  `ProtoValidationError` naming the field, a reader refuses deeper data with `ProtoCorruptionError`, at the
  same count. No format version change (no conforming writer could produce deeper data); the encoded bytes
  are unchanged. `proto_db` now requires msgpack 1.2.0 or later, the first release whose packer nests the 1024
  containers a record at the limit needs, and refuses to import with an older one. The limit binds collection
  and index nodes too: a Set, CountedSet, HashDictionary or RepeatedKeysDictionary, and an index (whose member
  keys nest an unorderable element about four times deeper: 254 nested lists, 169 nested maps), refuse an
  element they cannot store when it is added, naming the collection or the index and the effective limit,
  never later at commit. The query result cache, `explain()` and `to_arrow` work at any depth too.
  **Readers must accept records 1024 containers deep**: an implementation that stops at 509 levels is not
  conforming from rc2.

Defects of the 0.1 code base found by the 2026-11-01 review, none of which survives the rebuild: integer
values merged as `+1` and special cases for keys named `counter` and `k`; commits that were not durable;
indexes lost on the first write; queries that silently returned empty or partial results; about 280 broad
`except` handlers hiding errors; two writers appending to one segment.

### Known limitations
- No `proto_db.tools.inspect` browser (the spec's section 6 named one); the formats are specified in
  `docs/source/format-spec.rst`, `python -m proto_db.tools.verify` checks and dumps a space, and the operations
  tutorial shows both.
- No online-backup call; see the operations tutorial for what a copy of an open space contains.
- Cluster fairness for eight or more contending writer processes, and for hot-key read-modify-write
  transactions, is not guaranteed (open defect D-LIVE-2).
- A read-only linearizable read (`read_barrier`) is proposed in the D46 amendment but not implemented; the
  barrier commit is the supported path.
- On Python 3.14 an exception from a signal handler (Ctrl-C) can leave a `proto_db` lock held, and the process
  can then hang; end the process after such an interrupt. An interpreter defect (CPython gh-148874, fixed in
  3.15, not in 3.14); 3.12 and 3.13 are not affected. See the concurrency model.

## Before 2.0

Entries from the 0.1 line, kept as written. They describe code that 2.0 removed.

### Added (0.1 line, unreleased)
- Optional parallel scanning module (proto_db.parallel) with adaptive chunking and a simple work-stealing scheduler.
- Metrics hooks for per-worker and global stats (chunk latencies, steals, queue depths).
- parallel_scan helper for easy opt-in usage without altering existing query code.
- Documentation: user guide (Parallel Scans), API reference, and ADR.
- Unit tests for adaptive chunking, work-stealing behavior, cancellation, and fallback correctness.

### Changed
- Attribute access style: migrated docs/tests/examples to prefer dot notation; clarified DBObject semantics and dynamic getattr in README.

## [0.1.0] - 2023

### Added

#### Google Cloud Storage Support

- Added `GoogleCloudClient` as an implementation of `CloudStorageClient` for Google Cloud Storage
- Ensured that `CloudBlockProvider`, `CloudFileStorage`, and `CloudClusterFileStorage` work with both `S3Client` and
  `GoogleCloudClient`
- Added comprehensive test suites for Google Cloud Storage integration
- Added usage examples for Google Cloud Storage in documentation

#### Multiple Serialization Formats Support

- Added a format indicator after the size header to specify the serialization format
- Added support for JSON UTF-8 (the original format) and MessagePack (a new efficient binary format)
- Maintained backward compatibility with existing data
- Added new methods to support the new formats:
    - `push_atom(atom: dict, format_type: int = FORMAT_JSON_UTF8) -> Future[AtomPointer]`
    - `push_atom_msgpack(atom: dict) -> Future[AtomPointer]`
    - `push_bytes(data: bytes, format_type: int = FORMAT_RAW_BINARY) -> Future[tuple[uuid.UUID, int]]`
    - `push_bytes_msgpack(data: dict) -> Future[tuple[uuid.UUID, int]]`

#### PyPI Package Support

- Created `pyproject.toml` with package metadata, dependencies, and build system information
- Added MIT License file
- Created `MANIFEST.in` to include non-Python files in the package distribution
- Added `test_install.py` script to test the local installation of the package
- Created `PYPI_UPLOAD.md` with documentation on how to build and upload the package to PyPI
- Updated README.md with installation instructions and license information

#### Advanced Storage Options Documentation

- Added comprehensive documentation for `ClusterFileStorage` and `CloudFileStorage`
- Updated `__init__.py` to expose the advanced storage options
- Added detailed use cases for the advanced storage options

### Fixed

#### CloudFileStorage Test Hanging Issue

- Added an `uploader_running` flag to control the background uploader thread
- Modified the `_background_uploader` method to check both the `state` and the `uploader_running` flag
- Updated the `close` method to properly stop the background uploader thread and wait for it to finish
- Added test verification to ensure the uploader thread is properly stopped

### Changed

- Completed the implementation of `S3Client` by adding missing abstract methods:
    - `_init_client`: Initializes the S3 client using boto3 or falls back to a mock implementation
    - `get_object`: Gets an object from S3
    - `put_object`: Puts an object to S3
    - `list_objects`: Lists objects in S3
    - `delete_object`: Deletes an object from S3