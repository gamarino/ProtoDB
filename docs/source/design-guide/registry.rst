The runtime registry: one rule for data and behaviour
=====================================================

*Runtime registry plan of 2026-11-14. Decision D62; rulings R150-R154, R166-R176, R189-R190, R201-R209;
principles P1, P2 and P6. The code is* ``proto_db.registry``, ``proto_db.resolution`` *and*
``proto_db.modules.base``.

What it gives the ERP
---------------------

A small ERP is built from modules: contacts defines partners, CRM adds leads and changes how a partner's name
is shown, pricing changes a default currency, an activity module makes leads mailable and schedulable. None of
them edits another. The :doc:`extensible-models tutorial </tutorials/extensible-models>` builds exactly that,
and every behaviour this chapter describes is printed there by running code.

The database stays language-neutral while it does so: it stores data, the names of the installed modules, and
nothing that only Python can read.

The problem
-----------

Four requirements pull against each other.

* **One rule for fields and methods.** A module must be able to change a default or a method by the same
  mechanism, and ``o.name`` and ``o["name"]`` must find the same thing, so that a name computed at run time
  works like a written one.
* **Nothing language-specific is persisted** (anti-goal A1 of the design): no code, no Python class of a model
  or of an extension, no import path. protoBaseST reads and writes the same database.
* **Installs are never lost.** Two processes that install different modules at the same time must both land,
  without one silently overwriting the other.
* **Every error is explicit**, and a lookup never resolves silently from the wrong place. A *miss* is not an
  error: it returns ``None`` (the maintainer's ruling R150, so that a model can grow over time without
  breaking older objects or older code).

Why the obvious solutions fail
------------------------------

**Python subclasses of** ``DBObject``. They are still supported, and right for a fixed model. For an
extensible one they fail at the first record already stored: a record names its class's type name, so a
record written before a module subclassed the class loads as the old class and never sees the module's
behaviour. The tutorial shows it in section 3. The type name is also a Python class, which another language
cannot run.

**Dynamic Python classes per registry name**, Odoo's approach (``type(name, extensions, {})``). Python's MRO
and ``super()`` would do the chaining at native speed. It was rejected because it makes two mechanisms:
methods in the class, data in the objects, against the first requirement. An object's *own* parents (other
records) would still need a lookup of their own beside the class.

**Persisting the methods' source.** Rejected by A1, and because a record would then carry code into every
process that reads it.

**A module list kept in an ordinary root.** It is a read-modify-write: two installs conflict and one retries
(tutorial, section 4). A merge that takes the last writer would lose an install.

**Ties in the load order broken by installation order.** Modules are loaded dependencies first; two
modules that do not depend on each other need a tie-break, and the persisted list's order is at hand. It
fails the fourth requirement silently: the list's order is the commit order of the installs, so two
databases that installed the same modules in different orders, or two concurrent installs that landed in
either order, would chain the same extensions differently, and the method that wins would depend on which
commit came first. The tie-break is the module name in code-point order instead (R253, R254), with ``base``
explicitly first (R255): the registry becomes a function of the set of installed modules, which every
language can compute from the database alone (protoBaseST and the semantic catalog need exactly that).

**Python's** ``super()`` **for the next definition.** It needs ``self`` to be an instance of the class whose
method runs; an extension's container class has no instances, and the receiver is a ``DBObject`` (tutorial,
section 5).

The choice, and the invariants that make it correct
---------------------------------------------------

Parents
    A ``DBObject`` has an ordered tuple of parents, each another ``DBObject`` or a registry name (a non-empty
    string; nothing else, decision Q6). It is persisted as the envelope key ``"p"`` of the record, the
    format-5 feature ``parents`` (:doc:`/format-spec`). An object without parents writes the format-4 record
    byte for byte, so data that never uses the feature keeps its version.

One resolution rule
    ``resolve(o, name)`` returns the first definition along the C3 linearization of ``o``: its own
    attributes, then each node in C3 order. An object node contributes its own attributes; a registry-name
    node contributes its extension chain, newest module first. A value is returned as is, a method bound to
    ``o`` (``self is o``). Nothing else is computed (Q4): a computed value is a method. Writes never climb the
    parents. C3 is chosen over a depth-first walk because a depth-first walk visits a shared ancestor more
    than once and before the names that override it (tutorial, section 7); C3 orders every node after all of
    its children and refuses a hierarchy that has no such order (``ProtoValidationError``). ``o["name"]`` reads
    what ``o.name`` reads; an API name (``oid``, ``with_``, ...) or a ``_`` name is not an attribute name there,
    ``ProtoAttributeNameError``, which is also a ``KeyError`` so that mapping-first consumers such as Django
    templates fall back to attribute access (R238).

The catalog object, before the registry (Q1)
    ``install_module`` creates, for each registry name a module introduces, a plain ``DBObject`` whose only
    parent is that name, stored in the root ``__catalog``. Instances list it as a parent, so its data comes
    after the instance's and before the registry's, with no rule of its own. Its data is application data:
    ``set_catalog_object`` replaces it with a new version, under the normal conflict rules (the first updater
    wins, R189). A version is a new record with a new ``oid`` (D18), so the root entry, not the oid, carries the
    continuity (R190).

Modules and the registry
    A module declares itself from inside the Python module that implements it (``module(name,
    depends=...)``) and lists its extensions with ``@handle.extends(registry_name, parents=...)``. Only plain
    functions (methods) and canonical data defaults are definitions; a ``property``, a ``staticmethod``, a
    ``classmethod``, a callable instance, a lambda, a nested class, and a data default holding a database
    object at any depth (R166: a default is shared by every database the process opens) are refused at import
    with ``ProtoValidationError``. The registry of a module list is built per process and is immutable once built. Its
    load order is ``base`` first, then dependencies before the modules that need them, ties broken by module
    name in code-point order (R253-R255): the order of the persisted list, the install history, plays no part
    (R256), so the registry depends only on the set of installed modules, and one registry is shared per set
    (R257).
    Module names are process-global: one name maps to one Python module per process (R167). Running the same
    module scope again (a reload) replaces its declaration; two ``module()`` calls in one execution raise
    (R168).

The base module (R204-R209)
    ``base`` holds the default mechanisms. It is the implicit last parent of every registry name, so every
    module can override what it defines and reach it with ``_super()``. ``install_module`` adds it first,
    implicitly; a database that never installs a module never has it and pays nothing. Because every registry
    linearization ends with ``base``, its public names are reserved against Python members of every
    ``DBObject`` subclass that reaches a registry name (a Python member would hide the registry definition, so
    such a class is refused at its first resolution). ``base`` therefore keeps its names few: today only
    ``read_data_file``. Reading is lenient and writing strict (R235): a persisted list that does not start with
    ``base`` (another language's writer) is read as if ``base`` preceded it, and the next install writes it
    with ``base`` first. Every runtime provides a ``base`` module (``format-spec.rst``, "Registry roots").

``_super`` from the code object (Q3)
    At load the registry indexes every method's code object to its extension. ``self._super()`` reads the
    caller's code object, finds that extension's position in ``self``'s linearization, and resolves from the
    next position, binding to the same ``self``. A miss is ``None`` (R170), so the end of a chain is tested
    with ``if self._super().f:``. Code that is not a registry method is refused with ``ProtoUserError``: code
    at module level, a closure or helper. A method wrapped by a ``functools.wraps`` decorator is indexed by
    the code of the function it wraps (``inspect.unwrap``, R239), so one decorator may wrap many methods and
    ``_super()`` works inside the wrapped body; a decorator without ``__wrapped__`` makes its wrapper's code
    the method's, so wrapping two methods with it is refused at load and ``_super()`` in the wrapped body is
    refused, both errors naming ``functools.wraps``. Generator functions, async generators and generator expressions are
    refused by design (R172, R229), and coroutine methods are allowed (R176). The position would be found from
    the running code object in both cases, so the refusal of generators is a choice, not a necessity; whether
    to allow them too is an open question for the maintainer. Positions are compared for equality only; they
    are not ordered values (R175).

Installs merge add-if-absent
    At commit the two registry roots are merged against the root current at that moment, never through the
    read set: the list only grows and the catalog only gains names, so the union is always right, two installs
    never conflict with each other, and the order of the list is the commit order (which is why the load order
    ignores it, R256). Reading the roots through
    ``installed_modules`` and ``catalog_object`` (and for resolution) never enters the read set;
    ``get_root_object`` of a registry root is an ordinary, recorded read, so a concurrent install makes that
    transaction conflict (R241). A raw ``commit()`` on a
    storage whose root lock is not global (``CloudStorage`` across processes) can still lose the root's
    compare-and-swap like any commit; it raises ``ProtoConflictError`` and ``Database.run`` retries it. No
    module is ever imported under a lock: a module's import-time code may itself commit, and would wait for
    the lock its importer holds. That code must not resolve registry objects of a database that lists the
    module being imported, though: its declarations so far are half the module, so the build of that list's
    registry raises ``ModulesBeingImported`` (a ``ProtoUserError`` naming the module) and caches nothing; an
    import-time commit still succeeds, and the first lookup after the import builds the whole registry
    (R234).

Which registry resolves an object (R201)
    The code doing the resolving decides, never the order in which records were loaded or the state of a
    cache. An object created or derived in a transaction resolves through that transaction's registry for its
    whole life. A loaded object resolves through the current reader: the transaction of a ``with tr:`` block
    or of ``Database.run``, or the snapshot of ``with snapshot:``. Only when no reader of its space is current
    does it resolve through the space's fallback, and only if every database of the space that lists modules
    lists the same ones, in any order (R257); otherwise ``ProtoUserError``. A transaction or reader of a database that lists no
    modules decides like any other (R236): it has no registry, so a lookup that reaches a registry name raises
    ``ProtoNotSupportedError`` ("needs a registry"), as in a space with that one database; it never resolves
    through another database's modules. An as-of read
    resolves with the list of its version and never changes how other code resolves.

A mismatch is an error (Q2)
    A space binds each database to the module list it first sees. When another process installs a module,
    the next lookup through the registry in a transaction on the new list raises
    ``ProtoValidationError`` naming both lists. A loaded object resolved outside any block keeps the bound
    list until a transaction of the space has seen the new one, and then raises the same error. A snapshot
    resolves with its own version's list and never mismatches. There is no
    automatic rebuild: rebuilding between two lines of a running program would interleave transactions on two
    behaviours. ``ObjectSpace.reload_registry(database_name)`` is the explicit rebuild; it invalidates every
    registry of that database cached in the space, so later snapshots and installing transactions build fresh
    too and run the reloaded code (R243), while other databases keep the process's shared registries (R200). A module that cannot be
    imported raises ``ProtoNotSupportedError`` at the first lookup that needs the registry (opening the
    database and reading data never do), and the failure is remembered for that list until
    ``reload_registry`` (R202): Python does not cache a failed import, and every commit would retry it. A
    failed reload is not itself remembered: the next lookup tries the import once more.

    The space keeps the lists it **adopted** for each database: the one its first transaction bound, each one
    its own install commits published (or ended with an unknown outcome), and each one ``reload_registry``
    bound. A transaction whose snapshot lists an adopted list resolves with that list's registry, whatever its
    version and whatever the order of its snapshot read and the space's commits (a commit publishes and then
    rebinds, a transaction reads and then binds, and no clock decides which comes first, R82); while an install
    commit is between its publish and its rebind, its list counts as adopted. A list never adopted is the
    mismatch above. The database's binding, which a loaded object resolved outside any block uses, moves only
    forward in the order of the database's versions, (label, version), the cluster's root order, in which a
    later lineage orders after the one it replaced: concurrent installs never move it back, and a transaction
    on an earlier version gets a binding of its own. ``reload_registry`` always rebinds it to the list it
    reloads (unless an install of the space bound a newer version meanwhile, which it then reloads), also to a
    version below the bound one: the explicit recovery after a cluster failover beyond the failure model. A
    space without a coordinator gives every root label 0, cluster labels included, so a cluster's storage
    opened locally orders its versions alone and its own installs move the binding. In cluster mode, when a
    transaction on an earlier version finds the node's committed root ordered below the root the binding came
    from (``force_rejoin()`` on a survivor forgot that root), the binding belongs to a lineage the node no longer
    follows: the database binds to the transaction's list if its snapshot is that committed root, and is
    otherwise marked stale, so a loaded object resolved outside any block raises the mismatch until
    ``reload_registry``, or until a later transaction whose snapshot is the committed root binds the database
    again. The check runs when a transaction starts: beyond the failure model, between a ``force_rejoin()`` and
    the next transaction, a loaded object resolved outside any block still uses the forgotten lineage's list.
    The adopted lists are never pruned; they are bounded by the space's installs and reloads.

**The invariant behind the caches: what a cache entry is keyed by never changes.** The linearization of a
tuple of saved parents is cached by the parents' pointers and the registry. A record at a pointer never
changes (P1), its own parents are fixed, and a built registry is immutable, so an entry is never stale and
never needs invalidating. Each entry also remembers the position where each name was found, so a repeated
lookup reads one definition. Unsaved parents are cached only on their transaction, for its life.

Resolution never visits history: superseded versions are not parents, so the cost of a lookup does not depend
on how many versions the database holds.

Its cost, stated honestly
-------------------------

* **No latency is quoted here.** The cost of a lookup through the registry against an own attribute, and
  the cost of setting the reader around each commit's lock step, were not measured by this plan. The
  argument above is structural; the measurement is still to be made, with ``proto_db`` measured against itself.
* **Catalog updates do not reach existing objects.** An instance lists the catalog object's version that
  existed when it was created, and an immutable record never changes. Only objects created after an update
  see it. This is the current semantics; a value that must change for every existing object belongs in a
  module (new code) or behind a ``MutableObject`` the objects reference.
* **Indexes and query paths read own attributes only.** An inherited default is invisible to them, because
  an index of inherited values would have to change whenever a module or a catalog object did. An attribute
  that is queried or indexed is set on the object itself (tutorial, section 10).
* **A Python member that hides a registry definition is refused late.** The spec says "refused at load";
  the registry is not known when an object is created or decoded (a load never builds one, and a commit's
  lock step cannot), so a ``DBObject`` subclass that defines in Python a name a registry name it reaches
  defines is refused (``ProtoValidationError``) at the first resolution of one of its objects through that
  registry: an inherited lookup, ``name in o`` or ``o[name]`` of the member, ``_linearization()`` or
  ``_super()`` (R240). Plain attribute access of the Python member (``o.label()``) involves no resolution:
  until such a resolution has run, it silently runs the Python member.
* **A misspelt registry name is silent.** A name no module extends is an empty node, so lookups go on past
  it (the opposite was implemented first and overturned, below). ``Registry.unknown_names`` lists such names
  for tests and tools.
* **One process, one meaning per module name.** Two runtimes in one process whose mappings point the same
  module name at different Python modules are not supported.
* **Methods are per language.** A database used from Python and Smalltalk lists module names, and each
  language has its own version of each module. A process that cannot load a listed module raises; it never
  runs with part of the model missing.
* ``read_data_file`` **trusts the module's directory.** ``base.read_data_file(path)`` reads only inside the
  directory of the module that asked (symbolic links that stay inside are followed). An absolute path, a
  drive or UNC path, a path that leaves the directory once resolved, a NUL character and a file that is not a
  regular file (a FIFO, a device) are refused. The file is opened first, without blocking, and its real
  location is taken from the open descriptor (``/proc/self/fd`` on Linux, ``F_GETPATH`` on macOS) and checked
  again, so a component swapped between the check and the open is refused. Elsewhere (Windows, the BSDs) the
  fallback resolves the path again after the open and accepts it only if it names the very file opened. A
  double swap timed between those steps can still pass. That residual race is accepted: whoever can swap
  files inside a module's package directory can already replace the module's code, which is a larger power
  than reading one of its files.

Symbols: measured, and deferred
-------------------------------

The 2026-11-08 design made every attribute name a *symbol*, interned in the database. The 2026-11-14
decision (Q7) made a symbol a storage optimization only: a reference to a string already stored, which **is**
that string (equal, same hash and token, same key). Whether it pays was measured before building it (ruling
R153, criterion R213: references plus compression must be at least 10 % smaller than compression alone).
On a deterministic ERP data set of 13,964 objects (``benchmarks/symbol_bytes.py``; the figures and the method
are in ``docs/superpowers/2026-11-14-symbol-measurement.md``), the segment holds 11,393,442 bytes inline and
2,799,544 bytes compressed per 64 KiB block; with 17-byte content ids, references plus compression are larger
than compression alone at every minimum length measured (+7.15 % at 8 and 16 bytes, +4.63 % at 24), and the
best of any reference size, a 3-byte sequential id, is -5.99 %, short of 10 %. Compression already removes the
repeated names. Format 5 therefore has no symbol-reference feature (:doc:`/format-spec`, "Symbol references").
The reserved ``@sym`` tag, and the format's rule for a language's own symbol type, are a separate matter and
unchanged.

What was tried and rejected
---------------------------

* **An unknown registry name raised** when a lookup reached it (R171). It made a name listed before its module
  existed an error, against R150's "extension over time". Overturned: the name is an empty node, and
  ``unknown_names`` is the mitigation.
* **Per-database views of the shared atom cache**, to tell which database a loaded record belongs to (R197).
  Review found that a record shared by two databases then resolved by whichever loaded it first, and that
  query rows reloaded after eviction lost their database. Reverted: the code doing the resolving decides
  (R201), which depends on nothing a cache does.
* **As-of reads that bound the database** to the list they read. An old snapshot then made later lookups
  outside any block raise for the life of the space. As-of reads now never bind (R196).
* **Imports under the root lock.** A module whose import-time code commits deadlocked against the process
  importing it, across processes. The commit now builds the merged list's registry outside every lock and
  runs its lock step again when a module was missing.
* **A context variable as the** ``_super`` **cursor**, set around each bound call. A closure, a callback or a
  generator that outlives the call would keep a stale cursor and resolve silently wrong. The code-object
  lookup fails explicitly in the same cases.
* **A receiver view** ``(o, i)`` carrying the position. Rejected by Q3: ``self`` would be equal to ``o`` but not
  ``o``.

Back to the ERP
---------------

The ERP's modules each extend the partner and the lead by the rule that finds every attribute; the database
holds the records, their parents and the module names. A process that runs the CRM's new code reloads its
registry explicitly; a report of last quarter runs with last quarter's modules; and the lead's current
version is the only one its indexes see, so no query needs a "current only" condition.
