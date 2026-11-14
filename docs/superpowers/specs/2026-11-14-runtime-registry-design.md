# Runtime registry, modules and symbols: design

Status: **draft for the maintainer's review** (2026-11-14). This draft is written from the maintainer's decisions of
2026-11-08 and 2026-11-14. Points marked *(proposal)* are the author's calls, not decisions. Points under "Open
questions" need a decision before the plan.

Scope: course 4 of the course material ("objects, behaviour and extensible models"). It comes after 2.0.0. It is a
format-5 feature (symbols and the registry roots), so it is additive: data written without it keeps version 4 and
its meaning.

## 1. Intent

A multi-language server needs extensible models. A module must be able to change how a model behaves without
touching the module that defined it, in the manner of Odoo's `_inherit`. The small ERP running example shows the
need: users, groups, persons and opportunities.

The database stays language-neutral. It stores data and the list of installed modules, nothing more. Each language
turns module names into code in its own way. In Python that means importing the module.

The teaching point of course 4: in ProtoBase **a field and a method are the same thing found the same way**. An
attribute is resolved by one rule, through the object, its parents and finally the registry. What is found is either
a value or a callable. Data, defaults and behaviour therefore extend by the same mechanism.

## 2. Goals, non-goals, anti-goals

Goals:
- G1. One resolution rule for every attribute. `o.name` and `o["name"]` find the same thing by the same lookup. The
  name in `o["name"]` may be computed at run time.
- G2. Transparent use: `o.some_method(o.some_field)` reads like plain Python.
- G3. Extension chains per registry name. Each extension may redefine any attribute, data or method. A method reaches
  the next definition in the chain explicitly (section 6).
- G4. Ordered parents per object. A parent is either another object or a registry name.
- G5. Modules declare their dependencies and are loaded in topological order. Installing a module is additive and
  permanent.
- G6. Symbols: every attribute name is a symbol. A symbol is identified by its name and interned per process.
  Creating the same symbol twice never conflicts.
- G7. Every error is explicit; nothing fails silently.
- G8. Efficient resolution: cached linearizations keyed by pointers, never invalidated by hand.

Non-goals:
- NG1. No class extents and no automatic "all persons" list (decision 2). An application keeps such a list in a
  class attribute (a list or an indexed set).
- NG2. No uninstall (decision 4). Old objects may be read at any point in time.
- NG3. No access policy or permission model in the database. Access control is application code in modules.
- NG4. No erasure and no rewriting of history. Legal erasure demands (for example GDPR) are met outside the database,
  for example by crypto-shredding with keys held outside it. This is stated as a limit, not solved.
- NG5. No cross-language modules. One module is not written in several languages, and code in one language is not
  run from another (decided 2026-11-08). A database used from two languages has one module version per language.

Anti-goals: the feature fails, even if it works, if:
- A1. Anything language-specific is persisted: Python code, Python class names or import paths.
- A2. A lookup miss returns `None`, a default, or a stale result instead of raising.
- A3. A concurrent symbol creation, or two concurrent module installs, can produce two symbol objects, or lose an
  install by last-writer-wins.
- A4. Resolution cost grows with the database's history depth.

## 3. Persisted state (format 5)

All persisted state lives in database roots reserved by `format-spec.rst` ("The per-database class registry").

| Root | Type of its value | Content |
|---|---|---|
| `__currentModules` | built-in `proto_db.registry.ModuleList` *(proposal)* | the installed module names, as symbols, in installation order, with each module's declared dependencies |
| `__currentSymbols` | built-in `proto_db.registry.SymbolTable` *(proposal)* | string to symbol; only grows |
| `__catalog` *(proposal: the name is fixed here)* | built-in `proto_db.registry.Catalog` | registry name to catalog class object |

- A reader of format 5 identifies each root by its name **and** by the type of its value. A `__` root whose value is
  not of the registry's type is refused with `ProtoCorruptionError`, never read as registry state (format-spec
  rule).
- **Symbol** (option c2 of the 2026-11-08 investigation): the value kind `{"@sym": name}`, order rank 16, hash and
  token derived from its encoding. It is interned per process: equal names give the same Python object. Comparison
  is by identity, with a fallback to the name.
- `__currentSymbols` is updated at commit by add-if-absent in `_build_root`, from the version readable at commit
  time, never from the one read at the start. Two transactions that create the same symbol therefore both succeed and
  end with one entry, with no conflict and no redirection. A symbol created in an aborted transaction is discarded
  with it.
- `__currentModules` is updated at commit by add-if-absent as well (installing is additive, decision 4). Two
  concurrent installs of different modules both land. The order of installation is the commit order.
- **Every attribute name is a symbol (decision 7).** The new object type of section 4 stores its attribute names as
  symbols. Existing `DBObject` atoms keep their format-4 encoding and their meaning (additive rule); there are no
  existing registry databases to migrate.
- The cluster `PROTOCOL_VERSION` rises in the same change, since cluster messages carry values.

## 4. The object model

A new built-in atom type, `proto_db.Object` *(proposal for the name)*, is a registry-aware object. `DBObject`,
collections and `MutableObject` are unchanged. They are values the new objects can hold, and nothing about them
changes in this feature.

An `Object` holds:
- **its own attributes**: symbol to canonical value. Values only; a callable is never persisted (anti-goal A1);
- **its parents**: an ordered tuple, persisted. Each parent is either:
  - an `Object`, referenced like any other atom, or a `MutableObject` handle when the parent is an entity whose
    current version resolves in the transaction (the values-and-entities decision of 2026-11-08); or
  - a registry name: a symbol.

A **catalog class object** is an `Object` stored in `__catalog`. It gathers a class's definition data. It is a root
of the database, not of the object space, so each database has its own catalog.
- An instance lists its catalog class object as a parent (option a).
- The catalog class object lists its registry name among its parents. Lookup therefore continues from it into the
  in-memory registry chain.

Immutability is unchanged: an `Object` is a value. A changed object is a new version made with
`o._with(name=value, ...)` *(proposal, section 5)*. A stable identity is a `MutableObject` whose value is an
`Object`. The versioned entity is a documented pattern (a new version points at the previous one), not part of the
base module.

## 5. Resolution: one rule for fields and methods

`resolve(o, name)` returns the first definition of `name` along the **linearization** of `o`:

1. `o`'s own attributes;
2. then, in order, every node of the C3 linearization of `o`'s parents.
   - An `Object` node contributes its own attributes. A `MutableObject` node contributes the attributes of its
     current value in the transaction.
   - A registry-name node contributes its extension chain, from the newest extension to the oldest (section 6).
   - Each registry name is visited once, even when several parents reach it (C3, decided 2026-11-08).
3. The catalog class object's own attributes come after its registry chain (decision 1 of 2026-11-08: modules can
   redefine the catalog's data). See open question Q1.

What is found:
- **A value** (data on an object, or a data default of an extension) is returned as is.
- **A callable** (a function defined in an extension) is returned bound to the receiver `o` and to its position in
  the linearization (section 6). It is never persisted; it exists only in the process's registry.

A miss raises:
- `o.name` raises `AttributeError`, which subclasses `ProtoNotFoundError` *(proposal)*. `hasattr`, `getattr` with a
  default and the Python protocols that probe attributes keep working.
- `o["name"]` raises `KeyError`, which subclasses `ProtoNotFoundError` *(proposal)*.

The message names the object, the name and the linearization searched.

The Python surface:
- `o.name` and `o["name"]` run the same resolution. `o["name"]` accepts a `str` or a symbol, so the name may be
  computed at run time (`o[field_name]`). `o.name` covers every name that is a valid identifier.
- The Python-level API of `Object` uses only names that start with `_`: `_with`, `_parents`, `_own`, `_super`,
  `_resolve`, `_linearization` *(proposal)*. Attribute names never start with `_` (the existing `DBObject` rule), so
  no attribute can be shadowed by the API and no API member can be shadowed by an attribute. Dunder names
  (`__len__`, `__eq__`, ...) never go through resolution.
- `name in o` reports whether resolution finds `name`. `o._own` returns the own attributes only.
- Writes never go into a parent. `o._with(...)` sets own attributes, and `o["x"] = v` is refused: values are
  immutable. A data default that an extension defines is overridden by setting the attribute on the instance.

Late binding (see feedback "the platform is late-binding"): the linearization is resolved when it is used, against
the registry of the process and the transaction's snapshot. The spec states when that happens (section 8). A
registry name that no loaded module defines raises when it is resolved, never when the object is loaded.

## 6. Extensions, the chain and `_super`

An extension is declared in Python as a class body inside a module (section 7). Its attributes are the extension's
definitions: values are data defaults, functions are methods.

```python
from proto_db.registry import module

crm = module("crm", depends=("base", "contacts"))

@crm.extends("crm.Opportunity")
class Opportunity:
    stage = "new"                                  # a data default

    def win(self, tr):
        self._super().win(tr)                      # the next definition of `win` in the linearization
        return self._with(stage="won")
```

- The class is only a container. Its instances are never created. Its Python MRO is not used, and Python's
  `super()` is not the mechanism (it needs `self` to be an instance of the class).
- **`_super`** *(proposal)*: a method found at position `i` of `o`'s linearization is bound to a receiver view `(o, i)`.
  - Inside the method, `self.x` resolves from the start of the linearization (late binding: an override further down
    still wins).
  - `self._super()` returns a view that resolves from position `i + 1`. `self._super().win` is therefore the next
    `win`, wherever it is: another extension of the same name, a registry name further along, or the catalog's data.
  - The view is equal to `o` and hashes as `o`, and storing it stores `o` (it is canonicalized to `o`). Only `is`
    differs. The alternatives are compared in section 11.
- Within one registry name, extensions chain in module load order: the extension of the module loaded last is
  found first.

## 7. Modules

- **Declaration** (decided 2026-11-08): a module declares itself from inside the imported Python module, with an
  explicit list of the extensions it loads. There is no manifest file. `module(name, depends=(...))` returns the
  handle that `extends` and the data-file hooks use.
- **Name mapping**: the database stores a language-neutral module name, a symbol. Turning it into a Python import is
  the runtime's job.
  - The default maps the name to an importable path of the same name.
  - A configured mapping can replace it *(proposal)*.
  - A name that cannot be imported raises `ProtoNotSupportedError` naming the module and the database.
- **Loading**:
  1. read `__currentModules` from the snapshot;
  2. sort the dependency graph topologically; a cycle raises `ProtoValidationError`;
  3. import the modules in that order;
  4. build the registry in memory.

  The `base` module loads first. It has no dependencies, and it defines the default mechanisms (for example the
  data-file read method below).
- **Installing**: `tr.install_module(name)` adds the module and the dependencies it lacks to `__currentModules` at
  commit (add-if-absent). Installing is permanent.
  - Which modules may be installed is restricted by the runtime's configuration, not by the database (decision 6).
    A module outside the allowed set raises `ProtoValidationError`.
- **Data files** (decided 2026-11-08): a module reads a data file, such as a view definition, through a method that
  other modules may override. When nobody overrides it, the final default in `base` reads the file, as Odoo's
  `get_view` does.

## 8. The registry in memory, and consistency

- A registry is built per process for a module list. It is keyed by the list's root pointer (the `__currentModules`
  value of the snapshot). Equal pointers give the same registry.
- **Decision 3:** a transaction whose `__currentModules` differs from the list the process's registry was built from
  raises `ProtoValidationError` *(proposal for the class)* on its first resolution through the registry. There are
  no tracking tables. See open question Q2 on automatic rebuilds.
- A transaction that installs a module resolves through a registry built for its own uncommitted list (snapshot
  semantics, decision 5). Other transactions see the new module only after the commit.

## 9. Efficiency

- The linearization of a parent tuple is cached, keyed by the tuple's pointers and the registry. Objects are
  immutable, so a pointer-keyed entry never goes stale (D56). Instances of one class share their parents' tuple, so
  they share the cached linearization.
- A per-name cache maps (linearization, symbol) to the position found.
  - A miss walks the frames once.
  - Registry frames are immutable for the registry's life.
  - `Object` frames are immutable by version.
  - A `MutableObject` frame resolves its value in the transaction and is never cached across transactions.
- Symbols make each own-attribute lookup an identity-keyed dict probe.
- Superseded versions leave the current collections and indexes. Resolution never visits history, so its cost does
  not depend on history depth (anti-goal A4). This is a claim to verify with a ProtoBase-only measurement in the
  tutorial, not a rival benchmark.

## 10. Polyglot

protoBaseST reads and writes the same roots, symbols and `Object` records.
- The persisted state of sections 3 and 4 is normative and goes into `format-spec.rst` with test vectors.
- Resolution (section 5), `_super` and module loading are each language's own. Smalltalk resolves by its own rules
  over the same parents.
- The format-5 additions must be agreed with protoBaseST before they are frozen.

## 11. Alternatives considered

- **Dynamic Python classes per registry name** (Odoo's approach: `type(name, extensions, {})`): Python's own MRO and
  `super()` would do the chain at native speed. Rejected for this design. Methods would live in the class and data
  in the objects, which is two mechanisms against G1. Object parents and dynamic names would still need
  `__getattr__`.
- **`_super` through a context variable** set around each bound call, which keeps `self is o`. Rejected
  *(proposal)*: the cursor is lost in closures, callbacks and generators that outlive the call, and a stale cursor
  would resolve silently wrong (anti-goal A2).
- **Persisted method source**: rejected by anti-goal A1.

## 12. Acceptance tests (outline for the plan)

- One resolution: `o.x` and `o["x"]` agree for:
  - own data, parent data and registry data;
  - a registry method;
  - a computed name;
  - a miss (`AttributeError` and `KeyError`, both `ProtoNotFoundError`).
- C3 order on a diamond of registry names, with each name visited once. An inconsistent hierarchy raises.
- An extension chain across three modules:
  - `_super` reaches each definition once, in load order;
  - `self.x` inside a method finds the newest override.
- A data default overridden by a later module and by the instance.
- Catalog data after the registry chain (Q1).
- Symbols: two concurrent transactions create the same symbol; both commit and `__currentSymbols` holds one entry.
  An aborted transaction leaves none.
- Two concurrent installs of different modules both land. A module outside the allowed set is refused.
- A registry/module-list mismatch raises (Q2).
- A `__` root of the wrong type raises `ProtoCorruptionError`.
- A format-4 reader refuses a format-5 segment.
- Resolution cost against history depth: flat, in the tutorial measurement.
- Plants for each rule above.

## 13. Open questions for the maintainer

- **Q1. Order of the catalog class object.**
  - The 2026-11-08 decision puts the catalog's own data after the registry chain, so modules can redefine it.
  - Today's statement reads "the parent chain, and finally the registry".
  - Should the registry come last, with the catalog's data before it, or the catalog's data last, as decided on
    2026-11-08?
- **Q2. Mismatch handling.** On a mismatch between the process's registry and the transaction's `__currentModules`:
  - always an explicit error (decision 3 as written); or
  - rebuild automatically when every module of the new list is importable and allowed, and raise only otherwise.
- **Q3. `_super` identity.** Is it acceptable that `self` inside a method is a view equal to `o` but not `o`
  (section 6)?
- **Q4. Callable data defaults.** May an extension define a non-method callable, such as a computed default, which
  resolves as a callable? Or must every callable be a method bound to the receiver?
- **Q5. Names.** The names `proto_db.Object`, `__catalog`, `install_module` and `_with` are proposals.
- **Q6. How a registry name gets parents.** C3 needs each node's parents, and `extends("crm.Opportunity")` names a
  single registry name. Two candidates:
  - (a) An extension may declare parents, `extends("crm.Lead", parents=("mail.Thread",))`, as Odoo's multiple
    `_inherit` does. A name's parents are then the union in load order (*proposal*).
  - (b) The parents of the name's catalog class object.

  Under (a), the catalog class object is a node right after its own name's chain and before that name's parents, so
  it appears once.
- **Q7. The purpose of `__currentSymbols`.** Under option c2 a symbol is fully identified by `{"@sym": name}`, so the
  table no longer serves identity. Keep it as an enumerable catalog for tooling and protoBaseST, which costs a scan
  of the written values at commit to add every new symbol? Or drop it? A reader would never depend on the table to
  resolve a symbol either way.

## 14. Calls made after the builder check (proposals, for review)

1. **Record layout.** An `Object` persists two fields:
   - `a`: a map from `@sym` to value, the own attributes;
   - `p`: the parents tuple.

   Symbols are persisted, not only held in memory. `Object` reuses `Atom`'s lazy loading for these two fields
   alone. Its attributes are never Python instance attributes.
2. **Load-order ties.** The topological sort breaks ties by installation order, the order persisted in
   `__currentModules`. Every process therefore resolves a chain the same way.
3. **`__catalog` writes.** `install_module` adds, by add-if-absent at commit, a catalog class object for each
   registry name that the module's extensions introduce. The catalog object's data is then ordinary application
   data, written by normal transactions under normal conflict rules. Resolving a registry name that has no catalog
   entry skips the catalog step; it is not an error, because the chain may be pure code.
4. **Registry roots and the read set.** Reading `__currentModules` or `__catalog` for resolution does not enter the
   read set, so installs never conflict with ordinary work. This is safe because installs are additive (decision 4):
   a transaction that resolved with the older list committed a state that the older list described.
   - The process's registry is keyed by the **content** of the module list (the ordered names), not by the root's
     pointer. An add-if-absent merge then never makes the installing process mismatch itself.
   - The private write path for `__` roots is internal to `proto_db.registry`.
5. **Symbols and strings.** A `Symbol` is not a `str`, is never equal to one and does not hash like one, matching
   the format spec's warning that they are different keys. Only `o["name"]` and `o.name` convert a `str` name to its
   symbol, for attribute lookup. A read never creates a symbol in the database: a lookup miss interns the symbol in
   the process only.
6. **Reserved names.** `Atom`'s public members `oid`, `atom_pointer` and `context` are reserved attribute names for
   `Object` and are refused. `o.oid` is the atom's `oid`.
7. **Equality and cache keys.** An `Object` is an immutable atom compared by `oid` (D10), not by content. The
   `_super` view copies that.
   - The linearization cache uses one of two keys:
     - **parent tuples of saved atoms only:** keyed by the parents' pointers, process-wide;
     - **any parent unsaved, or any `MutableObject` parent:** keyed by the tuple object for the life of one
       transaction, because a mutable parent's current value can change between transactions.
8. **Dependencies.** At load time the code's `depends` wins. The dependencies persisted in `__currentModules` are a
   record of the install. A dependency the code declares that the list lacks is installed by the next
   `install_module`. Opening the database raises `ProtoNotSupportedError` until then. `install_module` imports the
   module inside the transaction to learn its dependencies.
9. **Modules of other languages.** By the 2026-11-08 decision, every module in the list has a version in each
   language that opens the database. A Python process that cannot import a listed module raises
   `ProtoNotSupportedError` naming it. The configured mapping (section 7) is the way to point a name at the Python
   version.
10. **Defaults are canonical.** Extension data defaults pass through `canonical()` when the module registers them. A
    mutable Python list given as a default becomes an immutable value, so no state is shared between the objects
    that inherit it.
