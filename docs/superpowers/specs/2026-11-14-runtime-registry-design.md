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
- **Every attribute name is a symbol (decision 7), with no byte change.** A key of an atom's state map `s` is
  always an attribute name, so the format-4 bytes (a msgpack string in key position) already identify the symbol
  unambiguously. Attribute names are interned symbols in memory and are written as they are today. Registry names
  in a parents tuple are plain strings (Q6). The registry itself therefore needs no `@sym` value (Q7, section 13).
- **Parents: the only new atom field.** An object that has parents writes the envelope key `"p"` (the parents tuple)
  beside `"t"`, `"oid"` and `"s"`. It is the format-5 feature `parents`. An object without parents writes no `"p"`
  and keeps version 4, byte for byte.
- The cluster `PROTOCOL_VERSION` rises in the same change, since cluster messages carry values.

## 4. The object model: `DBObject` gains parents

There is no new class (maintainer, 2026-11-14). The capability is added to `DBObject`. **A `DBObject` without parents,
in a database without modules, is today's base scheme, unchanged**: the same API, bytes and behaviour. Plain data
objects need nothing new.

A `DBObject` holds:
- **its own attributes**: symbol to canonical value. Values only; a callable is never persisted (anti-goal A1);
- **its parents** (new, optional): an ordered tuple, persisted as `"p"` (section 3). Each parent is either:
  - another `DBObject`, referenced like any other atom; or
  - a `str`: a key of the registry (a registry name).

  Nothing else is a parent (maintainer, 2026-11-14, Q6).

Construction: `DBObject(parents=(...), **attributes)` *(proposal)*; `parents` becomes a reserved keyword, like
`context`. `o.with_(...)` keeps the parents. `o.with_parents(...)` returns a version with other parents *(proposal)*.

A **catalog class object** is a `DBObject` stored in `__catalog`. It gathers a class's definition data. It is a root
of the database, not of the object space, so each database has its own catalog.
- An instance lists its catalog class object as a parent (option a).
- The catalog class object lists its registry name among its parents. Lookup therefore continues from it into the
  in-memory registry chain.

Immutability and identity are unchanged: a `DBObject` is an immutable atom compared by `oid` (D10). A stable identity
is a `MutableObject` whose value is a `DBObject`. The versioned entity is a documented pattern (a new version points
at the previous one), not part of the base module.

**Python subclasses of `DBObject`** (today's way to give an object methods) keep working. Their Python members are
found by Python before resolution runs, so they come first. Resolution covers everything else. A module may not
define, in a registry name, a member that a `DBObject` subclass it extends already defines in Python: that is
refused at load with `ProtoValidationError`, because it could never be reached (anti-goal A2). The registry path is
the recommended one for extensible models; subclasses remain for fixed, non-extensible types *(proposal)*.

## 5. Resolution: one rule for fields and methods

`resolve(o, name)` returns the first definition of `name` along the **linearization** of `o`:

1. `o`'s own attributes;
2. then, in order, every node of the C3 linearization of `o`'s parents.
   - A `DBObject` node contributes its own attributes.
   - A registry-name node contributes its extension chain, from the newest extension to the oldest (section 6).
   - Each node is visited once, even when several parents reach it (C3, decided 2026-11-08).

The catalog class object needs no rule of its own (maintainer, 2026-11-14, Q1: the catalog **before** the
registry). It is an ordinary `DBObject` node whose parents include its registry name, so C3 places its data before
that name's chain. The registry is the last thing searched.

What is found:
- **A value** (data on an object, or a data default of an extension) is returned as is.
- **A method** (a function defined in an extension) is returned bound to the receiver `o`, exactly like a Python
  bound method: `self is o`. It is never persisted; it exists only in the process's registry.
- Nothing else is ever computed (maintainer, 2026-11-14, Q4). There are no properties, computed defaults or other
  computation rules. A value that must be computed is a method.

A miss raises:
- `o.name` raises `AttributeError`, which subclasses `ProtoNotFoundError` *(proposal)*. `hasattr`, `getattr` with a
  default and the Python protocols that probe attributes keep working.
- `o["name"]` raises `KeyError`, which subclasses `ProtoNotFoundError` *(proposal)*.

The message names the object, the name and the linearization searched.

The Python surface:
- `o.name` and `o["name"]` run the same resolution. `o["name"]` accepts a `str` or a symbol, so the name may be
  computed at run time (`o[field_name]`). `o.name` covers every name that is a valid identifier.
- The Python-level API is `DBObject`'s own: `with_`, `has_defined_attr`, `oid`, `atom_pointer`, `context`, plus the
  new `parents` and `with_parents`, and the underscore helpers `_own`, `_super`, `_resolve` and `_linearization`
  *(proposal)*. These names are already refused as attribute names today (the `DBObject` rule: no leading `_`, no
  name of a class member). For the same reason a module may not define them in a registry name. So no attribute
  shadows the API and no API member is shadowed by resolution. Dunder names (`__len__`, `__eq__`, ...) never go
  through resolution.
- `name in o` reports whether resolution finds `name`. `o._own` returns the own attributes only.
- Writes never go into a parent. `o.with_(...)` sets own attributes, and `o["x"] = v` is refused: values are
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
        return self.with_(stage="won")
```

- The class is only a container. Its instances are never created. Its Python MRO is not used, and Python's
  `super()` is not the mechanism (it needs `self` to be an instance of the class).
- **What an extension may define (Q4).**
  - Plain functions are methods.
  - Every other attribute is a data default and passes through `canonical()`.
  - Anything else that is callable is refused at load with `ProtoValidationError`: a `property`, a `staticmethod`, a
    `classmethod`, a callable instance, or a lambda assigned as a default.
- **`_super` keeps `self is o`** (maintainer, 2026-11-14, Q3; *proposal for the mechanism*).
  - At load, the registry indexes each method's code object to its extension. A function used in two definitions
    is refused, because it would be ambiguous.
  - `self._super()` reads the caller's code object (`sys._getframe(1).f_code`). That code object names the
    extension that is running, and `_super()` finds the extension's position `i` in `self`'s linearization. It
    returns a transient lookup proxy, as Python's `super()` does, that resolves from position `i + 1` and binds
    what it finds to `self`.
  - `self._super().win(tr)` therefore runs the next `win` with the same `self`, wherever that `win` is: another
    extension of the same name, or a registry name further along.
  - Inside any method, `self.x` resolves from the start (late binding: an override further down still wins).
  - Calling `_super()` outside a registry method raises `ProtoUserError` naming the caller; that includes a closure,
    a helper and a generator body defined elsewhere. So does calling it on an object whose linearization does not
    contain the running extension. It never resolves silently from the wrong place (anti-goal A2).
  - The proxy is never stored, compared or returned as a value; storing it is refused.
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
  - `DBObject` frames are immutable by version.
- Symbols make each own-attribute lookup an identity-keyed dict probe.
- Superseded versions leave the current collections and indexes. Resolution never visits history, so its cost does
  not depend on history depth (anti-goal A4). This is a claim to verify with a ProtoBase-only measurement in the
  tutorial, not a rival benchmark.

## 10. Polyglot

protoBaseST reads and writes the same roots, symbols and the `"p"` field of `DBObject` records.
- The persisted state of sections 3 and 4 is normative and goes into `format-spec.rst` with test vectors.
- Resolution (section 5), `_super` and module loading are each language's own. Smalltalk resolves by its own rules
  over the same parents.
- The format-5 additions must be agreed with protoBaseST before they are frozen.

## 11. Alternatives considered

- **Dynamic Python classes per registry name** (Odoo's approach: `type(name, extensions, {})`): Python's own MRO and
  `super()` would do the chain at native speed. Rejected for this design. Methods would live in the class and data
  in the objects, which is two mechanisms against G1. Object parents and dynamic names would still need
  `__getattr__`.
- **`_super` through a receiver view** `(o, i)` that carries the position (the first draft). Rejected (Q3): `self`
  would be equal to `o` but not `o`.
- **`_super` through a context variable** set around each bound call. Rejected: the cursor is lost in closures,
  callbacks and generators that outlive the call, and a stale cursor would resolve silently wrong (anti-goal A2).
  The code-object lookup fails explicitly in the same cases.
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

## 13. Maintainer decisions (2026-11-14) and the open question

- **Q1, catalog before the registry.** The catalog class object is an ordinary node that comes before its registry
  name, and the registry is searched last (section 5).
- **Q2, a mismatch is an error.** A transaction whose `__currentModules` differs from the list the process's
  registry was built from raises an explicit error. There is no automatic rebuild (section 8, decision 3).
- **Q3, `self is o`.** `_super` finds its position from the running method's code object; there is no receiver
  view (section 6).
- **Q4, only methods.** There are no computation rules. A computed value is a method (sections 5 and 6).
- **Q5, the names are accepted:** `__catalog`, `install_module`, `parents`, `with_parents`.
- **Q6, parents.**
  - A registry name's parents are declared by its extensions: `extends("crm.Lead", parents=("mail.Thread",))`, the
    union in load order.
  - An object's parents are other `DBObject`s or strings, which are keys of the registry.
- **Writes never climb the parents (maintainer, 2026-11-14).** Every write goes to the object itself (section 5).

Open:
- **Q7. Do we need `__currentSymbols` and `@sym` at all?**
  - With Q6 and with attribute names written as today, the registry stores no symbol value. Attribute names are
    strings in key position, and registry names are strings in the parents tuple.
  - The table would therefore hold nothing that any read needs.
  - The remaining use is the 2026-11-08 idea of symbols as a value kind the developer chooses over strings. In
    memory, comparison by identity is faster. On disk, `{"@sym": name}` costs the same bytes as the name or more,
    and it shares nothing.
  - Proposal: drop `__currentSymbols`. Keep `@sym` as a value kind only if the developer-facing choice between
    symbols and strings is still wanted. Otherwise defer it, and the registry needs no symbol feature in the format.

## 14. Calls made after the builder check (proposals, for review)

1. **Record layout.** Superseded by the 2026-11-14 ruling (no new class). The state map `s` is unchanged, with
   attribute names as strings in key position that are read as symbols. The parents are the new envelope key `"p"`.
   Section 3 has the details.
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
6. **Reserved names.** `Atom`'s public members `oid`, `atom_pointer` and `context`, and the `DBObject` API, stay
   refused as attribute names, as they are today, and as registry definitions. `o.oid` is the atom's `oid`.
7. **Equality and cache keys.** A `DBObject` is an immutable atom compared by `oid` (D10), not by content. The
   `_super` view copies that.
   - The linearization cache uses one of two keys:
     - **parent tuples of saved atoms and strings:** keyed by the parents' pointers and names, process-wide;
     - **any parent unsaved:** keyed by the tuple object for the life of one transaction.
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
