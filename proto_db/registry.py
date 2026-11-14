"""The runtime registry and its modules, in memory (runtime registry, spec sections 6 to 8).

A **module** is declared from inside the Python module that implements it::

    from proto_db.registry import module

    crm = module("crm", depends=("base", "contacts"))

    @crm.extends("crm.Opportunity", parents=("mail.Thread",))
    class Opportunity:
        stage = "new"                      # a data default

        def win(self, tr):                 # a method
            return self.with_(stage="won")

Extensions
    The decorated class is only a container: it is returned unchanged, never instantiated, and
    its Python MRO is not used (it may not have a base class).  Its attributes are the
    extension's **definitions**: a plain function is a method (kind ``"method"``); every other
    attribute is a data default (kind ``"data"``) and passes through
    :func:`proto_db.values.canonical`, so a mutable list becomes an immutable value (spec 14
    item 10).  Refused at declaration, with ``ProtoValidationError`` naming the module, the
    registry name and the definition (spec Q4): a ``property``, a ``staticmethod``, a
    ``classmethod``, a callable instance (or any callable that is not a plain function), a
    lambda (recognized by its ``__name__``: a guard against the common mistake, not a security
    boundary), a nested class, a data default holding an atom at any depth (ruling R166: a default
    is shared by every object that inherits it), a name starting with ``_``, one of
    :data:`~proto_db.core.atom.RESERVED_NAMES` and a name of the ``DBObject`` API (computed from
    the class).  The names Python's class machinery puts in every class body (``__module__``,
    ``__doc__``, ``__firstlineno__``, ...) are not definitions.  ``extends`` may only be called
    from the Python module that declared the module.

Declarations
    ``module(name)`` records the declaration under ``name`` in the process, with its call site
    (the Python module, the calling code object and the line).  The same site run again (a
    re-import or ``importlib.reload``, or a loop), or a new execution of the same module scope
    (a reload after an edit moved the call), replaces the earlier declaration, with no
    extensions, so a reload never duplicates them; a registry built earlier keeps what it was
    built from.  A second call from another line of the same execution, from another scope or
    from another Python module is ``ProtoValidationError`` naming both sites: it would silently
    drop the first declaration's extensions.

    Declarations are process-global by module name (ruling R167): one process maps one registry
    module name to one Python module.  Two runtimes in one process whose ``mapping`` points the
    same registry module name at different Python modules are not supported.

The registry
    :meth:`Registry.build` imports each module (``importlib.import_module(mapping.get(name,
    name))``) in the order given, reads the declarations, and orders the modules topologically
    over the dependencies the code declares, ties broken by the position in the list given (the
    installation order, spec 14 item 2).  A built :class:`Registry` is immutable, so any number
    of threads may read it.  For each registry name it keeps the **chain** of extensions, newest
    first (the module loaded last first; within one module, the class declared last first), and
    the **parents** of the name: the union of the ``parents=`` its extensions declare, in load
    order, each name once (spec Q6).  It indexes each method's code object to its extension
    (for ``_super``, spec section 6), so one function object, or one code object, behind two
    definitions anywhere in the registry is refused.  A registry name no module extends is an
    empty node for resolution (no definitions, no parents), so a misspelt parent name is not an
    error there; :meth:`Registry.unknown_names` lists such names, for tooling to flag typos.

Errors (all explicit)
    - a module outside ``allowed``, a repeated or invalid name, a dependency cycle, a cycle of
      registry-name parents: ``ProtoValidationError``;
    - a module that cannot be imported, that declares no registry module of its name, or that
      depends on a module that is not in the list (the code's ``depends`` wins, spec 14 item 8):
      ``ProtoNotSupportedError`` naming the module;
    - a ``ProtoDBError`` raised while a module is imported (a refused definition) propagates
      unchanged, with a note naming the module.

Persisted state (spec section 3)
    The registry state of a database is two of its roots, each identified by its name **and** by
    the type of its value; a ``__`` root whose value is of another type is refused with
    ``ProtoCorruptionError``, never read as registry state (:func:`registry_root`):

    - :data:`MODULES_ROOT` (``__currentModules``) holds a :class:`ModuleList`: the installed
      module names in installation order, each with the dependencies its code declared when it
      was installed (a record of the install; at load time the code's ``depends`` wins, spec 14
      item 8);
    - :data:`CATALOG_ROOT` (``__catalog``) holds a :class:`Catalog`: registry name to its
      **catalog object** (spec Q1, 14 item 3).  The catalog object of the registry name ``N`` is
      a plain ``DBObject`` whose parents are exactly ``(N,)``, created once, by the install that
      first introduces ``N`` (a name some extension of the module ``extends``).  An object that
      lists it as a parent therefore finds it before ``N`` in its linearization, and the catalog
      object's own data before the registry's.  Its data is ordinary application data (ruling
      R189): ``Transaction.set_catalog_object(N, obj)`` replaces the entry with ``obj``, a new
      version (a plain ``DBObject``, ``N`` still among its persisted parents; a version has a new
      ``oid``, D18, ruling R190), under normal conflict rules (a concurrent update of the same
      entry conflicts; the first updater wins).  An entry is never removed.

    Both are records of built-in types that use the format feature ``registry`` (version 5).

Installing and the commit (spec section 7, 14 items 3 and 4)
    ``Transaction.install_module(name)`` imports the module to learn its ``depends``
    (:func:`install_plan`), adds the modules the list lacks, dependencies first, and a catalog
    object for each registry name that has none; it builds the resulting list's registry once
    to refuse a list that could never be loaded.  Nothing of a refused install stays pending.
    At commit both roots merge **add-if-absent against the root current at commit time**
    (:func:`merged_module_list`, :func:`merged_catalog`): never through the read set, never last
    writer wins.  Two concurrent installs of different modules both land, and the list's order
    is the commit order: the merge never makes installs conflict with each other, nor with
    updates of other catalog entries.  (A commit can still lose the compare-and-swap of a storage
    whose root lock is not global, ``CloudStorage`` across processes, like any commit; it then
    raises ``ProtoConflictError`` and ``Database.run`` retries it, merging onto the winner.)

    No module is imported under the root lock (review I-1 of Task 7): the import-time code of a
    module may itself commit, and would wait for the lock its importer holds.  When the merged
    list names a module this process has not fully imported, the commit's build publishes
    nothing; the commit then builds that list's registry outside the lock, importing what it
    lacks, and runs its lock step again (:meth:`Registry.build` with ``imports=False`` is the
    check made under the lock).  Every retry has strictly more of the list imported, so they are
    bounded by the modules installed concurrently.  Reading the registry roots for resolution
    (``installed_modules``, ``catalog_object``) never enters the read set: installs are additive,
    so a transaction that resolved with the older list committed a state that list described.

The current registry
    Resolution reads the registry of the running code from :func:`current_registry` when none is
    passed to it.  Until the registry is attached to transactions (Task 8), it is set by
    :func:`using`, a context manager meant for tests.  It is a :mod:`contextvars` variable, not a
    thread-local (ruling R169): it follows the code across ``asyncio`` tasks and is restored on
    exit even when ``using`` blocks nest.  A new thread starts from an empty context, unless the
    interpreter copies the starting thread's context into it (``sys.flags.thread_inherit_context``,
    on by default on free-threaded builds of Python 3.14), in which case it sees the registry that
    was current when the thread started.
"""
from __future__ import annotations

import contextlib
import importlib
import sys
import threading
import types
from collections.abc import Container, Mapping

from .collections.base import _Immutable
from .collections.dictionaries import Dictionary
from .core.atom import RESERVED_NAMES, Atom, PersistenceContext
from .errors import ProtoConflictError, ProtoCorruptionError, ProtoDBError, ProtoNotSupportedError, ProtoValidationError
from . import resolution as _resolution
from .values import canonical, check_text, is_atom

__all__ = ["module", "ModuleHandle", "Extension", "Registry", "METHOD", "DATA", "using",
           "current_registry", "ModuleList", "Catalog", "MODULES_ROOT", "CATALOG_ROOT"]

#: The root holding the installed modules (a :class:`ModuleList`).
MODULES_ROOT = "__currentModules"
#: The root holding the catalog objects by registry name (a :class:`Catalog`; spec Q5).
CATALOG_ROOT = "__catalog"
_REGISTRY_FEATURE = frozenset({"registry"})

#: The kinds of a definition.
METHOD = "method"
DATA = "data"

#: Names Python's class machinery puts in a class body; they are not definitions.
_CLASS_MACHINERY = frozenset({
    "__module__", "__qualname__", "__doc__", "__dict__", "__weakref__", "__firstlineno__",
    "__static_attributes__", "__annotations__", "__annotate__", "__annotate_func__",
    "__annotations_cache__", "__classcell__", "__orig_bases__", "__parameters__", "__type_params__",
})

#: The declarations of the process, by registry module name; guarded by ``_LOCK``, which is never
#: held while a module is imported.
_DECLARATIONS: dict[str, "ModuleHandle"] = {}
_LOCK = threading.Lock()

_API_NAMES: frozenset | None = None


def _api_names() -> frozenset:
    """The public members of ``DBObject`` and its bases, and the reserved names: computed from the
    classes (on first use, as ``proto_db.core.dbobject`` imports the resolution layer that will
    import this module), so the list never drifts from the API."""
    global _API_NAMES
    if _API_NAMES is None:              # an unlocked race computes the same value twice: benign
        from .core.dbobject import DBObject
        _API_NAMES = frozenset(
            name for cls in DBObject.__mro__ if cls is not object for name in vars(cls)
            if not name.startswith("_")) | RESERVED_NAMES
    return _API_NAMES


def _check_name(what: str, name) -> str:
    if type(name) is not str or not name:
        raise ProtoValidationError(f"{what} must be a non-empty str, not {name!r}")
    return check_text(name)


def _check_names(what: str, names, *, exclude: str | None = None, itself: str = "") -> tuple:
    """``names`` (a tuple or list of non-empty ``str``) as a tuple, each once; ``exclude`` among
    them is ``ProtoValidationError`` with the message ``itself``."""
    if not isinstance(names, (tuple, list)):
        raise ProtoValidationError(f"{what} must be a tuple of names, not {type(names).__name__}")
    seen = set()
    for name in names:
        _check_name(f"Each of {what}", name)
        if name in seen:
            raise ProtoValidationError(f"{what}: {name!r} appears more than once")
        seen.add(name)
    if exclude is not None and exclude in seen:
        raise ProtoValidationError(itself)
    return tuple(names)


def _caller_module(depth: int = 2) -> str | None:
    return sys._getframe(depth).f_globals.get("__name__")


def module(name: str, depends: tuple = ()) -> "ModuleHandle":
    """Declare the registry module ``name``, which depends on the modules ``depends``, and return
    the handle whose :meth:`ModuleHandle.extends` declares its extensions.  Called at import time
    by the Python module that implements it."""
    _check_name("A module name", name)
    depends = _check_names(f"The dependencies of module {name!r}", depends, exclude=name,
                           itself=f"Module {name!r} cannot depend on itself")
    frame = sys._getframe(1)
    source = frame.f_globals.get("__name__")
    site = _Site(source, frame.f_code, frame.f_lineno)
    handle = ModuleHandle(name, depends, source, site)
    with _LOCK:
        earlier = _DECLARATIONS.get(name)
        if earlier is not None and not earlier.site.replaced_by(site):
            raise ProtoValidationError(
                f"Module {name!r} is already declared at {earlier.site}; it cannot be declared again "
                f"at {site} (one registry module name has one declaration in a process)")
        if earlier is not None:
            earlier._superseded = True
        _DECLARATIONS[name] = handle
    return handle


class _Site:
    """Where :func:`module` was called: the Python module, the calling code object and the line."""

    __slots__ = ("source", "code", "line")

    def __init__(self, source, code, line):
        self.source = source
        self.code = code
        self.line = line

    def __str__(self) -> str:
        return f"{self.code.co_filename}:{self.line} (Python module {self.source!r})"

    def replaced_by(self, later: "_Site") -> bool:
        """Whether a declaration made here may be replaced by one made at ``later``: a call of
        the same scope (file and qualified name) of the same Python module, either at the same
        line (the same statement run again: a loop, or a reload of an unchanged file) or from a
        new code object (a new execution of the module: a reload, even after an edit moved the
        call).  A second call from another line of the same execution is a duplicate, which
        would drop the first declaration's extensions."""
        if (later.source != self.source or later.code.co_filename != self.code.co_filename
                or later.code.co_qualname != self.code.co_qualname):
            return False
        return later.line == self.line or later.code is not self.code


class _Declared:
    """An extension as declared: validated definitions, before a registry places it."""

    __slots__ = ("registry_name", "parents", "definitions", "qualname")

    def __init__(self, registry_name, parents, definitions, qualname):
        self.registry_name = registry_name
        self.parents = parents
        self.definitions = definitions
        self.qualname = qualname


class ModuleHandle:
    """The declaration of one registry module (returned by :func:`module`)."""

    def __init__(self, name: str, depends: tuple, source: str | None, site: "_Site"):
        self.name = name
        self.depends = depends
        #: The Python module that declared it.
        self.source = source
        #: Where :func:`module` was called (it keeps the calling code object alive, so a new code
        #: object is never mistaken for it).
        self.site = site
        self._declared: list[_Declared] = []
        self._superseded = False

    def __repr__(self) -> str:
        return f"<registry module {self.name!r} of {self.source!r}>"

    def extends(self, registry_name: str, *, parents: tuple = ()):
        """A class decorator: the class's attributes become an extension of ``registry_name``,
        which gains the registry-name parents ``parents``."""
        _check_name(f"The registry name of an extension in module {self.name!r}", registry_name)
        parents = _check_names(f"The parents of {registry_name!r} in module {self.name!r}",
                               parents, exclude=registry_name,
                               itself=f"{registry_name!r} cannot be its own parent")
        caller = _caller_module()
        if caller != self.source:
            raise ProtoValidationError(
                f"Extensions of module {self.name!r} are declared by its Python module "
                f"{self.source!r}, not from {caller!r}")
        if self._superseded:
            raise ProtoValidationError(
                f"This handle of module {self.name!r} was replaced by a later declaration (a reload)")

        def decorate(cls):
            declared = _Declared(registry_name, parents, self._definitions(registry_name, cls),
                                 getattr(cls, "__qualname__", repr(cls)))
            with _LOCK:
                self._declared.append(declared)
            return cls

        return decorate

    def _definitions(self, registry_name: str, cls) -> types.MappingProxyType:
        where = f"Module {self.name!r}, extension of {registry_name!r}"
        if type(cls) is not type:
            raise ProtoValidationError(f"{where}: extends decorates a plain class, not {cls!r}")
        if cls.__bases__ != (object,):
            raise ProtoValidationError(
                f"{where}: the class {cls.__qualname__!r} may not have a base class (its Python MRO "
                f"is not used; declare registry-name parents with parents=)")
        api = _api_names()
        definitions = {}
        for name, value in vars(cls).items():
            if name in _CLASS_MACHINERY:
                continue
            here = f"{where}: definition {name!r}"
            if name.startswith("_"):
                raise ProtoValidationError(f"{here}: a name starting with '_' is not a definition")
            if name in api:
                raise ProtoValidationError(f"{here}: the name is part of the DBObject API")
            definitions[name] = _definition(here, value)
        return types.MappingProxyType(definitions)


def _definition(here: str, value) -> tuple:
    """``(kind, value)`` for one definition; ``ProtoValidationError`` for a refused kind."""
    if type(value) is types.FunctionType:
        # A guard against the common mistake (``x = lambda self: ...``), not a security boundary: a
        # lambda renamed through ``__name__`` passes, and then behaves as any method.
        if value.__name__ == "<lambda>":
            raise ProtoValidationError(f"{here}: a lambda is refused; define a named function")
        return METHOD, value
    for kind in (property, staticmethod, classmethod):
        if isinstance(value, kind):
            raise ProtoValidationError(f"{here}: a {kind.__name__} is refused; define a plain function")
    if isinstance(value, type):
        raise ProtoValidationError(f"{here}: a nested class is refused")
    if callable(value):
        raise ProtoValidationError(
            f"{here}: a callable that is not a plain function ({type(value).__name__}) is refused")
    try:
        value = canonical(value)
    except ProtoValidationError as e:
        if type(e) is not ProtoValidationError:
            raise
        raise ProtoValidationError(f"{here}: {e}") from e
    found = _atom_in(value)
    if found is not None:
        raise ProtoValidationError(
            f"{here}: a data default may not hold an atom (found {type(found).__name__}); a default is "
            f"shared by every object that inherits it (R166)")
    return DATA, value


def _atom_in(value):
    """The first atom in the canonical value ``value`` (itself, or inside its tuples, lists and
    maps, keys included, at any depth), or ``None``; iterative."""
    stack = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, Atom) or is_atom(item):
            return item
        if isinstance(item, (tuple, list)):
            stack.extend(item)
        elif isinstance(item, dict):
            for pair in item.items():
                stack.extend(pair)
    return None


class Extension:
    """One extension of a registry name in a built :class:`Registry` (immutable).

    ``registry_name``, ``module`` (the registry module that declares it), ``position`` (its index
    in :meth:`Registry.chain` of its name, ``0`` for the newest), ``qualname`` (the container
    class's) and ``definitions`` (a read-only mapping ``name -> (kind, value)``, where kind is
    :data:`METHOD` for a plain function or :data:`DATA` for a canonical value)."""

    __slots__ = ("registry_name", "module", "position", "definitions", "qualname")

    def __init__(self, registry_name, module_name, position, definitions, qualname):
        osa = object.__setattr__
        osa(self, "registry_name", registry_name)
        osa(self, "module", module_name)
        osa(self, "position", position)
        osa(self, "definitions", definitions)
        osa(self, "qualname", qualname)

    def __setattr__(self, name, value):
        raise AttributeError(f"Extension is immutable; cannot set {name!r}")

    def __delattr__(self, name):
        raise AttributeError(f"Extension is immutable; cannot delete {name!r}")

    def __repr__(self) -> str:
        return (f"<Extension {self.registry_name!r} of module {self.module!r} at {self.position}: "
                f"{', '.join(self.definitions)}>")


class Registry:
    """The registry of a module list, in memory: its definitions are immutable once built (see the
    module docstring); it carries only an internal, idempotent memo of checks already passed."""

    __slots__ = ("key", "extensions", "_chains", "_parents", "_codes", "_checked")

    def __init__(self, key, extensions, chains, parents, codes):
        osa = object.__setattr__
        #: The module names in load order.
        osa(self, "key", key)
        #: Every extension, in load order (by module, then by declaration).
        osa(self, "extensions", extensions)
        osa(self, "_chains", chains)
        osa(self, "_parents", parents)
        osa(self, "_codes", codes)
        #: Resolution's memo of the (class, registry name) pairs whose Python members it checked
        #: (internal; only ever added to, so the registry's answers never change).
        osa(self, "_checked", {})

    def __setattr__(self, name, value):
        raise AttributeError(f"Registry is immutable; cannot set {name!r}")

    def __delattr__(self, name):
        raise AttributeError(f"Registry is immutable; cannot delete {name!r}")

    def __repr__(self) -> str:
        return f"<Registry {self.key!r}>"

    def chain(self, registry_name: str) -> tuple:
        """The extensions of ``registry_name``, newest first (``()`` for a name none extends)."""
        return self._chains.get(registry_name, ())

    def parents_of(self, registry_name: str) -> tuple:
        """The registry-name parents of ``registry_name``: the union of its extensions'
        ``parents=``, in load order, each once."""
        return self._parents.get(registry_name, ())

    def unknown_names(self, names) -> tuple:
        """The names among ``names`` (registry names, in the order given, each once) that no module
        of this registry extends.  Resolution treats such a name as an empty node (it contributes
        nothing and has no parents), so a misspelt parent name is silent there; tooling and tests
        use this to flag it."""
        return tuple(name for name in dict.fromkeys(names) if name not in self._chains)

    def extension_of_code(self, code) -> Extension | None:
        """The extension whose method has the code object ``code``, or ``None``."""
        return self._codes.get(code)

    @classmethod
    def build(cls, module_names: tuple, *, mapping: Mapping | None = None,
              allowed: Container | None = None, imports: bool = True) -> "Registry":
        """Import the modules ``module_names`` (in installation order) and build their registry.

        With ``imports=False`` nothing is imported: a module that is not already fully imported
        raises :class:`ModulesNotImported` (internal, not a ``ProtoDBError``), naming the modules
        to import first.  The commit uses it under the root lock, where an import may deadlock."""
        if not isinstance(module_names, tuple):
            raise ProtoValidationError(
                f"module_names must be a tuple of module names, not {type(module_names).__name__}")
        names = _check_names("The module list", module_names)
        for name in names:
            if allowed is not None and name not in allowed:
                raise ProtoValidationError(f"Module {name!r} is not allowed by this runtime's configuration")
        mapping = {} if mapping is None else mapping
        if imports:
            sources = {name: _import(name, mapping.get(name, name)) for name in names}
        else:
            sources = {name: _imported(mapping.get(name, name)) for name in names}
            missing = tuple(name for name in names if sources[name] is None)
            if missing:
                raise ModulesNotImported(missing)
        with _LOCK:
            handles = {name: _DECLARATIONS.get(name) for name in names}
            declared = {name: tuple(handle._declared) for name, handle in handles.items()
                        if handle is not None}
        for name in names:
            handle = handles[name]
            if handle is None or handle.source != sources[name]:
                raise ProtoNotSupportedError(
                    f"Module {name!r}: the Python module {sources[name]!r} declares no registry module "
                    f"named {name!r}")
            for dependency in handle.depends:
                if dependency not in handles:
                    raise ProtoNotSupportedError(
                        f"Module {name!r} depends on {dependency!r}, which is not installed")
        order = _load_order(names, {name: handles[name].depends for name in names})
        return _assemble(order, declared)


def _import(name: str, path) -> str:
    """Import ``path`` for the module ``name``; return the imported module's ``__name__``."""
    try:
        imported = importlib.import_module(path)
    except ProtoDBError as e:
        e.add_note(f"while importing {path!r} for registry module {name!r}")
        raise
    except Exception as e:
        raise ProtoNotSupportedError(f"Module {name!r} cannot be imported from {path!r}: {e}") from e
    return imported.__name__


class ModulesNotImported(Exception):
    """Internal signal of :meth:`Registry.build` with ``imports=False``: ``names`` are modules this
    process has not fully imported.  The commit catches it under the root lock and imports them
    outside it; it never reaches an application."""

    def __init__(self, names: tuple):
        super().__init__(f"Modules not imported yet: {', '.join(names)}")
        self.names = names


def _imported(path) -> str | None:
    """The ``__name__`` of the module imported as ``path``, or ``None`` when it is not imported or
    its import is still running (in any thread): its declarations may be incomplete."""
    found = sys.modules.get(path)
    if found is None or getattr(getattr(found, "__spec__", None), "_initializing", False):
        return None
    return found.__name__


def fully_imported(names: tuple, mapping: Mapping) -> bool:
    """Whether every module of ``names`` is imported, its import finished."""
    return all(_imported(mapping.get(name, name)) is not None for name in names)


def _load_order(names: tuple, depends: dict) -> tuple:
    """``names`` in topological order over ``depends``, ties broken by the position in ``names``."""
    placed, order = set(), []
    pending = list(names)
    while pending:
        ready = next((name for name in pending if all(d in placed for d in depends[name])), None)
        if ready is None:
            cycle = _cycle(pending, depends)
            raise ProtoValidationError(f"Module dependency cycle: {' -> '.join(cycle)}")
        pending.remove(ready)
        placed.add(ready)
        order.append(ready)
    return tuple(order)


def _cycle(nodes, edges: dict) -> list | None:
    """A cycle of ``edges`` among ``nodes`` (edges leaving ``nodes`` are ignored), as
    ``[a, b, ..., a]``, or ``None``; an iterative depth-first walk, nodes in the order given."""
    inside = set(nodes)
    state = {}                          # 1: on the current path, 2: done
    for root in nodes:
        if root in state:
            continue
        path, stack = [root], [iter(edges.get(root, ()))]
        state[root] = 1
        while stack:
            child = next((c for c in stack[-1] if c in inside and state.get(c) != 2), None)
            if child is None:
                state[path.pop()] = 2
                stack.pop()
            elif state.get(child) == 1:
                return path[path.index(child):] + [child]
            else:
                state[child] = 1
                path.append(child)
                stack.append(iter(edges.get(child, ())))
    return None


def _assemble(order: tuple, declared: dict) -> Registry:
    by_name: dict[str, list] = {}
    for name in order:
        for item in declared.get(name, ()):
            by_name.setdefault(item.registry_name, []).append((name, item))
    chains, parents, codes, seen, placed = {}, {}, {}, {}, {}
    for registry_name, items in by_name.items():
        chain = tuple(Extension(registry_name, module_name, position, item.definitions, item.qualname)
                      for position, (module_name, item) in enumerate(reversed(items)))
        chains[registry_name] = chain
        for (_, item), extension in zip(reversed(items), chain):
            placed[id(item)] = extension
        union = {}
        for _, item in items:
            union.update(dict.fromkeys(item.parents))
        parents[registry_name] = tuple(union)
    extensions = []
    for name in order:
        for item in declared.get(name, ()):
            extension = placed[id(item)]
            extensions.append(extension)
            for definition, (kind, value) in item.definitions.items():
                if kind is not METHOD:
                    continue
                here = (f"module {name!r}, extension of {item.registry_name!r}, "
                        f"definition {definition!r}")
                for key, what in ((value, "function object"), (value.__code__, "code object")):
                    earlier = seen.get(key)
                    if earlier is not None:
                        raise ProtoValidationError(
                            f"One {what} is used by two definitions ({earlier} and {here}); "
                            f"_super could not tell which extension runs")
                    seen[key] = here
                codes[value.__code__] = extension
    _check_parent_cycles(parents)
    return Registry(order, tuple(extensions), chains, parents, codes)


def _check_parent_cycles(parents: dict) -> None:
    """A registry name may not be its own ancestor through the parents its extensions declare."""
    cycle = _cycle(tuple(parents), parents)
    if cycle is not None:
        raise ProtoValidationError(f"Registry-name parent cycle: {' -> '.join(cycle)}")


def current_registry() -> "Registry | None":
    """The registry resolution uses when none is passed to it (``None`` outside :func:`using`)."""
    return _resolution.current_registry()


def _declared_method_of_code(code) -> str | None:
    """A description of the declared method whose code object is ``code`` (in any module declared
    in the process, built into a registry or not), or ``None``: for error messages only."""
    with _LOCK:
        handles = tuple(_DECLARATIONS.values())
        declared = [(handle.name, tuple(handle._declared)) for handle in handles]
    for module_name, items in declared:
        for item in items:
            for name, (kind, value) in item.definitions.items():
                if kind is METHOD and value.__code__ is code:
                    return f"method {name!r} of registry module {module_name!r} (extension of {item.registry_name!r})"
    return None


@contextlib.contextmanager
def using(registry: "Registry | None"):
    """Make ``registry`` the current registry for the body of the ``with`` block (``None`` makes
    no registry current); the previous one is restored on exit.  Meant for tests until the
    registry is attached to transactions (Task 8)."""
    if registry is not None and not isinstance(registry, Registry):
        raise ProtoValidationError(f"using() takes a Registry or None, not {type(registry).__name__}")
    token = _resolution.set_current_registry(registry)
    try:
        yield registry
    finally:
        _resolution.reset_current_registry(token)


# -- persisted state ---------------------------------------------------------------------------

def _corrupt(root: str, detail: str) -> ProtoCorruptionError:
    return ProtoCorruptionError(f"Registry root {root!r}: {detail}")


class ModuleList(_Immutable, Atom):
    """The value of :data:`MODULES_ROOT`: ``modules``, a tuple of ``(name, depends)`` pairs in
    installation order, ``depends`` a tuple of module names (see the module docstring)."""

    __type_name__ = "proto_db.registry.ModuleList"
    _required_fields = ("modules",)
    _format_features = _REGISTRY_FEATURE

    def __init__(self, *, modules: tuple = (), context: PersistenceContext | None = None):
        """A module list of the ``(name, depends)`` pairs ``modules``."""
        super().__init__(context=context)
        object.__setattr__(self, "modules", tuple((name, tuple(depends)) for name, depends in modules))

    def entries(self) -> tuple:
        """The ``(name, depends)`` pairs, in installation order, checked: anything but a tuple of
        pairs of a non-empty ``str`` and a tuple of non-empty ``str``, or a name listed twice, is
        ``ProtoCorruptionError``."""
        modules = self.modules
        if type(modules) is not tuple:
            raise _corrupt(MODULES_ROOT, f"modules is a {type(modules).__name__}, not a tuple")
        seen = set()
        for entry in modules:
            if (type(entry) is not tuple or len(entry) != 2 or type(entry[0]) is not str or not entry[0]
                    or type(entry[1]) is not tuple
                    or any(type(d) is not str or not d for d in entry[1])):
                raise _corrupt(MODULES_ROOT, f"{entry!r} is not a (module name, dependencies) pair")
            if entry[0] in seen:
                raise _corrupt(MODULES_ROOT, f"module {entry[0]!r} is listed twice")
            seen.add(entry[0])
        return modules

    def names(self) -> tuple:
        """The installed module names, in installation order."""
        return tuple(name for name, _ in self.entries())


class Catalog(_Immutable, Atom):
    """The value of :data:`CATALOG_ROOT`: ``entries``, a
    :class:`~proto_db.collections.dictionaries.Dictionary` from registry name to its catalog
    object (see the module docstring)."""

    __type_name__ = "proto_db.registry.Catalog"
    _required_fields = ("entries",)
    _format_features = _REGISTRY_FEATURE

    def __init__(self, *, entries: Dictionary, context: PersistenceContext | None = None):
        """A catalog over the ``entries`` dictionary."""
        super().__init__(context=context)
        object.__setattr__(self, "entries", entries)

    def _entries(self) -> Dictionary:
        entries = self.entries
        if not isinstance(entries, Dictionary):
            raise _corrupt(CATALOG_ROOT, f"entries is a {type(entries).__name__}, not a Dictionary")
        return entries

    def get(self, registry_name: str):
        """The catalog object of ``registry_name``, or ``None``.  An entry that is not a plain
        ``DBObject`` (exactly that type) whose persisted parents include its name is
        ``ProtoCorruptionError``."""
        from .core.dbobject import DBObject
        found = self._entries().get_at(registry_name, None)
        if found is None:
            return None
        if type(found) is not DBObject or registry_name not in found._persisted_parents():
            raise _corrupt(CATALOG_ROOT, f"the entry of {registry_name!r} is not a DBObject with that registry "
                                         f"name among its parents")
        return found

    def names(self) -> tuple:
        """The registry names that have a catalog object, in key order."""
        return tuple(self._entries().keys())


def registry_root(objects, name: str, cls: type):
    """The value of the registry root ``name`` in the roots dictionary ``objects``, or ``None``
    when it is absent; a value that is not a ``cls`` is ``ProtoCorruptionError``."""
    value = objects.get_at(name, None)
    if value is not None and not isinstance(value, cls):
        raise _corrupt(name, f"its value is a {type(value).__name__}, not a {cls.__type_name__}: it is not "
                             f"registry state")
    return value


def merged_module_list(current: ModuleList | None, additions: Mapping, context) -> ModuleList | None:
    """The module list for ``current`` (the list current at commit, or ``None``) with the
    ``name -> depends`` ``additions`` it lacks appended in their order (add-if-absent), or
    ``None`` when it lacks none of them."""
    entries = current.entries() if current is not None else ()
    present = {name for name, _ in entries}
    added = tuple((name, depends) for name, depends in additions.items() if name not in present)
    if not added:
        return None
    return ModuleList(modules=entries + added, context=context)


def merged_catalog(current: Catalog | None, additions: Mapping, handed_out, context,
                   updates: Mapping | None = None) -> Catalog | None:
    """The catalog for ``current`` (the catalog current at commit, or ``None``) with each
    ``registry name -> catalog object`` of ``additions`` whose name it lacks (add-if-absent) and
    each update of ``updates``, or ``None`` when nothing changes.

    A name it already has keeps its object; when that object is not the one in ``additions``
    and the name is in ``handed_out`` (the transaction gave its own object to the application,
    which may reference it), ``ProtoConflictError``: keeping either silently would leave the
    other unreachable from the catalog.  ``updates`` maps a registry name to ``(pointer, obj)``:
    the entry is replaced by ``obj`` when it is still the record at ``pointer`` (the version the
    transaction's snapshot held), otherwise ``ProtoConflictError`` (normal conflict rules: the
    first updater wins, ruling R189).  Additions never conflict with updates of other names."""
    entries = current._entries() if current is not None else None
    changed = False
    for name, (pointer, obj) in (updates or {}).items():
        existing = current.get(name) if current is not None else None
        if existing is None or existing.atom_pointer != pointer:
            raise ProtoConflictError(f"The catalog object of {name!r} was changed by a concurrent commit")
        entries = entries.set_at(name, obj)
        changed = True
    for name, obj in additions.items():
        existing = current.get(name) if current is not None else None
        if existing is not None:
            if name in handed_out and existing.oid != obj.oid:
                raise ProtoConflictError(
                    f"The catalog object of {name!r} was created by a concurrent install, and this "
                    f"transaction used its own")
            continue
        if entries is None:
            entries = Dictionary(context=context)
        entries = entries.set_at(name, obj)
        changed = True
    return Catalog(entries=entries, context=context) if changed else None


def install_plan(name: str, installed: tuple, mapping: Mapping, allowed: Container | None) -> tuple:
    """``(additions, introduced)`` for installing ``name`` over the list ``installed``.

    The modules reachable from ``name`` through the ``depends`` their code declares are imported
    (spec 14 item 8).  ``additions`` are the ``(name, depends)`` pairs of those not in
    ``installed``, dependencies first; ``introduced`` the registry names their extensions
    declare, each once.  A module outside ``allowed`` or a dependency cycle is
    ``ProtoValidationError``; a module that cannot be imported, or that declares no registry
    module of its name, ``ProtoNotSupportedError``."""
    _check_name("A module name", name)
    present = set(installed)
    handles = {}
    order = []
    state = {}                          # 1: on the current path, 2: placed
    path, stack = [], []

    def enter(module_name):
        handle = _declaration(module_name, mapping, allowed)
        handles[module_name] = handle
        state[module_name] = 1
        path.append(module_name)
        stack.append(iter(handle.depends))

    enter(name)
    while stack:
        child = next((d for d in stack[-1] if state.get(d) != 2), None)
        if child is None:
            done = path.pop()
            stack.pop()
            state[done] = 2
            order.append(done)
        elif state.get(child) == 1:
            cycle = path[path.index(child):] + [child]
            raise ProtoValidationError(f"Module dependency cycle: {' -> '.join(cycle)}")
        else:
            enter(child)
    additions = tuple((m, handles[m].depends) for m in order if m not in present)
    with _LOCK:
        declared = [item.registry_name for m in order for item in handles[m]._declared]
    return additions, tuple(dict.fromkeys(declared))


def _declaration(name: str, mapping: Mapping, allowed: Container | None) -> ModuleHandle:
    """The declaration of the module ``name``, imported through ``mapping``; refused outside
    ``allowed`` (see :func:`install_plan`)."""
    if allowed is not None and name not in allowed:
        raise ProtoValidationError(f"Module {name!r} is not allowed by this runtime's configuration")
    source = _import(name, mapping.get(name, name))
    with _LOCK:
        handle = _DECLARATIONS.get(name)
    if handle is None or handle.source != source:
        raise ProtoNotSupportedError(
            f"Module {name!r}: the Python module {source!r} declares no registry module named {name!r}")
    return handle
