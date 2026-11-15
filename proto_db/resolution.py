"""Resolution of attribute names over a DBObject's parents (runtime registry, spec section 5).

``resolve(o, name)`` returns the first definition of ``name`` along the **linearization** of
``o``: ``o``'s own attributes, then each node of the C3 linearization of ``o``'s parents.

Nodes
    A node is a :class:`~proto_db.core.dbobject.DBObject` (it contributes its own attributes,
    including the legacy fields of ruling R157, never its Python class members), an
    :class:`~proto_db.core.atom.OpaqueAtom` (an atom of another implementation's type, ruling
    R161: its own fields as data and its persisted parents; no methods.  It is read through
    ``_probe``, which skips a name its stand-in class defines, so a field named like an
    ``OpaqueAtom`` class member would not be seen through it; ``OpaqueAtom`` and ``Atom`` define
    no public member today, so no field is hidden, but a public member added to either later
    would hide the fields of that name in opaque parents), or a registry name (a ``str``).
    Nodes are identified by ``oid`` (D10, ruling R159): two parent objects that are
    different stubs or records of one oid are one node, and the object used is the one met first
    in a left-to-right, depth-first walk of the parents; the other one's fields and parents are
    never read.

Registry names
    A registry-name node ``N`` contributes the chain of ``N`` in the registry
    (:meth:`~proto_db.registry.Registry.chain`): its extensions, newest first, each a frame of
    the walk.  A data default is returned as is; a method (a plain function) is returned bound
    to the object looked up, ``types.MethodType(fn, o)``, so ``self is o``.  The registry is the
    one passed, or else the object's (below).  A name node's parents are ``Registry.parents_of(N)``:
    registry names (spec Q6), never objects, so every node placed before the first name node is
    placed exactly as it would be with the registry: a hit before that node is the true result.
    A lookup that reaches a name node without a registry raises ``ProtoNotSupportedError``
    ("registry name X needs a registry"); loading an object and an own or earlier hit never do.
    A name no module of the registry extends is an **empty node** (controller ruling, replacing
    R171): it contributes no definition, has no parents other than ``base`` (below), and the
    lookup goes on past it, so a miss stays ``None`` (R150).  In a registry that has the module
    ``base``, every other registry name has ``base`` as its implicit last parent (ruling R204,
    :meth:`~proto_db.registry.Registry.parents_of`), so ``base``'s definitions are found last
    from any registry name, and never from an object with no registry-name parent.  A misspelt name is therefore silent here; the mitigation is
    :meth:`proto_db.registry.Registry.unknown_names`, which tooling and tests use to flag names
    no module extends.

    Python members first (spec section 4): Python finds a ``DBObject`` subclass's members before
    resolution runs, so a registry definition of the same name could never be reached.  The first
    resolution of an object of such a class through a linearization with registry names raises
    ``ProtoValidationError`` naming the class, the name, the module and the registry name
    (memoized per class and registry name on the registry, so it runs once per pair even when
    the object's context caches no linearization).

The registry of an object (Task 8, ruling R201)
    The registry is decided by the code doing the resolving, never by load order or cache
    state.  With no registry passed, :func:`registry_of` uses:

    1. for an object of a transaction (its ``context``: created or derived in it, or loaded
       through it when the space's atom cache is off): that transaction's registry; inside a
       block of another database of the space whose list differs, ``ProtoUserError``;
    2. for any other object (a loaded one, read through the space's shared atom cache or a
       space-level context, which records no database): the **reader** of the running code
       (:func:`current_reader`: the transaction of a ``with tr:`` block or of ``Database.run``,
       or the snapshot of a ``with snapshot:`` block) when it is of the object's space; outside
       any reader, the space's fallback: its one distinct non-empty bound list, else
       ``ProtoUserError``;
    3. :func:`proto_db.registry.using` (tests), when the above give none.

    An object of a transaction keeps that transaction's registry for life: after the
    transaction ended (committed or aborted), after a later install of its database (also by
    the same space) and after :meth:`ObjectSpace.reload_registry`, it still resolves with the
    module list its transaction started on, also inside a newer block of the same database
    (where a freshly loaded copy of the same record resolves with the newer list).  It is the
    registry of the version the object was derived from, as running transactions keep the
    registry they started with.  A commit's lock step (:meth:`Transaction._build_root`, run by
    the committing thread, a group-commit leader or a cluster round leader) resolves with the
    transaction being built as the reader.

    A source whose database lists no modules has no registry and passes to the next one.  The
    registry is looked up only when the linearization has a registry-name node: one computed
    without a registry shows whether it has any (its names), and one with none is the same
    under every registry.  So reading own attributes, or inheriting along object parents only,
    never builds a registry (an unimportable listed module raises only at a lookup that needs
    it, Review Focus 4), and a linearization with a name node is never computed nor walked
    without the database's registry when it has one (ruling M3 of the Task 4 review).

Positions
    Where a definition was found, as :func:`resolve` returns it: ``0`` for an own attribute; an
    ``int`` ``k >= 1`` for the object node at index ``k`` of :func:`linearize`; a pair ``(k, j)``
    for the extension at index ``j`` of the chain of the registry name at index ``k`` (``j`` is
    :attr:`~proto_db.registry.Extension.position`, ``0`` for the newest).  Positions are walked in
    the order ``0, 1, ..., (k, 0), (k, 1), ..., k + 1, ...``.  Positions are compared for equality
    only; they are not ordered values (``1 < (1, 0)`` raises ``TypeError``; ruling R175).

``_super`` (spec section 6, Q3)
    ``o._super()`` reads its caller's code object, which the registry indexes to the extension
    ``E`` of the running method (:meth:`~proto_db.registry.Registry.extension_of_code`).  With
    ``k`` the index of ``E``'s registry name in ``o``'s linearization, the proxy resolves from
    the position ``(k, E.position + 1)``: the older extensions of the same name first, then the
    following nodes, binding methods to ``o``.  A miss through it is ``None`` (R150, ruling
    R170), so the end of a chain is tested with the idiom ``if self._super().f:
    self._super().f(x)`` (calling ``self._super().f(x)`` past the end raises ``TypeError``, as
    calling ``None`` does).  ``ProtoUserError`` (naming the caller) when the object has no
    registry (see above), when the code is not a registry method's (outside a method, a closure, a helper, a generator
    expression), when it is a method of a module the current registry does not contain (a method
    bound under another registry), when it is a generator's or an async generator's (ruling
    R172: such a frame may run after the call returned), and when ``o``'s linearization does not
    contain ``E``'s registry name.  A coroutine method (``async def``) is a method: its frame's
    code is the function's, and ``_super()`` works in it (ruling R176).

Errors
    An inconsistent hierarchy (no C3 order exists) raises ``ProtoValidationError`` naming the
    nodes that cannot be ordered.  A parent cycle, which only records written by another writer
    can hold (the API cannot build one: a parent must exist before its child), raises
    ``ProtoCorruptionError`` naming the oids; the walk is iterative and never recurses.  A miss is
    not an error: it returns ``(None, None)`` (ruling R150).

Cache (spec section 9)
    The linearization of a parents tuple is cached, without the object itself:

    - when every parent is saved, keyed by the registry and the parents'
      pointers and names, in the cache of the context the parents were loaded through: the
      space's shared atom cache, so every transaction of the space shares the entry (or the
      transaction itself when the space's atom cache is off).  Records never change at a
      pointer, and a saved parent's own parents are saved and fixed, so an entry never goes
      stale; instances decoded from different records share it.  The cache lives as long as
      that context, so it never keeps a closed space's storage alive; each cache is bounded
      (:data:`CACHE_NODES` nodes and per-name positions, below; least recently used first out);
    - when a parent is unsaved, only on the transaction that is the object's context, for the
      life of that transaction, keyed by that tuple object (``with_`` keeps it, so the versions
      of one object share the entry).  With any other context -- none, a plain test context, or
      the space's shared atom cache that a version of a loaded object inherits -- it is not
      cached, so unsaved state is never retained beyond its transaction.

    An object whose context keeps no cache has its linearization computed at each lookup that
    needs it.  Each node keeps its full linearization while the walk runs, so a parent chain of
    depth d costs O(d^2) to linearize once; the cached result is then reused.
    Per-name positions (spec section 9): a cached entry also maps each name looked up past the
    object's own attributes to its position, or to a miss (at most :data:`NAMES_PER_ENTRY`
    names; later names are walked each time).  The entry's nodes are immutable (records by
    version, unsaved objects by construction) and its registry is immutable and part of the key,
    so a position never goes stale and lives exactly as long as its entry.  An entry is charged
    its worst case when it is stored -- its nodes plus :data:`NAMES_PER_ENTRY` -- so
    :data:`CACHE_NODES` bounds the nodes and the cached positions together.  A repeated lookup
    walks no frame: a miss reads nothing, a hit reads the one definition at its position.  A
    lookup that raises (a registry name it cannot resolve) caches nothing.  ``_super`` walks from
    its start position without this cache.
    Resolution never visits history: superseded versions are not parents, so the cost of a
    lookup does not depend on how many versions a database holds (anti-goal A4).
"""
from __future__ import annotations

import contextvars
import inspect
import types

from .core.atom import _ABSENT, RESERVED_NAMES, OpaqueAtom, class_defines
from .errors import ProtoCorruptionError, ProtoNotSupportedError, ProtoUserError, ProtoValidationError
from .storage.cache import BoundedCache

__all__ = ["linearize", "resolve", "new_cache", "super_proxy", "SuperProxy", "CACHE_NODES",
           "NAMES_PER_ENTRY", "current_registry", "set_current_registry", "reset_current_registry",
           "current_reader", "set_reader", "reset_reader", "enter_reader", "exit_reader", "registry_of"]

#: Capacity of each linearization cache, counted in nodes and per-name positions: an entry is
#: charged ``len(tail) + NAMES_PER_ENTRY`` (a configuration bound, not a measured figure).
CACHE_NODES = 100_000

#: How many names one linearization entry remembers positions for (a configuration bound, not a
#: measured figure): names past it are resolved by walking, never cached, so names computed at
#: run time (``o[name]``) cannot grow an entry without bound.
NAMES_PER_ENTRY = 256

#: The virtual root of a walk: its children are the parents being linearized.
_ROOT = object()

#: A cached miss in an entry's positions.
_MISS = object()

#: The registry resolution uses when none is passed (``proto_db.registry.using``, ruling R169).
_CURRENT_REGISTRY: contextvars.ContextVar = contextvars.ContextVar("proto_db_registry", default=None)

#: The reader whose registry resolution uses for objects that do not belong to a transaction: the
#: transaction of a ``with tr:`` block or of ``Database.run``, or the snapshot of a ``with snapshot:``
#: block (Task 8).  Its value is ``None`` or a link ``(reader, previous link)``: each context
#: (thread, ``asyncio`` task) holds its own chain, so no reader state is shared between them.
#: A reader has ``_reader_space``, ``_reader_database``, ``_reader_registry()`` and
#: ``_reader_names()`` (:func:`registry_of`).
_READER: contextvars.ContextVar = contextvars.ContextVar("proto_db_reader", default=None)

#: The code flags of a frame that may run after its call returned (R172): a generator (also set
#: on a generator-based coroutine) or an async generator.  A coroutine method is allowed (R176).
_SUSPENDABLE = inspect.CO_GENERATOR | inspect.CO_ASYNC_GENERATOR


def current_registry():
    """The current registry (``None`` when none is): what resolution uses when none is passed."""
    return _CURRENT_REGISTRY.get()


def set_current_registry(registry) -> contextvars.Token:
    """Make ``registry`` current in this context; return the token that
    :func:`reset_current_registry` takes (``proto_db.registry.using`` pairs them)."""
    return _CURRENT_REGISTRY.set(registry)


def reset_current_registry(token: contextvars.Token) -> None:
    """Restore the registry that was current before :func:`set_current_registry` returned
    ``token``."""
    _CURRENT_REGISTRY.reset(token)


def current_reader():
    """The reader of the running code (``None`` when no ``with tr:``, ``Database.run`` or
    ``with snapshot:`` block is active in this context)."""
    link = _READER.get()
    return None if link is None else link[0]


def set_reader(reader) -> contextvars.Token:
    """Make ``reader`` (a transaction or a snapshot) the reader of this context; return the token
    :func:`reset_reader` takes (for a call that sets and resets in one frame, ``Database.run``)."""
    return _READER.set((reader, _READER.get()))


def enter_reader(reader) -> None:
    """Make ``reader`` the reader of this context until :func:`exit_reader` (a ``with`` block's
    ``__enter__``): nothing is stored on the reader, so one reader may be entered by several
    threads or tasks at once (review minor 1)."""
    _READER.set((reader, _READER.get()))


def exit_reader(reader) -> bool:
    """End the innermost block of ``reader`` in this context (``__exit__``): the reader before it
    is current again.  The block is always removed from this context's chain, even when it is
    not the innermost one (blocks that do not nest); returns whether it was the innermost.
    ``ProtoUserError`` only when ``reader`` has no block in this context."""
    link = _READER.get()
    if link is not None and link[0] is reader:
        _READER.set(link[1])
        return True
    kept = []
    while link is not None and link[0] is not reader:
        kept.append(link[0])
        link = link[1]
    if link is None:
        raise ProtoUserError(f"{reader!r} has no reader block in this context: a block ends in the context "
                             f"it started in")
    rest = link[1]
    for other in reversed(kept):
        rest = (other, rest)
    _READER.set(rest)
    return False


def reset_reader(token: contextvars.Token) -> None:
    """Restore the reader that was current before :func:`set_reader` returned ``token``."""
    _READER.reset(token)


def registry_of(obj):
    """The registry resolution uses for ``obj`` when none is passed (ruling R201: the code doing
    the resolving decides, never load order or cache state).  An object of a transaction (its
    ``context``) resolves with that transaction's registry, and raises inside a block of another
    database of the space whose list differs.  Any other object (loaded through the space's
    shared atom cache or a space-level context, or never bound) resolves with the current
    reader of its space, else with its space's fallback (one distinct bound list), else
    :func:`proto_db.registry.using` (tests).  Building it may import modules and raise."""
    context = obj.context
    link = _READER.get()
    reader = None if link is None else link[0]
    if isinstance(context, _transaction_class()):
        if reader is not None and reader is not context:
            context._check_reader(reader)
        found = context._resolution_registry()
        if found is not None:
            return found                # else: its database lists no modules, as a loaded object
    space = getattr(context, "_space_registries", None)
    if reader is not None and (space is None or space is reader._reader_space):
        found = reader._reader_registry()
        if found is not None:
            return found
    if space is not None:
        found = space.fallback()
        if found is not None:
            return found
    return _CURRENT_REGISTRY.get()


def _entry_and_registry(obj, parents: tuple, registry) -> tuple:
    """``(entry, registry)``: the linearization of ``obj``'s non-empty ``parents`` and the registry
    it was computed under.  With no registry passed, the object's registry is looked up
    (:func:`registry_of`) only when the linearization has a registry-name node: a parent that is
    a name, or a name further up, which the linearization computed without a registry shows
    (its ``names``; that entry is cached under ``None``).  A linearization without any name node
    is the same under every registry (a registry only adds the parents of names), so it is
    exact as it is, and an unimportable module never breaks such a lookup (Review Focus 4)."""
    if registry is None:
        if not any(type(parent) is str for parent in parents):
            entry = _entry(obj, None, parents)
            if not entry.names:
                return entry, None
        registry = registry_of(obj)
        if registry is None:
            return _entry(obj, None, parents), None
    return _entry(obj, registry, parents), registry


def linearize(obj, registry=None) -> tuple:
    """The linearization of ``obj``: ``obj`` itself, then the C3 order of its parents' nodes
    (DBObjects, opaque atoms and registry names), each node once."""
    parents = _parents_of(obj)
    if not parents:
        return (obj,)
    return (obj,) + _entry_and_registry(obj, parents, registry)[0].tail


def resolve(obj, name, registry=None) -> tuple:
    """``(value, position)``: the first definition of ``name`` along the linearization of ``obj``
    and its position there (see "Positions"); ``(None, None)`` on a miss.  A method is returned
    bound to ``obj``."""
    found = _probe_node(obj, name)
    if found is not _ABSENT:
        return found, 0
    return _resolve_inherited(obj, name, registry)


def _resolve_inherited(obj, name, registry=None) -> tuple:
    """:func:`resolve` past ``obj``'s own attributes (the caller has looked there already)."""
    parents = _parents_of(obj)
    if not parents:
        return None, None
    entry, registry = _entry_and_registry(obj, parents, registry)
    _check_python_members(type(obj), entry, registry)
    positions = entry.positions
    if positions is not None:
        position = positions.get(name)
        if position is not None:
            if position is _MISS:
                return None, None
            return _value_at(obj, entry, name, position, registry), position
    value, position = _walk(obj, entry, name, registry, 1, 0)
    if positions is not None and len(positions) < NAMES_PER_ENTRY:
        positions[name] = _MISS if position is None else position
    return value, position


def _walk(receiver, entry, name, registry, start: int, start_extension: int) -> tuple:
    """``(value, position)`` of the first definition of ``name`` at or after the position
    ``(start, start_extension)`` of the linearization ``entry`` (``start`` >= 1; the extension
    index applies only when the node ``start`` is a registry name); ``(None, None)`` on a miss."""
    tail = entry.tail
    for index in range(start, len(tail) + 1):
        node = tail[index - 1]
        if type(node) is str:
            chain = _chain(node, registry)
            for j in range(start_extension if index == start else 0, len(chain)):
                definition = chain[j].definitions.get(name)
                if definition is not None:
                    return _bound(definition, receiver), (index, j)
            continue
        found = _lookup(node, name)
        if found is not _ABSENT:
            return found, index
    return None, None


def _value_at(receiver, entry, name, position, registry):
    """The definition of ``name`` at the cached ``position`` (read once, never walked)."""
    if type(position) is int:
        return _read(entry.tail[position - 1], name)
    index, j = position
    return _bound(registry.chain(entry.tail[index - 1])[j].definitions[name], receiver)


def _bound(definition, receiver):
    """A registry definition as resolution returns it: a data default as is, a method bound to
    ``receiver`` (``self is receiver``)."""
    kind, value = definition
    if kind == "method":
        return types.MethodType(value, receiver)
    return value


def _chain(name: str, registry) -> tuple:
    """The extensions of the registry name ``name`` that a lookup reached, newest first (``()``
    for a name no module extends: an empty node)."""
    if registry is None:
        raise ProtoNotSupportedError(f"registry name {name!r} needs a registry")
    return registry.chain(name)


def _check_python_members(cls, entry, registry) -> None:
    """``ProtoValidationError`` if a registry name of ``entry`` defines a name that ``cls`` (a
    ``DBObject`` subclass) defines in Python: Python finds the member first, so the registry
    definition could never be reached (spec section 4).  Memoized per (class, registry name) on
    the registry, so it runs once per pair whether or not the entry is cached.  The memo holds
    its classes strongly for the registry's life (dynamically created subclasses stay alive
    while it lives); it is bounded by classes times extended registry names."""
    if registry is None or not entry.names or cls is _dbobject_class():
        return
    checked = registry._checked
    for node in entry.names:
        if (cls, node) in checked:
            continue
        chain = registry.chain(node)
        if not chain:
            continue                # an empty node defines nothing; never memoized, so names
                                    # from stored parents cannot grow the memo without bound
        for extension in chain:
            for name in extension.definitions:
                if class_defines(cls, name):
                    raise ProtoValidationError(
                        f"{cls.__qualname__} defines {name!r} in Python, and module "
                        f"{extension.module!r} defines it in registry name {node!r} "
                        f"({extension.qualname}): the registry definition could never be reached")
        checked[(cls, node)] = True


def _dbobject_class() -> type:
    global _DBObject
    if _DBObject is None:
        from .core.dbobject import DBObject
        _DBObject = DBObject
    return _DBObject


_DBObject = None


def _probe_node(node, name):
    """The own definition of ``name`` in the object node ``node``, or ``_ABSENT``."""
    if isinstance(node, OpaqueAtom):
        if type(name) is not str or not name or name.startswith("_") or name in RESERVED_NAMES:
            return _ABSENT
        return node._probe(name)         # its fields only: never its absent-attribute answer
    return node._own_probe(name)


def _lookup(node, name):
    """A frame probe of a walk: the own definition of ``name`` in ``node``, or ``_ABSENT``."""
    return _probe_node(node, name)


def _read(node, name):
    """The read of a definition at a cached position (not a walk)."""
    return _probe_node(node, name)


def _parents_of(node) -> tuple:
    """The persisted parents of an object node (``()`` for none), never a class's ``parents``."""
    found = node._probe("_parents")
    return () if found is _ABSENT else found


# -- _super (spec section 6) ----------------------------------------------------------------------

def super_proxy(obj, code, registry=None) -> "SuperProxy":
    """The lookup proxy ``obj._super()`` returns when called from the code object ``code``: it
    resolves from the position after the running extension in ``obj``'s linearization."""
    if registry is None:
        registry = registry_of(obj)
    where = f"{code.co_qualname} ({code.co_filename}:{code.co_firstlineno})"
    if registry is None:
        raise ProtoUserError(f"_super() called from {where} with no registry current")
    if code.co_flags & _SUSPENDABLE:
        raise ProtoUserError(
            f"_super() called from {where}, a generator or coroutine: its frame may run after the "
            f"method returned; call _super() in the method and pass the result to the generator")
    extension = registry.extension_of_code(code)
    if extension is None:
        from .registry import _declared_method_of_code
        declared = _declared_method_of_code(code)
        if declared is not None:
            raise ProtoUserError(
                f"_super() called from {where}, the {declared}, bound under another registry: the "
                f"current registry {registry.key!r} does not contain it")
        raise ProtoUserError(
            f"_super() must be called directly in the body of a registry method; it was called "
            f"from {where}, which is not one (a closure, a helper or a generator expression is not)")
    parents = _parents_of(obj)
    entry = _entry(obj, registry, parents) if parents else None
    index = None
    if entry is not None:
        index = next((i for i, node in enumerate(entry.tail, 1)
                      if type(node) is str and node == extension.registry_name), None)
    if index is None:
        raise ProtoUserError(
            f"_super() in {where} (registry name {extension.registry_name!r}, module "
            f"{extension.module!r}): the linearization of {obj!r} does not contain "
            f"{extension.registry_name!r}")
    _check_python_members(type(obj), entry, registry)
    return SuperProxy(obj, entry, registry, index, extension.position + 1)


class SuperProxy:
    """A transient lookup from a position of a linearization, binding to the receiver (what
    ``o._super()`` returns).  ``proxy.name`` and ``proxy["name"]`` resolve ``name`` from the
    position after the running extension (``None`` on a miss, R150); ``"name" in proxy`` tells a
    definition from a miss.  It is not a value: it cannot be stored, compared or hashed."""

    __slots__ = ("_receiver", "_entry", "_registry", "_start", "_start_extension")

    def __init__(self, receiver, entry, registry, start, start_extension):
        osa = object.__setattr__
        osa(self, "_receiver", receiver)
        osa(self, "_entry", entry)
        osa(self, "_registry", registry)
        osa(self, "_start", start)
        osa(self, "_start_extension", start_extension)

    def _find(self, name) -> tuple:
        if type(name) is not str:
            raise TypeError(f"attribute names are str, not {type(name).__name__}")
        if not name or name.startswith("_") or name in RESERVED_NAMES:
            raise ProtoValidationError(f"{name!r} is not an attribute name")
        return _walk(self._receiver, self._entry, name, self._registry, self._start,
                     self._start_extension)

    def __getattr__(self, name):
        if name.startswith("_") or name in RESERVED_NAMES:
            # Not attribute names (``oid``, ``context``, ...): what library code probes on a value.
            raise AttributeError(f"A _super() proxy has no {name!r}")
        return self._find(name)[0]

    def __getitem__(self, name):
        return self._find(name)[0]

    def __contains__(self, name) -> bool:
        try:
            return self._find(name)[1] is not None
        except (TypeError, ProtoValidationError):
            return False

    def __setattr__(self, name, value):
        raise ProtoUserError("A _super() proxy is a transient lookup; it has no attributes to set")

    def __eq__(self, other):
        raise ProtoUserError("A _super() proxy is a transient lookup, not a value; it is never compared")

    __hash__ = None

    def __repr__(self) -> str:
        return f"<_super() of {self._receiver!r} from position {self._start}, {self._start_extension}>"


def new_cache() -> BoundedCache:
    """A linearization cache for a context (``_linearizations``): :class:`_Linearization`
    entries, bounded by :data:`CACHE_NODES` nodes, least recently used first out."""
    return BoundedCache(CACHE_NODES, sizeof=_entry_nodes)


def _entry_nodes(entry) -> int:
    """What an entry is charged, once, when stored: its nodes plus the most positions it may hold."""
    return len(entry.tail) + NAMES_PER_ENTRY


class _Linearization:
    """The linearization of one parents tuple under one registry: ``tail`` (the nodes, the object
    itself excluded), ``names`` (its registry-name nodes), and, for a cached entry only,
    ``positions`` (name -> position or a miss, at most :data:`NAMES_PER_ENTRY` names).  ``held`` is what the entry
    holds to stay valid (the parents tuple of an unsaved-parent entry).  The map is only ever
    added to, one item at a time; two threads that race compute and store the same value."""

    __slots__ = ("held", "tail", "names", "positions")

    def __init__(self, held, tail: tuple, cached: bool):
        self.held = held
        self.tail = tail
        self.names = tuple(node for node in tail if type(node) is str)
        self.positions = {} if cached else None


#: The linearizations of parents tuples made of registry names only (no storage involved).
_names_cache = new_cache()


def _entry(obj, registry, parents: tuple) -> _Linearization:
    """The linearization of ``obj``'s non-empty ``parents``, from a cache when possible."""
    store, key, held = _cache_slot(obj, parents, registry)
    if store is None:
        return _Linearization(None, _compute(parents, registry), cached=False)
    entry = store.get(key)
    if entry is not None and entry.held is held:
        return entry
    entry = _Linearization(held, _compute(parents, registry), cached=True)
    store.put(key, entry)
    return entry


def _cache_slot(obj, parents: tuple, registry) -> tuple:
    """``(store, key, held)``: where the linearization of ``parents`` is cached (``store`` is
    ``None`` when it is not), under which key, and what the entry holds to be valid.

    - Every parent saved: the ``_linearizations`` of the first object parent's context (the
      space's shared atom cache, or the transaction when that cache is off), keyed by the
      registry and the parents' pointers and names; registry names only: a module-level cache.
      A pointer names one record of that context's storage forever.  The object's class is not
      part of the key: the linearization of a parents tuple does not depend on the child.
    - A parent unsaved: the ``_linearizations`` of ``obj``'s context when it is a
      :class:`~proto_db.transactions.Transaction`, keyed by the identity of the tuple, which the
      entry holds so that the identity stays unique; with any other context, not cached.
    """
    keys = []
    context = None
    for parent in parents:
        if type(parent) is str:
            keys.append(parent)
            continue
        ptr = parent.atom_pointer
        if ptr is None:
            # Only a transaction holds such an entry, for its own life (spec 14.7): a version of
            # a loaded object has the space's shared atom cache as its context, which must never
            # retain unsaved (perhaps aborted) state (review I1).
            owner = obj.context
            if not isinstance(owner, _transaction_class()):
                return None, None, None
            return owner._linearizations, (id(parents), registry), parents
        if context is None:
            context = parent.context
        keys.append(ptr)
    key = (registry, tuple(keys))
    if context is None:
        return _names_cache, key, None
    return getattr(context, "_linearizations", None), key, None


def _transaction_class() -> type:
    """:class:`proto_db.transactions.Transaction`, imported on first use (that module imports
    this one)."""
    global _Transaction
    if _Transaction is None:
        from .transactions import Transaction
        _Transaction = Transaction
    return _Transaction


_Transaction = None


def _node_key(node):
    return node if type(node) is str else node.oid


def _children(node, registry) -> tuple:
    if type(node) is str:
        return tuple(registry.parents_of(node)) if registry is not None else ()
    return _parents_of(node)


def _compute(parents: tuple, registry) -> tuple:
    """The C3 linearization of ``parents``, computed by an iterative depth-first walk."""
    reps = {}                   # node key -> the node object met first (R159)
    lin = {}                    # node key -> its linearization, as keys, itself first
    on_path = {}                # node key -> its index in ``stack`` while it is being walked
    stack = [[_ROOT, parents, 0, []]]       # [key, child nodes, next child, child keys so far]
    while True:
        frame = stack[-1]
        key, kids, i, kid_keys = frame
        if i < len(kids):
            frame[2] = i + 1
            child = kids[i]
            ck = _node_key(child)
            if ck in kid_keys:
                continue                    # one node listed twice by a parent (R159)
            kid_keys.append(ck)
            if ck in lin:
                continue
            if ck in on_path:
                _cycle([frame[0] for frame in stack[on_path[ck]:]], reps)
            reps[ck] = child
            on_path[ck] = len(stack)
            stack.append([ck, _children(child, registry), 0, []])
            continue
        stack.pop()
        merged = _merge([lin[k] for k in kid_keys] + [kid_keys], reps)
        if key is _ROOT:
            return tuple(reps[k] for k in merged)
        del on_path[key]
        lin[key] = [key] + merged


def _merge(sequences: list, reps: dict) -> list:
    """The C3 merge of ``sequences`` (lists of node keys); ``ProtoValidationError`` if none."""
    seqs = [list(s) for s in sequences if s]
    out = []
    while True:
        seqs = [s for s in seqs if s]
        if not seqs:
            return out
        for seq in seqs:
            head = seq[0]
            if not any(head in other[1:] for other in seqs):
                break
        else:
            heads = list(dict.fromkeys(seq[0] for seq in seqs))
            raise ProtoValidationError(
                f"Inconsistent parent hierarchy: no C3 linearization can order "
                f"{', '.join(_describe(k, reps) for k in heads)}")
        out.append(head)
        for seq in seqs:
            if seq[0] == head:
                del seq[0]


def _cycle(keys: list, reps: dict):
    names = ", ".join(_describe(k, reps) for k in keys)
    if all(type(k) is str for k in keys):
        raise ProtoValidationError(f"Registry names that are their own ancestors: {names}")
    raise ProtoCorruptionError(f"A parent cycle among the stored objects {names}: an object is its own ancestor")


def _describe(key, reps: dict) -> str:
    if type(key) is str:
        return f"registry name {key!r}"
    return f"{type(reps[key]).__type_name__} {key}"
