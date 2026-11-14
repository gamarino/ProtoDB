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
    would hide the fields of that name in opaque parents), or a registry name (a ``str``).  Nodes are identified by ``oid`` (D10, ruling R159): two parent objects that are
    different stubs or records of one oid are one node, and the object used is the one met first
    in a left-to-right, depth-first walk of the parents; the other one's fields and parents are
    never read.

Registry names
    Without a registry a name node has no known parents.  A registry name's parents are
    themselves registry names (spec Q6, ``Registry.parents_of``), never objects, so every node
    placed before the first name node is placed exactly as it would be with the registry: a hit
    before that node is the true result.  A lookup that reaches a name node without a registry
    raises ``ProtoNotSupportedError`` ("registry name X needs a registry"); loading an object and
    an own or earlier hit never do.

Errors
    An inconsistent hierarchy (no C3 order exists) raises ``ProtoValidationError`` naming the
    nodes that cannot be ordered.  A parent cycle, which only records written by another writer
    can hold (the API cannot build one: a parent must exist before its child), raises
    ``ProtoCorruptionError`` naming the oids; the walk is iterative and never recurses.  A miss is
    not an error: it returns ``(None, None)`` (ruling R150).

Cache (spec section 9)
    The linearization of a parents tuple is cached, without the object itself:
    - when every parent is saved, keyed by the object's class, the registry and the parents'
      pointers and names, in the cache of the context the parents were loaded through: the
      space's shared atom cache, so every transaction of the space shares the entry (or the
      transaction itself when the space's atom cache is off).  Records never change at a
      pointer, and a saved parent's own parents are saved and fixed, so an entry never goes
      stale; instances decoded from different records share it.  The cache lives as long as
      that context, so it never keeps a closed space's storage alive; each cache is bounded
      (:data:`CACHE_NODES` nodes, least recently used first out);
    - when a parent is unsaved, only on the transaction that is the object's context, for the
      life of that transaction, keyed by that tuple object (``with_`` keeps it, so the versions
      of one object share the entry).  With any other context -- none, a plain test context, or
      the space's shared atom cache that a version of a loaded object inherits -- it is not
      cached, so unsaved state is never retained beyond its transaction.
    An object whose context keeps no cache has its linearization computed at each lookup that
    needs it.  Each node keeps its full linearization while the walk runs, so a parent chain of
    depth d costs O(d^2) to linearize once; the cached result is then reused.
    Resolution never visits history: superseded versions are not parents, so the cost of a
    lookup does not depend on how many versions a database holds (anti-goal A4).
"""
from __future__ import annotations

from .core.atom import _ABSENT, RESERVED_NAMES, OpaqueAtom
from .errors import ProtoCorruptionError, ProtoNotSupportedError, ProtoValidationError
from .storage.cache import BoundedCache

__all__ = ["linearize", "resolve", "new_cache", "CACHE_NODES"]

#: Capacity of each linearization cache, counted in nodes (a configuration bound, not a measured
#: figure).
CACHE_NODES = 100_000

#: The virtual root of a walk: its children are the parents being linearized.
_ROOT = object()


def linearize(obj, registry=None) -> tuple:
    """The linearization of ``obj``: ``obj`` itself, then the C3 order of its parents' nodes
    (DBObjects, opaque atoms and registry names), each node once."""
    return (obj,) + _tail(obj, registry)


def resolve(obj, name, registry=None) -> tuple:
    """``(value, position)``: the first definition of ``name`` along the linearization of ``obj``
    and its index there (``0`` for an own attribute); ``(None, None)`` on a miss."""
    found = _lookup(obj, name)
    if found is not _ABSENT:
        return found, 0
    return _resolve_inherited(obj, name, registry)


def _resolve_inherited(obj, name, registry=None) -> tuple:
    """:func:`resolve` past ``obj``'s own attributes (the caller has looked there already)."""
    parents = _parents_of(obj)
    if not parents:
        return None, None
    for position, node in enumerate(_tail(obj, registry, parents), 1):
        if type(node) is str:
            _registry_node(node, registry)
            continue
        found = _lookup(node, name)
        if found is not _ABSENT:
            return found, position
    return None, None


def _registry_node(name: str, registry) -> None:
    """A lookup reached the registry name ``name``."""
    if registry is None:
        raise ProtoNotSupportedError(f"registry name {name!r} needs a registry")
    raise ProtoNotSupportedError(f"registry name {name!r}: resolution in registry chains is not available yet")


def _lookup(node, name):
    """The own definition of ``name`` in the object node ``node``, or ``_ABSENT``."""
    if isinstance(node, OpaqueAtom):
        if type(name) is not str or not name or name.startswith("_") or name in RESERVED_NAMES:
            return _ABSENT
        return node._probe(name)         # its fields only: never its absent-attribute answer
    return node._own_probe(name)


def _parents_of(node) -> tuple:
    """The persisted parents of an object node (``()`` for none), never a class's ``parents``."""
    found = node._probe("_parents")
    return () if found is _ABSENT else found


def new_cache() -> BoundedCache:
    """A linearization cache for a context (``_linearizations``): entries ``(parents, tail)``,
    bounded by :data:`CACHE_NODES` nodes, least recently used first out."""
    return BoundedCache(CACHE_NODES, sizeof=_entry_nodes)


def _entry_nodes(entry) -> int:
    return len(entry[1])


#: The linearizations of parents tuples made of registry names only (no storage involved).
_names_cache = new_cache()


def _tail(obj, registry, parents=None) -> tuple:
    """The linearization of ``obj``'s parents (``obj`` excluded), from a cache when possible."""
    if parents is None:
        parents = _parents_of(obj)
    if not parents:
        return ()
    store, key, held = _cache_slot(obj, parents, registry)
    if store is None:
        return _compute(parents, registry)
    entry = store.get(key)
    if entry is not None and entry[0] is held:
        return entry[1]
    tail = _compute(parents, registry)
    store.put(key, (held, tail))
    return tail


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
