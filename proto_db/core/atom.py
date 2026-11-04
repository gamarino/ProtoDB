"""Atoms: the unit of persistence (spec section 5, decisions D10 and R3).

An atom has a stable identity (``oid``, a UUID4, persisted), an
``atom_pointer`` once saved, and a ``context`` (a :class:`PersistenceContext`)
through which it is saved and its references are loaded.  Its persisted state is
its public instance attributes; attributes whose name starts with an underscore
are runtime only.

Identity (D10, D21)
    The ``oid`` of a new atom is assigned lazily: constructing an atom creates
    neither a UUID nor a lock.  The UUID4 is generated on first need -- the
    first ``oid`` read, ``==``/``hash``, or ``_save`` -- under the atom's striped
    lock, so racing threads all observe the same value; once observed it never
    changes.  A stub takes its ``oid`` from the payload when it is loaded.

Locking (D21)
    Atoms share a module-level pool of ``_STRIPES`` re-entrant locks;
    :func:`_lock_for` maps an atom to its stripe.  Lock order rule: *a thread
    holding an atom stripe never acquires another atom stripe* (re-acquiring the
    same stripe is allowed: the locks are re-entrant).  Every critical section
    below touches only its own atom and storage, whose locks are leaves that
    never call back into atoms; work that reaches other atoms (encoding children,
    which saves them, and decoding a payload, which may hash child stubs) runs
    with no stripe held.  With no nested acquisition there is no lock-order cycle,
    so stripes shared by unrelated atoms cannot deadlock.

Lazy loading
    An atom obtained from a pointer is a *stub*: only ``context`` and
    ``atom_pointer`` are set.  The first access to any other public attribute,
    ``oid`` included, loads the state.  The payload is fetched once, under the
    stub's stripe, and kept on the stub; it is decoded with no stripe held (two
    racing threads may both decode it; that work is pure) and the first decoded
    state is published under the stripe with the loaded flag set last, so a
    thread either finds an attribute with its final value or waits for the load
    to finish.  No thread ever observes a half-loaded atom.
    Equality and hashing use ``oid`` (D10), therefore comparing or hashing a
    stub loads it.

    Persisted attributes must not shadow class attributes (methods, properties,
    class-level defaults): normal attribute lookup would find the class
    attribute on a stub and skip the load.  ``_save`` rejects such state with
    ``ProtoValidationError`` and ``_load`` with ``ProtoCorruptionError``.

Saving
    :meth:`Atom._save` is idempotent and thread-safe: children are saved first
    (through ``context.save``, with no stripe held), then, under the atom's
    stripe, the atom's own payload
    ``{"t": type_tag, "oid": oid_bytes, "s": encoded_state}`` is pushed to
    ``context.storage`` unless another thread published a pointer first.
    Concurrent calls push exactly one payload and return one pointer (racing
    callers may encode the state more than once; only one result is pushed).
    After its push, the call that pushed tells the context through the optional hook
    ``context.saved(atom, ptr, payload)`` (no stripe held): a context that caches decoded
    atoms keeps a stub holding that payload (:meth:`Atom._preloaded_stub`, Task 8.3i), never
    the live object, whose runtime state (an op log, for example) a loaded atom does not have.

Type tags (format version 3, D51)
    A built-in type is written as its small integer alias, its index in
    :data:`BUILTIN_TYPE_ALIASES` (:func:`type_tag`); any other type as its name.  Readers
    accept both forms (:func:`type_name_of`), so format-2 data, which always wrote names,
    still loads.
"""
from __future__ import annotations

import importlib
import threading
import uuid
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from ..errors import ProtoCorruptionError, ProtoUserError, ProtoValidationError

__all__ = ["AtomPointer", "PersistenceContext", "Atom", "register_atom_class", "RESERVED_NAMES",
           "BUILTIN_TYPE_ALIASES", "type_tag", "type_name_of"]

#: Public attribute names managed by :class:`Atom` itself; never part of the persisted state.
RESERVED_NAMES = frozenset({"oid", "atom_pointer", "context"})


@dataclass(frozen=True)
class AtomPointer:
    """Location of a saved atom: a storage segment and an offset inside it."""

    segment: uuid.UUID
    offset: int

    def __post_init__(self):
        if not isinstance(self.segment, uuid.UUID):
            raise ProtoValidationError(f"AtomPointer segment must be a UUID, not {type(self.segment).__name__}")
        if type(self.offset) is not int or self.offset < 0:
            raise ProtoValidationError(f"AtomPointer offset must be a non-negative int, not {self.offset!r}")


@runtime_checkable
class PersistenceContext(Protocol):
    """What an atom needs to persist itself (ruling R3).

    Implemented by transactions (Task 5.1) and by simple direct contexts.
    ``storage`` must offer ``push_atom(payload: dict) -> AtomPointer`` and
    ``get_atom(ptr: AtomPointer) -> dict``.
    """

    storage: Any

    def save(self, atom: "Atom") -> AtomPointer:
        """Persist ``atom`` (children first) if needed and return its pointer."""
        ...

    def load(self, ptr: AtomPointer, type_name: str) -> "Atom":
        """Return the atom of class ``type_name`` stored at ``ptr`` (usually a lazy stub)."""
        ...


#: Number of locks in the striped pool shared by all atoms (D21).
_STRIPES = 64
_LOCKS = tuple(threading.RLock() for _ in range(_STRIPES))


def _lock_for(atom) -> "threading.RLock":
    """Return the striped lock that guards ``atom`` (load, oid assignment, save publication).

    Object addresses are aligned, so the low bits of ``id`` carry no information;
    they are discarded and two higher bit ranges are folded together.
    """
    i = id(atom) >> 4
    return _LOCKS[(i ^ (i >> 6)) & (_STRIPES - 1)]


_registry: dict[str, type] = {}
_registry_lock = threading.Lock()

#: Module defining each built-in 2.0 atom type, imported on a registry miss so that
#: stored atoms load in a process that never imported their module.  Kept complete
#: by tests/unit/test_atom_type_modules.py (every built-in Atom subclass must appear).
BUILTIN_ATOM_MODULES: dict[str, str] = {
    "proto_db.core.dbobject.DBObject": "proto_db.core.dbobject",
    "proto_db.avl._Node": "proto_db.collections.avl",
    "proto_db.avl.PosNode": "proto_db.collections.avl",
    "proto_db.avl.KeyNode": "proto_db.collections.avl",
    "proto_db.avl.ValueBox": "proto_db.collections.avl",
    "proto_db.collections.IndexSet": "proto_db.collections.base",
    "proto_db.collections.IndexedCollection": "proto_db.collections.base",
    "proto_db.collections.List": "proto_db.collections.lists",
    "proto_db.collections.Dictionary": "proto_db.collections.dictionaries",
    "proto_db.collections.RepeatedKeysDictionary": "proto_db.collections.dictionaries",
    "proto_db.collections.HashDictionary": "proto_db.collections.hash_dictionaries",
    "proto_db.collections.sets._HashBuckets": "proto_db.collections.sets",
    "proto_db.collections.Set": "proto_db.collections.sets",
    "proto_db.collections.CountedSet": "proto_db.collections.sets",
    "proto_db.indexing.Index": "proto_db.indexing.base",
    "proto_db.indexing.keyed.KeyGroupIndex": "proto_db.indexing.keyed",
    "proto_db.indexing.EqualityIndex": "proto_db.indexing.equality",
    "proto_db.indexing.RangeIndex": "proto_db.indexing.range",
    "proto_db.indexing.VectorIndex": "proto_db.indexing.vector",
    "proto_db.transactions.SpaceRoot": "proto_db.transactions",
    "proto_db.transactions.DatabaseRoot": "proto_db.transactions",
}


#: Built-in atom type names by their persisted alias (the index): APPEND-ONLY -- an alias, once
#: written, must keep naming the same type forever (format version 3, D51).
BUILTIN_TYPE_ALIASES: tuple[str, ...] = (
    "proto_db.avl.KeyNode",
    "proto_db.avl.PosNode",
    "proto_db.avl.ValueBox",
    "proto_db.collections.IndexSet",
    "proto_db.collections.IndexedCollection",
    "proto_db.collections.List",
    "proto_db.collections.Dictionary",
    "proto_db.collections.RepeatedKeysDictionary",
    "proto_db.collections.HashDictionary",
    "proto_db.collections.sets._HashBuckets",
    "proto_db.collections.Set",
    "proto_db.collections.CountedSet",
    "proto_db.indexing.Index",
    "proto_db.indexing.keyed.KeyGroupIndex",
    "proto_db.indexing.EqualityIndex",
    "proto_db.indexing.RangeIndex",
    "proto_db.indexing.VectorIndex",
    "proto_db.transactions.SpaceRoot",
    "proto_db.transactions.DatabaseRoot",
    "proto_db.core.dbobject.DBObject",
    "proto_db.avl._Node",
)
_ALIAS_OF = {name: i for i, name in enumerate(BUILTIN_TYPE_ALIASES)}


def type_tag(type_name: str):
    """Return the persisted tag of ``type_name``: its alias for a built-in type, else the name."""
    alias = _ALIAS_OF.get(type_name)
    return type_name if alias is None else alias


def type_name_of(tag) -> str:
    """Return the type name a persisted tag stands for; ``ProtoCorruptionError`` if malformed."""
    if type(tag) is str:
        return tag
    if type(tag) is int and 0 <= tag < len(BUILTIN_TYPE_ALIASES):
        return BUILTIN_TYPE_ALIASES[tag]
    raise ProtoCorruptionError(f"Malformed atom type tag {tag!r}")


def register_atom_class(cls: type) -> type:
    """Register ``cls`` under ``cls.__type_name__``; return ``cls``.

    Called automatically for every :class:`Atom` subclass.  Registering the same
    class again is a no-op; a different class with an already registered name
    raises ``ProtoUserError``.
    """
    name = cls.__dict__.get("__type_name__")
    if not isinstance(name, str) or not name:
        raise ProtoValidationError(f"{cls!r} has no valid __type_name__")
    with _registry_lock:
        current = _registry.get(name)
        if current is not None and current is not cls:
            raise ProtoUserError(f"Atom type name {name!r} is already registered by {current!r}")
        _registry[name] = cls
    return cls


#: Classes whose instances passed the :class:`PersistenceContext` check (Task 8.3i).  The protocol
#: check (``isinstance`` on a runtime-checkable protocol) cost about 5 us of the 7 us a tree node
#: takes to build; its methods are class attributes, so it is made once per class, and only
#: ``storage``, usually an instance attribute, is checked on every instance.
_CONTEXT_CLASSES: set = set()


def _check_context(context) -> None:
    if context is None:
        return
    cls = type(context)
    if cls in _CONTEXT_CLASSES and hasattr(context, "storage"):
        return
    if not isinstance(context, PersistenceContext):
        raise ProtoValidationError(f"context must be a PersistenceContext, not {cls.__name__}")
    _CONTEXT_CLASSES.add(cls)


class Atom:
    """Base class of every persistent object.

    Subclasses call ``super().__init__(context=...)`` and store their persisted
    state as public instance attributes.  Each subclass is registered under
    ``__type_name__`` (default ``"module.QualifiedName"``); set it explicitly to
    keep a stable name across refactors.
    """

    __type_name__ = "proto_db.core.atom.Atom"
    #: Whether a context that caches decoded atoms keeps this type's atoms when they are saved
    #: (Task 8.3i); a type a build only references, never reads, opts out.
    _cache_on_save = True

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if "__type_name__" not in cls.__dict__:
            cls.__type_name__ = f"{cls.__module__}.{cls.__qualname__}"
        register_atom_class(cls)

    def __init__(self, *, context: PersistenceContext | None = None):
        _check_context(context)
        osa = object.__setattr__
        osa(self, "_loaded", True)
        osa(self, "atom_pointer", None)
        osa(self, "context", context)

    # -- construction paths ---------------------------------------------------------

    @staticmethod
    def _class_for(type_name: str) -> type:
        """Return the registered class for ``type_name``; ``ProtoCorruptionError`` if unknown.

        On a registry miss for a built-in type (:data:`BUILTIN_ATOM_MODULES`) its
        module is imported (registering the class) and the lookup retried.
        """
        with _registry_lock:
            cls = _registry.get(type_name)
        if cls is None:
            module = BUILTIN_ATOM_MODULES.get(type_name)
            if module is not None:
                importlib.import_module(module)  # outside the lock: registration takes it
                with _registry_lock:
                    cls = _registry.get(type_name)
        if cls is None:
            raise ProtoCorruptionError(f"Unknown atom type {type_name!r}")
        return cls

    @classmethod
    def _stub(cls, context: PersistenceContext, ptr: AtomPointer, type_name: str) -> "Atom":
        """Return an unloaded atom of class ``type_name`` stored at ``ptr``.

        The class is resolved through the registry; it must be ``cls`` or a subclass.
        """
        target = Atom._class_for(type_name)
        if not issubclass(target, cls):
            raise ProtoCorruptionError(f"Atom type {type_name!r} is not a {cls.__type_name__}")
        stub = object.__new__(target)
        osa = object.__setattr__
        osa(stub, "_loaded", False)
        osa(stub, "atom_pointer", ptr)
        osa(stub, "context", context)
        return stub

    def _bind(self, context: PersistenceContext) -> None:
        """Attach ``context`` to an atom that has none (protected; used by contexts)."""
        with _lock_for(self):
            if self.context is None:
                object.__setattr__(self, "context", context)

    # -- lazy load ----------------------------------------------------------------

    def __getattr__(self, name: str):
        # Only reached when normal lookup fails: an unloaded stub or a truly absent attribute.
        if name.startswith("_"):
            raise AttributeError(name)
        try:
            loaded = object.__getattribute__(self, "_loaded")
        except AttributeError:
            raise AttributeError(name) from None
        if not loaded:
            self._load()
        # Look again even when the atom was already loaded: another thread may have
        # finished the load between the failed normal lookup and the flag check.
        try:
            return object.__getattribute__(self, name)
        except AttributeError:
            if name == "oid":
                return self._assign_oid()
            return self._absent_attribute(name)

    def _assign_oid(self) -> uuid.UUID:
        """Return the oid of a loaded atom, generating it on first need (D21)."""
        with _lock_for(self):
            return self._oid_locked()

    def _oid_locked(self) -> uuid.UUID:
        """Body of :meth:`_assign_oid`; the caller holds ``_lock_for(self)``."""
        oid = self.__dict__.get("oid")
        if oid is None:
            oid = uuid.uuid4()
            object.__setattr__(self, "oid", oid)
        return oid

    def _absent_attribute(self, name: str):
        """Value of a public attribute that is not set on a loaded atom (default: raise)."""
        raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")

    def _load(self) -> None:
        """Load the state of a stub, once; a no-op for loaded atoms.

        Three steps, so that no stripe is held while other atoms are touched:
        fetch the payload once under the stripe, decode it unlocked (decoding
        creates child stubs and may hash them, which loads them), publish under
        the stripe unless another thread published first.
        """
        if self._loaded:
            return
        lock = _lock_for(self)
        with lock:
            if self._loaded:
                return
            payload = self.__dict__.get("_payload")
            if payload is None:
                payload = self.context.storage.get_atom(self.atom_pointer)
                if not isinstance(payload, dict) or set(payload) != {"t", "oid", "s"}:
                    raise ProtoCorruptionError(f"Malformed atom payload at {self.atom_pointer}")
                stored = type_name_of(payload["t"])
                if stored != type(self).__type_name__:
                    raise ProtoCorruptionError(
                        f"Atom at {self.atom_pointer} is a {stored!r}, expected {type(self).__type_name__!r}")
                raw_oid = payload["oid"]
                if not isinstance(raw_oid, bytes) or len(raw_oid) != 16:
                    raise ProtoCorruptionError(f"Malformed oid in atom at {self.atom_pointer}")
                object.__setattr__(self, "_payload", payload)
        from . import codec
        state = codec.decode_state(payload["s"], self.context.load)
        oid = uuid.UUID(bytes=payload["oid"])
        with lock:
            if self._loaded:
                return
            self._restore(state)
            object.__setattr__(self, "oid", oid)
            object.__setattr__(self, "_loaded", True)
            object.__delattr__(self, "_payload")

    def _restore(self, state: dict) -> None:
        """Install a decoded state on a stub (protected hook; default sets attributes)."""
        cls = type(self)
        for name in state:
            if name.startswith("_") or name in RESERVED_NAMES or hasattr(cls, name):
                raise ProtoCorruptionError(f"Invalid persisted attribute name {name!r} for {cls.__type_name__}")
        osa = object.__setattr__
        for name, value in state.items():
            osa(self, name, value)

    # -- save -----------------------------------------------------------------------

    def _state(self) -> dict:
        """Return the persisted state: public instance attributes except the reserved ones.

        Iterates over a copy of the instance dictionary (copied atomically): other
        threads may add runtime keys concurrently, such as a lazily assigned ``oid``.
        """
        self._load()
        return {k: v for k, v in vars(self).copy().items()
                if not k.startswith("_") and k not in RESERVED_NAMES}

    def _save(self) -> AtomPointer:
        """Save this atom (children first) unless already saved; return its pointer.

        Children are saved while no stripe is held; only the publication of this
        atom's payload runs under its stripe (see the module lock order rule).
        """
        ptr = self.atom_pointer
        if ptr is not None:
            return ptr
        context = self.context
        if context is None:
            raise ProtoUserError(f"Cannot save {self!r}: it has no persistence context")
        from . import codec
        state = self._state()
        cls = type(self)
        for name in state:
            if hasattr(cls, name):
                raise ProtoValidationError(
                    f"Attribute {name!r} of {cls.__type_name__} shadows a class attribute and "
                    f"could not be loaded back lazily")
        encoded = codec.encode_state(state, context.save)
        with _lock_for(self):
            ptr = self.atom_pointer
            if ptr is not None:
                return ptr
            payload = {"t": type_tag(cls.__type_name__), "oid": self._oid_locked().bytes, "s": encoded}
            ptr = context.storage.push_atom(payload)
            object.__setattr__(self, "atom_pointer", ptr)
        saved = getattr(context, "saved", None)
        if saved is not None:
            saved(self, ptr, payload)       # no stripe held (lock order rule)
        return ptr

    @classmethod
    def _preloaded_stub(cls, context: PersistenceContext, ptr: AtomPointer, payload: dict) -> "Atom":
        """Return a stub of ``cls`` at ``ptr`` that already holds ``payload``, the payload just
        pushed there (protected; used by contexts that cache what they save, Task 8.3i).

        Its first access decodes ``payload`` exactly as a load would decode the bytes read back
        from storage (:meth:`_load`), without the fetch, the CRC check and the msgpack step.
        ``payload`` must not be modified afterwards (storages only serialize it)."""
        stub = object.__new__(cls)
        osa = object.__setattr__
        osa(stub, "_loaded", False)
        osa(stub, "atom_pointer", ptr)
        osa(stub, "context", context)
        osa(stub, "_payload", payload)
        return stub

    # -- identity -------------------------------------------------------------------

    def __eq__(self, other):
        if not isinstance(other, Atom):
            return NotImplemented
        return self is other or self.oid == other.oid

    def __hash__(self):
        return hash(self.oid)

    def __repr__(self):
        if self.__dict__.get("_loaded", True):
            return f"<{type(self).__type_name__} oid={self.__dict__.get('oid')}>"
        return f"<{type(self).__type_name__} stub at {self.atom_pointer}>"
