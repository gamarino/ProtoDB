"""Atoms: the unit of persistence (spec section 5, decisions D10 and R3).

An atom has a stable identity (``oid``, a UUID4 created at construction and
persisted), an ``atom_pointer`` once saved, and a ``context`` (a
:class:`PersistenceContext`) through which it is saved and its references are
loaded.  Its persisted state is its public instance attributes; attributes whose
name starts with an underscore are runtime only.

Lazy loading
    An atom obtained from a pointer is a *stub*: only ``context`` and
    ``atom_pointer`` are set.  The first access to any other public attribute,
    ``oid`` included, loads the state.  Loading runs under a per-atom lock; the
    state is fully decoded before it is published and the loaded flag is set
    last, so a thread either finds an attribute with its final value or waits
    for the load to finish.  No thread ever observes a half-loaded atom.
    Equality and hashing use ``oid`` (D10), therefore comparing or hashing a
    stub loads it.

    Persisted attributes must not have class-level defaults: normal attribute
    lookup would find the class attribute on a stub and skip the load.

Saving
    :meth:`Atom._save` is idempotent and thread-safe: children are saved first
    (through ``context.save``), then the atom's own payload
    ``{"t": type_name, "oid": oid_bytes, "s": encoded_state}`` is pushed to
    ``context.storage``.  Concurrent calls produce exactly one pointer.
"""
from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from ..errors import ProtoCorruptionError, ProtoUserError, ProtoValidationError

__all__ = ["AtomPointer", "PersistenceContext", "Atom", "register_atom_class", "RESERVED_NAMES"]

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


_registry: dict[str, type] = {}
_registry_lock = threading.Lock()


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


def _check_context(context) -> None:
    if context is not None and not isinstance(context, PersistenceContext):
        raise ProtoValidationError(f"context must be a PersistenceContext, not {type(context).__name__}")


class Atom:
    """Base class of every persistent object.

    Subclasses call ``super().__init__(context=...)`` and store their persisted
    state as public instance attributes.  Each subclass is registered under
    ``__type_name__`` (default ``"module.QualifiedName"``); set it explicitly to
    keep a stable name across refactors.
    """

    __type_name__ = "proto_db.core.atom.Atom"

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if "__type_name__" not in cls.__dict__:
            cls.__type_name__ = f"{cls.__module__}.{cls.__qualname__}"
        register_atom_class(cls)

    def __init__(self, *, context: PersistenceContext | None = None):
        _check_context(context)
        osa = object.__setattr__
        osa(self, "_lock", threading.RLock())
        osa(self, "_loaded", True)
        osa(self, "oid", uuid.uuid4())
        osa(self, "atom_pointer", None)
        osa(self, "context", context)

    # -- construction paths ---------------------------------------------------------

    @staticmethod
    def _class_for(type_name: str) -> type:
        """Return the registered class for ``type_name``; ``ProtoCorruptionError`` if unknown."""
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
        osa(stub, "_lock", threading.RLock())
        osa(stub, "_loaded", False)
        osa(stub, "atom_pointer", ptr)
        osa(stub, "context", context)
        return stub

    def _bind(self, context: PersistenceContext) -> None:
        """Attach ``context`` to an atom that has none (protected; used by contexts)."""
        with self._lock:
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
            return self._absent_attribute(name)

    def _absent_attribute(self, name: str):
        """Value of a public attribute that is not set on a loaded atom (default: raise)."""
        raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")

    def _load(self) -> None:
        """Load the state of a stub, once; a no-op for loaded atoms."""
        if self._loaded:
            return
        with self._lock:
            if self._loaded:
                return
            payload = self.context.storage.get_atom(self.atom_pointer)
            if not isinstance(payload, dict) or set(payload) != {"t", "oid", "s"}:
                raise ProtoCorruptionError(f"Malformed atom payload at {self.atom_pointer}")
            if payload["t"] != type(self).__type_name__:
                raise ProtoCorruptionError(
                    f"Atom at {self.atom_pointer} is a {payload['t']!r}, expected {type(self).__type_name__!r}")
            raw_oid = payload["oid"]
            if not isinstance(raw_oid, bytes) or len(raw_oid) != 16:
                raise ProtoCorruptionError(f"Malformed oid in atom at {self.atom_pointer}")
            from . import codec
            state = codec.decode_state(payload["s"], self.context.load)
            self._restore(state)
            object.__setattr__(self, "oid", uuid.UUID(bytes=raw_oid))
            object.__setattr__(self, "_loaded", True)

    def _restore(self, state: dict) -> None:
        """Install a decoded state on a stub (protected hook; default sets attributes)."""
        for name in state:
            if name.startswith("_") or name in RESERVED_NAMES:
                raise ProtoCorruptionError(f"Invalid persisted attribute name {name!r}")
        osa = object.__setattr__
        for name, value in state.items():
            osa(self, name, value)

    # -- save -----------------------------------------------------------------------

    def _state(self) -> dict:
        """Return the persisted state: public instance attributes except the reserved ones."""
        self._load()
        return {k: v for k, v in vars(self).items()
                if not k.startswith("_") and k not in RESERVED_NAMES}

    def _save(self) -> AtomPointer:
        """Save this atom (children first) unless already saved; return its pointer."""
        ptr = self.atom_pointer
        if ptr is not None:
            return ptr
        with self._lock:
            if self.atom_pointer is not None:
                return self.atom_pointer
            context = self.context
            if context is None:
                raise ProtoUserError(f"Cannot save {self!r}: it has no persistence context")
            from . import codec
            encoded = codec.encode_state(self._state(), context.save)
            payload = {"t": type(self).__type_name__, "oid": self.oid.bytes, "s": encoded}
            ptr = context.storage.push_atom(payload)
            object.__setattr__(self, "atom_pointer", ptr)
            return ptr

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
