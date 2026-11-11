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
    ``context._atom_saved(atom, ptr, state)`` (private, so it never meets a user context's own
    attribute; no stripe held): a context that caches decoded atoms
    keeps the atom a load of ``ptr`` would produce (:meth:`Atom._decoded_twin`, Task 8.3i),
    never the live object, whose runtime state (an op log, for example) a loaded atom has not.

History links (Task 8.3l, ruling R92)
    A class lists in ``_history_links`` the persisted attributes that link an older version
    of the data (``DatabaseRoot.previous``, ``SpaceRoot.cluster_previous``).  Such an
    attribute is stored like any atom reference (an ``@ref``; the format is unchanged), but in
    memory the atom holds only the linked atom's pointer and class: every read of the attribute
    asks the atom's ``context`` to ``load`` it, which returns the shared atom of a bounded
    cache or a fresh stub.  A loaded atom therefore never keeps its predecessor loaded, and
    walking history loads one version at a time: history is kept on disk, never in memory
    (P1).  A link to an atom not yet saved holds that atom until it has a pointer.

Residency (Task 8.3m, rulings R102-R104)
    A context that caches decoded atoms (the space's ``_AtomCache``) bounds how many of them are
    loaded.  The objects it produces -- stubs from its ``load`` and decoded twins -- carry the mark
    in the value of ``_loaded`` itself: :data:`_UNLOADED` (falsy) while unloaded, a
    :class:`_Generation` token while loaded; every other atom holds ``False`` / ``True``.  Only a
    marked atom is ever *unloaded* (:meth:`Atom._unload`), never a live atom, an unsaved one or
    one of another context.  (A separate mark key would cost the inline-values layout: an
    attribute added after the class's usual keys materializes the instance dict, and every read
    of that atom then takes the slower dict path.)  Unloading returns the atom to the stub state
    in place, under its stripe: ``_loaded`` is set to :data:`_UNLOADED` FIRST, then every key but
    :data:`_STUB_KEYS` (pointer, context, oid) is deleted.  A load publishes the decoded
    state and sets ``_loaded`` LAST -- to a fresh generation token for a marked atom -- and tells the
    context through ``_atom_loaded(atom)`` so that it is admitted again.

    Why readers stay correct: a pointer's content never changes (P1), so every generation of an
    atom's state holds equal values, and any interleaving of an old and a reloaded state reads
    equal values.  Only two failures are possible, and both are excluded here:

    - a missing attribute taken as absent: a reader that misses a name re-checks that the
      generation token it saw is still the current one (:meth:`__getattr__`, :meth:`_probe`); a
      multi-key reader reads the names the token recorded and trusts the copy only if the token
      is unchanged afterwards (:meth:`_snapshot`);
    - a publish into the wrong generation, or a publish that is not admitted (review 8.3m C1):
      phase 3 of :meth:`_load` publishes only if ``_loaded`` is still the :class:`_Pending` its
      phase 1 installed (:func:`_same_generation`), otherwise the atom was loaded and unloaded
      meanwhile and the load restarts; and every publish tells the context, which admits it.

Type tags (format version 3, D51)
    A built-in type is written as its small integer alias, its index in
    :data:`BUILTIN_TYPE_ALIASES` (:func:`type_tag`); any other type as its name.  Readers
    accept both forms (:func:`type_name_of`), so format-2 data, which always wrote names,
    still loads.

Type names (ruling R83, D57)
    A persisted type name is language-neutral: a dotted name of ASCII identifiers with at
    least two parts (:data:`TYPE_NAME_PATTERN`, for example ``"acme.sales.Order"``), chosen
    by the class author and declared as ``__type_name__`` in the class's own body; another
    implementation of the format (protoBaseST) maps it to its own class.  A class that does not
    declare one gets its Python ``module.QualifiedName`` only as a *legacy read name*: data
    written under it before D57 still loads, but saving an instance raises
    ``ProtoValidationError``, so no new data depends on a Python module path.  A class lists
    the names it was persisted under before (a rename, or the legacy default it had) in
    ``__type_aliases__`` (its own body, not inherited): each alias loads as the class and
    :func:`type_name_of` maps it to the declared name, so one stored atom has one identity
    whichever name a reference spells.  The format is unchanged: names were always strings.
"""
from __future__ import annotations

import importlib
import re
import threading
import uuid
from dataclasses import dataclass
from typing import Any, Optional, Protocol, runtime_checkable

from ..errors import ProtoCorruptionError, ProtoNotSupportedError, ProtoUserError, ProtoValidationError

__all__ = ["AtomPointer", "PersistenceContext", "Atom", "register_atom_class", "RESERVED_NAMES",
           "BUILTIN_TYPE_ALIASES", "TYPE_NAME_PATTERN", "type_tag", "type_name_of"]

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

    Not part of the protocol: a context may define the private hook
    ``_atom_saved(atom, ptr, state)``, which :meth:`Atom._save` calls after each push (the
    decoded-atom cache uses it, Task 8.3i).  A context without it is unaffected.
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
#: Alias -> the declared name of the class it names (``__type_aliases__``, D57).
_alias_target: dict[str, str] = {}

#: Grammar of a declared atom type name (D57): two or more ASCII identifiers joined by dots.
TYPE_NAME_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+\Z")

#: Module defining each built-in 2.0 atom type, imported on a registry miss so that
#: stored atoms load in a process that never imported their module.  Kept complete
#: by tests/unit/test_atom_type_modules.py (every built-in Atom subclass must appear).
BUILTIN_ATOM_MODULES: dict[str, str] = {
    "proto_db.core.dbobject.DBObject": "proto_db.core.dbobject",
    "proto_db.avl._Node": "proto_db.collections.avl",
    "proto_db.avl.PosNode": "proto_db.collections.avl",
    "proto_db.avl.KeyNode": "proto_db.collections.avl",
    "proto_db.avl.CountedKeyNode": "proto_db.collections.avl",
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
    "proto_db.avl.CountedKeyNode",
)
_ALIAS_OF = {name: i for i, name in enumerate(BUILTIN_TYPE_ALIASES)}


def type_tag(type_name: str):
    """Return the persisted tag of ``type_name``: its alias for a built-in type, else the name."""
    alias = _ALIAS_OF.get(type_name)
    return type_name if alias is None else alias


def _walk_mro(cls: type, name: str) -> bool:
    return any(name in klass.__dict__ for klass in cls.__mro__)


#: Atom class -> {name the class read does not find: whether its MRO defines it anyway} (R137a).
_unreadable_names: dict = {}


def class_defines(cls: type, name: str) -> bool:
    """Whether ``cls`` or a class of its MRO defines ``name``: what an instance attribute read
    would find before a persisted field.  Attributes of the metaclass (``mro``, ``register``)
    are not seen by instance reads and do not count (pre-tag review m1).

    The rule is the MRO walk; this answers it without walking for every field of every saved and
    loaded atom (R137a).  A name ``hasattr`` finds on the class is walked each time (a metaclass
    attribute is found too, so the walk decides); every class attribute, however added, is found.
    A name it does not find is defined only by a descriptor that raises ``AttributeError`` when
    read on the class: that answer is walked once per class and name and kept, so it assumes no
    such descriptor is added to or removed from a class after the class is in use.  The cache
    keeps its classes alive, as the atom type registry already does for every Atom class."""
    try:
        if hasattr(cls, name):
            return _walk_mro(cls, name)
    except Exception:
        return _walk_mro(cls, name)     # a descriptor raising another error is still in the MRO
    known = _unreadable_names.get(cls)
    if known is None:
        known = _unreadable_names.setdefault(cls, {})
    answer = known.get(name)
    if answer is None:
        answer = known[name] = _walk_mro(cls, name)
    return answer


def type_name_of(tag) -> str:
    """Return the type name a persisted tag stands for; ``ProtoCorruptionError`` if malformed.

    An alias (``__type_aliases__``, D57) gives the declared name of its class."""
    if type(tag) is str:
        return _alias_target.get(tag, tag)
    if type(tag) is int and 0 <= tag < len(BUILTIN_TYPE_ALIASES):
        return BUILTIN_TYPE_ALIASES[tag]
    raise ProtoCorruptionError(f"Malformed atom type tag {tag!r}")


def register_atom_class(cls: type) -> type:
    """Register ``cls`` under ``cls.__type_name__`` and its ``__type_aliases__``; return ``cls``.

    Called automatically for every :class:`Atom` subclass.  Registering the same
    class again is a no-op; a different class with an already registered name or
    alias raises ``ProtoUserError``.
    """
    name = cls.__dict__.get("__type_name__")
    if not isinstance(name, str) or not name:
        raise ProtoValidationError(f"{cls!r} has no valid __type_name__")
    aliases = _declared_aliases(cls, name)
    with _registry_lock:
        for key in (name,) + aliases:
            current = _registry.get(key)
            if current is not None and current is not cls:
                raise ProtoUserError(f"Atom type name {key!r} is already registered by {current!r}")
        for key in (name,) + aliases:
            _registry[key] = cls
        for alias in aliases:
            _alias_target[alias] = name
    return cls


def _declared_aliases(cls: type, name: str) -> tuple:
    """The ``__type_aliases__`` of ``cls``'s own body, validated (D57)."""
    aliases = cls.__dict__.get("__type_aliases__", ())
    if not isinstance(aliases, (tuple, list)) or not all(isinstance(a, str) and a for a in aliases):
        raise ProtoUserError(f"{cls.__qualname__}.__type_aliases__ must be a tuple of non-empty str, "
                             f"not {aliases!r}")
    if name in aliases or len(set(aliases)) != len(aliases):
        raise ProtoUserError(f"{cls.__qualname__}.__type_aliases__ repeats a name: {aliases!r}")
    return tuple(aliases)


#: Context types whose instances passed the :class:`PersistenceContext` check and whose ``save``
#: and ``load`` are callable class attributes (Task 8.3i).  The protocol check (``isinstance`` on
#: a runtime-checkable protocol) cost about 5 us of the 7 us a tree node takes to build.  For
#: such a type only ``storage``, usually an instance attribute, can differ between instances, so
#: only it is checked again; any other type (methods carried per instance, a SimpleNamespace)
#: gets the full check every time (review 8.3i M2).
_CONTEXT_CLASSES: set = set()


def _check_context(context) -> None:
    if context is None:
        return
    cls = type(context)
    if cls in _CONTEXT_CLASSES and hasattr(context, "storage"):
        return
    if not isinstance(context, PersistenceContext):
        raise ProtoValidationError(f"context must be a PersistenceContext, not {cls.__name__}")
    if callable(getattr(cls, "save", None)) and callable(getattr(cls, "load", None)):
        _CONTEXT_CLASSES.add(cls)


def _link_ref(value):
    """How a history link holds ``value``: ``(pointer, class)`` for a saved atom, else the
    value itself (``None``, or an atom without a pointer yet)."""
    if isinstance(value, Atom):
        ptr = value.atom_pointer
        if ptr is not None:
            return ptr, type(value)
    return value


#: Keys an unloaded atom keeps: what a stub has and its oid (equality and hashing never load).
_STUB_KEYS = frozenset({"_loaded", "atom_pointer", "context", "oid"})

#: Result of :meth:`Atom._probe` for a name the loaded state does not define.
_ABSENT = object()

#: The residency paths -- load, admission, unload, the readers' re-checks -- read attributes,
#: never the instance ``__dict__``: materializing it costs the atom its specialized attribute
#: reads for good (CPython 3.14: ``LOAD_ATTR_INSTANCE_VALUE`` is never chosen for an object whose
#: dict exists; measured 4x per read, Task 8.3m).  Plain attribute syntax is used wherever the
#: name is present or starts with ``_`` (``__getattr__`` refuses those at once); this reads a
#: public name that may be absent without entering ``__getattr__`` (about 5x slower than the
#: syntax, so kept off the paths that do not need it).
_get = object.__getattribute__


class _Generation:
    """A load generation token: the truthy value of ``_loaded`` for one loaded state of an atom
    that may be unloaded (a fresh object per load, so a reader can tell two generations apart).
    ``names``: the keys that load installed, which an unload deletes."""

    __slots__ = ("names",)

    def __init__(self, names: tuple = ()):
        self.names = names

    def __repr__(self):
        return "<loaded>"


def _installed_names(cls, state: dict) -> tuple:
    """The keys :meth:`Atom._restore` installs for ``state``: its attributes, and ``_links`` for
    the history links among them."""
    links = cls._history_links
    if not links:
        return tuple(state)
    names = tuple(name for name in state if name not in links)
    if any(name in state for name in links):
        names += ("_links",)
    return names


class _Unloaded:
    """The type of :data:`_UNLOADED`."""

    __slots__ = ()

    def __bool__(self):
        return False

    def __repr__(self):
        return "<unloaded>"


#: ``_loaded`` of an unloaded atom produced by a decoded-atom cache (falsy, like ``False``).
_UNLOADED = _Unloaded()


def _is_cache_produced(atom) -> bool:
    """Whether the loaded ``atom`` may be unloaded: produced by a decoded-atom cache (its
    ``_loaded`` is a generation token) and saved (Task 8.3m, review I4)."""
    try:
        return isinstance(atom._loaded, _Generation) and _get(atom, "atom_pointer") is not None
    except AttributeError:
        return False


class _Pending:
    """``_loaded`` of a stub being loaded (falsy): the payload phase 1 fetched, shared by
    concurrent loaders, and whether the stub carried the cache's mark.  Held in ``_loaded``
    rather than in a key of its own, which would cost the inline-values layout (Task 8.3m)."""

    __slots__ = ("payload", "marked")

    def __init__(self, payload: dict, marked: bool):
        self.payload = payload
        self.marked = marked

    def __bool__(self):
        return False

    def __repr__(self):
        return "<loading>"


def _same_generation(atom, pending) -> bool:
    """Whether phase 3 of a load may publish: ``_loaded`` is still the :class:`_Pending` phase 1
    installed, so no load published (and no unload followed) in between (review 8.3m C1)."""
    return atom._loaded is pending


def _contract_error(cls, missing, extra) -> ProtoUserError:
    """The error of a :meth:`Atom._restore` that does not install exactly the decoded state's keys."""
    return ProtoUserError(
        f"{cls.__type_name__}._restore broke the residency contract: it must install exactly the "
        f"keys of the decoded state (history links under _links), because an unload deletes those "
        f"and a load reads them back; missing {sorted(missing)}, extra {sorted(extra)} "
        f"(Atom._restore docstring, D53)")


def _restore_checked(atom, state: dict, names: tuple) -> None:
    """Install ``state`` on the stub ``atom`` through its :meth:`Atom._restore`, running the hook
    exactly once (re-review 8.3m N2, ruling R114).

    A class that keeps the default hooks installs ``names`` by construction and restores in
    place.  For a class that overrides :meth:`Atom._restore` or :meth:`Atom._set_link`, the hook
    runs on a scratch instance (whose dict may be inspected at no cost to ``atom``); the keys it
    installed must be exactly ``names`` (review 8.3m I1 / M7, ruling R105), or one
    ``ProtoUserError`` names the class and the keys; then the installed values are moved onto
    ``atom`` in the order the hook installed them.  The hook never runs twice on one ``state``,
    so an override may transform the values in place."""
    cls = type(atom)
    if not cls._custom_restore:
        atom._restore(state)
        return
    scratch = object.__new__(cls)
    osa = object.__setattr__
    osa(scratch, "_loaded", False)
    osa(scratch, "atom_pointer", None)
    osa(scratch, "context", _get(atom, "context"))
    scratch._restore(state)
    installed = {name: value for name, value in _get(scratch, "__dict__").items()
                 if name not in _STUB_KEYS}
    expected = set(names)
    if installed.keys() != expected:
        raise _contract_error(cls, expected - installed.keys(), installed.keys() - expected)
    for name, value in installed.items():
        osa(atom, name, value)


def _drop_state(atom, names: tuple) -> None:
    """Delete the keys ``names`` that the unloading atom's load installed (caller holds its
    stripe and has set ``_loaded`` to :data:`_UNLOADED`).  Runtime keys a reader added since
    (``KeyNode._okey``) stay: they hold values computed from the immutable content."""
    for name in names:
        if name in _STUB_KEYS:
            continue
        try:
            object.__delattr__(atom, name)
        except AttributeError:
            pass        # not installed by this load (defensive): nothing to drop


class Atom:
    """Base class of every persistent object.

    Subclasses call ``super().__init__(context=...)`` and store their persisted
    state as public instance attributes.  Each subclass is registered under
    ``__type_name__``, a language-neutral dotted name declared in its own body
    (:data:`TYPE_NAME_PATTERN`, D57), and under the old names it lists in
    ``__type_aliases__``.  A subclass that declares no name is registered under its
    Python ``"module.QualifiedName"`` for reading data written before D57 only:
    saving its instances raises ``ProtoValidationError``.
    """

    __type_name__ = "proto_db.core.atom.Atom"
    #: Whether ``__type_name__`` was declared in the class's own body (D57): only such a class
    #: is saved.  The base class itself is never persisted.
    _type_name_declared = False
    #: Whether a context that caches decoded atoms keeps this type's atoms when they are saved
    #: (Task 8.3i); a type a build only references, never reads, opts out.
    _cache_on_save = True
    #: Whether such a context keeps this type's atoms decoded on save even when they hold large
    #: values inline (review 8.3i I1); otherwise those are left to the read path.
    _cache_large_on_save = False
    #: Persisted attributes linking an older version, held as pointers in memory and loaded on
    #: each read through the context (Task 8.3l, see the module docstring).
    _history_links: tuple = ()
    #: Persisted fields every record of this exact type must hold (D58): a record without one is
    #: ``ProtoCorruptionError`` when it is loaded.  Fields a type may omit are not listed.
    _required_fields: tuple = ()
    #: Whether this class overrides the hooks that install a loaded state, so that each load
    #: checks they installed exactly the decoded state's keys (:func:`_restore_checked`).
    _custom_restore = False

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._custom_restore = cls._restore is not Atom._restore or cls._set_link is not Atom._set_link
        if cls.__dict__.get("_opaque_stand_in"):
            cls._type_name_declared = True      # an OpaqueAtom stand-in: never registered (D59)
            return
        declared = "__type_name__" in cls.__dict__
        if declared:
            name = cls.__dict__["__type_name__"]
            if not isinstance(name, str) or not TYPE_NAME_PATTERN.match(name):
                raise ProtoUserError(
                    f"{cls.__qualname__}.__type_name__ must be a dotted name of ASCII identifiers with at "
                    f"least two parts, such as 'acme.sales.Order', not {name!r} (D57)")
        else:
            cls.__type_name__ = f"{cls.__module__}.{cls.__qualname__}"     # legacy read name only
        cls._type_name_declared = declared
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

        The class is resolved through the registry; it must be ``cls`` or a subclass.  A type
        name this process has no class for, written by another implementation, gives an
        :class:`OpaqueAtom` (D59) when it is a valid name outside the reserved ``proto_db.``
        namespace; using it as a ``cls`` other than :class:`Atom` is ``ProtoNotSupportedError``.
        """
        try:
            target = Atom._class_for(type_name)
        except ProtoCorruptionError:
            target = opaque_class(type_name)
        if not issubclass(target, cls):
            if issubclass(target, OpaqueAtom):
                raise ProtoNotSupportedError(
                    f"The atom at {ptr} is of type {type_name!r}, which has no class registered in this "
                    f"process: it cannot be used as a {cls.__type_name__}")
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
        # Only reached when normal lookup fails: an unloaded stub, an atom unloaded meanwhile, or a
        # truly absent attribute.  Absence is concluded only from a generation that is still
        # current after the miss (see "Residency" in the module docstring).
        if name.startswith("_"):
            raise AttributeError(name)
        while True:
            try:
                token = self._loaded
            except AttributeError:
                raise AttributeError(name) from None
            if not token:
                snapshot = self._load()
                if snapshot is not None:
                    return self._attribute_in(snapshot, name)
                continue                    # another thread published first: read again
            try:
                return _get(self, name)
            except AttributeError:
                pass
            try:
                links = self._links
            except AttributeError:
                links = None
            if self._loaded is token:
                return self._missing_attribute(links, name)

    def _attribute_in(self, snapshot: dict, name: str):
        """The attribute ``name`` read from ``snapshot``, a complete loaded state."""
        value = snapshot.get(name, _ABSENT)
        if value is not _ABSENT:
            return value
        return self._missing_attribute(snapshot.get("_links"), name)

    def _missing_attribute(self, links, name: str):
        """``name`` is not an attribute of the current loaded state: the lazily assigned oid, a
        history link, or the class's answer for an absent attribute."""
        if name == "oid":
            return self._assign_oid()
        if links is not None and name in links:
            return self._follow_link(links, name)
        return self._absent_attribute(name)

    def _adopt_stored_type(self, stored: str) -> bool:
        """Overridden by :class:`OpaqueAtom`: a known class never adopts another stored type."""
        return False

    def _probe(self, name: str):
        """The persisted attribute ``name`` of the loaded state, or :data:`_ABSENT` (loading the
        atom if needed; safe across an unload, like :meth:`__getattr__`).  A persisted name never
        names a class attribute (construction and :meth:`_restore` refuse it), so an instance
        read answers without materializing the dict."""
        if class_defines(type(self), name):
            return _ABSENT
        while True:
            try:
                token = self._loaded
            except AttributeError:
                token = True
            if not token:
                snapshot = self._load()
                if snapshot is not None:
                    return snapshot.get(name, _ABSENT)
                continue
            try:
                return _get(self, name)
            except AttributeError:
                pass
            try:
                current = self._loaded
            except AttributeError:
                current = True
            if current is token:
                return _ABSENT

    def _snapshot(self) -> dict:
        """A complete copy of the loaded state (loading the atom if needed).  An atom a cache may
        unload is read by the names its load installed, and trusted only if its generation is
        unchanged afterwards; any other atom is never unloaded, and its dict is copied (an atomic
        copy, as before Task 8.3m)."""
        while True:
            try:
                token = self._loaded
            except AttributeError:
                token = True
            if isinstance(token, _Generation):
                copy = {}
                missing = None
                try:
                    for missing in token.names:
                        copy[missing] = _get(self, missing)
                    missing = "context"
                    copy["context"] = _get(self, "context")
                except AttributeError:
                    if self._loaded is token:
                        # Not an unload: the load did not install a name it recorded (I1, R105).
                        raise _contract_error(type(self), [missing], ()) from None
                    continue                # unloaded meanwhile
                if self._loaded is token:
                    return copy
                continue
            if token:
                return _get(self, "__dict__").copy()
            snapshot = self._load()
            if snapshot is not None:
                return snapshot

    # -- history links (Task 8.3l) --------------------------------------------------------

    def _set_link(self, name: str, value) -> None:
        """Install the history link ``name`` (one of ``_history_links``) to ``value`` (protected:
        used by constructors and :meth:`_restore`): a saved atom is held as ``(pointer, class)``,
        anything else (``None``, an atom not saved yet) as itself."""
        try:
            links = self._links
        except AttributeError:
            links = {}
            object.__setattr__(self, "_links", links)
        try:
            context = _get(self, "context")
        except AttributeError:
            context = None
        # Without a context the link could not be loaded again: the atom is held until one is bound.
        links[name] = _link_ref(value) if context is not None else value

    def _follow_link(self, links: dict, name: str):
        """The value of the history link ``name``: the linked atom loaded on demand through
        this atom's context, never kept by this atom."""
        ref = links[name]
        if type(ref) is tuple:
            return self.context.load(ref[0], ref[1].__type_name__)
        if isinstance(ref, Atom) and ref.atom_pointer is not None and self.context is not None:
            links[name] = _link_ref(ref)        # saved since: from now on held by pointer only
        return ref

    def _link_values(self, links, context) -> dict:
        """The history links ``links`` (of a loaded state whose context is ``context``) as values
        to persist: stubs for the links held by pointer."""
        if not links:
            return {}
        osa = object.__setattr__
        out = {}
        for name, ref in links.items():
            if type(ref) is tuple:
                stub = object.__new__(ref[1])       # what Atom._stub builds; the class is known
                osa(stub, "_loaded", False)
                osa(stub, "atom_pointer", ref[0])
                osa(stub, "context", context)
                ref = stub
            out[name] = ref
        return out

    def _assign_oid(self) -> uuid.UUID:
        """Return the oid of a loaded atom, generating it on first need (D21)."""
        with _lock_for(self):
            return self._oid_locked()

    def _oid_locked(self) -> uuid.UUID:
        """Body of :meth:`_assign_oid`; the caller holds ``_lock_for(self)``."""
        try:
            oid = _get(self, "oid")
        except AttributeError:
            oid = None
        if oid is None:
            oid = uuid.uuid4()
            object.__setattr__(self, "oid", oid)
        return oid

    def _absent_attribute(self, name: str):
        """Value of a public attribute that is not set on a loaded atom (default: raise)."""
        raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")

    def _load(self) -> Optional[dict]:
        """Load the state of a stub; a no-op for loaded atoms.

        Three steps, so that no stripe is held while other atoms are touched:
        fetch the payload once under the stripe (kept in ``_loaded`` as a :class:`_Pending`),
        decode it unlocked (decoding
        creates child stubs and may hash them, which loads them), publish under
        the stripe unless another thread published first.  Publication happens only into the
        generation phase 1 saw (:func:`_same_generation`); if the atom was loaded and unloaded
        meanwhile, the load starts again (review 8.3m C1).  The publishing call returns a copy of
        the dict it published (complete, so its caller reads it without retrying) after telling
        the context (``_atom_loaded``, a decoded-atom cache admits the atom); otherwise ``None``.
        """
        try:
            if self._loaded:
                return None
        except AttributeError:
            return None
        lock = _lock_for(self)
        from . import codec
        while True:
            with lock:
                marker = self._loaded
                if marker:
                    return None
                if type(marker) is _Pending:
                    pending = marker        # another loader fetched it: share its payload
                else:
                    ptr = self.atom_pointer
                    payload = self.context.storage.get_atom(ptr)
                    if not isinstance(payload, dict) or set(payload) != {"t", "oid", "s"}:
                        raise ProtoCorruptionError(f"Malformed atom payload at {ptr}")
                    stored = type_name_of(payload["t"])
                    if stored != type(self).__type_name__ and not self._adopt_stored_type(stored):
                        raise ProtoCorruptionError(
                            f"Atom at {ptr} is a {stored!r}, expected {type(self).__type_name__!r}")
                    raw_oid = payload["oid"]
                    if not isinstance(raw_oid, bytes) or len(raw_oid) != 16:
                        raise ProtoCorruptionError(f"Malformed oid in atom at {ptr}")
                    pending = _Pending(payload, marker is _UNLOADED)
                    object.__setattr__(self, "_loaded", pending)
            context = self.context
            payload = pending.payload
            state = codec.decode_state(payload["s"], context.load)
            missing = [name for name in type(self)._required_fields if name not in payload["s"]]
            if missing:
                raise ProtoCorruptionError(f"Atom at {self.atom_pointer} "
                                           f"({type(self).__type_name__}) lacks the required fields {missing}")
            oid = uuid.UUID(bytes=payload["oid"])
            with lock:
                if self._loaded:
                    return None
                if not _same_generation(self, pending):
                    continue                # loaded and unloaded since phase 1: fetch again
                names = _installed_names(type(self), state)
                _restore_checked(self, state, names)
                object.__setattr__(self, "oid", oid)
                snapshot = {name: getattr(self, name) for name in names}    # installed just above
                snapshot["oid"] = oid
                snapshot["context"] = context
                object.__setattr__(self, "_loaded", _Generation(names) if pending.marked else True)
            hook = getattr(context, "_atom_loaded", None)
            if hook is not None:
                hook(self)                  # no stripe held (lock order rule)
            return snapshot

    def _cache_produced(self) -> bool:
        """Whether a decoded-atom cache may admit and unload this atom (:func:`_is_cache_produced`)."""
        return _is_cache_produced(self)

    def _unload(self) -> bool:
        """Return this atom to the stub state, in place, if a decoded-atom cache produced it
        (:func:`_is_cache_produced`) and it is loaded; return whether it did (Task 8.3m).

        Under the stripe: ``_loaded`` :data:`_UNLOADED` first, then every key but
        :data:`_STUB_KEYS` is deleted.  The next read loads it again through ``context``."""
        with _lock_for(self):
            if not _is_cache_produced(self):
                return False
            token = self._loaded
            object.__setattr__(self, "_loaded", _UNLOADED)
            _drop_state(self, token.names)
        return True

    def _restore(self, state: dict) -> None:
        """Install a decoded state on a stub (protected hook; default sets attributes).

        Contract (Task 8.3m, ruling R105): an override installs exactly the keys of ``state`` --
        each as an attribute, the history links through :meth:`_set_link` -- and nothing else.
        It may transform the values (``Index._restore`` rebuilds its KeySpecs), not rename, drop or
        add keys: an unload deletes the keys of the decoded state and a load reads them back, so a
        renamed key would be read as missing and an added one would survive the unload.  Every
        load of a class that overrides this hook checks the contract and raises
        ``ProtoUserError`` naming the class if it is broken; a schema change belongs in a new
        version written by a transaction, not in the load.  The hook runs once per load (on a
        scratch instance for an override, whose installed values are then moved onto the atom),
        so it may transform the values of ``state`` in place; it must not depend on the
        identity of ``self``."""
        cls = type(self)
        for name in state:
            if name.startswith("_") or name in RESERVED_NAMES:
                raise ProtoCorruptionError(f"Invalid persisted attribute name {name!r} for {cls.__type_name__}")
            if class_defines(cls, name):
                raise ProtoCorruptionError(
                    f"Invalid persisted attribute name {name!r} for {cls.__type_name__}: the Python class "
                    f"defines an attribute of that name, which would hide the field (in Python the field is "
                    f"its own accessor, so even a property written as an accessor is refused; Task 10.P A4)")
        osa = object.__setattr__
        links = cls._history_links
        for name, value in state.items():
            if name in links:
                self._set_link(name, value)
            else:
                osa(self, name, value)

    # -- save -----------------------------------------------------------------------

    def _state(self) -> dict:
        """Return the persisted state: public instance attributes except the reserved ones.

        Reads one complete copy of the loaded state (:meth:`_snapshot`): other threads may add
        runtime keys concurrently (a lazily assigned ``oid``), and a cache may unload the atom.
        """
        d = self._snapshot()
        state = {k: v for k, v in d.items() if not k.startswith("_") and k not in RESERVED_NAMES}
        links = type(self)._history_links
        if links:
            for name in links:
                if name in state:
                    # Review 8.3l M1: an assignment after construction (a class that is not
                    # immutable) would shadow the link in memory and be lost on save.
                    raise ProtoValidationError(
                        f"{name!r} of {type(self).__type_name__} is a history link: set it in the "
                        f"constructor (_set_link), not by assignment")
            state.update(self._link_values(d.get("_links"), d.get("context")))
        return state

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
        cls = type(self)
        if not cls._type_name_declared:
            raise ProtoValidationError(
                f"Cannot save an instance of {cls.__qualname__}: the class declares no __type_name__.  "
                f"Declare a stable, language-neutral name in its body, such as 'acme.sales.Order' (D57); "
                f"to keep reading data written under its old default name, also declare "
                f"__type_aliases__ = ({cls.__type_name__!r},)")
        from . import codec
        state = self._state()
        for name in state:
            if class_defines(cls, name):
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
        saved = getattr(context, "_atom_saved", None)
        if saved is not None:
            saved(self, ptr, state)         # no stripe held (lock order rule)
        return ptr

    def _decoded_twin(self, context: PersistenceContext, ptr: AtomPointer, state: dict,
                      inline: Optional[list] = None) -> "Atom":
        """Return the atom a load of ``ptr`` through ``context`` would produce, built from the
        ``state`` this atom was just saved with (protected; used by contexts that cache what
        they save, Task 8.3i).

        A new object of this atom's class, loaded, with this atom's ``oid``: every persisted
        attribute is :func:`~proto_db.core.codec.redecode_value` of the saved value -- what
        decoding its wire form gives, nested atoms obtained from ``context.load`` by pointer --
        and no runtime state (no op log, for example).  This atom itself is left untouched.
        ``inline``, a one-item list, accumulates the bytes of its ``str``/``bytes`` values."""
        from . import codec
        load = context.load
        decoded = {name: codec.redecode_value(value, load, inline) for name, value in state.items()}
        twin = object.__new__(type(self))
        osa = object.__setattr__
        osa(twin, "_loaded", False)
        osa(twin, "atom_pointer", ptr)
        osa(twin, "context", context)
        names = _installed_names(type(twin), decoded)
        _restore_checked(twin, decoded, names)
        osa(twin, "oid", self.oid)
        osa(twin, "_loaded", _Generation(names))
        return twin

    # -- identity -------------------------------------------------------------------

    def __eq__(self, other):
        if not isinstance(other, Atom):
            return NotImplemented
        return self is other or self.oid == other.oid

    def __hash__(self):
        return hash(self.oid)

    def _loaded_now(self) -> bool:
        """Whether the state is loaded at this moment (for ``__repr__``, which never loads a stub
        to describe it); read as an attribute, never through ``__dict__`` (review 8.3m M4)."""
        try:
            return bool(self._loaded)
        except AttributeError:
            return True

    def __repr__(self):
        if self._loaded_now():
            try:
                oid = _get(self, "oid")
            except AttributeError:
                oid = None
            return f"<{type(self).__type_name__} oid={oid}>"
        return f"<{type(self).__type_name__} stub at {self.atom_pointer}>"


# -- opaque atoms: types written by another implementation (D59, Task 10.P A3) ----------------

class OpaqueAtom(Atom):
    """An atom of a type this process has no class for: a record written by another
    implementation sharing the database (each language registers its own classes).

    Its fields are read as attributes, its type name is ``type(atom).__type_name__`` and its
    oid is ``atom.oid``; it is never written again, so its record stays byte for byte as it was
    read, and references to it keep its pointer and type name.  Anything that needs the type's
    behaviour (a method, a field it does not hold, a change, a construction) raises
    ``ProtoNotSupportedError`` naming the type.  Instances are always loaded stubs of a
    per-name subclass (:func:`opaque_class`), never registered: a class registered later under
    the same name takes over for loads that follow.
    """

    __type_name__ = "proto_db.core.atom.OpaqueAtom"
    #: Never registered, nor its per-name subclasses: no record is ever of this type.
    _opaque_stand_in = True

    def __init__(self, *args, **kwargs):
        raise ProtoNotSupportedError(
            f"{type(self).__type_name__!r} has no class registered in this process: an opaque atom "
            f"is only read from storage, never constructed")

    def _absent_attribute(self, name: str):
        raise ProtoNotSupportedError(
            f"{name!r} is not a field of this {type(self).__type_name__!r} atom, and that type has no class "
            f"registered in this process: its behaviour is not available here")

    def __setattr__(self, name, value):
        if name.startswith("_") or name in RESERVED_NAMES:
            object.__setattr__(self, name, value)
            return
        raise ProtoNotSupportedError(
            f"Cannot change {name!r} of an opaque {type(self).__type_name__!r} atom: the type has no class "
            f"registered in this process, so a new version of it cannot be written here")

    def __delattr__(self, name):
        raise ProtoNotSupportedError(f"Cannot change an opaque {type(self).__type_name__!r} atom")

    def _adopt_stored_type(self, stored: str) -> bool:
        """The record's ``t`` may differ from the name the reference spells: the owning
        implementation writes references under the type's declared name, and an older record of
        the type keeps the name it was written under (an alias only the owner knows).  When the
        record's name is unknown here too, the two are names of one type this process cannot
        tell apart: accepted, and the atom keeps the name it was referenced by, which is what
        it is written under in references (pre-tag review I-1).  A record of a type this process
        knows behind an unknown reference name stays corruption."""
        try:
            Atom._class_for(stored)
        except ProtoCorruptionError:
            opaque_class(stored)             # ProtoCorruptionError for a proto_db. or invalid name
            return True
        return False

    def _save(self) -> AtomPointer:
        ptr = self.atom_pointer
        if ptr is None:
            raise ProtoNotSupportedError(f"An opaque {type(self).__type_name__!r} atom has no record to keep")
        return ptr

    def __repr__(self):
        return f"<OpaqueAtom {type(self).__type_name__} at {self.atom_pointer}>"


_opaque_classes: dict = {}


def opaque_class(type_name: str) -> type:
    """The :class:`OpaqueAtom` subclass standing in for ``type_name`` (one per name, cached).
    ``ProtoCorruptionError`` when ``type_name`` cannot be another implementation's type: a name
    outside :data:`TYPE_NAME_PATTERN`, or in the reserved ``proto_db.`` namespace (every
    built-in type is known to every reader)."""
    if not isinstance(type_name, str) or not TYPE_NAME_PATTERN.match(type_name) \
            or type_name.startswith("proto_db."):
        raise ProtoCorruptionError(f"Unknown atom type {type_name!r}")
    with _registry_lock:
        cls = _opaque_classes.get(type_name)
    if cls is None:
        made = type(f"Opaque[{type_name}]", (OpaqueAtom,),
                    {"__type_name__": type_name, "_opaque_stand_in": True, "__module__": __name__})
        with _registry_lock:
            cls = _opaque_classes.setdefault(type_name, made)
    return cls
