"""Linked collections: the binding of a collection root to the transaction that reads it (spec
section 9, R40-R47; B3, N5).

``Transaction.get_root_object(n)`` on a root whose value is a fine-grained collection returns a
**linked copy** (:func:`linked_copy`): a new instance of the transaction's own with the persisted
fields and the storage pointer of the base collection, carrying a :class:`Link` (transaction,
root name, base pointer) as runtime state.  The cached atom is never touched (R41).  Every
version a mutator derives from it inherits the link (R42) and carries its own record of
intentions, ``_changes`` (R43); reads are recorded in the transaction (R44).

What a read records (spec section 6):

* the operations a collection class lists record their own units (a Dictionary's ``get_at(k)``
  records ``Unit(KEY, root, key=k)``, a Set's ``has(e)`` ``Unit(ELEMENT, root, key=e)``, a
  CountedSet's ``count_of(e)`` ``Unit(COUNT, root, key=e)``, a RepeatedKeysDictionary's
  ``get_at(k)`` ``Unit(RECORDS, root, key=k)``, a List's ``get_at(i)`` ``Unit(POSITION, root,
  key=i)``), through :func:`record_key`; the expected value
  is what the collection's ``_unit_token(kind, key)`` gives;
* every other read by user code of a linked instance -- a persisted field (``count``, ``root``,
  ``indexes``, ...), iteration, ``len`` -- records ``Unit(WHOLE, root)`` through
  :func:`record_whole` (P-B, R18, B3);
* reads made by the engine itself (the body of a mutator, of a listed read) are not recorded:
  they run inside :func:`engine_reads`.

A link is valid while its transaction is active (R46, R47): :func:`link_of` returns ``None``
afterwards, and nothing is recorded any more.

Classification (R1, N5): a class is **fine-grained** (:func:`fine_grained`) if it is one of the
classes registered with :func:`register_fine_grained` (Dictionary, HashDictionary, Set,
CountedSet, RepeatedKeysDictionary and List) or a subclass of one that
overrides none of its listed operations, compared by method identity.  Any other root is an
object root.

Nothing here runs user code (P-C).
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

from ..core.atom import AtomPointer
from ..preconditions import Unit, UnitKind, token
from ..values import order_key
from . import avl

if TYPE_CHECKING:
    from ..transactions import Transaction

__all__ = ["Link", "ExactKey", "linked_copy", "link_of", "record_read", "record_whole", "record_key",
           "engine_reads", "engine_reading", "fine_grained", "register_fine_grained", "unit_key", "lookup_key",
           "NOT_A_UNIT", "wrote", "written_with"]


@dataclass(frozen=True)
class Link:
    """What binds a linked collection to its transaction (R40): the transaction, the root's name
    and the storage pointer of the root's base collection."""

    transaction: "Transaction"
    root: str
    base_pointer: AtomPointer | None


# -- engine reads (B3) ----------------------------------------------------------------------

_engine = threading.local()


def engine_reading() -> bool:
    """Whether this thread is inside :func:`engine_reads` (its reads of collections are not recorded)."""
    return getattr(_engine, "depth", 0) > 0


class _EngineReads:
    """The context manager :func:`engine_reads` returns (one per process; the depth is per thread)."""

    __slots__ = ()

    def __enter__(self):
        _engine.depth = getattr(_engine, "depth", 0) + 1
        return self

    def __exit__(self, *exc):
        _engine.depth -= 1
        return False


_ENGINE_READS = _EngineReads()


def engine_reads() -> _EngineReads:
    """A context manager under which this thread's reads of linked collections are engine reads:
    nothing is recorded (B3).  The engine wraps the internal reads of a mutator or of a listed read
    in it; a listed read records its own unit before entering it."""
    return _ENGINE_READS


# -- links ----------------------------------------------------------------------------------

def linked_copy(atom, link: Link):
    """A linked instance of the saved collection ``atom`` (R41): a new instance with the same
    persisted fields, ``oid`` and ``atom_pointer`` (``atom`` is loaded first and left untouched;
    the copy is outside the atom cache), carrying ``link`` and every other runtime field at its
    class default: no intentions, no whole mark, nothing written (the saved atom may be a version
    a transaction built and committed, with runtime state of its own).

    Its persisted fields are held apart (``_linked_state``) so that reading one by user code goes
    through :meth:`IndexedCollection.__getattr__`, which records the whole collection (B3)."""
    copy = atom._pointer_copy()
    state = copy.__dict__
    held = {}
    for name in type(atom)._fields:
        if name in state:
            held[name] = state.pop(name)
    osa = object.__setattr__
    osa(copy, "_linked_state", held)
    cls = type(atom)
    for name in cls._all_runtime_fields:
        osa(copy, name, getattr(cls, name, None))
    osa(copy, "_link", link)
    return copy


def link_of(value, transaction: Optional["Transaction"] = None) -> Optional[Link]:
    """The link of ``value`` while it is valid, else ``None`` (R46, R47): ``value`` is a linked
    collection whose transaction is still active and, when ``transaction`` is given, is that one."""
    try:
        link = value.__dict__.get("_link")
    except AttributeError:
        return None
    if link is None:
        return None
    if transaction is not None and link.transaction is not transaction:
        return None
    if not link.transaction.is_active:
        return None
    return link


def _recording_link(collection) -> Optional[Link]:
    """The link a read of ``collection`` records in, or ``None`` (unlinked, ended, or an engine read)."""
    if engine_reading():
        return None
    return link_of(collection)


def _whole_expected(link: Link):
    """The expected value of ``Unit(WHOLE, root)``: the token of the root's base value (R8)."""
    return link.transaction._base_whole_token(link.root)


def record_read(collection, unit: Unit, expected) -> None:
    """Record the precondition ``(unit, expected)`` of a read of the linked ``collection`` in its
    transaction (R44); a no-op on an unlinked collection, after the transaction ended, and inside
    :func:`engine_reads`."""
    link = _recording_link(collection)
    if link is not None:
        link.transaction._read_log.record(unit, expected)


def record_whole(collection) -> None:
    """Record ``Unit(WHOLE, root)`` for a read of the linked ``collection`` that reveals the whole
    collection (P-B, R18, B3); a no-op where :func:`record_read` is."""
    link = _recording_link(collection)
    if link is not None:
        log = link.transaction._read_log
        if not log.whole(link.root):
            log.record(Unit(UnitKind.WHOLE, link.root), _whole_expected(link))


def record_key(collection, key, kind: UnitKind = UnitKind.KEY, *, from_base: bool = False) -> None:
    """Record ``Unit(kind, root, key)`` for an operation of the linked ``collection`` on ``key`` (a
    read, or a write: section 5's precondition), expecting the unit's token in the base
    (``_unit_token``, ABSENT included; D51: a large value by its box's pointer).

    ``key`` is validated first (``order_key``: ``ProtoValidationError`` for a list, a dict or a NaN),
    so an invalid key raises the documented error, never a ``TypeError`` from building the Unit.

    Nothing is recorded when the unit or the whole root is recorded already (R9).  Without
    ``from_base`` the token is taken from ``collection`` itself: right when the caller knows that
    this version did not write the unit (a Dictionary key: its first write recorded it; a Set
    element: the version's ``_written`` does not hold it).  With ``from_base`` it is taken from the
    root's base collection: for units whose value in the version composes the base's with the
    version's own changes (a CountedSet count, the records of a RepeatedKeysDictionary key, R18).
    A version marked whole (an unlisted operation produced it) records the whole collection instead."""
    link = _recording_link(collection)
    if link is None:
        return
    order_key(key)                      # ruling: validate before the Unit is built
    if collection._whole:
        record_whole(collection)
        return
    log = link.transaction._read_log
    unit = Unit(kind, link.root, key=unit_key(key))
    if unit in log or log.whole(link.root):
        return
    source = link.transaction._base_root_value(link.root) if from_base else collection
    with engine_reads():
        expected = source._unit_token(kind, key)
    log.record(unit, expected)


def wrote(collection, key) -> bool:
    """Whether the linked version ``collection`` determines the unit of ``key`` by its own writes
    (its ``_written`` holds ``key``): a read of that unit reveals nothing of the base and records
    nothing (R9).  ``key`` is validated (``order_key``) when there is something to look up."""
    written = collection._written
    return written is not None and avl.key_get(written, key, False)


def written_with(collection, key) -> dict:
    """The ``_with`` change adding ``key`` to the linked ``collection``'s ``_written`` (a runtime
    persistent tree, O(log n) per write); nothing for an unlinked collection."""
    if collection._link is None:
        return {}
    return {"_written": avl.key_put(collection._written, key, True)[0]}


#: What ``_unit_token`` returns for a unit kind the collection does not have: equal to no token,
#: so a precondition on a root whose class no longer has the unit fails (R12).
NOT_A_UNIT = type("_NotAUnit", (), {"__slots__": (), "__repr__": lambda self: "NOT_A_UNIT"})()


# -- unit keys ------------------------------------------------------------------------------

@dataclass(frozen=True, eq=False)
class ExactKey:
    """A key in a ``Unit`` whose Python equality is coarser than key equality (``True == 1`` in
    Python, two keys of a Dictionary): compared by ``order_key``, so two such keys are two units."""

    key: Any

    def __eq__(self, other):
        return type(other) is ExactKey and order_key(self.key) == order_key(other.key)

    def __hash__(self):
        return hash(order_key(self.key))

    def __repr__(self):
        return f"ExactKey({self.key!r})"


def _holds_bool(key) -> bool:
    if type(key) is bool:
        return True
    return type(key) is tuple and any(_holds_bool(item) for item in key)


def unit_key(key):
    """The ``key`` of the Unit for collection key ``key``: the key itself, or an :class:`ExactKey`
    when it holds a ``bool`` (whose Python equality with ``1``/``0`` would merge two units)."""
    return ExactKey(key) if _holds_bool(key) else key


def lookup_key(key):
    """The collection key of a Unit's ``key`` (the inverse of :func:`unit_key`)."""
    return key.key if type(key) is ExactKey else key


# -- classification (R1, N5) ------------------------------------------------------------------

_FINE: dict = {}            # class -> the names of its listed operations
_CLASSIFIED: dict = {}      # class -> bool (memo of fine_grained)
_LOCK = threading.Lock()


def register_fine_grained(cls: type, operations: tuple) -> None:
    """Declare ``cls`` fine-grained, with the names of its listed operations (sections 5 and 6):
    a subclass that overrides any of them is an object root (N5)."""
    with _LOCK:
        _FINE[cls] = tuple(operations)
        _CLASSIFIED.clear()


def fine_grained(cls: type) -> bool:
    """Whether a root holding an instance of ``cls`` gets fine-grained treatment (R1, N5): ``cls``
    is a registered class, or derives from one and overrides none of its listed operations
    (compared by method identity)."""
    known = _CLASSIFIED.get(cls)
    if known is not None:
        return known
    result = False
    for base in cls.__mro__:
        operations = _FINE.get(base)
        if operations is not None:
            result = all(getattr(cls, name, None) is getattr(base, name, None) for name in operations)
            break
    with _LOCK:
        _CLASSIFIED[cls] = result
    return result

