"""ProtoDB value model.

Single source of truth for what a storable value is and how values order,
compare and hash.  Collections, indexes and the query evaluator all rely on
:func:`order_key`, :func:`values_equal` and :func:`hash_key`.

Rank order (see decision D17)::

    MISSING < None < bool < number < str < bytes < date < time-naive
    < time-aware < datetime-naive < datetime-aware < timedelta < UUID
    < tuple < atom < mutable handle

The last rank holds :class:`MutableObject` handles (re-exported by
:mod:`proto_db.core.dbobject`),
ordered by name (D35); it was appended after ``atom`` so that the order keys
and hash keys of every other value are unchanged.

Numbers (``int``, ``float``, ``Decimal``) share one rank and are compared
exactly by Python's native mixed comparison; they are never converted to
``float``.  ``list`` and ``dict`` are storable (see :func:`canonical`) but are
not orderable keys.
"""
from __future__ import annotations

import copy
import hashlib
import re
import uuid
from datetime import date, datetime, time, timedelta
from decimal import Decimal

import msgpack
from msgpack import packb as _packb

from .errors import ProtoNotSupportedError, ProtoUserError, ProtoValidationError

__all__ = ["MISSING", "FrozenList", "FrozenDict", "canonical", "check_text", "order_key", "rank_bounds", "values_equal", "hash_key",
           "is_atom", "is_orderable", "exact_token", "MutableObject", "ATOM_TOKEN", "map_key_ids",
           "check_map_keys", "MAX_VALUE_DEPTH", "leaf_containers", "fold", "fold_hybrid", "value_depth", "check_wrapped",
           "canonical_for"]


class _Missing:
    """Type of the MISSING sentinel: an absent value, distinct from None."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "MISSING"

    def __reduce__(self):
        return "MISSING"

    def __bool__(self):
        return False


MISSING = _Missing()

(_R_MISSING, _R_NONE, _R_BOOL, _R_NUMBER, _R_STR, _R_BYTES, _R_DATE,
 _R_TIME_NAIVE, _R_TIME_AWARE, _R_DT_NAIVE, _R_DT_AWARE, _R_TIMEDELTA,
 _R_UUID, _R_TUPLE, _R_ATOM, _R_MUTABLE) = range(16)


_MICROSECOND = timedelta(microseconds=1)
_NAIVE_ORIGIN = datetime(1, 1, 1)


def is_atom(value) -> bool:
    """Return True if ``value`` looks like an atom: it has a ``uuid.UUID`` ``oid``."""
    return isinstance(getattr(value, "oid", None), uuid.UUID)


def _is_nan(value) -> bool:
    if isinstance(value, Decimal):
        return value.is_nan()
    return isinstance(value, float) and value != value


def _is_aware(value) -> bool:
    return value.tzinfo is not None and value.utcoffset() is not None


def _frozen(kind: str):
    """Build a mutator that always refuses, pointing the user at ``.copy()``."""
    def refuse(self, *args, **kwargs):
        raise ProtoValidationError(
            f"Stored {kind} values are immutable; call .copy() to get a mutable copy")
    return refuse


class FrozenList(list):
    """Immutable ``list`` used for every list value stored in the database (D25).

    Reads behave like ``list`` and ``FrozenList([1]) == [1]``.  Every mutator
    raises ``ProtoValidationError``.  Like ``list`` it is unhashable.
    :meth:`copy` returns a plain mutable ``list`` (shallow: nested values stay
    frozen).  ``copy.copy``/``copy.deepcopy`` and pickling give an equal
    ``FrozenList``.
    """

    __slots__ = ()

    def __new__(cls, iterable=()):
        self = list.__new__(cls)
        list.extend(self, iterable)
        return self

    def __init__(self, iterable=()):
        """No-op: elements are set in ``__new__``, so ``__init__`` can never mutate."""

    def copy(self) -> list:
        """Return a plain, mutable, shallow copy."""
        return list(self)

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return FrozenList(copy.deepcopy(v, memo) for v in self)

    def __reduce__(self):
        return (FrozenList, (list(self),))

    append = extend = insert = pop = remove = clear = sort = reverse = _frozen("list")
    __setitem__ = __delitem__ = __iadd__ = __imul__ = _frozen("list")


class FrozenDict(dict):
    """Immutable ``dict`` used for every dict value stored in the database (D25).

    Same contract as :class:`FrozenList`: equal to the plain ``dict``, every
    mutator raises ``ProtoValidationError``, unhashable, :meth:`copy` returns
    a plain shallow ``dict``, deepcopy and pickle give an equal ``FrozenDict``.
    """

    __slots__ = ()

    def __new__(cls, *args, **kwargs):
        self = dict.__new__(cls)
        dict.update(self, *args, **kwargs)
        return self

    def __init__(self, *args, **kwargs):
        """No-op: items are set in ``__new__``, so ``__init__`` can never mutate."""

    def copy(self) -> dict:
        """Return a plain, mutable, shallow copy."""
        return dict(self)

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return FrozenDict((copy.deepcopy(k, memo), copy.deepcopy(v, memo)) for k, v in self.items())

    def __reduce__(self):
        return (FrozenDict, (dict(self),))

    update = setdefault = popitem = pop = clear = __setitem__ = __delitem__ = __ior__ = _frozen("dict")


class MutableObject:
    """Handle on the named mutable cell ``name`` of a database (D11, D35).

    The handle holds only the name; the cell's value lives in the database's mutable
    table and is read and written through a transaction (:meth:`get`, :meth:`set`,
    or ``Transaction.get_mutable``/``set_mutable``/``new_mutable``/``remove_mutable``),
    so reads enter the transaction's read set and writes are applied at commit.

    A handle is an immutable canonical value: it can be stored in atoms, collections
    and other mutables (persisted as ``{"@mut": name}``), compares and hashes by
    name, and orders after every atom (D35).  It is never persisted as an atom and
    carries no reference to a transaction or storage, so one handle is valid in any
    transaction of any database that has a cell of that name.  Thread-safe (immutable).
    """

    __slots__ = ("_name",)

    def __init__(self, name: str):
        """Create a handle on the cell ``name`` (a non-empty ``str``); does not create the cell."""
        if not isinstance(name, str) or not name:
            raise ProtoValidationError(f"A mutable name must be a non-empty str, not {name!r}")
        object.__setattr__(self, "_name", check_text(str(name)))

    @property
    def name(self) -> str:
        """The cell name."""
        return self._name

    def get(self, transaction):
        """Return the cell's value as seen by ``transaction`` (``None`` if the cell is absent)."""
        return transaction.get_mutable(self._name)

    def set(self, transaction, value) -> None:
        """Set the cell's value in ``transaction`` (applied at commit); the cell must exist."""
        transaction.set_mutable(self._name, value)

    def __setattr__(self, name, value):
        raise ProtoUserError("MutableObject is immutable")

    def __delattr__(self, name):
        raise ProtoUserError("MutableObject is immutable")

    def __eq__(self, other):
        if not isinstance(other, MutableObject):
            return NotImplemented
        return self._name == other._name

    def __hash__(self):
        return hash(("MutableObject", self._name))

    def __reduce__(self):
        return (MutableObject, (self._name,))

    def __repr__(self):
        return f"MutableObject({self._name!r})"


#: Key types for which distinct keys of a Python dict are distinct map keys of the format (D58).
_PLAIN_KEY_TYPES = frozenset({str, int, bytes})


#: Scalar types stored as they are; an instance of a subclass is stored as its base value.
_EXACT_SCALARS = frozenset({type(None), bool, int, float, Decimal, str, bytes, date, time, datetime, timedelta,
                            uuid.UUID})


def plain_scalar(value):
    """The value of the built-in scalar type that ``value`` (an instance of a subclass of one) holds,
    read with the base type's own methods, never the subclass's overrides: the class of a
    built-in subclass, and every state it adds, is not persisted (D58, re-reviews C-1 and
    Minor 1).  ``Color.RED`` of ``class Color(str, Enum)`` gives ``"red"``; an ``IntEnum`` member
    gives its int.  ``None`` when ``value`` is not an instance of a built-in scalar type.

    The base data is read through the base type's C-level methods: ``str.__str__``,
    ``int.__index__``, ``float.__float__``, the ``Decimal`` copy constructor (the coefficient and
    exponent, not ``str()``), ``bytes.__bytes__`` and a ``bytearray`` slice (not the buffer
    protocol, which a subclass may override), the base ``__reduce_ex__(4)`` of the date and time types
    (their stored fields, not the ``year``/``days``/... attributes a subclass may redefine) and
    the ``int`` slot of ``UUID``."""
    if isinstance(value, bool):
        return bool(int.__index__(value))
    if isinstance(value, int):
        return int.__index__(value)
    if isinstance(value, str):
        return str.__str__(value)
    if isinstance(value, float):
        return float.__float__(value)
    if isinstance(value, Decimal):
        return Decimal(value)
    if isinstance(value, bytes):
        return bytes.__bytes__(value)
    if isinstance(value, bytearray):
        return bytes(bytearray.__getitem__(value, slice(None)))
    if isinstance(value, (datetime, time)):
        base = datetime if isinstance(value, datetime) else time
        return base(*base.__reduce_ex__(value, 4)[1])       # their own C __reduce_ex__; keeps fold
    if isinstance(value, date):
        # date and timedelta have no __reduce_ex__ of their own (object's calls the subclass's
        # __reduce__): read their C field descriptors (pre-tag review m4).
        return date(*(field.__get__(value) for field in _DATE_FIELDS))
    if isinstance(value, timedelta):
        return timedelta(*(field.__get__(value) for field in _TIMEDELTA_FIELDS))
    if isinstance(value, uuid.UUID):
        return uuid.UUID(int=_UUID_INT.__get__(value))
    return None


_DATE_FIELDS = tuple(date.__dict__[n] for n in ("year", "month", "day"))
_TIMEDELTA_FIELDS = tuple(timedelta.__dict__[n] for n in ("days", "seconds", "microseconds"))
#: The slot that holds a UUID's 128-bit integer (``uuid.UUID`` declares ``__slots__``).
_UUID_INT = uuid.UUID.__dict__["int"]


_SURROGATE = re.compile("[\ud800-\udfff]")


def check_text(text: str) -> str:
    """Return ``text``; ``ProtoValidationError`` if it holds a surrogate code point (U+D800-U+DFFF).

    Such a ``str`` (from ``surrogateescape`` decoding, or a pair written as two escapes) has no
    UTF-8 encoding, so the format cannot store it (R128): it is refused where the value is
    validated, not when its transaction commits."""
    if not text.isascii():
        found = _SURROGATE.search(text)
        if found is not None:
            raise ProtoValidationError(
                f"A str holds the surrogate code point U+{ord(found.group()):04X} at index {found.start()}: "
                f"it has no UTF-8 encoding and cannot be stored (decode the text with a proper error "
                f"handler, or join surrogate pairs)")
    return text


#: The deepest nesting of MessagePack containers (arrays and maps) a stored value may have,
#: counted from the value itself (format spec, "Value nesting depth"; D60).  An atom
#: payload adds two (its envelope and its state map), so a record never nests deeper than 1024,
#: the depth MessagePack's C packer (msgpack >= 1.2) and unpacker (every release) support.
MAX_VALUE_DEPTH = 1022

#: The oldest msgpack release whose packer nests the 1024 containers of a record holding a value
#: of MAX_VALUE_DEPTH (earlier releases stop at 511, so they would refuse values this format
#: allows, with a bare ``ValueError`` at commit).  Checked when this module is imported.
MSGPACK_MINIMUM = (1, 2, 0)


def check_msgpack(version) -> None:
    """Raise ``ProtoNotSupportedError`` when the msgpack release ``version`` (a tuple such as
    ``msgpack.version``) is older than :data:`MSGPACK_MINIMUM`."""
    if tuple(version[:3]) < MSGPACK_MINIMUM:
        raise ProtoNotSupportedError(
            f"ProtoBase needs msgpack {'.'.join(map(str, MSGPACK_MINIMUM))} or later, but msgpack "
            f"{'.'.join(map(str, version))} is installed: older releases cannot write a value nested "
            f"MAX_VALUE_DEPTH = {MAX_VALUE_DEPTH} containers deep, which the storage format allows (format "
            f"spec, \"Value nesting depth\"). Upgrade it: pip install 'msgpack>=1.2.0'")


check_msgpack(msgpack.version)

#: The native integer range of the wire form; an int outside it is written as ``{"@int": text}``.
_INT_MIN = -2**63
_INT_MAX = 2**64 - 1


def too_deep_error() -> ProtoValidationError:
    """The error of a writer asked to store a value nested deeper than :data:`MAX_VALUE_DEPTH`."""
    return ProtoValidationError(
        f"The value nests deeper than MAX_VALUE_DEPTH = {MAX_VALUE_DEPTH} MessagePack containers, "
        f"the most the storage format allows (format spec, \"Value nesting depth\")")


def leaf_containers(value) -> int:
    """The MessagePack containers the wire form of the canonical non-container value ``value``
    nests (format spec, "Value nesting depth"): 0 for a value written natively, 1 for a tag whose
    body is a scalar (``@int``, ``@dec``, ``@uuid``, ``@missing``, ``@mut``), 2 for a tag whose body
    is an array of scalars (``@date``, ``@td``, ``@ref``, a naive ``@time`` or ``@dt``), 3 for an
    aware ``@time`` or ``@dt`` (its time zone is an array inside the body)."""
    t = type(value)
    if t is int:
        return 0 if _INT_MIN <= value <= _INT_MAX else 1
    if t is str or t is bytes or t is float or t is bool or value is None:
        return 0
    if t is datetime or t is time:
        return 2 if value.tzinfo is None else 3
    if t is date or t is timedelta:
        return 2
    if t is Decimal or t is uuid.UUID or value is MISSING or isinstance(value, MutableObject):
        return 1
    return 2 if is_atom(value) else 0


def fold(value, expand):
    """Fold the nested value ``value`` bottom-up with an explicit stack, never by recursion, so
    any depth works whatever the Python stack in use.

    ``expand(v)`` returns ``(None, result)`` for a leaf, or ``(children, finish)`` for a node:
    ``children`` a sequence whose items are expanded in order, ``finish(results)`` the node's
    result from the list of its children's results."""
    children, result = expand(value)
    if children is None:
        return result
    stack = [[children, 0, [], result]]
    while True:
        frame = stack[-1]
        kids, i = frame[0], frame[1]
        if i < len(kids):
            frame[1] = i + 1
            children, result = expand(kids[i])
            if children is None:
                frame[2].append(result)
            else:
                stack.append([children, 0, [], result])
            continue
        stack.pop()
        result = frame[3](frame[2])
        if not stack:
            return result
        stack[-1][2].append(result)


_NATIVE_LEAVES = frozenset({str, bytes, float, bool, type(None)})


def value_depth(value, _depth: int = 0) -> int:
    """The MessagePack containers the wire form of ``value`` nests (format spec, "Value nesting
    depth"): a list adds 1, a tuple 2 (``{"@tup": [...]}``), a map 3 (``{"@map": [[k, v]]}``, 2 when
    empty), and a scalar its :func:`leaf_containers`.  ``value`` is a canonical value, or a
    structure a collection or an index stores (tuples of canonical values).  Recursive over the
    first :data:`RECURSIVE_DEPTH` levels, then an explicit stack (:func:`_value_depth_tree`)."""
    t = type(value)
    if t in _NATIVE_LEAVES:
        return 0
    if t is tuple:
        cost, items = 2, value
    elif t is list or t is FrozenList:
        cost, items = 1, value
    elif t is dict or t is FrozenDict:
        if not value:
            return 2
        cost, items = 3, [x for pair in value.items() for x in pair]
    elif isinstance(value, (list, tuple, dict)):
        return _value_depth_tree(value)
    else:
        return leaf_containers(value)
    if _depth >= RECURSIVE_DEPTH:
        return _value_depth_tree(value)
    best = 0
    for item in items:
        if type(item) not in _NATIVE_LEAVES:
            d = value_depth(item, _depth + 1)
            if d > best:
                best = d
    return cost + best


def _value_depth_tree(value) -> int:
    """:func:`value_depth` with an explicit stack: any depth, whatever the Python stack in use."""
    best = 0
    stack = [(value, 0)]
    pop, push = stack.pop, stack.append
    while stack:
        item, depth = pop()
        if isinstance(item, list):
            depth += 1
            children = item
        elif isinstance(item, tuple):
            depth += 2
            children = item
        elif isinstance(item, dict):
            if not item:
                depth += 2
                children = ()
            else:
                depth += 3
                children = [x for pair in item.items() for x in pair]
        else:
            depth += leaf_containers(item)
            if depth > best:
                best = depth
            continue
        if not children and depth > best:
            best = depth
        for child in children:
            push((child, depth))
    return best


def check_wrapped(value, wrapping: int, holder: str, wrapper: str) -> None:
    """Refuse the canonical value ``value`` when ``wrapping`` containers that a structure stores
    around it would make the stored attribute value nest deeper than :data:`MAX_VALUE_DEPTH`.

    ``holder`` names the value in the error (``"Set element"``), ``wrapper`` what adds the
    containers (``"its bucket"``).  A scalar nests at most 3 containers, so only a container is
    walked."""
    limit = MAX_VALUE_DEPTH - wrapping
    if isinstance(value, (list, tuple, dict)):
        depth = value_depth(value)
        if depth > limit:
            raise ProtoValidationError(
                f"{holder} nests {depth} MessagePack containers deep, and {wrapper} adds {wrapping} "
                f"around it, so it may nest at most {limit} (MAX_VALUE_DEPTH = {MAX_VALUE_DEPTH}, "
                f"format spec \"Value nesting depth\")")


def canonical_for(value, holder: str):
    """:func:`canonical` of ``value``, with any ``ProtoValidationError`` naming ``holder`` (a root,
    a mutable cell, a collection's element, key or value) the way a DBObject names its field."""
    try:
        return canonical(value)
    except ProtoValidationError as e:
        if type(e) is not ProtoValidationError:
            raise
        raise ProtoValidationError(f"{holder}: {e}") from e


#: MessagePack containers above which :func:`canonical` stops recursing and goes on with an
#: explicit stack (the codec's walks do the same, :data:`proto_db.core.codec.RECURSIVE_DEPTH`):
#: values of ordinary shape take the plain recursive code, deeper ones the iterative walk, so the
#: Python stack a call needs stays a few dozen frames at any depth of the value.
RECURSIVE_DEPTH = 12


def fold_hybrid(value, expand, _depth: int = 0):
    """:func:`fold`, by plain recursion over the first :data:`RECURSIVE_DEPTH` levels and with
    the explicit stack below them: the same calls of ``expand`` in the same order, and the same
    result, at any depth whatever the Python stack in use, without the stack's cost on the
    shallow values that are the common case."""
    if _depth >= RECURSIVE_DEPTH:
        return fold(value, expand)
    children, result = expand(value)
    if children is None:
        return result
    return result([fold_hybrid(child, expand, _depth + 1) for child in children])


def canonical(value, _depth: int = 0):
    """Validate ``value`` and return its canonical form.

    Supported: MISSING, None, bool, int, float (NaN allowed as a stored value),
    Decimal, str, bytes (``bytearray`` is converted), date, time, datetime,
    timedelta, UUID, atoms, ``MutableObject`` handles, tuples, and lists/dicts of supported values
    (processed at any depth).  An instance of a subclass of a scalar type (an ``Enum`` with a
    ``str`` or ``int`` mixin, a ``StrEnum``, an ``IntEnum``, any subclass of ``str``, ``int``,
    ``float``, ``Decimal``, ``bytes`` or of the date and time types) becomes its plain base value
    (:func:`plain_scalar`): the class of a built-in subclass is not persisted, so the bytes
    written, the order key and the hash all come from one value.  Lists and dicts become
    :class:`FrozenList` and :class:`FrozenDict`; already-canonical input is returned as the same
    object.  Raises ``ProtoValidationError`` otherwise, and for a value whose wire form would nest
    deeper than :data:`MAX_VALUE_DEPTH` MessagePack containers.  ``_depth`` (internal) is the
    number of containers enclosing ``value``; past :data:`RECURSIVE_DEPTH` the walk goes on with
    an explicit stack (:func:`_canonical_tree`).
    """
    cls = type(value)
    if cls is str:
        return check_text(value)
    if cls in _EXACT_SCALARS or value is MISSING:
        return value
    plain = plain_scalar(value)
    if plain is not None:
        return check_text(plain) if type(plain) is str else plain
    if isinstance(value, tuple):
        if _depth >= RECURSIVE_DEPTH:
            return _canonical_tree(value, _depth)
        _depth += 2
        items = tuple([canonical(v, _depth) for v in value])
        return value if type(value) is tuple and all(a is b for a, b in zip(items, value)) else items
    if isinstance(value, list):
        if _depth >= RECURSIVE_DEPTH:
            return _canonical_tree(value, _depth)
        _depth += 1
        items = [canonical(v, _depth) for v in value]
        if type(value) is FrozenList and all(a is b for a, b in zip(items, value)):
            return value
        return FrozenList(items)
    if isinstance(value, dict):
        if _depth >= RECURSIVE_DEPTH:
            return _canonical_tree(value, _depth)
        _depth += 3
        return _canonical_map(value, [(canonical(k, _depth), canonical(v, _depth)) for k, v in value.items()])
    if is_atom(value) or isinstance(value, MutableObject):
        return value
    raise ProtoValidationError(f"Unsupported value type: {type(value).__name__}")


def _canonical_leaf(value):
    """:func:`canonical` of a value that is not a tuple, list or dict."""
    cls = type(value)
    if cls is str:
        return check_text(value)
    if cls in _EXACT_SCALARS or value is MISSING:
        return value
    plain = plain_scalar(value)
    if plain is not None:
        return check_text(plain) if type(plain) is str else plain
    if is_atom(value) or isinstance(value, MutableObject):
        return value
    raise ProtoValidationError(f"Unsupported value type: {type(value).__name__}")


_TUPLE, _LIST, _DICT = 0, 1, 2


def _canonical_tree(value, depth: int):
    """:func:`canonical` of a tuple, list or dict enclosed by ``depth`` MessagePack containers,
    walked with an explicit stack.

    Each pending entry is ``(item, results of its container, slot, containers enclosing its wire
    form)``.  A tuple adds two containers (``{"@tup": [...]}``), a list one, a map three
    (``{"@map": [[k, v]]}``; two when empty).  Containers are expanded in pre-order and finished
    in reverse, so every container is finished after the containers it holds."""
    root = [None]
    nodes = []
    pending = [(value, root, 0, depth)]
    while pending:
        item, parent, slot, depth = pending.pop()
        if isinstance(item, tuple):
            kind, items, inner = _TUPLE, item, depth + 2
        elif isinstance(item, list):
            kind, items, inner = _LIST, item, depth + 1
        elif isinstance(item, dict):
            kind, items = _DICT, [x for pair in item.items() for x in pair]
            inner = depth + (3 if items else 2)
        else:
            leaf = _canonical_leaf(item)
            if depth + leaf_containers(leaf) > MAX_VALUE_DEPTH:
                raise too_deep_error()
            parent[slot] = leaf
            continue
        if inner > MAX_VALUE_DEPTH:
            raise too_deep_error()
        results = [None] * len(items)
        nodes.append((item, kind, results, parent, slot))
        later = []
        for i, v in enumerate(items):
            if type(v) is str and v.isascii():
                results[i] = v
            else:
                later.append((v, results, i, inner))
        pending.extend(reversed(later))
    for item, kind, results, parent, slot in reversed(nodes):
        parent[slot] = _canonical_finish(item, kind, results)
    return root[0]


def _canonical_finish(value, kind, results):
    """The canonical container of the kind ``kind`` holding ``results``, the canonical forms of
    the items of ``value`` (a dict's keys and values alternating): ``value`` itself when it is
    already canonical."""
    if kind == _TUPLE:
        items = tuple(results)
        return value if type(value) is tuple and all(a is b for a, b in zip(items, value)) else items
    if kind == _LIST:
        if type(value) is FrozenList and all(a is b for a, b in zip(results, value)):
            return value
        return FrozenList(results)
    return _canonical_map(value, list(zip(results[0::2], results[1::2])))


def _canonical_map(value, pairs: list):
    """The canonical map of ``value`` (a dict), whose canonical ``(key, value)`` pairs are ``pairs``."""
    if type(value) is FrozenDict and all(
            ka is kb and va is vb for (ka, va), (kb, vb) in zip(pairs, value.items())):
        return value
    if not all(type(k) in _PLAIN_KEY_TYPES for k, _ in pairs):
        check_map_keys(k for k, _ in pairs)   # Python's dict already told these apart
    out = FrozenDict(pairs)
    if len(out) != len(pairs):                # subclass keys that are one base value
        raise ProtoValidationError("Two keys of one map conflict as map keys (D58): "
                                   "they hold one base value")
    return out


def order_key(value, _depth: int = 0) -> tuple:
    """Return ``(rank, payload)``, a key that totally orders orderable values.

    Nested tuples are walked recursively for :data:`RECURSIVE_DEPTH` levels, then with an
    explicit stack (``_depth``, internal, counts the levels above ``value``).

    Invariant (relied upon by :func:`rank_bounds` and by index range scans):
    every order key is a 2-element tuple ``(rank, payload)`` whose ``rank`` is
    an int in ``range(16)``; payloads are compared only between keys of one rank.

    Raises ``ProtoValidationError`` for NaN, lists, dicts and unsupported types.
    """
    if value is MISSING:
        return (_R_MISSING, 0)
    if value is None:
        return (_R_NONE, 0)
    if isinstance(value, bool):
        return (_R_BOOL, int(value))
    if isinstance(value, (int, float, Decimal)):
        if _is_nan(value):
            raise ProtoValidationError("NaN cannot be used as a key")
        return (_R_NUMBER, value)
    if isinstance(value, str):
        return (_R_STR, value)
    if isinstance(value, (bytes, bytearray)):
        return (_R_BYTES, bytes(value))
    if isinstance(value, datetime):
        if _is_aware(value):
            # UTC microseconds from the fold-resolved offset; never compare the raw
            # aware datetime, whose comparison is fold-sensitive (PEP 495).
            local = (value.replace(tzinfo=None) - _NAIVE_ORIGIN) // _MICROSECOND
            return (_R_DT_AWARE, local - value.utcoffset() // _MICROSECOND)
        return (_R_DT_NAIVE, value)
    if isinstance(value, date):
        return (_R_DATE, value)
    if isinstance(value, time):
        micros = ((value.hour * 60 + value.minute) * 60 + value.second) * 1_000_000 + value.microsecond
        if _is_aware(value):
            off = value.utcoffset()
            return (_R_TIME_AWARE, micros - (off // timedelta(microseconds=1)))
        return (_R_TIME_NAIVE, micros)
    if isinstance(value, timedelta):
        return (_R_TIMEDELTA, value)
    if isinstance(value, uuid.UUID):
        return (_R_UUID, value.int)
    if isinstance(value, tuple):
        if _depth >= RECURSIVE_DEPTH:
            return fold(value, _order_key_expand)   # deeper: an explicit stack
        _depth += 1
        return (_R_TUPLE, tuple([order_key(v, _depth) for v in value]))
    if is_atom(value):
        return (_R_ATOM, value.oid.int)
    if isinstance(value, MutableObject):
        return (_R_MUTABLE, value.name)
    if isinstance(value, (list, dict)):
        raise ProtoValidationError(f"{type(value).__name__} is not an orderable key")
    raise ProtoValidationError(f"Unsupported value type: {type(value).__name__}")


def _order_key_expand(value):
    if isinstance(value, tuple):
        return value, _tuple_order_key
    return None, order_key(value)


def _tuple_order_key(items: list) -> tuple:
    return (_R_TUPLE, tuple(items))


_ORDERABLE_TYPES = (bool, int, str, bytes, bytearray, date, time, timedelta, uuid.UUID, MutableObject)


def is_orderable(value) -> bool:
    """True iff :func:`order_key` accepts ``value`` (no NaN, list or dict anywhere; a supported type).

    The single definition of orderability: the term model and the indexes use it.
    """
    if value is None or value is MISSING or isinstance(value, _ORDERABLE_TYPES):
        return True
    if isinstance(value, float):
        return value == value
    if isinstance(value, Decimal):
        return not value.is_nan()
    if isinstance(value, tuple):
        pending = list(value)                       # any depth: no recursion
        while pending:
            item = pending.pop()
            if isinstance(item, tuple):
                pending.extend(item)
            elif not is_orderable(item):
                return False
        return True
    if isinstance(value, (list, dict)):
        return False
    return is_atom(value)


def rank_bounds(value) -> tuple:
    """Return ``(low, high)``: order keys bracketing every value of ``value``'s type rank (D30).

    For every orderable ``x``, ``low < order_key(x) < high`` holds exactly when
    ``x`` has the same rank as ``value`` (``low`` and ``high`` are never the
    order key of any value).  Range scans use them to stay within the operand's
    rank, since ordering comparisons match same-rank keys only (D28).  Relies on
    the :func:`order_key` invariant that every order key is a 2-tuple
    ``(rank, payload)``: the 1-tuple ``(rank,)`` sorts before every
    ``(rank, payload)`` and ``(rank + 1,)`` after it.  Raises
    ``ProtoValidationError`` for unorderable values, like :func:`order_key`.
    """
    rank = order_key(value)[0]
    return (rank,), (rank + 1,)


def values_equal(a, b) -> bool:
    """Return True iff ``order_key(a) == order_key(b)``."""
    return order_key(a) == order_key(b)


#: Modulus of the exact numeric hash (a Mersenne prime, as in CPython's numeric hash).
_HASH_MODULUS = (1 << 61) - 1
_HASH_INV10 = pow(10, _HASH_MODULUS - 2, _HASH_MODULUS)


def _number_residue(x) -> bytes:
    """Encode the exact value of a non-NaN number for hashing: equal numbers give equal bytes.

    Finite numbers map to their rational value modulo the prime ``_HASH_MODULUS``
    (``m * n**-1 mod P`` for ``m / n``), computed without expanding huge
    exponents; infinities get their own tags.
    """
    p = _HASH_MODULUS
    if isinstance(x, Decimal):
        if x.is_infinite():
            return b"+inf" if x > 0 else b"-inf"
        sign, digits, exp = x.as_tuple()
        m = 0
        for d in digits:
            m = (m * 10 + d) % p
        m = m * (pow(10, exp, p) if exp >= 0 else pow(_HASH_INV10, -exp, p)) % p
        residue = (p - m) % p if sign else m
    elif isinstance(x, float):
        if x in (float("inf"), float("-inf")):
            return b"+inf" if x > 0 else b"-inf"
        n, d = x.as_integer_ratio()
        residue = n * pow(d, p - 2, p) % p
    else:
        residue = x % p
    return residue.to_bytes(8, "little")


def _feed_hash(h, key: tuple) -> None:
    """Feed an order key into the hash object ``h`` with an unambiguous, process-independent encoding.

    A tuple's items follow its length, depth first (an explicit stack: any depth)."""
    pending = [key]
    while pending:
        rank, payload = pending.pop()
        h.update(rank.to_bytes(1, "little"))
        if rank == _R_NUMBER:
            h.update(_number_residue(payload))
            continue
        if rank == _R_TUPLE:
            h.update(len(payload).to_bytes(8, "little"))
            pending.extend(reversed(payload))
            continue
        if isinstance(payload, str):
            data = payload.encode("utf-8", "surrogatepass")
        elif isinstance(payload, bytes):
            data = payload
        else:
            if isinstance(payload, datetime):  # naive datetime
                payload = (payload - _NAIVE_ORIGIN) // _MICROSECOND
            elif isinstance(payload, date):
                payload = payload.toordinal()
            elif isinstance(payload, timedelta):
                payload = payload // _MICROSECOND
            if type(payload) is not int:
                raise ProtoValidationError(f"Cannot hash order-key payload of type {type(payload).__name__}")
            data = payload.to_bytes(payload.bit_length() // 8 + 1, "little", signed=True)
        h.update(len(data).to_bytes(8, "little"))
        h.update(data)


def hash_key(value) -> int:
    """Return a signed 64-bit hash consistent with :func:`values_equal`, identical in every process.

    Python's ``hash`` of ``str``, ``bytes`` and date/time objects is salted per
    process (``PYTHONHASHSEED``), so it cannot order persisted structures.  This
    hash is a BLAKE2b digest of an exact encoding of :func:`order_key` (numbers
    by their exact rational value), stable across processes, platforms and
    releases (D24).  Raises ``ProtoValidationError`` for unorderable values.
    """
    h = hashlib.blake2b(digest_size=8)
    _feed_hash(h, order_key(value))
    return int.from_bytes(h.digest(), "little", signed=True)


# -- exact tokens (D30, D34, D58) ---------------------------------------------------------------

#: The token of every atom, and the placeholder of an atom nested in a value: the msgpack bytes of
#: ``{"@ref": nil}``, which is not the encoding of any canonical value (D58).
ATOM_TOKEN = b"\x81\xa4@ref\xc0"


class TokenAtoms:
    """The ``save_child`` the codec receives when it encodes a token: every atom met becomes the
    placeholder ``{"@ref": nil}`` and is remembered, in encoding order, in :attr:`atoms`."""

    __slots__ = ("atoms",)

    def __init__(self):
        self.atoms: list = []


_codec = None


def _encode_for_token(value, collector: TokenAtoms):
    global _codec
    if _codec is None:
        from .core import codec as _module      # deferred: the codec imports this module
        _codec = _module
    return _codec.encode_value(value, collector)


def exact_token(value, atom_token=None):
    """What tells apart values that are equal keys but not identical (D30): a function of the
    value's canonical encoding alone (D58, ruling R129).

    The token of a value that holds no atom is its canonical encoding: the msgpack bytes of
    :func:`proto_db.core.codec.encode_value`, as a record state holds it.  Every atom is
    encoded as the placeholder ``{"@ref": nil}`` (:data:`ATOM_TOKEN`), so the token of an atom is
    :data:`ATOM_TOKEN`: atoms are identified elsewhere (by oid in an index member key).  The
    encoding reads only data of the format, so values built differently share a token when they
    hold the same data: ``timezone.utc`` and ``timezone(timedelta(0), "UTC")``; a built-in
    subclass and its base value (``Color.RED`` of a ``str`` Enum and ``"red"``: the subclass is
    converted with the base type's methods, never its own ``__str__``).  ``1``, ``1.0``, ``Decimal("1")``, ``Decimal("1.00")``, ``0.0``/``-0.0`` and aware
    datetimes of one instant in different zones are equal keys with distinct tokens.

    ``atom_token(atom)``, when given, identifies the atoms instead (the transaction read set
    passes the storage pointer): an atom's token is then ``atom_token(atom)``, and a value
    holding atoms gets ``(encoding, (atom_token(a) for each atom in encoding order))``.  That
    form is never persisted.  Raises ``ProtoValidationError`` for a value the codec refuses.
    """
    if is_atom(value):
        return ATOM_TOKEN if atom_token is None else atom_token(value)
    collector = TokenAtoms()
    data = _msgpack_packb(_encode_for_token(value, collector))
    if atom_token is None or not collector.atoms:
        return data
    return data, tuple(atom_token(a) for a in collector.atoms)


def _msgpack_packb(data) -> bytes:
    return _packb(data, use_bin_type=True)


# -- map keys (D58, review I5) --------------------------------------------------------------------

def _zone_datetime(value) -> bool:
    if not isinstance(value, datetime) or value.tzinfo is None:
        return False
    from zoneinfo import ZoneInfo
    return isinstance(value.tzinfo, ZoneInfo)


def map_key_ids(value, *, reading: bool = False, zone_form: bool = False) -> frozenset:
    """The identities under which ``value`` is a key of a map; two keys of one map conflict when
    their identities intersect (D58).  Raises ``ProtoValidationError`` for a value that cannot be a
    map key.

    A key is identified by its order key with every bool read as the number 0 or 1 (Python's
    dict, and so the decoded map, cannot tell ``True`` from ``1``); a NaN by its canonical encoding;
    a tuple by the combinations of its items' identities.  Lists, maps, signaling decimal NaNs
    and datetimes in an IANA zone (whose equality in Python depends on the wall clock, which
    depends on the time-zone database) cannot be map keys, nor tuples holding one.

    A reader (``reading``) accepts the two key forms format 3 could write (re-review I-2): NaN
    keys that conflict (a Python dict holds two NaN objects as two keys) and, when the key's wire
    form holds a format-3 ``["zone", key]`` datetime (``zone_form``), an IANA-zone datetime.  A NaN
    key is then its own identity (it conflicts with nothing); a zone key is identified by
    :func:`_format3_zone_key_id`.  A writer refuses both.
    """
    if isinstance(value, (list, dict)):
        raise ProtoValidationError(f"A {type(value).__name__} cannot be a map key")
    if isinstance(value, bool):
        return frozenset({order_key(int(value))})
    if isinstance(value, Decimal) and value.is_snan():
        raise ProtoValidationError("A signaling decimal NaN cannot be a map key")
    if _is_nan(value):
        return frozenset({("nan", object() if reading else exact_token(value))})
    if _zone_datetime(value):
        if reading and zone_form:
            return frozenset({_format3_zone_key_id(value)})
        raise ProtoValidationError(
            "A datetime in an IANA time zone cannot be a map key: use its UTC or fixed-offset form")
    if isinstance(value, tuple):
        def expand(item):                           # any depth: no recursion
            if isinstance(item, tuple):
                return item, _tuple_key_ids
            return None, map_key_ids(item, reading=reading, zone_form=zone_form)
        return fold(value, expand)
    return frozenset({order_key(value)})


def _tuple_key_ids(item_ids: list) -> frozenset:
    combos = [()]
    for ids in item_ids:
        combos = [c + (i,) for c in combos for i in ids]
    return frozenset(("tuple", c) for c in combos)


def _format3_zone_key_id(value):
    """The map-key identity of a format-3 zone-datetime key (re-review 2, Minor 5), the rule
    Python's dict applied when format 3 wrote it (PEP 495): a wall clock that occurs once in its
    zone is identified by its instant, so it conflicts with every aware key of that instant; a
    wall clock that is ambiguous or skipped in its zone (its offset depends on ``fold``) is
    identified by its zone and its wall-clock fields without ``fold``, so it conflicts only with
    a format-3 key of the same zone and wall clock."""
    if value.replace(fold=0).utcoffset() == value.replace(fold=1).utcoffset():
        return order_key(value)
    return ("format-3 zone wall clock", value.tzinfo.key, value.year, value.month, value.day, value.hour,
            value.minute, value.second, value.microsecond)


def check_map_keys(keys, zone_forms=None) -> None:
    """``ProtoValidationError`` unless every key of ``keys`` may be a map key and no two conflict
    (:func:`map_key_ids`).  A reader passes ``zone_forms``, one flag per key (true when the key's
    wire form holds a format-3 zone datetime), and so accepts the format-3 key forms; a writer
    passes nothing."""
    seen: set = set()
    reading = zone_forms is not None
    flags = iter(zone_forms) if reading else None
    for key in keys:
        ids = map_key_ids(key, reading=reading, zone_form=reading and next(flags))
        if not seen.isdisjoint(ids):
            raise ProtoValidationError(f"Two keys of one map conflict as map keys (D58): {key!r}")
        seen |= ids
