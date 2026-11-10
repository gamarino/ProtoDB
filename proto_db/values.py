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
import uuid
from datetime import date, datetime, time, timedelta
from decimal import Decimal

from msgpack import packb as _packb

from .errors import ProtoUserError, ProtoValidationError

__all__ = ["MISSING", "FrozenList", "FrozenDict", "canonical", "order_key", "rank_bounds", "values_equal", "hash_key",
           "is_atom", "is_orderable", "exact_token", "MutableObject", "ATOM_TOKEN", "map_key_ids",
           "check_map_keys"]


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
        object.__setattr__(self, "_name", str(name))

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
    for base in _DATE_TIME_TYPES:                 # datetime before date: it is a subclass of date
        if isinstance(value, base):
            return base(*base.__reduce_ex__(value, 4)[1])   # protocol 4 keeps fold
    if isinstance(value, uuid.UUID):
        return uuid.UUID(int=_UUID_INT.__get__(value))
    return None


_DATE_TIME_TYPES = (datetime, date, time, timedelta)
#: The slot that holds a UUID's 128-bit integer (``uuid.UUID`` declares ``__slots__``).
_UUID_INT = uuid.UUID.__dict__["int"]


def canonical(value):
    """Validate ``value`` and return its canonical form.

    Supported: MISSING, None, bool, int, float (NaN allowed as a stored value),
    Decimal, str, bytes (``bytearray`` is converted), date, time, datetime,
    timedelta, UUID, atoms, ``MutableObject`` handles, tuples, and lists/dicts of supported values
    (processed recursively).  An instance of a subclass of a scalar type (an ``Enum`` with a
    ``str`` or ``int`` mixin, a ``StrEnum``, an ``IntEnum``, any subclass of ``str``, ``int``,
    ``float``, ``Decimal``, ``bytes`` or of the date and time types) becomes its plain base value
    (:func:`plain_scalar`): the class of a built-in subclass is not persisted, so the bytes
    written, the order key and the hash all come from one value.  Lists and dicts become
    :class:`FrozenList` and :class:`FrozenDict`; already-canonical input is returned as the same
    object.  Raises ``ProtoValidationError`` otherwise.
    """
    cls = type(value)
    if cls in _EXACT_SCALARS or value is MISSING:
        return value
    plain = plain_scalar(value)
    if plain is not None:
        return plain
    if isinstance(value, tuple):
        items = tuple(canonical(v) for v in value)
        return value if type(value) is tuple and all(a is b for a, b in zip(items, value)) else items
    if isinstance(value, list):
        items = [canonical(v) for v in value]
        if type(value) is FrozenList and all(a is b for a, b in zip(items, value)):
            return value
        return FrozenList(items)
    if isinstance(value, dict):
        pairs = [(canonical(k), canonical(v)) for k, v in value.items()]
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
    if is_atom(value) or isinstance(value, MutableObject):
        return value
    raise ProtoValidationError(f"Unsupported value type: {type(value).__name__}")


def order_key(value) -> tuple:
    """Return ``(rank, payload)``, a key that totally orders orderable values.

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
        return (_R_TUPLE, tuple(order_key(v) for v in value))
    if is_atom(value):
        return (_R_ATOM, value.oid.int)
    if isinstance(value, MutableObject):
        return (_R_MUTABLE, value.name)
    if isinstance(value, (list, dict)):
        raise ProtoValidationError(f"{type(value).__name__} is not an orderable key")
    raise ProtoValidationError(f"Unsupported value type: {type(value).__name__}")


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
        return all(is_orderable(v) for v in value)
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
    """Feed an order key into the hash object ``h`` with an unambiguous, process-independent encoding."""
    rank, payload = key
    h.update(rank.to_bytes(1, "little"))
    if rank == _R_NUMBER:
        h.update(_number_residue(payload))
        return
    if rank == _R_TUPLE:
        h.update(len(payload).to_bytes(8, "little"))
        for item in payload:
            _feed_hash(h, item)
        return
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
        combos = [()]
        for item in value:
            ids = map_key_ids(item, reading=reading, zone_form=zone_form)
            combos = [c + (i,) for c in combos for i in ids]
        return frozenset(("tuple", c) for c in combos)
    return frozenset({order_key(value)})


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
