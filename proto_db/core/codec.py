"""Atom state <-> msgpack-ready dict, with explicit type tags (spec section 5, D3, D7).

Wire form of a value:

* ``None``, ``bool``, ``float``, ``str``, ``bytes`` and ``int`` within the msgpack
  range (``-2**63 .. 2**64-1``) are written natively.  Strings are inline (D7).
* ``list`` is a msgpack array.
* Every other value is a single-key map whose key is a tag:

  ===========  ==================================================================
  ``@int``     ``format(n, "x")`` (signed hexadecimal) for integers outside the
               msgpack range; base 16 is not subject to the int/str digit limit
  ``@dec``     ``str(d)`` (keeps sign, exponent, NaN and infinities exactly)
  ``@date``    ``[year, month, day]``
  ``@time``    ``[hour, minute, second, microsecond, fold, tz]``
  ``@dt``      ``[year, month, day, hour, minute, second, microsecond, fold, tz]``
  ``@td``      ``[days, seconds, microseconds]``
  ``@uuid``    the 16 UUID bytes
  ``@tup``     array of encoded items
  ``@map``     array of ``[key, value]`` pairs in insertion order (keys may be any
               canonical value, so a msgpack map is never used for a dict)
  ``@missing`` the ``MISSING`` sentinel (value ``0``)
  ``@ref``     ``[segment, offset, type]`` for a nested atom: the 16 segment UUID bytes
               and the type tag of :func:`proto_db.core.atom.type_tag` (an alias for a
               built-in type, else the name).  Format version 2 wrote the segment as 32
               hex digits and the type as its name; both forms are still read (D51).
  ``@mut``     the cell name of a ``MutableObject`` handle (a non-empty str, D35)
  ===========  ==================================================================

  ``tz`` is ``None`` (naive), ``["fixed", offset_microseconds, name]`` for a
  ``datetime.timezone``, and for a ``zoneinfo.ZoneInfo``: ``["zone", key]`` in a ``@time``
  (the wall clock; a time has no offset) and ``["instant", key]`` in a ``@dt``, whose fields
  are then the UTC instant with fold 0 (D58: the instant is fixed at write time, so its order
  and hash never depend on the reader's time-zone database).  ``["zone", key]`` in a ``@dt``
  (format 3: the wall clock, resolved with the reader's database) is still read.  Other
  ``tzinfo`` implementations are rejected with ``ProtoValidationError``.

Every text form has one spelling (D58): ``@int`` and ``@dec`` bodies must be the exact text
the encoder writes, ``@td`` must be normalized, and the keys of a ``@map`` must not conflict
(:func:`proto_db.values.map_key_ids`); otherwise decoding raises ``ProtoCorruptionError``, so a
decoded value re-encodes to the bytes it was read from.

Decoding returns ``FrozenList``/``FrozenDict`` for arrays and ``@map`` (D25); encoding accepts
both frozen and plain containers.

Decoding validates the structure: an unknown tag or a malformed value raises
``ProtoCorruptionError``.

A value nests at most :data:`MAX_VALUE_DEPTH` MessagePack containers (arrays and maps, counted
from the value; format spec, "Value nesting depth"): the encoder refuses a deeper value with
``ProtoValidationError`` and the decoder deeper data with ``ProtoCorruptionError``, at the same
count.  Below :data:`RECURSIVE_DEPTH` containers the walks recurse; deeper they go on with an
explicit stack, so no depth limit comes from the Python stack in use.
"""
from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..errors import ProtoCorruptionError, ProtoValidationError
from ..values import _EXACT_SCALARS as _ENCODED_AS_IS
from ..values import (MAX_VALUE_DEPTH, MISSING, FrozenDict, FrozenList, TokenAtoms, canonical, check_map_keys,
                      check_text, plain_scalar, too_deep_error)
from .atom import Atom, AtomPointer, type_name_of, type_tag
from .dbobject import MutableObject

__all__ = ["encode_state", "decode_state", "encode_value", "decode_value", "redecode_value", "MAX_VALUE_DEPTH",
           "RECURSIVE_DEPTH"]

_PLAIN_KEY_TYPES = frozenset({str, int, bytes})
_INT_MIN = -2**63
_INT_MAX = 2**64 - 1
_MICROSECOND = timedelta(microseconds=1)


# -- encoding ---------------------------------------------------------------------------

def encode_state(state: dict, save_child: Callable[[Atom], AtomPointer]) -> dict:
    """Encode an atom state (attribute name -> canonical value) into msgpack-ready form.

    ``save_child`` is called for every nested atom and must return its pointer.
    Raises ``ProtoValidationError`` for a non-``str`` attribute name or an
    unsupported value.
    """
    out = {}
    for name, value in state.items():
        if type(name) is not str:
            raise ProtoValidationError(f"Attribute names must be str, not {type(name).__name__}")
        try:
            out[name] = encode_value(value, save_child)
        except ProtoValidationError as e:
            # Name the field of a value of this state only; an error raised by the save of a
            # nested atom is about that atom and passes unchanged (one message at any depth).
            if type(e) is not ProtoValidationError or getattr(e, "_from_nested_save", False):
                raise
            raise ProtoValidationError(f"Attribute {name!r}: {e}") from e
        if not name.isascii():
            check_text(name)
    return out


#: Classes the isinstance chain of :func:`_encode_other` would encode as an atom reference: an
#: Atom subclass that is no subclass of a type tested before ``Atom`` (Task 8.3i).  An atom is the
#: most frequent non-scalar value of a tree node and was tested last.
_ATOM_CLASSES: set = set()
_BEFORE_ATOM = (str, int, float, bytes, bytearray, list, tuple, dict, Decimal, datetime, date, time,
                timedelta, uuid.UUID, MutableObject)


#: MessagePack containers above which the walks of this module stop recursing and go on with an
#: explicit stack (:func:`_encode_tree`, :func:`_decode_tree`, :func:`_redecode_tree`).  Values
#: of ordinary shape are walked by the plain recursive code, the fastest; deeper ones by the
#: iterative walks, at any depth up to :data:`MAX_VALUE_DEPTH`.  The recursion uses at most
#: about two Python frames per container level, so a walk never needs more than a few dozen
#: frames of the Python stack, whatever the depth of the value.
RECURSIVE_DEPTH = 12


def encode_value(value, save_child: Callable[[Atom], AtomPointer], _depth: int = 0):
    """Encode one canonical value (see the module docstring for the wire form).

    Values of the exact built-in types, and atoms, take fast paths that give the same result as
    the full isinstance chain (:func:`_encode_other`), which every other value goes through.
    ``ProtoValidationError`` for a value nested deeper than :data:`MAX_VALUE_DEPTH` MessagePack
    containers.  ``_depth`` (internal) is the number of containers enclosing ``value``."""
    t = type(value)
    if t is int:
        return value if _INT_MIN <= value <= _INT_MAX else {"@int": format(value, "x")}
    if t is str:
        return value if value.isascii() else check_text(value)     # no UTF-8 for surrogates (R128)
    if t is bytes or t is float or value is None or t is bool:
        return value
    if t is tuple:
        if _depth >= RECURSIVE_DEPTH:
            return _encode_tree(value, save_child, _depth)
        _depth += 2
        return {"@tup": [encode_value(v, save_child, _depth) for v in value]}
    if t in _ATOM_CLASSES:
        return _encode_ref(value, save_child)
    if isinstance(value, Atom) and not issubclass(t, _BEFORE_ATOM) and value is not MISSING:
        _ATOM_CLASSES.add(t)
        return _encode_ref(value, save_child)
    return _encode_other(value, save_child, _depth)


def _wire_containers(data) -> int:
    """The MessagePack containers the wire form ``data`` of a value that is not a list, ``@tup``
    or ``@map`` nests: 0 for a native value, 1 for a tag with a scalar body, 2 for a tag whose body
    is an array, 3 when that array holds an array (the time zone of an aware ``@time`` or ``@dt``)."""
    if type(data) is not dict:
        return 0
    for body in data.values():
        if type(body) is not list:
            return 1
        return 3 if any(type(item) is list for item in body) else 2
    return 1


def _encode_tree(value, save_child, depth: int):
    """:func:`encode_value` of the list, tuple or dict ``value``, enclosed by ``depth`` MessagePack
    containers, walked with an explicit stack in the order the items are written (so
    ``save_child`` meets the atoms in encoding order).  Each pending entry is ``(item, container
    of its wire form, slot, containers enclosing it)``; a list adds one container, a tuple two
    (``{"@tup": [...]}``), a map three (``{"@map": [[k, v]]}``, two when empty)."""
    root = [None]
    pending = [(value, root, 0, depth)]
    while pending:
        item, parent, slot, depth = pending.pop()
        if isinstance(item, list):
            items = item
            out = [None] * len(items)
            parent[slot] = out
            inner = depth + 1
        elif isinstance(item, tuple):
            items = item
            out = [None] * len(items)
            parent[slot] = {"@tup": out}
            inner = depth + 2
        elif isinstance(item, dict):
            item = _checked_map(item)
            pairs = [[None, None] for _ in range(len(item))]
            parent[slot] = {"@map": pairs}
            inner = depth + (3 if pairs else 2)
            if inner > MAX_VALUE_DEPTH:
                raise too_deep_error()
            later = []
            for (k, v), pair in zip(item.items(), pairs):
                later.append((k, pair, 0, inner))
                later.append((v, pair, 1, inner))
            pending.extend(reversed(later))
            continue
        else:
            leaf = _encode_leaf(item, save_child)
            if depth + 3 > MAX_VALUE_DEPTH and depth + _wire_containers(leaf) > MAX_VALUE_DEPTH:
                raise too_deep_error()
            parent[slot] = leaf
            continue
        if inner > MAX_VALUE_DEPTH:
            raise too_deep_error()
        later = []
        for i, v in enumerate(items):
            # A native value is its own wire form; its encoding neither fails nor saves an atom.
            t = type(v)
            if (t is str and v.isascii() or t is int and _INT_MIN <= v <= _INT_MAX or t is float or v is None
                    or t is bool or t is bytes):
                out[i] = v
            else:
                later.append((v, out, i, inner))
        pending.extend(reversed(later))
    return root[0]


def _encode_leaf(value, save_child):
    """:func:`encode_value` of a value that is not a list, tuple or dict."""
    t = type(value)
    if t is int:
        return value if _INT_MIN <= value <= _INT_MAX else {"@int": format(value, "x")}
    if t is str:
        return value if value.isascii() else check_text(value)
    if t is bytes or t is float or value is None or t is bool:
        return value
    if t in _ATOM_CLASSES:
        return _encode_ref(value, save_child)
    if isinstance(value, Atom) and not issubclass(t, _BEFORE_ATOM) and value is not MISSING:
        _ATOM_CLASSES.add(t)
        return _encode_ref(value, save_child)
    return _encode_other(value, save_child, 0)


def _checked_map(value):
    """``value`` (a dict) as the map to encode: a raw dict's keys are converted (built-in subclasses
    to their base values) and checked by canonical(), so two keys that become one base value are
    refused; a ``FrozenDict``'s keys are checked unless all are of a plain key type."""
    if type(value) is not FrozenDict:
        return canonical(value)
    if not all(type(k) in _PLAIN_KEY_TYPES for k in value):
        check_map_keys(value)
    return value


def redecode_value(value, load_ref: Callable[[AtomPointer, str], Atom], inline: Optional[list] = None,
                   _depth: int = 0):
    """``decode_value(encode_value(value, ...), load_ref)`` for a value whose nested atoms are all
    saved, without building the wire form (Task 8.3i: an atom just saved is cached decoded).

    Same result, type for type: the exact built-in scalars come back as themselves (decoding
    their wire form gives an equal object of the same type), tuples as tuples, lists as
    ``FrozenList`` and dicts as ``FrozenDict`` of redecoded items, an atom as
    ``load_ref(its pointer, its type name)``; any other value goes through the codec itself.
    ``inline``, a one-item list, accumulates the length of every ``str`` and ``bytes`` value met
    (nested atoms excluded): what the decoded value holds inline (review 8.3i I1).  ``_depth``
    (internal) counts the containers above ``value``; past :data:`RECURSIVE_DEPTH` the walk goes
    on with an explicit stack (:func:`_redecode_tree`)."""
    t = type(value)
    if t is str or t is bytes:
        if inline is not None:
            inline[0] += len(value)
        return value
    if t is int or t is float or value is None or t is bool:
        return value
    if t in _REDECODED_CONTAINERS and _depth >= RECURSIVE_DEPTH:
        return _redecode_tree(value, load_ref, inline)
    if t is tuple:
        _depth += 1
        return tuple([redecode_value(v, load_ref, inline, _depth) for v in value])
    if t in _ATOM_CLASSES:
        return load_ref(_saved_pointer(value), type(value).__type_name__)
    if t is list or t is FrozenList:
        _depth += 1
        return FrozenList([redecode_value(v, load_ref, inline, _depth) for v in value])
    if t is dict or t is FrozenDict:
        _depth += 1
        out = {}
        for k, v in value.items():
            out[redecode_value(k, load_ref, inline, _depth)] = redecode_value(v, load_ref, inline, _depth)
        return FrozenDict(out)
    return _redecode_other(value, load_ref, inline)


_REDECODED_CONTAINERS = frozenset({tuple, list, FrozenList, dict, FrozenDict})


def _redecode_other(value, load_ref, inline):
    if inline is not None and isinstance(value, (str, bytes, bytearray)):
        inline[0] += len(value)
    return decode_value(encode_value(value, _saved_pointer), load_ref)


def _redecode_tree(value, load_ref, inline):
    """:func:`redecode_value` of a tuple, list or dict: containers expanded in pre-order with an
    explicit stack, items met in the order the recursive form meets them (a dict's value before
    its key: ``out[redecode(k)] = redecode(v)`` evaluates the value first), containers finished in
    reverse, so each after the containers it holds."""
    root = [None]
    nodes = []
    pending = [(value, root, 0)]
    while pending:
        item, parent, slot = pending.pop()
        t = type(item)
        if t in _REDECODED_CONTAINERS:
            if t is tuple:
                kind, items = _TUP, item
            elif t is list or t is FrozenList:
                kind, items = _LIST, item
            else:
                kind, items = _MAP, [x for k, v in item.items() for x in (v, k)]
            results = [None] * len(items)
            nodes.append((kind, results, parent, slot))
            later = []
            for i, v in enumerate(items):
                t = type(v)
                if t is str or t is bytes:
                    if inline is not None:
                        inline[0] += len(v)
                    results[i] = v
                elif t is int or t is float or v is None or t is bool:
                    results[i] = v
                else:
                    later.append((v, results, i))
            pending.extend(reversed(later))
        elif t in _ATOM_CLASSES:
            parent[slot] = load_ref(_saved_pointer(item), t.__type_name__)
        else:
            parent[slot] = _redecode_other(item, load_ref, inline)
    for kind, results, parent, slot in reversed(nodes):
        if kind == _TUP:
            parent[slot] = tuple(results)
        elif kind == _LIST:
            parent[slot] = FrozenList(results)
        else:
            out = {}
            for k, v in zip(results[1::2], results[0::2]):
                out[k] = v
            parent[slot] = FrozenDict(out)
    return root[0]


def _saved_pointer(atom: Atom) -> AtomPointer:
    ptr = atom.atom_pointer
    if ptr is None:
        raise ProtoValidationError(f"{atom!r} is not saved: it has no pointer to refer to")
    return ptr


#: What an atom encodes to inside an exact token (D58): not the encoding of any canonical value.
_TOKEN_REF = {"@ref": None}


def _encode_ref(value, save_child):
    if type(save_child) is TokenAtoms:
        save_child.atoms.append(value)
        return _TOKEN_REF
    try:
        ptr = save_child(value)
    except ProtoValidationError as e:
        e._from_nested_save = True
        raise
    if not isinstance(ptr, AtomPointer):
        raise ProtoValidationError(f"save_child returned {type(ptr).__name__}, expected AtomPointer")
    return {"@ref": [ptr.segment.bytes, ptr.offset, type_tag(type(value).__type_name__)]}


def _encode_other(value, save_child: Callable[[Atom], AtomPointer], depth: int = 0):
    """The full isinstance chain of :func:`encode_value`; ``depth`` is the number of MessagePack
    containers enclosing ``value``."""
    if value is None or value is True or value is False:
        return value
    if type(value) not in _ENCODED_AS_IS:
        # A subclass of a scalar type is written as its base value, read with the base type's
        # methods (canonical() does the same): never through the subclass's __str__ or __float__.
        plain = plain_scalar(value)
        if plain is not None:
            value = plain
    if type(value) is str:
        return check_text(value)
    if type(value) is int:
        return value if _INT_MIN <= value <= _INT_MAX else {"@int": format(value, "x")}
    if type(value) is float or type(value) is bytes or type(value) is bool:
        return value
    if isinstance(value, list):
        if depth >= RECURSIVE_DEPTH:
            return _encode_tree(value, save_child, depth)
        depth += 1
        return [encode_value(v, save_child, depth) for v in value]
    if isinstance(value, tuple):
        if depth >= RECURSIVE_DEPTH:
            return _encode_tree(value, save_child, depth)
        depth += 2
        return {"@tup": [encode_value(v, save_child, depth) for v in value]}
    if isinstance(value, dict):
        if depth >= RECURSIVE_DEPTH:
            return _encode_tree(value, save_child, depth)
        value = _checked_map(value)
        depth += 3
        return {"@map": [[encode_value(k, save_child, depth), encode_value(v, save_child, depth)]
                         for k, v in value.items()]}
    if isinstance(value, Decimal):
        return {"@dec": str(value)}
    if isinstance(value, datetime):
        if isinstance(value.tzinfo, ZoneInfo):
            return {"@dt": _instant_fields(value)}
        return {"@dt": [value.year, value.month, value.day, value.hour, value.minute,
                        value.second, value.microsecond, value.fold, _encode_tz(value.tzinfo)]}
    if isinstance(value, date):
        return {"@date": [value.year, value.month, value.day]}
    if isinstance(value, time):
        return {"@time": [value.hour, value.minute, value.second, value.microsecond,
                          value.fold, _encode_tz(value.tzinfo)]}
    if isinstance(value, timedelta):
        return {"@td": [value.days, value.seconds, value.microseconds]}
    if isinstance(value, uuid.UUID):
        return {"@uuid": value.bytes}
    if value is MISSING:
        return {"@missing": 0}
    if isinstance(value, MutableObject):
        return {"@mut": check_text(value.name)}
    if isinstance(value, Atom):
        return _encode_ref(value, save_child)
    raise ProtoValidationError(f"Unsupported value type: {type(value).__name__}")


def _instant_fields(value: datetime) -> list:
    """``@dt`` fields of a datetime in an IANA zone: its UTC instant, fold 0, ``["instant", key]``."""
    key = value.tzinfo.key
    if key is None:
        raise ProtoValidationError("A ZoneInfo without a key cannot be stored")
    try:
        utc = value.astimezone(timezone.utc)
    except (OverflowError, ValueError) as e:
        raise ProtoValidationError(f"{value!r} has no UTC instant between years 1 and 9999") from e
    return [utc.year, utc.month, utc.day, utc.hour, utc.minute, utc.second, utc.microsecond, 0,
            ["instant", key]]


def _encode_tz(tz):
    if tz is None:
        return None
    if isinstance(tz, ZoneInfo):
        if tz.key is None:
            raise ProtoValidationError("A ZoneInfo without a key cannot be stored")
        return ["zone", tz.key]
    if isinstance(tz, timezone):
        return ["fixed", tz.utcoffset(None) // _MICROSECOND, tz.tzname(None)]
    raise ProtoValidationError(f"Unsupported tzinfo type: {type(tz).__name__}")


# -- decoding ---------------------------------------------------------------------------

def decode_state(data: dict, load_ref: Callable[[AtomPointer, str], Atom]) -> dict:
    """Decode the output of :func:`encode_state`.

    ``load_ref(ptr, type_name)`` is called for every nested atom reference.
    Raises ``ProtoCorruptionError`` for malformed data or an unknown tag.
    """
    if not isinstance(data, dict):
        raise ProtoCorruptionError(f"Encoded state must be a map, not {type(data).__name__}")
    out = {}
    for name, value in data.items():
        if type(name) is not str:
            raise ProtoCorruptionError(f"Encoded attribute name must be str, not {type(name).__name__}")
        out[name] = decode_value(value, load_ref)
    return out


_NATIVE_TYPES = frozenset({bool, int, float, str, bytes})
_LIST, _TUP, _MAP = range(3)
_MALFORMED = (TypeError, ValueError, KeyError, IndexError, OverflowError, InvalidOperation, ZoneInfoNotFoundError)


def decode_value(data, load_ref: Callable[[AtomPointer, str], Atom], _depth: int = 0):
    """Decode one value produced by :func:`encode_value`.

    ``ProtoCorruptionError`` for malformed data, and for data nested deeper than
    :data:`MAX_VALUE_DEPTH` MessagePack containers.  ``_depth`` (internal) is the number of
    containers enclosing ``data``; past :data:`RECURSIVE_DEPTH` the walk goes on with an explicit
    stack (:func:`_decode_tree`)."""
    if data is None or type(data) in _NATIVE_TYPES:
        return data
    if type(data) is list:
        if _depth >= RECURSIVE_DEPTH:
            return _decode_tree(data, load_ref, _depth)
        _depth += 1
        return FrozenList([decode_value(v, load_ref, _depth) for v in data])
    if type(data) is not dict or len(data) != 1:
        raise ProtoCorruptionError(f"Malformed encoded value: {_preview(data)}")
    (tag, body), = data.items()
    if tag == "@tup":
        if _depth >= RECURSIVE_DEPTH:
            return _decode_tree(data, load_ref, _depth)
        if type(body) is not list:
            raise ProtoCorruptionError(f"Malformed @tup value: {_preview(body)}")
        _depth += 2
        return tuple([decode_value(v, load_ref, _depth) for v in body])
    if tag == "@map":
        if _depth >= RECURSIVE_DEPTH:
            return _decode_tree(data, load_ref, _depth)
        items = _map_items(body)
        _depth += 3
        return _decoded_map([decode_value(v, load_ref, _depth) for v in items], items, body)
    return _decode_leaf(tag, body, _depth, load_ref)


def _decode_tree(data, load_ref, depth: int):
    """:func:`decode_value` of an array, ``@tup`` or ``@map`` enclosed by ``depth`` MessagePack
    containers, walked with an explicit stack: containers expanded in pre-order (items in their
    order, so ``load_ref`` meets the references in encoding order), finished in reverse, so each
    after the containers it holds."""
    root = [None]
    nodes = []
    pending = [(data, root, 0, depth)]
    while pending:
        item, parent, slot, depth = pending.pop()
        t = type(item)
        if t is list:
            kind, items, body, inner = _LIST, item, None, depth + 1
        elif t is dict and len(item) == 1:
            (tag, body), = item.items()
            if tag == "@tup":
                if type(body) is not list:
                    raise ProtoCorruptionError(f"Malformed @tup value: {_preview(body)}")
                kind, items, inner = _TUP, body, depth + 2
            elif tag == "@map":
                kind, items, inner = _MAP, _map_items(body), depth + (3 if body else 2)
            else:
                parent[slot] = _decode_leaf(tag, body, depth, load_ref)
                continue
        else:
            raise ProtoCorruptionError(f"Malformed encoded value: {_preview(item)}")
        if inner > MAX_VALUE_DEPTH:
            raise _too_deep_data()
        results = [None] * len(items)
        nodes.append((kind, results, items, body, parent, slot))
        later = []
        for i, v in enumerate(items):
            if v is None or type(v) in _NATIVE_TYPES:
                results[i] = v
            else:
                later.append((v, results, i, inner))
        pending.extend(reversed(later))
    for kind, results, items, body, parent, slot in reversed(nodes):
        if kind == _LIST:
            parent[slot] = FrozenList(results)
        elif kind == _TUP:
            parent[slot] = tuple(results)
        else:
            parent[slot] = _decoded_map(results, items, body)
    return root[0]


def _too_deep_data() -> ProtoCorruptionError:
    return ProtoCorruptionError(
        f"The encoded value nests deeper than MAX_VALUE_DEPTH = {MAX_VALUE_DEPTH} MessagePack containers, "
        f"the most the storage format allows (format spec, \"Value nesting depth\")")


def _decode_leaf(tag, body, depth: int, load_ref):
    """The value of the tagged wire form ``{tag: body}`` (not ``@tup`` or ``@map``), enclosed by
    ``depth`` MessagePack containers."""
    decoder = _DECODERS.get(tag)
    if decoder is None:
        raise ProtoCorruptionError(f"Unknown codec tag {_preview(tag)}")
    if depth + 3 > MAX_VALUE_DEPTH and depth + _wire_containers({tag: body}) > MAX_VALUE_DEPTH:
        raise _too_deep_data()
    try:
        return decoder(body, load_ref)
    except _MALFORMED as e:
        raise ProtoCorruptionError(f"Malformed {tag} value: {_preview(body)}") from e


def _map_items(body) -> list:
    """The keys and values of the ``@map`` body ``body``, flattened: ``[k0, v0, k1, v1, ...]``."""
    flat = []
    try:
        if type(body) is not list:
            raise TypeError("expected an array")
        for pair in body:
            k, v = _fields(pair, 2, (None, None))
            flat.append(k)
            flat.append(v)
    except (TypeError, ValueError) as e:
        raise ProtoCorruptionError(f"Malformed @map value: {_preview(body)}") from e
    return flat


def _decoded_map(results: list, flat: list, body) -> FrozenDict:
    """The ``FrozenDict`` of the decoded keys and values ``results`` of the ``@map`` whose raw
    keys and values are ``flat``; ``ProtoCorruptionError`` unless its keys are valid and distinct."""
    pairs = list(zip(results[0::2], results[1::2]))
    try:
        if not all(type(k) is str for k, _ in pairs):
            try:
                check_map_keys((k for k, _ in pairs), [_holds_zone_form(k) for k in flat[0::2]])
            except ProtoValidationError as e:
                raise ProtoCorruptionError(f"Malformed @map: {e}") from e
        out = FrozenDict(pairs)
    except _MALFORMED as e:
        raise ProtoCorruptionError(f"Malformed @map value: {_preview(body)}") from e
    if len(out) != len(pairs):
        raise ProtoCorruptionError("Malformed @map: a map key appears twice")
    return out


def _preview(x):
    text = repr(x)
    return text if len(text) <= 80 else text[:77] + "..."


def _fields(body, n, types):
    if type(body) is not list or len(body) != n:
        raise ValueError(f"expected an array of {n} items")
    for item, t in zip(body, types):
        if t is not None and type(item) is not t:
            raise TypeError(f"expected {t.__name__}, got {type(item).__name__}")
    return body


def _decode_tz(tz):
    if tz is None:
        return None
    if type(tz) is list and len(tz) == 2 and tz[0] == "zone" and type(tz[1]) is str:  # wall clock
        return ZoneInfo(tz[1])
    if type(tz) is list and len(tz) == 3 and tz[0] == "fixed" and type(tz[1]) is int \
            and type(tz[2]) is str:
        result = timezone(timedelta(microseconds=tz[1]))
        return result if result.tzname(None) == tz[2] else timezone(result.utcoffset(None), tz[2])
    raise ValueError("malformed tzinfo")


def _dec_int(body, _):
    if type(body) is not str:
        raise TypeError("expected str")
    value = int(body, 16)
    if format(value, "x") != body or _INT_MIN <= value <= _INT_MAX:
        raise ValueError("not the canonical @int text of an integer outside the native range")
    return value


def _dec_decimal(body, _):
    if type(body) is not str:
        raise TypeError("expected str")
    value = Decimal(body)
    if str(value) != body:
        raise ValueError("not the canonical @dec text (the to-scientific-string form)")
    return value


def _dec_date(body, _):
    return date(*_fields(body, 3, (int, int, int)))


def _dec_time(body, _):
    h, mi, s, us, fold, tz = _fields(body, 6, (int, int, int, int, int, None))
    return time(h, mi, s, us, tzinfo=_decode_tz(tz), fold=fold)


def _dec_datetime(body, _):
    y, mo, d, h, mi, s, us, fold, tz = _fields(body, 9, (int,) * 8 + (None,))
    if type(tz) is list and len(tz) == 2 and tz[0] == "instant" and type(tz[1]) is str:
        if fold != 0:
            raise ValueError("an instant has fold 0")
        return datetime(y, mo, d, h, mi, s, us, tzinfo=timezone.utc).astimezone(ZoneInfo(tz[1]))
    return datetime(y, mo, d, h, mi, s, us, tzinfo=_decode_tz(tz), fold=fold)


def _dec_timedelta(body, _):
    days, seconds, micros = _fields(body, 3, (int, int, int))
    if not (0 <= seconds < 86400 and 0 <= micros < 1_000_000):
        raise ValueError("a timedelta is normalized: 0 <= seconds < 86400, 0 <= microseconds < 10**6")
    return timedelta(days, seconds, micros)


def _dec_uuid(body, _):
    if type(body) is not bytes:
        raise TypeError("expected bytes")
    return uuid.UUID(bytes=body)


def _holds_zone_form(raw) -> bool:
    """Whether the wire form of a map key holds a format-3 ``["zone", key]`` datetime (re-review I-2),
    alone or in a ``@tup`` at any depth (an explicit stack)."""
    pending = [raw]
    while pending:
        raw = pending.pop()
        if type(raw) is not dict or len(raw) != 1:
            continue
        (tag, body), = raw.items()
        if tag == "@dt":
            if type(body) is list and len(body) == 9 and type(body[8]) is list and body[8][:1] == ["zone"]:
                return True
        elif tag == "@tup" and type(body) is list:
            pending.extend(body)
    return False


def _dec_missing(body, _):
    if body != 0 or type(body) is not int:
        raise ValueError("expected 0")
    return MISSING


def _dec_ref(body, load_ref):
    segment, offset, tag = _fields(body, 3, (None, int, None))
    if type(segment) is bytes and len(segment) == 16:
        segment = uuid.UUID(bytes=segment)
    elif type(segment) is str:                       # format version 2
        segment = uuid.UUID(hex=segment)
    else:
        raise TypeError("expected 16 segment bytes")
    try:
        ptr = AtomPointer(segment, offset)
    except ProtoValidationError as e:
        raise ValueError(str(e)) from e
    try:
        type_name = type_name_of(tag)
    except ProtoCorruptionError as e:
        raise ValueError(str(e)) from e
    return load_ref(ptr, type_name)


def _dec_mutable(body, _):
    if type(body) is not str or not body:
        raise ValueError("expected a non-empty str")
    return MutableObject(body)


_DECODERS = {
    "@int": _dec_int, "@dec": _dec_decimal, "@date": _dec_date, "@time": _dec_time,
    "@dt": _dec_datetime, "@td": _dec_timedelta, "@uuid": _dec_uuid,
    "@missing": _dec_missing, "@ref": _dec_ref,
    "@mut": _dec_mutable,
}
