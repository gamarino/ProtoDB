"""The JSON notations of the conformance manifest (``tests/conformance/README.md``).

Two notations describe what a vector's bytes mean, independently of Python:

* the **value** notation describes a canonical value as the codec decodes it
  (``docs/source/format-spec.rst``, "Canonical values"): ``{"int": "-5"}``,
  ``{"tuple": [...]}``, ``{"ref": {"segment": ..., "offset": ..., "type": ...}}``;
* the **raw** notation describes plain msgpack data (the envelope of an atom record, a root
  record, a cluster message): ``{"array": [...]}`` and ``{"mpmap": [[key, value], ...]}`` instead
  of the value notation's lists and maps.

Both are single-key JSON objects.  Integers are decimal strings (JSON numbers lose precision
past 2**53), floats C99 hexadecimal strings (``float.hex``: exact, with ``"nan"``, ``"inf"`` and
``"-inf"``), bytes and UUIDs lowercase hexadecimal.

:func:`build` turns a value description into the Python value; :func:`describe` does the
reverse.  :func:`describe_raw` describes plain msgpack data.  :func:`build_object` builds an
unsaved stored-object description (``{"object": ...}``, ``{"collection": ...}``), and
:func:`describe_ast` / :func:`build_ast` give an expression tree in the manifest (spec section 7):
its operators and names as JSON strings, each ``const`` operand as a value description.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from proto_db.collections.lists import List
from proto_db.core.atom import AtomPointer
from proto_db.core.dbobject import DBObject
from proto_db.tools import notation
from proto_db.values import MISSING, FrozenDict, FrozenList, MutableObject

__all__ = ["build", "describe", "describe_raw", "RefTarget", "build_object", "describe_ast", "build_ast"]



class RefTarget:
    """What a ``ref`` describes when no storage backs it: a pointer and a type name."""

    __slots__ = ("pointer", "type_name")

    def __init__(self, pointer: AtomPointer, type_name: str):
        self.pointer = pointer
        self.type_name = type_name


def _float(text: str) -> float:
    return float(text) if text in ("nan", "inf", "-inf") else float.fromhex(text)


def _tz_build(tz):
    if tz is None:
        return None
    if tz[0] == "zone":
        return ZoneInfo(tz[1])
    result = timezone(timedelta(microseconds=tz[1]))
    return result if result.tzname(None) == tz[2] else timezone(result.utcoffset(None), tz[2])


def build(desc, load_ref=None):
    """The Python value a value description stands for.  ``load_ref(pointer, type_name)``
    supplies the object of a ``ref`` (default: a :class:`RefTarget`)."""
    (kind, body), = desc.items()
    if kind == "nil":
        return None
    if kind == "bool":
        return bool(body)
    if kind == "int":
        return int(body)
    if kind == "float":
        return _float(body)
    if kind == "str":
        return body
    if kind == "bytes":
        return bytes.fromhex(body)
    if kind == "decimal":
        return Decimal(body)
    if kind == "date":
        return date(*body)
    if kind == "time":
        h, mi, s, us, fold, tz = body
        return time(h, mi, s, us, tzinfo=_tz_build(tz), fold=fold)
    if kind == "datetime":
        y, mo, d, h, mi, s, us, fold, tz = body
        if tz is not None and tz[0] == "instant":
            return datetime(y, mo, d, h, mi, s, us, tzinfo=timezone.utc).astimezone(ZoneInfo(tz[1]))
        return datetime(y, mo, d, h, mi, s, us, tzinfo=_tz_build(tz), fold=fold)
    if kind == "timedelta":
        return timedelta(*body)
    if kind == "uuid":
        return uuid.UUID(hex=body)
    if kind == "list":
        return [build(v, load_ref) for v in body]
    if kind == "tuple":
        return tuple(build(v, load_ref) for v in body)
    if kind == "map":
        return {build(k, load_ref): build(v, load_ref) for k, v in body}
    if kind == "missing":
        return MISSING
    if kind == "mutable":
        return MutableObject(body)
    if kind == "ref":
        ptr = AtomPointer(uuid.UUID(hex=body["segment"]), body["offset"])
        return (load_ref or RefTarget)(ptr, body["type"])
    raise ValueError(f"unknown value kind {kind!r}")


def describe(value):
    """The value description of a decoded canonical value (types are matched exactly):
    :func:`proto_db.tools.notation.describe`, plus the :class:`RefTarget` of :func:`build`."""
    if type(value) is RefTarget:
        return {"ref": {"segment": value.pointer.segment.hex, "offset": value.pointer.offset,
                        "type": value.type_name}}
    return notation.describe(value)


def describe_raw(data):
    """The raw description of plain msgpack data (as ``msgpack.unpackb(raw=False)`` returns it)."""
    t = type(data)
    if data is None:
        return {"nil": None}
    if t is bool:
        return {"bool": data}
    if t is int:
        return {"int": str(data)}
    if t is float:
        return {"float": float.hex(data) if data == data and abs(data) != float("inf") else repr(data)}
    if t is str:
        return {"str": data}
    if t is bytes:
        return {"bytes": data.hex()}
    if t is list or t is tuple:
        return {"array": [describe_raw(v) for v in data]}
    if t is dict:
        return {"mpmap": [[describe_raw(k), describe_raw(v)] for k, v in data.items()]}
    raise TypeError(f"no raw description for {t.__name__}")


def build_object(desc):
    """The unsaved object a stored-object description stands for (the ``expressions`` vectors): a
    ``proto_db.core.dbobject.DBObject`` record, a ``proto_db.collections.List``, or containers and
    values whose items may be either, built bottom-up."""
    if "object" in desc:
        if desc["object"] != DBObject.__type_name__:
            raise ValueError(f"the expression vectors hold plain records only, not {desc['object']!r}")
        return DBObject(**{name: build_object(value) for name, value in desc["attrs"]})
    if "collection" in desc:
        if desc["collection"] != List.__type_name__:
            raise ValueError(f"the expression vectors hold Lists only, not {desc['collection']!r}")
        return List.from_iterable(build_object(e) for e in desc["elements"])
    (kind, body), = desc.items()
    if kind in ("list", "tuple"):
        items = [build_object(v) for v in body]
        return items if kind == "list" else tuple(items)
    if kind == "map":
        return {build_object(k): build_object(v) for k, v in body}
    return build(desc)


def describe_ast(ast):
    """An expression tree in the manifest's form: lists as JSON arrays, operators and names as JSON
    strings, the operand of every ``const`` node as its value description."""
    if isinstance(ast, list):
        if len(ast) == 2 and ast[0] == "const":
            return ["const", notation.describe(ast[1])]
        return [describe_ast(item) for item in ast]
    return ast


def build_ast(desc):
    """The expression tree :func:`describe_ast` described."""
    if isinstance(desc, list):
        if len(desc) == 2 and desc[0] == "const":
            return ["const", build(desc[1])]
        return [build_ast(item) for item in desc]
    return desc
