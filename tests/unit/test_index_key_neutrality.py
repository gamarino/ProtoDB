"""Index keys carry no Python text (R128 item 6; format 4, D58, review C1 of Task 10.F).

Before format 4 an index member key embedded the Python ``repr`` of a ``timezone`` and the
``module.QualifiedName`` of a built-in subclass, so the same stored value got different index
bytes depending on how the object was built.  Catches: any index whose persisted records (every
node of its trees, the index atom itself) differ between a value as the application built it and
the same value as a load decodes it, per case: a fixed offset named explicitly ``"UTC"``, an
offset named with its default name, a ``str`` Enum, a ``StrEnum``, an ``IntEnum``, a ``str``
subclass with its own ``__str__``, the same values inside a tuple, as keys extracted from a
record field, and as keys returned by a registered extractor.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from enum import Enum, IntEnum, StrEnum

import msgpack
import pytest

from proto_db.core.dbobject import DBObject
from proto_db.indexing import EqualityIndex, RangeIndex
from proto_db.indexing.keyspec import KeySpec, register_extractor, unregister_extractor
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace


class Color(str, Enum):
    RED = "red"


class Shade(StrEnum):
    DARK = "dark"


class Level(IntEnum):
    HIGH = 3


class _LoudStr(str):
    def __str__(self):
        return "LOUD"

    def __repr__(self):
        return "LoudStr()"


CASES = {
    "utc-named": (datetime(2026, 1, 1, tzinfo=timezone(timedelta(0), "UTC")),
                  datetime(2026, 1, 1, tzinfo=timezone.utc)),
    "offset-default-name": (datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=1), "UTC+01:00")),
                            datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=1)))),
    "str-enum": (Color.RED, "red"),
    "strenum": (Shade.DARK, "dark"),
    "intenum": (Level.HIGH, 3),
    "str-subclass": (_LoudStr("quiet"), "quiet"),
    "in-a-tuple": ((Color.RED, Level.HIGH, datetime(2026, 1, 1, tzinfo=timezone(timedelta(0), "UTC"))),
                   ("red", 3, datetime(2026, 1, 1, tzinfo=timezone.utc))),
}


def _records(space, root) -> list:
    """The persisted payload of every atom reachable from the saved ``root``, in walk order."""
    out, stack, seen = [], [root.atom_pointer], set()
    while stack:
        ptr = stack.pop()
        if ptr in seen:
            continue
        seen.add(ptr)
        record = msgpack.unpackb(msgpack.packb(space.storage.get_atom(ptr), use_bin_type=True), raw=False)
        out.append(_strip_identity(record))
        stack.extend(_refs(record, type(ptr)))
    return out


def _strip_identity(record):
    """The record without what differs between two builds of equal content: oids and pointers."""
    if isinstance(record, dict):
        return {k: _strip_identity(v) for k, v in record.items() if k != "oid"}
    if isinstance(record, list):
        if len(record) == 3 and isinstance(record[0], bytes) and isinstance(record[1], int):
            return ["@ref", record[2]]
        return [_strip_identity(v) for v in record]
    return record


def _refs(record, pointer_type):
    found = []
    if isinstance(record, dict):
        for k, v in record.items():
            if k == "@ref" and v is not None:
                found.append(_pointer(pointer_type, v))
            else:
                found.extend(_refs(v, pointer_type))
    elif isinstance(record, list):
        for v in record:
            found.extend(_refs(v, pointer_type))
    return found


def _pointer(pointer_type, ref):
    import uuid
    return pointer_type(uuid.UUID(bytes=ref[0]), ref[1])


def _index_bytes(make_index, elements) -> list:
    """The persisted records of an index built over ``elements`` and saved on its own."""
    space = ObjectSpace(MemoryStorage())
    try:
        with space.new_database("d").new_transaction() as tr:
            lst = tr.new_list().add_index(make_index())
            for e in elements:
                lst = lst.append_last(e)
            tr.set_root_object("l", lst)
        tr = space.open_database("d").new_transaction()
        index = tr.get_root_object("l").get_index("ix")
        return _records(space, index)
    finally:
        space.close()


@pytest.mark.parametrize("case", sorted(CASES))
@pytest.mark.parametrize("kind", [EqualityIndex, RangeIndex])
def test_an_element_index_has_the_same_bytes_however_the_value_was_built(case, kind):
    built, plain = CASES[case]
    make = lambda: kind("ix", KeySpec.element())
    assert _index_bytes(make, [built]) == _index_bytes(make, [plain])


@pytest.mark.parametrize("case", sorted(CASES))
def test_a_field_index_has_the_same_bytes_however_the_value_was_built(case):
    built, plain = CASES[case]
    make = lambda: EqualityIndex("ix", KeySpec.path("k"))
    assert _index_bytes(make, [DBObject(k=built, n=1)]) == _index_bytes(make, [DBObject(k=plain, n=1)])


@pytest.mark.parametrize("case", sorted(CASES))
def test_an_extractor_index_has_the_same_bytes_however_the_key_was_built(case):
    built, plain = CASES[case]
    name = f"tests.index_key_neutrality.{case}"
    key = [built]
    try:
        register_extractor(name, lambda record: key[0])
        make = lambda: EqualityIndex("ix", KeySpec.extractor(name))
        with_built = _index_bytes(make, [DBObject(n=1)])
        key[0] = plain
        assert with_built == _index_bytes(make, [DBObject(n=1)])
    finally:
        unregister_extractor(name)
