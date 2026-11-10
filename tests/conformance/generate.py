"""Generate the conformance vectors of the storage format (ruling R83, D57).

Run from the repository root::

    python -m tests.conformance.generate

It rewrites ``tests/conformance/vectors/`` and ``tests/conformance/manifest.json`` from the
code of this checkout: every byte is produced by the library's own encoders, never typed by
hand.  Identifiers that are random in normal use (atom oids, segment UUIDs, database origins,
commit timestamps) are made deterministic, so a regeneration with an unchanged format writes
the same bytes.  ``tests/conformance/test_conformance.py`` checks the committed files; it never
regenerates them.  Regenerate only when the format changes, and review the diff.
"""
from __future__ import annotations

import datetime
import json
import os
import shutil
import uuid
from decimal import Decimal
from pathlib import Path
from unittest import mock
from zoneinfo import ZoneInfo

import msgpack

from proto_db.cluster import protocol
from proto_db.cluster import rootlog
from proto_db.collections.dictionaries import Dictionary, RepeatedKeysDictionary
from proto_db.collections.hash_dictionaries import HashDictionary
from proto_db.collections.lists import List
from proto_db.collections.sets import CountedSet, Set
from proto_db.core import codec
from proto_db.core.atom import BUILTIN_TYPE_ALIASES, Atom, type_name_of
from proto_db.core.dbobject import DBObject
from proto_db.indexing import EqualityIndex, RangeIndex, VectorIndex
from proto_db.storage import cloud, format as fmt, logfile, tiered
from proto_db.storage.file import FileStorage
from proto_db.transactions import ObjectSpace, SpaceRoot, _ClusterStamp
from proto_db.values import MISSING, MutableObject
from tests.conformance import typed
from tests.conformance.model import Invoice
from tests.support.persistence import DirectContext

HERE = Path(__file__).resolve().parent
VECTORS = HERE / "vectors"
MANIFEST = HERE / "manifest.json"
PTR_SEGMENT = uuid.UUID("0123456789abcdef0123456789abcdef")


def _pack(data) -> bytes:
    return msgpack.packb(data, use_bin_type=True)


def _write(rel: str, data: bytes) -> str:
    path = VECTORS / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return rel


class _Deterministic:
    """Patches ``uuid.uuid4`` and the commit clock with deterministic sequences."""

    def __init__(self, seed: int):
        self.n = seed
        self.clock = datetime.datetime(2026, 11, 10, 12, 0, 0, tzinfo=datetime.timezone.utc)

    def uuid4(self):
        self.n += 1
        return uuid.UUID(int=(0x5EED << 112) | self.n, version=4)

    def now(self):
        self.clock += datetime.timedelta(seconds=1)
        return self.clock

    def __enter__(self):
        self._patches = [mock.patch("uuid.uuid4", self.uuid4),
                         mock.patch("proto_db.transactions._utc_now", self.now)]
        for p in self._patches:
            p.start()
        return self

    def __exit__(self, *exc):
        for p in reversed(self._patches):
            p.stop()


# -- canonical values ----------------------------------------------------------------------

def _V(kind, body=None):
    return {kind: body}


def _I(n):
    return {"int": str(n)}


def _S(s):
    return {"str": s}


#: (id, value description, note).  Every one round-trips byte for byte.
VALUES = [
    ("nil", _V("nil"), "msgpack nil"),
    ("true", _V("bool", True), "msgpack true"),
    ("false", _V("bool", False), "msgpack false"),
    ("int-zero", _I(0), "positive fixint"),
    ("int-127", _I(127), "largest positive fixint"),
    ("int-128", _I(128), "uint8"),
    ("int-minus-1", _I(-1), "negative fixint"),
    ("int-minus-32", _I(-32), "smallest negative fixint"),
    ("int-minus-33", _I(-33), "int8"),
    ("int-256", _I(256), "uint16"),
    ("int-65536", _I(65536), "uint32"),
    ("int-2p32", _I(2 ** 32), "uint64"),
    ("int-minus-2p31-1", _I(-2 ** 31 - 1), "int64"),
    ("int-max-int64", _I(2 ** 63 - 1), "uint64 (msgpack writes non-negative integers unsigned)"),
    ("int-max-uint64", _I(2 ** 64 - 1), "largest native integer"),
    ("int-min-int64", _I(-2 ** 63), "smallest native integer"),
    ("int-2p64", _I(2 ** 64), "@int: just above the native range"),
    ("int-min-int64-minus-1", _I(-2 ** 63 - 1), "@int: just below the native range"),
    ("int-10p40", _I(10 ** 40), "@int: signed hexadecimal text"),
    ("int-minus-10p40", _I(-10 ** 40), "@int with a minus sign"),
    ("float-1.5", _V("float", float.hex(1.5)), "float64, always 8 bytes"),
    ("float-0.1", _V("float", float.hex(0.1)), "float64"),
    ("float-minus-zero", _V("float", float.hex(-0.0)), "negative zero keeps its sign"),
    ("float-nan", _V("float", "nan"), "NaN: storable as a value, not as a key"),
    ("float-inf", _V("float", "inf"), "positive infinity"),
    ("float-minus-inf", _V("float", "-inf"), "negative infinity"),
    ("str-empty", _S(""), "fixstr"),
    ("str-ascii", _S("hello"), "fixstr"),
    ("str-unicode", _S("año € \U0001F600"), "UTF-8"),
    ("str-32", _S("x" * 32), "str8"),
    ("str-256", _S("y" * 256), "str16"),
    ("bytes-empty", _V("bytes", ""), "bin8"),
    ("bytes-short", _V("bytes", "00ff10"), "bin8"),
    ("bytes-256", _V("bytes", "ab" * 256), "bin16"),
    ("decimal", _V("decimal", "1.20"), "@dec: the exact text, trailing zeros kept"),
    ("decimal-exponent", _V("decimal", "-1.5E+30"), "@dec"),
    ("decimal-minus-zero", _V("decimal", "-0"), "@dec"),
    ("decimal-nan", _V("decimal", "NaN"), "@dec"),
    ("decimal-snan", _V("decimal", "sNaN"), "@dec"),
    ("decimal-inf", _V("decimal", "-Infinity"), "@dec"),
    ("date", _V("date", [2026, 11, 10]), "@date"),
    ("date-min", _V("date", [1, 1, 1]), "@date"),
    ("time-naive", _V("time", [23, 59, 58, 999999, 0, None]), "@time, no time zone"),
    ("time-fold", _V("time", [1, 30, 0, 0, 1, None]), "@time with fold 1"),
    ("time-utc", _V("time", [12, 0, 0, 0, 0, ["fixed", 0, "UTC"]]), "@time, fixed offset"),
    ("datetime-naive", _V("datetime", [2026, 11, 10, 12, 30, 45, 123456, 0, None]), "@dt, naive"),
    ("datetime-utc", _V("datetime", [2026, 11, 10, 12, 30, 45, 0, 0, ["fixed", 0, "UTC"]]),
     "@dt, UTC: what DatabaseRoot.committed_at holds"),
    ("datetime-offset", _V("datetime", [2026, 1, 2, 3, 4, 5, 6, 0, ["fixed", 19800000000, "UTC+05:30"]]),
     "@dt, fixed offset whose name is the default one"),
    ("datetime-offset-named", _V("datetime", [2026, 1, 2, 3, 4, 5, 6, 0, ["fixed", -10800000000, "ART"]]),
     "@dt, fixed offset with its own name"),
    ("datetime-zone", _V("datetime", [2026, 11, 10, 9, 0, 0, 0, 0, ["zone", "America/Argentina/Buenos_Aires"]]),
     "@dt, IANA zone"),
    ("datetime-zone-fold", _V("datetime", [2026, 11, 1, 1, 30, 0, 0, 1, ["zone", "America/New_York"]]),
     "@dt, IANA zone, the second 01:30 of a DST transition"),
    ("timedelta", _V("timedelta", [-1, 86399, 999999]), "@td, normalized (days, seconds, microseconds)"),
    ("uuid", _V("uuid", "12345678123456781234567812345678"), "@uuid: the 16 bytes"),
    ("list-empty", _V("list", []), "array"),
    ("list-nested", _V("list", [_I(1), _V("list", [_S("a"), _V("nil")]), _V("tuple", [])]), "array"),
    ("tuple-empty", _V("tuple", []), "@tup"),
    ("tuple", _V("tuple", [_I(1), _S("x"), _V("tuple", [_V("bool", True)])]), "@tup"),
    ("map", _V("map", [[_S("b"), _I(2)], [_S("a"), _V("list", [_I(1)])]]), "@map keeps insertion order"),
    ("map-keys", _V("map", [[_I(1), _S("one")], [_V("tuple", [_I(1), _I(2)]), _V("nil")],
                            [_V("nil"), _V("bool", False)]]), "@map with non-str keys"),
    ("map-empty", _V("map", []), "@map"),
    ("missing", _V("missing", True), "@missing"),
    ("mutable", _V("mutable", "counter"), "@mut: a MutableObject handle"),
    ("ref-builtin", _V("ref", {"segment": PTR_SEGMENT.hex, "offset": 26, "type": "proto_db.collections.Dictionary"}),
     "@ref to a built-in type: tag 6"),
    ("ref-node", _V("ref", {"segment": PTR_SEGMENT.hex, "offset": 4294967296, "type": "proto_db.avl.CountedKeyNode"}),
     "@ref to a built-in type: tag 21, offset beyond 32 bits"),
    ("ref-user", _V("ref", {"segment": PTR_SEGMENT.hex, "offset": 100, "type": "conformance.Invoice"}),
     "@ref to an application type: tag is the name"),
]


class _NoContext:
    """The context of the stubs that stand for referenced atoms: never used to load."""


def stub(ptr, type_name):
    """A lazy stub of the atom of ``type_name`` at ``ptr``: what a reference decodes to."""
    return Atom._stub(_NoContext(), ptr, type_name)


def saved_pointer(atom):
    return atom.atom_pointer


def _encode(desc) -> bytes:
    return _pack(codec.encode_value(typed.build(desc, stub), saved_pointer))


def gen_values(manifest):
    out = []
    for vid, desc, note in VALUES:
        data = _encode(desc)
        out.append({"id": vid, "file": _write(f"values/{vid}.msgpack", data), "value": desc, "note": note})
    manifest["values"] = out
    # Decode-only: older forms every reader must accept; the writer emits the canonical file.
    legacy = [
        ("ref-format2", {"@ref": [PTR_SEGMENT.hex, 26, "proto_db.collections.Dictionary"]}, "ref-builtin",
         "format 2 @ref: segment as 32 hexadecimal digits, type as its name"),
        ("ref-name-of-builtin", {"@ref": [PTR_SEGMENT.bytes, 26, "proto_db.collections.Dictionary"]}, "ref-builtin",
         "a built-in type spelled by name instead of its alias"),
    ]
    manifest["values_decode_only"] = [
        {"id": vid, "file": _write(f"values/{vid}.msgpack", _pack(wire)), "canonical": f"values/{canon}.msgpack",
         "value": next(d for i, d, _ in VALUES if i == canon), "note": note}
        for vid, wire, canon, note in legacy]


def gen_invalid(manifest):
    seg = PTR_SEGMENT.bytes
    cases = [
        ("value-unknown-tag", "value", _pack({"@nope": 1}), "an unknown tag"),
        ("value-two-keys", "value", _pack({"@int": "1", "@dec": "1"}), "a map value must have exactly one key"),
        ("value-int-not-hex", "value", _pack({"@int": "zz"}), "@int body is not hexadecimal"),
        ("value-ref-short-segment", "value", _pack({"@ref": [seg[:15], 0, 6]}), "@ref segment of 15 bytes"),
        ("value-ref-bad-alias", "value", _pack({"@ref": [seg, 0, 99]}), "@ref alias outside the table"),
        ("value-missing-not-zero", "value", _pack({"@missing": 1}), "@missing body must be 0"),
        ("value-mut-empty", "value", _pack({"@mut": ""}), "@mut needs a non-empty name"),
        ("value-symbol", "value", _pack({"@sym": "person"}),
         "@sym is reserved (runtime decision of 2026-11-08) and not implemented by this revision"),
    ]
    record = fmt.encode_record(fmt.RecordKind.ATOM, _pack({"t": 19, "oid": bytes(16), "s": {}}))
    bad_header = bytearray(record)
    bad_header[3] ^= 0x01            # a length bit: the header CRC no longer matches
    bad_payload = bytearray(record)
    bad_payload[-1] ^= 0x01
    cases += [
        ("record-header-crc", "record", bytes(bad_header), "length changed, header CRC mismatch"),
        ("record-payload-crc", "record", bytes(bad_payload), "payload changed, payload CRC mismatch"),
        ("record-bad-kind", "record", _reframe_kind(record, 3), "kind 3 is not defined"),
    ]
    root = fmt.encode_root(fmt.RootRecord(3, PTR_SEGMENT, 26, 1))
    bad_root = bytearray(root)
    bad_root[-1] ^= 0x01
    cases.append(("root-crc", "root", bytes(bad_root), "root CRC mismatch"))
    manifest["invalid"] = [{"id": i, "layer": layer, "file": _write(f"invalid/{i}.bin", data), "note": note}
                           for i, layer, data, note in cases]


def _reframe_kind(record: bytes, kind: int) -> bytes:
    """``record`` with another kind byte and a header CRC recomputed over it."""
    import zlib
    body = bytearray(record[:11])
    body[2] = kind
    return bytes(body) + zlib.crc32(bytes(body)).to_bytes(4, "little") + record[15:]


# -- the value model: order, hash and exact tokens -------------------------------------------

#: Values in ascending ``order_key`` order (equal keys adjacent and marked), covering every rank.
ORDERED = [
    _V("missing", True), _V("nil"), _V("bool", False), _V("bool", True),
    _V("float", "-inf"), _I(-10 ** 40), _V("decimal", "-1.5"), _I(-1), _I(0), _V("float", float.hex(0.5)),
    _I(1), _V("decimal", "1.10"), _I(2 ** 64), _V("float", "inf"),
    _S(""), _S("A"), _S("a"), _S("ab"), _S("\u00e9"), _S("\U0001F600"),
    _V("bytes", ""), _V("bytes", "00"), _V("bytes", "ff"),
    _V("date", [1, 1, 1]), _V("date", [2026, 11, 10]),
    _V("time", [10, 0, 0, 0, 0, None]), _V("time", [23, 0, 0, 0, 0, None]),
    _V("time", [12, 0, 0, 0, 0, ["fixed", 7200000000, "UTC+02:00"]]), _V("time", [11, 0, 0, 0, 0, ["fixed", 0, "UTC"]]),
    _V("datetime", [2026, 1, 1, 0, 0, 0, 0, 0, None]), _V("datetime", [2026, 1, 1, 0, 0, 0, 1, 0, None]),
    _V("datetime", [2026, 1, 1, 0, 0, 0, 0, 0, ["fixed", 3600000000, "UTC+01:00"]]),
    _V("datetime", [2026, 1, 1, 0, 0, 0, 0, 0, ["fixed", 0, "UTC"]]),
    _V("timedelta", [-1, 0, 0]), _V("timedelta", [0, 1, 0]),
    _V("uuid", "00000000000000000000000000000001"), _V("uuid", "ff000000000000000000000000000000"),
    _V("tuple", []), _V("tuple", [_I(1)]), _V("tuple", [_I(1), _S("a")]), _V("tuple", [_I(2)]),
    _V("mutable", "a"), _V("mutable", "b"),
]
#: Values whose ``hash_key`` is published (equal numbers of every type share one hash).
HASHED = [
    _V("missing", True), _V("nil"), _V("bool", False), _V("bool", True), _I(0), _I(1), _V("float", float.hex(1.0)),
    _V("decimal", "1.00"), _I(-1), _V("float", float.hex(0.1)), _V("decimal", "0.1"), _I(2 ** 61 - 1), _I(-10 ** 40),
    _V("float", "inf"), _V("decimal", "-Infinity"), _S(""), _S("person"), _S("a\u00f1o \U0001F600"),
    _V("bytes", ""), _V("bytes", "00ff"), _V("date", [2026, 11, 10]),
    _V("time", [12, 0, 0, 0, 0, None]), _V("time", [12, 0, 0, 0, 0, ["fixed", 3600000000, "UTC+01:00"]]),
    _V("datetime", [2026, 11, 10, 12, 0, 0, 0, 0, None]),
    _V("datetime", [2026, 11, 10, 12, 0, 0, 0, 0, ["zone", "America/Argentina/Buenos_Aires"]]),
    _V("timedelta", [1, 2, 3]), _V("uuid", "12345678123456781234567812345678"),
    _V("tuple", []), _V("tuple", [_I(1), _S("a"), _V("tuple", [_V("nil")])]), _V("mutable", "counter"),
]

#: Values whose exact token (part of every key-group and vector index member key) is published, as
#: the token of the stored (canonical) value: a stored list is a FrozenList, a dict a FrozenDict.
TOKENS = [
    _V("nil"), _V("bool", True), _I(1), _V("float", float.hex(1.0)), _V("decimal", "1.00"), _S("a"),
    _V("bytes", "00ff"), _V("date", [2026, 11, 10]), _V("time", [12, 0, 0, 5, 0, None]),
    _V("datetime", [2026, 11, 10, 12, 0, 0, 0, 0, ["fixed", 0, "UTC"]]),
    _V("datetime", [2026, 11, 10, 12, 0, 0, 0, 0, ["fixed", 19800000000, "UTC+05:30"]]),
    _V("datetime", [2026, 11, 10, 12, 0, 0, 0, 0, ["fixed", -10800000000, "ART"]]),
    _V("datetime", [2026, 11, 10, 12, 0, 0, 0, 1, ["zone", "America/New_York"]]),
    _V("timedelta", [1, 2, 3]), _V("uuid", "12345678123456781234567812345678"),
    _V("tuple", [_I(1), _S("a")]), _V("list", [_I(1)]), _V("map", [[_S("k"), _I(1)]]), _V("mutable", "counter"),
]


def gen_value_model(manifest):
    from proto_db.values import canonical, exact_token, hash_key, order_key
    values = [typed.build(d) for d in ORDERED]
    keys = [order_key(v) for v in values]
    assert all(a < b for a, b in zip(keys, keys[1:])), "ORDERED is not strictly ascending"
    manifest["order"] = {"ascending": ORDERED,
                         "equal": [[_I(1), _V("float", float.hex(1.0)), _V("decimal", "1.000")],
                                   [_V("float", float.hex(0.0)), _V("float", float.hex(-0.0)), _V("decimal", "-0")],
                                   [_V("datetime", [2026, 1, 1, 1, 0, 0, 0, 0, ["fixed", 3600000000, "UTC+01:00"]]),
                                    _V("datetime", [2026, 1, 1, 0, 0, 0, 0, 0, ["fixed", 0, "UTC"]])]],
                         "unorderable": [_V("float", "nan"), _V("decimal", "NaN"), _V("list", []), _V("map", []),
                                         _V("tuple", [_V("float", "nan")])]}
    for group in manifest["order"]["equal"]:
        assert len({order_key(typed.build(d)) for d in group}) == 1
    manifest["hashes"] = [{"value": d, "hash_key": str(hash_key(typed.build(d)))} for d in HASHED]
    manifest["exact_tokens"] = [{"value": d, "token": typed.describe(exact_token(canonical(typed.build(d))))} for d in TOKENS]


# -- a database written through the public API ---------------------------------------------

def _fill_first(tx):
    people = Dictionary().add_index(EqualityIndex("by_city", "city")).add_index(RangeIndex("by_age", "age"))
    for key, person in (("ann", DBObject(name="ann", city="Paris", age=25)),
                        ("bob", DBObject(name="bob", city="Rome", age=40)),
                        ("cid", DBObject(name="cid", city="Paris"))):
        people = people.set_at(key, person)
    tx.set_root_object("people", people)


def _fill_second(tx):
    tx.set_root_object("tags", List.from_iterable([1, "two", 3.5, None, "z" * 300, b"\x00\x01"]))
    tx.set_root_object("set", Set.from_iterable([1, "a", (1, 2)]))
    tx.set_root_object("bag", CountedSet.from_iterable([1, 1, 2]))
    tx.set_root_object("hash", HashDictionary().set_at("k", "v").set_at(2, datetime.date(2026, 1, 1)))
    tx.set_root_object("multi", RepeatedKeysDictionary().set_at("x", DBObject(n=1)).set_at("x", DBObject(n=2)))
    vectors = List().add_index(VectorIndex("near", "emb", dim=2, metric="l2"))
    for emb in ([1.0, 0.0], [0.0, 1.0]):
        vectors = vectors.append_last(DBObject(emb=emb))
    tx.set_root_object("vectors", vectors)
    counter = tx.new_mutable("counter", 5)
    customer = DBObject(name="ACME")
    tx.set_root_object("invoice", Invoice(
        number=Decimal("1001"), total=Decimal("1234.50"),
        issued=datetime.datetime(2026, 11, 10, 9, 0, tzinfo=ZoneInfo("America/Argentina/Buenos_Aires")),
        ref=uuid.UUID("12345678123456781234567812345678"), lines=(("bolt", 3), ("nut", 7)),
        extra={"paid": False, 1: None}, sequence=counter, customer=customer, note=MISSING))


def _records(path: Path) -> list:
    data = path.read_bytes()
    out = []
    pos = fmt.SEGMENT_HEADER_SIZE
    while pos < len(data):
        kind, payload = fmt.read_record(data, pos)
        size = fmt.record_size(data[pos:pos + fmt.RECORD_HEADER_SIZE])
        entry = {"offset": pos, "kind": int(kind), "size": size}
        if kind == fmt.RecordKind.ATOM:
            p = msgpack.unpackb(payload, raw=False)
            state = codec.decode_state(p["s"], stub)
            entry.update({"tag": p["t"], "type": type_name_of(p["t"]), "oid": p["oid"].hex(),
                          "state": [[k, typed.describe(v)] for k, v in state.items()]})
        out.append(entry)
        pos += size
    return out


def gen_database(manifest):
    target = VECTORS / "database"
    shutil.rmtree(target, ignore_errors=True)
    with _Deterministic(0x1000):
        space = ObjectSpace(FileStorage(str(target)))
        db = space.new_database("db")
        db.run(_fill_first)
        db.run(_fill_second)
        space.close()
    for name in os.listdir(target):
        if not (name == "space.root" or name.endswith(".seg")):
            os.remove(target / name)                 # the lock file: no data
    segments = sorted(n for n in os.listdir(target) if n.endswith(".seg"))
    assert len(segments) == 1, segments
    root = fmt.decode_root((target / "space.root").read_bytes())
    manifest["database"] = {
        "dir": "database",
        "segment": segments[0],
        "root": {"version": root.version, "segment": root.segment.hex, "offset": root.offset, "epoch": root.epoch},
        "records": _records(target / segments[0]),
        "contents": _contents(target),
    }


def _contents(target: Path) -> dict:
    """What a reader must find in the database vector (checked through the public API)."""
    work = VECTORS.parent / ".contents-work"
    shutil.rmtree(work, ignore_errors=True)
    shutil.copytree(target, work)
    try:
        space = ObjectSpace(FileStorage(str(work)))
        try:
            db = space.open_database("db")
            return db.run(lambda tx: describe_database(tx))
        finally:
            space.close()
    finally:
        shutil.rmtree(work, ignore_errors=True)


def describe_database(tx) -> dict:
    """A language-neutral description of the database vector's content (also used by the test)."""
    def obj(o):
        if isinstance(o, DBObject):
            return {"object": type(o).__type_name__,
                    "attrs": [[k, obj(v)] for k, v in sorted(o._state().items())]}
        if isinstance(o, (List, Set)):
            return {"collection": type(o).__type_name__, "elements": [obj(e) for e in o.elements()]}
        if isinstance(o, CountedSet):
            return {"collection": type(o).__type_name__, "items": [[obj(e), _I(n)] for e, n in o.items()]}
        if isinstance(o, (Dictionary, HashDictionary)):
            return {"collection": type(o).__type_name__, "items": [[obj(k), obj(v)] for k, v in o.items()]}
        if isinstance(o, RepeatedKeysDictionary):
            return {"collection": type(o).__type_name__, "items": [[obj(k), obj(v)] for k, v in o.items()]}
        if isinstance(o, tuple):
            return {"tuple": [obj(v) for v in o]}
        if isinstance(o, list):
            return {"list": [obj(v) for v in o]}
        if isinstance(o, dict):
            return {"map": [[obj(k), obj(v)] for k, v in o.items()]}
        return typed.describe(o)

    out = {"roots": [[name, obj(tx.get_root_object(name))] for name in sorted(tx.root_names())],
           "mutables": [["counter", obj(tx.get_mutable("counter"))]]}
    people = tx.get_root_object("people")
    out["indexes"] = {name: type(people.get_index(name)).__type_name__ for name in people.index_names()}
    return out


# -- framing -------------------------------------------------------------------------------

def gen_framing(manifest):
    atom = _pack({"t": 19, "oid": uuid.UUID("00112233445566778899aabbccddeeff").bytes, "s": {"a": 1}})
    blob = b"opaque bytes"
    root = fmt.RootRecord(fmt.FORMAT_VERSION, PTR_SEGMENT, 26, 7)
    root_v2 = msgpack.packb({"version": 2, "segment": PTR_SEGMENT.bytes, "offset": 26, "epoch": 7},
                            use_bin_type=True)
    import zlib
    manifest["framing"] = {
        "segment_header": {"file": _write("framing/segment-header.bin", fmt.encode_segment_header(PTR_SEGMENT)),
                           "segment": PTR_SEGMENT.hex, "version": fmt.FORMAT_VERSION},
        "segment_header_v2": {"file": _write("framing/segment-header-v2.bin", fmt.SEGMENT_MAGIC
                                             + (2).to_bytes(2, "little") + PTR_SEGMENT.bytes),
                              "segment": PTR_SEGMENT.hex, "version": 2, "note": "format 2: still read"},
        "atom_record": {"file": _write("framing/atom.record", fmt.encode_record(fmt.RecordKind.ATOM, atom)),
                        "kind": 1, "payload": typed.describe_raw(msgpack.unpackb(atom, raw=False))},
        "blob_record": {"file": _write("framing/blob.record", fmt.encode_record(fmt.RecordKind.BLOB, blob)),
                        "kind": 2, "payload_hex": blob.hex()},
        "root": {"file": _write("framing/space.root", fmt.encode_root(root)),
                 "fields": {"version": 3, "segment": PTR_SEGMENT.hex, "offset": 26, "epoch": 7}},
        "root_v2": {"file": _write("framing/space-v2.root", zlib.crc32(root_v2).to_bytes(4, "little") + root_v2),
                    "fields": {"version": 2, "segment": PTR_SEGMENT.hex, "offset": 26, "epoch": 7},
                    "note": "a format-2 root record is read as a current one"},
        "cloud_root": {"file": _write("framing/cloud-space.root", cloud._encode_root_object(
                           root, [(PTR_SEGMENT, 10, 6), (PTR_SEGMENT, 0, 5)])),
                       "fields": {"version": 3, "segment": PTR_SEGMENT.hex, "offset": 26, "epoch": 7},
                       "recent": [[PTR_SEGMENT.hex, 10, 6], [PTR_SEGMENT.hex, 0, 5]]},
        "tiered_root": {"file": _write("framing/tiered-space.root", tiered._encode_root_object(root, 9)),
                        "fields": {"version": 3, "segment": PTR_SEGMENT.hex, "offset": 26, "epoch": 7},
                        "term": 9},
    }


# -- logs and the cluster ------------------------------------------------------------------

def _cluster_commit(directory: Path):
    """Records of one cluster commit: a SpaceRoot stamped with its lease term and epoch, built on
    the database vector's root (its ``cluster_previous``), in a segment of its own."""
    db_dir = VECTORS / "database"
    base = fmt.decode_root((db_dir / "space.root").read_bytes())
    shutil.rmtree(directory, ignore_errors=True)
    shutil.copytree(db_dir, directory)
    with _Deterministic(0x2000):
        storage = FileStorage(str(directory))
        try:
            ctx = DirectContext(storage)
            previous = ctx.load(base.pointer, SpaceRoot.__type_name__)
            root = SpaceRoot(databases=previous.databases, cluster=_ClusterStamp(7, base.epoch + 1),
                             previous=previous)
            ptr = ctx.save(root)
            storage.flush()
        finally:
            storage.close()
    seg = directory / (ptr.segment.hex + ".seg")
    data = seg.read_bytes()
    records = []
    pos = fmt.SEGMENT_HEADER_SIZE
    while pos < len(data):
        size = fmt.record_size(data[pos:pos + fmt.RECORD_HEADER_SIZE])
        records.append([ptr.segment.bytes, pos, data[pos:pos + size]])
        pos += size
    return base, fmt.RootRecord(fmt.FORMAT_VERSION, ptr.segment, ptr.offset, base.epoch + 1), records, data


def gen_cluster(manifest):
    work = VECTORS.parent / ".cluster-work"
    try:
        base, root, records, segment = _cluster_commit(work)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    seg_uuid = root.segment
    seg_file = _write(f"cluster/{seg_uuid.hex}.seg", segment)
    ranges = protocol.record_ranges(records)
    entries = [
        ("rejoined", rootlog._payload(4, 0, None)),
        ("promise", rootlog._payload(3, 7, None)),
        ("accepted-records", rootlog.encode_records_entry(7, root, 1, (0, base), records)),
        ("committed", rootlog._payload(2, 7, root)),
        ("accepted", rootlog._payload(1, 7, root)),
        ("replica-accepted", rootlog.encode_replica_entry(7, root, 2, (7, root), [], ranges, accepted=True)),
        ("replica-stored", rootlog.encode_replica_entry(7, root, 3, (7, root), records, ranges, accepted=False)),
    ]
    log = logfile.encode_header(rootlog.LOG_KIND) + b"".join(logfile.encode_frame(p) for _, p in entries)
    ranges_payload = tiered.TieredStorage._ranges_payload((root.segment, root.offset), (7, root.epoch), ranges)
    ranges_log = logfile.encode_header(tiered.RANGES_KIND) + logfile.encode_frame(ranges_payload)
    segs_log = logfile.encode_header(tiered.MANIFEST_KIND) + logfile.encode_frame(seg_uuid.bytes)

    def root_desc(r):
        return None if r is None else {"segment": r.segment.hex, "offset": r.offset, "epoch": r.epoch}

    manifest["cluster"] = {
        "segment": {"file": seg_file, "records": _records(VECTORS / seg_file)},
        "roots_log": {
            "file": _write("cluster/roots.log", log), "kind": "ROOT",
            "entries": [{"name": n, "size": len(p)} for n, p in entries],
            "base": root_desc(base), "root": root_desc(root), "label": 7,
            "loaded": {"accepted": [7, root_desc(root)], "committed": [7, root_desc(root)], "promised": 7,
                       "top": 7, "empty": False},
        },
        "ranges_log": {"file": _write("cluster/ranges.log", ranges_log), "kind": "RANG",
                       "entries": [{"root": root_desc(root), "label": 7,
                                    "ranges": [[s.hex, a, b] for s, a, b in ranges]}]},
        "segments_log": {"file": _write("cluster/segments.log", segs_log), "kind": "SEGS",
                         "entries": [seg_uuid.hex]},
        "messages": _messages(root, base, records, ranges),
    }


def _messages(root, base, records, ranges) -> list:
    wire = protocol.encode_root(root, known=True, term=7)
    wire_base = protocol.encode_root(base, known=True, term=0)
    rng = protocol.encode_ranges(ranges)
    seg = root.segment.bytes
    msgs = [
        ("request", dict(term=7, rid=1, inc=41, vs=3)),
        ("grant", dict(term=7, rid=1, inc=41, root=wire_base, committed=wire_base, certain=True)),
        ("reject", dict(term=7, rid=1, inc=41, root=wire_base, certain=True, reason="promised", highest=9,
                        remaining=0.25)),
        ("release", dict(term=7, inc=41, root=wire, committed=wire, certain=True, held=True, next="n2", more=False)),
        ("want", dict()),
        ("records", dict(term=7, rid=2, inc=41, index=0, records=records[:1])),
        ("replicate", dict(term=7, rid=2, inc=41, root=wire, committed=wire_base, records=records[1:], chunks=1,
                           base=None, base_ranges=None, seq=1, pred=wire_base, vs=3, copies=["n1"], ranges=rng)),
        ("store", dict(term=7, rid=3, inc=41, root=wire, records=records, chunks=0, ranges=rng, copies=["n1"], vs=3)),
        ("propose", dict(term=7, seq=1, inc=41, root=wire, pred=wire_base)),
        ("renew", dict(term=7, rid=4, inc=41, vs=3)),
        ("permit_req", dict(term=7, rid=5, inc=42)),
        ("permit", dict(term=7, rid=5, seq=2, inc=42, root=wire, certain=False, valid=1.5, after=[1, "n1"])),
        ("permit_no", dict(term=7, rid=6, inc=42, reason="not owner")),
        ("done", dict(term=7, seq=1, inc=41, outcome="committed", root=wire, copies=["n1", "n2"], ranges=rng)),
        ("ack", dict(term=7, rid=2, inc=41, ok=True, root=wire, stored=True)),
        ("query", dict(rid=7, inc=43, ballot=0, vs=3)),
        ("answer", dict(rid=7, inc=43, root=wire, committed=wire, certain=True, promised=7, state="ok", ballot=0)),
        ("fetch", dict(rid=8, inc=43, segment=seg, offset=records[0][1])),
        ("fetched", dict(rid=8, inc=43, segment=seg, offset=records[0][1], data=records[0][2])),
        ("warm", dict(term=7, records=records)),
        ("commit_query", dict(rid=9, inc=43, root=wire)),
        ("commit_info", dict(rid=9, inc=43, persisted=False, ranges=rng)),
        ("unknown-root", dict(rid=10, inc=43, root=None)),
    ]
    out = []
    for name, fields in msgs:
        kind = "commit_query" if name == "unknown-root" else name
        msg = protocol.make_message(kind, "n1", **fields)
        frame = protocol.encode_frame(msg)
        out.append({"id": name, "type": kind, "file": _write(f"cluster/messages/{name}.frame", frame),
                    "message": typed.describe_raw(msgpack.unpackb(frame[4:], raw=False))})
    return out


def main():
    shutil.rmtree(VECTORS, ignore_errors=True)
    VECTORS.mkdir(parents=True)
    manifest = {
        "about": "Conformance vectors of the ProtoDB storage format; see tests/conformance/README.md "
                 "and docs/source/format-spec.rst.",
        "versions": {"format": fmt.FORMAT_VERSION, "readable_formats": sorted(fmt.READABLE_FORMAT_VERSIONS),
                     "log_format": logfile.LOG_FORMAT_VERSION, "protocol": protocol.PROTOCOL_VERSION},
        "builtin_type_aliases": list(BUILTIN_TYPE_ALIASES),
    }
    gen_values(manifest)
    gen_invalid(manifest)
    gen_value_model(manifest)
    gen_framing(manifest)
    gen_database(manifest)
    gen_cluster(manifest)
    MANIFEST.write_text(json.dumps(manifest, indent=1, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
