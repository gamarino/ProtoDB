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

import contextlib
import datetime
import enum
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
from proto_db.collections import avl
from proto_db.collections.base import IndexedCollection
from proto_db.collections.dictionaries import Dictionary, RepeatedKeysDictionary
from proto_db.collections.hash_dictionaries import HashDictionary
from proto_db.collections.lists import List
from proto_db.collections.sets import CountedSet, Set
from proto_db.core import codec
from proto_db.core.atom import BUILTIN_TYPE_ALIASES, Atom, type_name_of
from proto_db.core.dbobject import DBObject
from proto_db.indexing import EqualityIndex, RangeIndex, VectorIndex
from proto_db.indexing.keyspec import KeySpec
from proto_db.storage import cloud, format as fmt, logfile, tiered
from proto_db.storage.file import FileStorage
from proto_db.transactions import ObjectSpace, SpaceRoot, _ClusterStamp
from proto_db.errors import ProtoValidationError
from proto_db.values import MISSING, FrozenDict, FrozenList, MutableObject, is_atom
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
    ("float-subnormal", _V("float", float.hex(5e-324)), "the smallest subnormal"),
    ("float-max", _V("float", float.hex(1.7976931348623157e308)), "the largest finite float"),
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
    ("decimal-small-exponent", _V("decimal", "1E-7"), "@dec: adjusted exponent below -6 gives the exponent form"),
    ("decimal-small-plain", _V("decimal", "0.000001"), "@dec: adjusted exponent -6 keeps the plain form"),
    ("decimal-positive-exponent", _V("decimal", "1E+2"), "@dec: a positive exponent always gives the exponent form"),
    ("decimal-zero-exponent", _V("decimal", "0E+2"), "@dec: zero with an exponent"),
    ("decimal-nan-payload", _V("decimal", "-NaN123"), "@dec: a signed NaN with a payload"),
    ("date", _V("date", [2026, 11, 10]), "@date"),
    ("date-min", _V("date", [1, 1, 1]), "@date"),
    ("time-naive", _V("time", [23, 59, 58, 999999, 0, None]), "@time, no time zone"),
    ("time-fold", _V("time", [1, 30, 0, 0, 1, None]), "@time with fold 1"),
    ("time-utc", _V("time", [12, 0, 0, 0, 0, ["fixed", 0, "UTC"]]), "@time, fixed offset"),
    ("time-zone", _V("time", [12, 0, 0, 0, 0, ["zone", "America/New_York"]]),
     "@time in an IANA zone: the wall clock (orders as the naive time, rank 7)"),
    ("datetime-naive", _V("datetime", [2026, 11, 10, 12, 30, 45, 123456, 0, None]), "@dt, naive"),
    ("datetime-utc", _V("datetime", [2026, 11, 10, 12, 30, 45, 0, 0, ["fixed", 0, "UTC"]]),
     "@dt, UTC: what DatabaseRoot.committed_at holds"),
    ("datetime-offset", _V("datetime", [2026, 1, 2, 3, 4, 5, 6, 0, ["fixed", 19800000000, "UTC+05:30"]]),
     "@dt, fixed offset whose name is the default one"),
    ("datetime-offset-named", _V("datetime", [2026, 1, 2, 3, 4, 5, 6, 0, ["fixed", -10800000000, "ART"]]),
     "@dt, fixed offset with its own name"),
    ("datetime-zone", _V("datetime", [2026, 11, 10, 12, 0, 0, 0, 0, ["instant", "America/Argentina/Buenos_Aires"]]),
     "@dt, IANA zone: the UTC instant (09:00 in Buenos Aires)"),
    ("datetime-zone-fold", _V("datetime", [2026, 11, 1, 6, 30, 0, 0, 0, ["instant", "America/New_York"]]),
     "@dt, IANA zone: the instant of the second 01:30 of a DST transition (fold 1 in New York)"),
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
        ("ref-format2", _pack({"@ref": [PTR_SEGMENT.hex, 26, "proto_db.collections.Dictionary"]}), "ref-builtin",
         "format 2 @ref: segment as 32 hexadecimal digits, type as its name"),
        ("ref-name-of-builtin", _pack({"@ref": [PTR_SEGMENT.bytes, 26, "proto_db.collections.Dictionary"]}),
         "ref-builtin", "a built-in type spelled by name instead of its alias"),
        ("ref-alias", _pack({"@ref": [PTR_SEGMENT.bytes, 100, "conformance.LegacyInvoice"]}), "ref-user",
         "an application type spelled by an alias (__type_aliases__): read as the declared name"),
        ("datetime-zone-format3", _pack({"@dt": [2026, 11, 1, 1, 30, 0, 0, 1, ["zone", "America/New_York"]]}),
         "datetime-zone-fold", "format 3 zone datetime: the wall clock and fold, resolved with the reader's tz "
                               "database; written back as the instant"),
        ("float-32", b"\xca" + bytes.fromhex("3fc00000"), "float-1.5", "a float 32: read, written as float 64"),
        ("int-wide", b"\xd3" + bytes(8), "int-zero", "an integer in a wider encoding than needed"),
    ]
    zone_key = {"@dt": [2026, 3, 8, 12, 0, 0, 0, 0, ["zone", "America/New_York"]]}
    manifest["values_read_not_rewritten"] = [
        {"id": vid, "file": _write(f"values/{vid}.msgpack", data), "value": typed.describe(_decode_for_manifest(data)),
         "note": note}
        for vid, data, note in [
            ("map-zone-key-format3", _pack({"@map": [[zone_key, "a"], [{"@tup": [zone_key, 1]}, "b"]]}),
             "format 3: a datetime in an IANA zone as a map key (alone and in a tuple)"),
            ("map-nan-keys-format3", _pack({"@map": [[float("nan"), 1], [float("nan"), 2]]}),
             "format 3: two NaN keys with one encoding, kept as two keys"),
            ("map-decimal-nan-keys-format3", _pack({"@map": [[{"@dec": "NaN"}, 1], [{"@dec": "NaN"}, 2]]}),
             "format 3: two decimal NaN keys with one encoding, kept as two keys"),
        ]]
    manifest["values_decode_only"] = [
        {"id": vid, "file": _write(f"values/{vid}.msgpack", data), "canonical": f"values/{canon}.msgpack",
         "value": next(d for i, d, _ in VALUES if i == canon), "note": note}
        for vid, data, canon, note in legacy]


def _decode_for_manifest(data: bytes):
    return codec.decode_value(msgpack.unpackb(data, raw=False, strict_map_key=False), lambda p, t: None)


def gen_invalid(manifest):
    """Inputs a reader must refuse: ``layer`` says how the file is read, ``error`` the refusal
    (``corruption``: ProtoCorruptionError; ``not_supported``: ProtoNotSupportedError;
    ``protocol``: the cluster receiver drops the connection)."""
    seg = PTR_SEGMENT.bytes
    dt = [2026, 1, 1, 0, 0, 0, 0]
    cases = [
        ("value-unknown-tag", "value", _pack({"@nope": 1}), "an unknown tag"),
        ("value-two-keys", "value", _pack({"@int": "1", "@dec": "1"}), "a map value must have exactly one key"),
        ("value-zero-keys", "value", _pack({}), "a map value must have exactly one key"),
        ("value-ext", "value", _pack(msgpack.ExtType(5, b"x")), "msgpack ext types are never values"),
        ("value-timestamp", "value", _pack(msgpack.Timestamp(0, 0)), "the msgpack timestamp extension"),
        ("value-int-not-hex", "value", _pack({"@int": "zz"}), "@int body is not hexadecimal"),
        ("value-int-prefix", "value", _pack({"@int": "0x10000000000000000"}), "@int: only the canonical text"),
        ("value-int-upper", "value", _pack({"@int": "1FFFFFFFFFFFFFFFF"}), "@int: lowercase digits only"),
        ("value-int-native", "value", _pack({"@int": "ff"}), "@int of an integer in the native range"),
        ("value-dec-not-canonical", "value", _pack({"@dec": "1.0E+1"}), "@dec: not the to-scientific-string"),
        ("value-dec-lowercase-nan", "value", _pack({"@dec": "nan"}), "@dec: NaN is spelled NaN"),
        ("value-td-not-normalized", "value", _pack({"@td": [0, 86400, 0]}), "@td seconds must be < 86400"),
        ("value-date-out-of-range", "value", _pack({"@date": [2026, 2, 30]}), "@date: no such day"),
        ("value-unknown-zone", "value", _pack({"@dt": dt + [0, ["instant", "No/Such_Zone"]]}),
         "a time zone key the reader does not know"),
        ("value-instant-fold", "value", _pack({"@dt": dt + [1, ["instant", "America/New_York"]]}),
         "an instant has fold 0"),
        ("value-instant-wall-out-of-range", "value",
         _pack({"@dt": [9999, 12, 31, 23, 0, 0, 0, 0, ["instant", "Asia/Tokyo"]]}),
         "an instant whose wall clock in its zone is after year 9999"),
        ("value-time-instant", "value", _pack({"@time": [12, 0, 0, 0, 0, ["instant", "America/New_York"]]}),
         "the instant form is for @dt only"),
        ("value-ref-short-segment", "value", _pack({"@ref": [seg[:15], 0, 6]}), "@ref segment of 15 bytes"),
        ("value-ref-bad-alias", "value", _pack({"@ref": [seg, 0, 99]}), "@ref alias outside the table"),
        ("value-missing-not-zero", "value", _pack({"@missing": 1}), "@missing body must be 0"),
        ("value-mut-empty", "value", _pack({"@mut": ""}), "@mut needs a non-empty name"),
        ("value-symbol", "value", _pack({"@sym": "person"}),
         "@sym is reserved (runtime decision of 2026-11-08) and not implemented by this revision"),
        ("map-key-bool-and-int", "value", _pack({"@map": [[1, "a"], [True, "b"]]}), "true conflicts with 1"),
        ("map-key-equal-numbers", "value", _pack({"@map": [[1, "a"], [{"@dec": "1.0"}, "b"]]}),
         "1 and Decimal('1.0') conflict"),
        ("map-key-same-instant", "value", _pack({"@map": [
            [{"@dt": dt + [0, ["fixed", 0, "UTC"]]}, 1],
            [{"@dt": [2025, 12, 31, 21, 0, 0, 0, 0, ["fixed", -10800000000, "UTC-03:00"]]}, 2]]}),
         "two aware datetimes of one instant conflict"),
        ("map-key-duplicate", "value", _pack({"@map": [["k", 1], ["k", 2]]}), "one key twice"),
        ("map-key-list", "value", _pack({"@map": [[[1], 1]]}), "a list cannot be a map key"),
        ("map-key-snan", "value", _pack({"@map": [[{"@dec": "sNaN"}, 1]]}), "a signaling NaN cannot be a map key"),
        ("map-key-zone-datetime", "value", _pack({"@map": [[{"@dt": dt + [0, ["instant", "America/New_York"]]}, 1]]}),
         "a datetime in an IANA zone cannot be a map key"),
    ]
    out = [(i, layer, data, note, "corruption", None) for i, layer, data, note in cases]
    record = fmt.encode_record(fmt.RecordKind.ATOM, _pack({"t": 19, "oid": bytes(16), "s": {}}))
    bad_header = bytearray(record)
    bad_header[3] ^= 0x01            # a length bit: the header CRC no longer matches
    bad_payload = bytearray(record)
    bad_payload[-1] ^= 0x01
    out += [
        ("record-header-crc", "record", bytes(bad_header), "length changed, header CRC mismatch", "corruption", None),
        ("record-payload-crc", "record", bytes(bad_payload), "payload changed, payload CRC mismatch", "corruption",
         None),
        ("record-bad-kind", "record", _reframe_kind(record, 3), "kind 3 is not defined", "corruption", None),
    ]
    root = fmt.encode_root(fmt.RootRecord(fmt.FORMAT_VERSION, PTR_SEGMENT, 26, 1))
    bad_root = bytearray(root)
    bad_root[-1] ^= 0x01
    out += [
        ("root-crc", "root", bytes(bad_root), "root CRC mismatch", "corruption", None),
        ("root-version-5", "root", fmt.encode_root(fmt.RootRecord(5, PTR_SEGMENT, 26, 1)),
         "a root record of an unknown version", "not_supported", None),
        ("segment-bad-magic", "segment", b"PDB2SEX\0" + (4).to_bytes(2, "little") + seg, "another magic",
         "corruption", None),
        ("segment-version-5", "segment", fmt.SEGMENT_MAGIC + (5).to_bytes(2, "little") + seg,
         "a segment of an unknown version", "not_supported", None),
    ]
    oid = uuid.UUID(int=1).bytes
    atoms = [
        ("atom-extra-key", {"t": 19, "oid": oid, "s": {}, "x": 1}, "proto_db.core.dbobject.DBObject",
         "an envelope with another key"),
        ("atom-short-oid", {"t": 19, "oid": oid[:15], "s": {}}, "proto_db.core.dbobject.DBObject",
         "an oid of 15 bytes"),
        ("atom-tag-float", {"t": 1.5, "oid": oid, "s": {}}, "proto_db.core.dbobject.DBObject",
         "a type tag that is neither an integer nor a string"),
        ("atom-unknown-alias", {"t": 99, "oid": oid, "s": {}}, "proto_db.core.dbobject.DBObject",
         "an alias outside the table"),
        ("atom-other-type", {"t": 5, "oid": oid, "s": {"root": None, "count": 0, "indexes": None}},
         "proto_db.core.dbobject.DBObject", "a record of another type than the reference's"),
        ("atom-missing-field", {"t": 5, "oid": oid, "s": {"root": None, "indexes": None}},
         "proto_db.collections.List", "a List without its required count"),
        ("atom-reserved-name", {"t": 19, "oid": oid, "s": {"with_": 1}}, "proto_db.core.dbobject.DBObject",
         "an attribute named like behaviour of DBObject"),
        ("atom-underscore-name", {"t": 19, "oid": oid, "s": {"_x": 1}}, "proto_db.core.dbobject.DBObject",
         "an attribute starting with _"),
        ("atom-bytes-name", {"t": 19, "oid": oid, "s": {b"x": 1}}, "proto_db.core.dbobject.DBObject",
         "an attribute name that is not a string"),
    ]
    out += [(i, "atom", _pack(payload), note, "corruption", {"type": t}) for i, payload, t, note in atoms]
    header = logfile.encode_header(rootlog.LOG_KIND)
    entry = rootlog._payload(1, 7, fmt.RootRecord(fmt.FORMAT_VERSION, PTR_SEGMENT, 26, 3))
    out += [
        ("log-version-3", "log", fmt_log_header(3, rootlog.LOG_KIND) + logfile.encode_frame(entry),
         "a log of another format version", "not_supported", {"kind": "ROOT"}),
        ("log-kind-mismatch", "log", logfile.encode_header(tiered.MANIFEST_KIND) + logfile.encode_frame(seg),
         "a SEGS log opened as ROOT", "corruption", {"kind": "ROOT"}),
        ("log-mid-corruption", "log", header + _damaged(logfile.encode_frame(entry)) + logfile.encode_frame(entry),
         "a damaged frame followed by a valid one: mid-log corruption, never a torn tail", "corruption",
         {"kind": "ROOT"}),
    ]
    other = fmt.RootRecord(fmt.FORMAT_VERSION, PTR_SEGMENT, 90, 3)
    replica = rootlog.encode_replica_entry(7, other, 1, None, [], [], accepted=True)
    bad_flags = bytearray(replica)
    bad_flags[1] |= 0x02
    bad_range = rootlog.encode_replica_entry(7, other, 1, None, [], [(PTR_SEGMENT, 10, 5)], accepted=True)
    out += [
        ("rootlog-unknown-flags", "roots_log", header + logfile.encode_frame(bytes(bad_flags)),
         "a kind-6 entry with an undefined flag bit", "corruption", None),
        ("rootlog-range-reversed", "roots_log", header + logfile.encode_frame(bad_range),
         "a kind-6 range with start > end", "corruption", None),
        ("rootlog-same-label-epoch", "roots_log",
         header + logfile.encode_frame(entry) + logfile.encode_frame(rootlog._payload(1, 7, other)),
         "two different roots with one (label, epoch)", "corruption", None),
        ("rootlog-short-entry", "roots_log", header + logfile.encode_frame(entry[:40]),
         "a kind-1 entry of 40 bytes", "corruption", None),
        ("rootlog-unknown-kind", "roots_log", header + logfile.encode_frame(b"\x09" + entry[1:]),
         "an unknown entry kind", "corruption", None),
    ]
    good = protocol.make_message("renew", "n1", term=7, rid=4, inc=41, vs=3)

    def frame(msg):
        body = msgpack.packb(msg, use_bin_type=True)
        return len(body).to_bytes(4, "big") + body
    out += [
        ("message-version", "message", frame(dict(good, v=protocol.PROTOCOL_VERSION - 1)), "another protocol version",
         "protocol", None),
        ("message-unknown-type", "message", frame(dict(good, type="nope")), "an unknown message type", "protocol",
         None),
        ("message-ill-typed", "message", frame(dict(good, term="7")), "a field of the wrong type", "protocol", None),
        ("message-missing-field", "message", frame({k: v for k, v in good.items() if k != "vs"}),
         "a missing field", "protocol", None),
        ("message-not-a-map", "message", frame([1, 2]), "a body that is not a map", "protocol", None),
        ("message-oversized", "message", (protocol.MAX_FRAME_BYTES + 1).to_bytes(4, "big"),
         "a length over 64 MiB", "protocol", None),
    ]
    manifest["invalid"] = []
    for i, layer, data, note, error, extra in out:
        entry_desc = {"id": i, "layer": layer, "error": error, "file": _write(f"invalid/{i}.bin", data), "note": note}
        if extra:
            entry_desc.update(extra)
        manifest["invalid"].append(entry_desc)


def fmt_log_header(version: int, kind: bytes) -> bytes:
    return logfile.LOG_MAGIC + version.to_bytes(2, "little") + kind


def _damaged(frame: bytes) -> bytes:
    """``frame`` with one payload byte flipped: its header is valid, its payload CRC is not."""
    data = bytearray(frame)
    data[-1] ^= 0x01
    return bytes(data)


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
    _V("datetime", [2026, 11, 10, 12, 0, 0, 0, 0, ["instant", "America/Argentina/Buenos_Aires"]]),
    _V("time", [12, 0, 0, 0, 0, ["zone", "America/New_York"]]), _V("float", float.hex(-0.5)),
    _V("decimal", "-2.5E-3"), _V("float", "-inf"), _I(-1 - 2 ** 61), _V("uuid", "ffffffffffffffffffffffffffffffff"),
    _V("timedelta", [1, 2, 3]), _V("uuid", "12345678123456781234567812345678"),
    _V("tuple", []), _V("tuple", [_I(1), _S("a"), _V("tuple", [_V("nil")])]), _V("mutable", "counter"),
]

#: Values whose exact token (part of every key-group and vector index member key) is published, as
#: the token of the stored (canonical) value: a stored list is a FrozenList, a dict a FrozenDict.
TOKENS = [
    _V("nil"), _V("bool", True), _I(1), _I(2 ** 64), _V("float", float.hex(1.0)), _V("float", float.hex(0.0)),
    _V("float", float.hex(-0.0)), _V("float", float.hex(5e-324)), _V("float", "inf"), _V("float", "-inf"),
    _V("float", "nan"), _V("decimal", "1.00"), _V("decimal", "1E+2"), _V("decimal", "1E-7"),
    _V("decimal", "-NaN123"), _V("decimal", "sNaN"), _V("decimal", "-Infinity"), _S("a"),
    _V("bytes", "00ff"), _V("date", [2026, 11, 10]), _V("time", [12, 0, 0, 5, 0, None]),
    _V("time", [12, 0, 0, 0, 0, ["fixed", 3600000000, "UTC+01:00"]]),
    _V("time", [12, 0, 0, 0, 0, ["zone", "America/New_York"]]),
    _V("datetime", [2026, 11, 10, 12, 0, 0, 0, 0, ["fixed", 0, "UTC"]]),
    _V("datetime", [2026, 11, 10, 12, 0, 0, 0, 0, ["fixed", 19800000000, "UTC+05:30"]]),
    _V("datetime", [2026, 11, 10, 12, 0, 0, 0, 0, ["fixed", -10800000000, "ART"]]),
    _V("datetime", [2026, 11, 1, 6, 30, 0, 0, 0, ["instant", "America/New_York"]]),
    _V("timedelta", [1, 2, 3]), _V("uuid", "12345678123456781234567812345678"),
    _V("tuple", [_I(1), _S("a")]), _V("list", [_I(1)]), _V("map", [[_S("k"), _I(1)]]), _V("mutable", "counter"),
    _V("missing", True),
]
#: Atom oids whose hash (rank 14) is published.
ATOM_OIDS = ["00000000000000000000000000000001", "12345678123456781234567812345678", "ff" * 16]


def gen_value_model(manifest):
    from proto_db.values import canonical, exact_token, hash_key, order_key
    values = [typed.build(d) for d in ORDERED]
    keys = [order_key(v) for v in values]
    assert all(a < b for a, b in zip(keys, keys[1:])), "ORDERED is not strictly ascending"
    manifest["order"] = {"ascending": ORDERED,
                         "equal": [[_I(1), _V("float", float.hex(1.0)), _V("decimal", "1.000")],
                                   [_V("float", float.hex(0.0)), _V("float", float.hex(-0.0)), _V("decimal", "-0")],
                                   [_V("datetime", [2026, 1, 1, 1, 0, 0, 0, 0, ["fixed", 3600000000, "UTC+01:00"]]),
                                    _V("datetime", [2026, 1, 1, 0, 0, 0, 0, 0, ["fixed", 0, "UTC"]]),
                                    _V("datetime", [2026, 1, 1, 0, 0, 0, 0, 0, ["instant", "Asia/Tokyo"]])],
                                   [_V("time", [12, 0, 0, 0, 0, ["zone", "America/New_York"]]),
                                    _V("time", [12, 0, 0, 0, 0, None]),
                                    _V("time", [12, 0, 0, 0, 1, ["zone", "Asia/Tokyo"]])]],
                         "unorderable": [_V("float", "nan"), _V("decimal", "NaN"), _V("list", []), _V("map", []),
                                         _V("tuple", [_V("float", "nan")])]}
    for group in manifest["order"]["equal"]:
        assert len({order_key(typed.build(d)) for d in group}) == 1
    manifest["hashes"] = [{"value": d, "hash_key": str(hash_key(typed.build(d)))} for d in HASHED]
    manifest["exact_tokens"] = [{"value": d, "token": exact_token(canonical(typed.build(d))).hex()} for d in TOKENS]
    manifest["atom_hashes"] = [{"oid": oid, "hash_key": str(hash_key(_atom_with_oid(oid)))} for oid in ATOM_OIDS]
    atom = DBObject(x=1)
    manifest["atom_tokens"] = [
        {"value": "any atom", "token": exact_token(atom).hex()},
        {"value": "the tuple (atom, 1)", "token": exact_token((atom, 1)).hex()},
        {"value": "the list [1, atom] holding a map {\"k\": atom}",
         "token": exact_token(canonical([1, {"k": atom}])).hex()},
    ]


def _atom_with_oid(hex_oid: str):
    atom = DBObject(x=1)
    object.__setattr__(atom, "oid", uuid.UUID(hex=hex_oid))
    return atom


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
    ivf = List().add_index(VectorIndex("ivf", "emb", dim=2, metric="l2", backend="ivf_flat", nlist=2, nprobe=1,
                                       train_iters=3, seed=1))
    for emb in ([1.0, 0.0], [0.0, 1.0], [0.9, 0.1], [0.1, 0.8], [5, None]):
        ivf = ivf.append_last(DBObject(emb=emb))
    tx.set_root_object("ivf", ivf)
    tx.set_root_object("hnsw", List().add_index(VectorIndex("hnsw", "emb", dim=2, metric="cosine", backend="hnsw"))
                       .append_last(DBObject(emb=(1, 2))).append_last(DBObject(emb=(2, 1))))
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
            try:
                again = _pack({"t": p["t"], "oid": p["oid"], "s": codec.encode_state(state, saved_pointer)})
            except ProtoValidationError:          # a format-3 map key form: read, never written again
                again = None
            entry.update({"tag": p["t"], "type": type_name_of(p["t"]), "oid": p["oid"].hex(),
                          "state": [[k, typed.describe(v)] for k, v in state.items()],
                          "canonical": again == payload, "rewritable": again is not None})
        out.append(entry)
        pos += size
    return out


def gen_database(manifest):
    with _Deterministic(0x1000):
        manifest["database"] = _write_space("database", [_fill_first, _fill_second])


def _write_space(directory: str, fills: list) -> dict:
    """A file-storage space under ``vectors/<directory>`` holding the database ``db`` written by
    ``fills`` (one transaction each), and its manifest entry."""
    target = VECTORS / directory
    shutil.rmtree(target, ignore_errors=True)
    space = ObjectSpace(FileStorage(str(target)))
    db = space.new_database("db")
    for fill in fills:
        db.run(fill)
    space.close()
    for name in os.listdir(target):
        if not (name == "space.root" or name.endswith(".seg")):
            os.remove(target / name)                 # the lock file: no data
    segments = sorted(n for n in os.listdir(target) if n.endswith(".seg"))
    assert len(segments) == 1, segments
    root = fmt.decode_root((target / "space.root").read_bytes())
    return {
        "dir": directory,
        "segment": segments[0],
        "format": int.from_bytes((target / segments[0]).read_bytes()[8:10], "little"),
        "root": {"version": int(msgpack.unpackb((target / "space.root").read_bytes()[4:])["version"]),
                 "segment": root.segment.hex, "offset": root.offset, "epoch": root.epoch},
        "records": _records(target / segments[0]),
        "contents": _contents(target),
    }


# -- a database as format 3 wrote it ------------------------------------------------------------

_FORMAT3_TAGS = {type(None): "N", bool: "b", int: "i", float: "f", Decimal: "D", str: "s", bytes: "y",
                 datetime.date: "d", datetime.time: "t", datetime.datetime: "dt", datetime.timedelta: "td",
                 uuid.UUID: "u", tuple: "T", list: "L", FrozenList: "FL", dict: "M", FrozenDict: "FM"}


def _format3_tz(value):
    tz = value.tzinfo
    if tz is None:
        return None
    offset = value.utcoffset()
    return (None if offset is None else offset // datetime.timedelta(microseconds=1), repr(tz))


def _format3_token(value) -> tuple:
    """The exact token format 3 wrote (D30, replaced by D58): this is the code of 313d085's
    ``exact_token``, kept here so the legacy vector holds the tokens format 3 really wrote."""
    if is_atom(value):
        return ("a",)
    cls = type(value)
    tag = _FORMAT3_TAGS.get(cls) or f"{cls.__module__}.{cls.__qualname__}"
    if value is None:
        return (tag,)
    if isinstance(value, (bool, int)):
        return (tag, int(value))
    if isinstance(value, float):
        return (tag, float.hex(value))
    if isinstance(value, Decimal):
        return (tag, str(value))
    if isinstance(value, (str, bytes)):
        return (tag, value)
    if isinstance(value, datetime.datetime):
        return (tag, value.replace(tzinfo=None).isoformat(), value.fold, _format3_tz(value))
    if isinstance(value, datetime.date):
        return (tag, value.toordinal())
    if isinstance(value, datetime.time):
        return (tag, value.replace(tzinfo=None).isoformat(), value.fold, _format3_tz(value))
    if isinstance(value, datetime.timedelta):
        return (tag, value.days, value.seconds, value.microseconds)
    if isinstance(value, uuid.UUID):
        return (tag, value.int)
    if isinstance(value, (tuple, list)):
        return (tag, tuple(_format3_token(v) for v in value))
    if isinstance(value, dict):
        return (tag, tuple((_format3_token(k), _format3_token(v)) for k, v in value.items()))
    return (tag, repr(value))


def _format3_members(members):
    old = None
    for (eq, _token), entry in avl.key_iter(members):
        old = avl.key_put(old, (eq, _format3_token(entry[0])), entry)[0]
    return old


def _format3_index(index):
    """``index`` as format 3 before D56 wrote it: plain KeyNode trees, format-3 member tokens,
    no ``member_tokens`` field.  A key-group index or a vector index."""
    if isinstance(index, VectorIndex):
        store = None
        for (list_no, (eq, _token)), entry in avl.key_iter(index.store):
            store = avl.key_put(store, (list_no, (eq, _format3_token(entry[0]))), entry)[0]
        members, total = index.invalid
        legacy = index._with(store=store, invalid=(_format3_members(members), total))
    else:
        groups = None
        for key, (members, total) in avl.key_iter(index.groups):
            groups = avl.key_put(groups, key, (_format3_members(members), total))[0]
        side_members, side_total = index.side
        legacy = index._with(groups=groups, side=(_format3_members(side_members), side_total))
    object.__delattr__(legacy, "member_tokens")
    return legacy


def _with_format3_index(collection, name):
    return collection._with(indexes=collection.indexes.without(name).with_index(
        _format3_index(collection.get_index(name))))


#: The elements of the legacy list: every kind of member token format 3 wrote, a zone datetime
#: (written in the wall-clock form) among them.
LEGACY_ELEMENTS = [1, 2, 2, "x", 1.5, -0.0, Decimal("2.50"), (1, "a"), b"\x00",
                   datetime.datetime(2026, 11, 1, 1, 30, fold=1, tzinfo=ZoneInfo("America/New_York")),
                   datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone(datetime.timedelta(hours=-3))),
                   datetime.date(2026, 1, 1), None, [1, 2]]


class LegacyColor(str, enum.Enum):
    """A ``(str, Enum)``: format 3 wrote its members through ``str()`` (``"LegacyColor.RED"``) but
    filed them by their value (``"red"``), misfiling them (D58, re-review 2 I-A)."""
    RED = "red"


#: Keys of the misfiled structures; LegacyColor.RED is stored among them as "LegacyColor.RED".
MISFILED_KEYS = ["apple", "dog", "pear", "zebra"]
#: Roots of the legacy space that format 3 misfiled, and every key or element each must hold.
MISFILED_ROOTS = ["misfiled_counted_set", "misfiled_dictionary", "misfiled_hash_dictionary", "misfiled_list",
                  "misfiled_repeated_keys", "misfiled_set"]


def _fill_misfiled(tx):
    keys = MISFILED_KEYS
    red = LegacyColor.RED
    tx.set_root_object("misfiled_dictionary", Dictionary.from_items([(k, 1) for k in keys]).set_at(red, 2))
    tx.set_root_object("misfiled_hash_dictionary", HashDictionary.from_items([(k, 1) for k in keys]).set_at(red, 2))
    tx.set_root_object("misfiled_set", Set.from_iterable(keys).add(red))
    tx.set_root_object("misfiled_counted_set", CountedSet.from_iterable(keys).add(red).add(red))
    tx.set_root_object("misfiled_repeated_keys",
                       RepeatedKeysDictionary.from_items([(k, 1) for k in keys]).add(red, 2))
    tx.set_root_object("misfiled_list", List.from_iterable(keys).add_index(
        EqualityIndex("e", KeySpec.element())).append_last(red))


def _fill_legacy(tx):
    numbers = List.from_iterable(LEGACY_ELEMENTS).add_index(EqualityIndex("e", KeySpec.element()))
    tx.set_root_object("numbers", _with_format3_index(numbers, "e"))
    vectors = List().add_index(VectorIndex("near", "emb", dim=2, metric="l2"))
    for emb in ((1.0, 0.0), (0.0, 1.0), (1, 2, 3)):                     # the last one is invalid
        vectors = vectors.append_last(DBObject(emb=emb))
    tx.set_root_object("vectors", _with_format3_index(vectors, "near"))
    tx.set_root_object("invoice", Invoice(
        number=Decimal("7"), issued=datetime.datetime(2026, 11, 1, 1, 30, fold=1, tzinfo=ZoneInfo("America/New_York"))))
    tx.set_root_object("invoices", List().append_last(Invoice(number=Decimal("8"))))
    zone = datetime.datetime(2026, 3, 8, 12, tzinfo=ZoneInfo("America/New_York"))
    tx.set_root_object("maps", DBObject(
        zone_keys={zone: "a", (zone, 1): "b"},
        nan_keys={float("nan"): 1, float("nan"): 2},
        decimal_nan_keys={Decimal("NaN"): 1, Decimal("NaN"): 2}))


@contextlib.contextmanager
def _format3_subclass_encoding():
    """Built-in subclasses handled as format 3 did: kept as they are by canonical() (so hashed
    and ordered by their base value) and written through their own ``str()``."""
    import proto_db.values as values_module
    real = values_module.plain_scalar

    def keep(value):
        return value if isinstance(value, (str, int, float, Decimal, bytes)) else real(value)

    def written_through_str(value):
        return str(value) if isinstance(value, str) else real(value)
    with mock.patch.object(values_module, "plain_scalar", keep), \
            mock.patch.object(codec, "plain_scalar", written_through_str):
        yield


def gen_legacy_database(manifest):
    """A format-3 space: format-3 segment and root, a DatabaseRoot without ``origin``, a
    key-group and a vector index without ``member_tokens`` (plain KeyNode trees, format-3
    tokens of every kind), zone datetimes in the wall-clock form, maps with the key forms format
    3 allowed (zone datetimes, NaNs twice), and an application record and reference spelled by
    an alias."""
    from proto_db.core import atom as atom_module
    from proto_db.storage import file as file_module
    import proto_db.transactions as tx_module
    from proto_db.transactions import DatabaseRoot
    real_tag = atom_module.type_tag

    def alias_tag(name):
        return real_tag("conformance.LegacyInvoice" if name == Invoice.__type_name__ else name)

    def wall_clock(value):
        return [value.year, value.month, value.day, value.hour, value.minute, value.second, value.microsecond,
                value.fold, ["zone", value.tzinfo.key]]
    real_init = DatabaseRoot.__init__

    def no_origin(self, *args, origin=None, **kwargs):
        real_init(self, *args, origin=None, **kwargs)
    import proto_db.values as values_module

    def any_keys(keys, zone_forms=None):
        """Format 3 had no map-key rule: Python's dict decided."""
    patches = [mock.patch.object(atom_module, "type_tag", alias_tag), mock.patch.object(codec, "type_tag", alias_tag),
               mock.patch.object(codec, "_instant_fields", wall_clock),
               mock.patch.object(codec, "check_map_keys", any_keys),
               mock.patch.object(values_module, "check_map_keys", any_keys),
               mock.patch.object(DatabaseRoot, "__init__", no_origin)]
    patches += [mock.patch.object(m, "FORMAT_VERSION", 3) for m in (fmt, file_module, tx_module)]
    with _Deterministic(0x3000):
        for p in patches:
            p.start()
        try:
            with _format3_subclass_encoding():
                entry = _write_space("legacy", [_fill_legacy, _fill_misfiled])
        finally:
            for p in reversed(patches):
                p.stop()
        # Read by this revision, unpatched: the records' decoded states and the contents.
        entry["records"] = _records(VECTORS / "legacy" / entry["segment"])
        entry["contents"] = _contents(VECTORS / "legacy")
    entry["misfiled"] = {"roots": MISFILED_ROOTS, "keys": MISFILED_KEYS + ["LegacyColor.RED"],
                         "note": "format 3 wrote LegacyColor.RED (a (str, Enum)) as 'LegacyColor.RED' but "
                                 "filed it as 'red': validate() raises ProtoMisfiledError, rebuilt() repairs"}
    manifest["legacy_database"] = entry


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
    out["indexes"] = {}
    for name in sorted(tx.root_names()):
        root = tx.get_root_object(name)
        if isinstance(root, IndexedCollection) and root.index_names():
            out["indexes"][name] = {i: {"type": type(root.get_index(i)).__type_name__,
                                        "member_tokens": root.get_index(i).has_current_tokens()}
                                    for i in root.index_names()}
    out["origin"] = typed.describe(tx._db_root.origin)
    if "ivf" in tx.root_names():
        index = tx.get_root_object("ivf").get_index("ivf")
        out["ivf"] = {"centroids": [[typed.describe(x) for x in c] for c in index.centroids],
                      "trained_size": index.trained_size,
                      "lists": [[key[0], [typed.describe(x) for x in vector]]
                                for key, (_, _, vector) in avl.key_iter(index.store)]}
    return out


# -- framing -------------------------------------------------------------------------------

def gen_framing(manifest):
    atom = _pack({"t": 19, "oid": uuid.UUID("00112233445566778899aabbccddeeff").bytes, "s": {"a": 1}})
    blob = b"opaque bytes"
    root = fmt.RootRecord(fmt.FORMAT_VERSION, PTR_SEGMENT, 26, 7)
    root_v2 = msgpack.packb({"version": 2, "segment": PTR_SEGMENT.bytes, "offset": 26, "epoch": 7},
                            use_bin_type=True)
    root_v3 = msgpack.packb({"version": 3, "segment": PTR_SEGMENT.bytes, "offset": 26, "epoch": 7},
                            use_bin_type=True)
    import zlib
    manifest["framing"] = {
        "segment_header": {"file": _write("framing/segment-header.bin", fmt.encode_segment_header(PTR_SEGMENT)),
                           "segment": PTR_SEGMENT.hex, "version": fmt.FORMAT_VERSION},
        "segment_header_v3": {"file": _write("framing/segment-header-v3.bin", fmt.SEGMENT_MAGIC
                                             + (3).to_bytes(2, "little") + PTR_SEGMENT.bytes),
                              "segment": PTR_SEGMENT.hex, "version": 3, "note": "format 3: still read"},
        "segment_header_v2": {"file": _write("framing/segment-header-v2.bin", fmt.SEGMENT_MAGIC
                                             + (2).to_bytes(2, "little") + PTR_SEGMENT.bytes),
                              "segment": PTR_SEGMENT.hex, "version": 2, "note": "format 2: still read"},
        "atom_record": {"file": _write("framing/atom.record", fmt.encode_record(fmt.RecordKind.ATOM, atom)),
                        "kind": 1, "payload": typed.describe_raw(msgpack.unpackb(atom, raw=False))},
        "blob_record": {"file": _write("framing/blob.record", fmt.encode_record(fmt.RecordKind.BLOB, blob)),
                        "kind": 2, "payload_hex": blob.hex()},
        "root": {"file": _write("framing/space.root", fmt.encode_root(root)),
                 "fields": {"version": fmt.FORMAT_VERSION, "segment": PTR_SEGMENT.hex, "offset": 26, "epoch": 7}},
        "root_v2": {"file": _write("framing/space-v2.root", zlib.crc32(root_v2).to_bytes(4, "little") + root_v2),
                    "fields": {"version": 2, "segment": PTR_SEGMENT.hex, "offset": 26, "epoch": 7},
                    "note": "a format-2 root record is read as a current one"},
        "root_v3": {"file": _write("framing/space-v3.root", zlib.crc32(root_v3).to_bytes(4, "little") + root_v3),
                    "fields": {"version": 3, "segment": PTR_SEGMENT.hex, "offset": 26, "epoch": 7},
                    "note": "a format-3 root record is read as a current one"},
        "cloud_root": {"file": _write("framing/cloud-space.root", cloud._encode_root_object(
                           root, [(PTR_SEGMENT, 10, 6), (PTR_SEGMENT, 0, 5)])),
                       "fields": {"version": fmt.FORMAT_VERSION, "segment": PTR_SEGMENT.hex, "offset": 26, "epoch": 7},
                       "recent": [[PTR_SEGMENT.hex, 10, 6], [PTR_SEGMENT.hex, 0, 5]]},
        "tiered_root": {"file": _write("framing/tiered-space.root", tiered._encode_root_object(root, 9)),
                        "fields": {"version": fmt.FORMAT_VERSION, "segment": PTR_SEGMENT.hex, "offset": 26, "epoch": 7},
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
    def root_desc(r):
        return None if r is None else {"segment": r.segment.hex, "offset": r.offset, "epoch": r.epoch}

    def items(recs):
        return [[raw.hex(), offset, len(data)] for raw, offset, data in recs]

    def ranges_desc(rs):
        return [[sg.hex, a, b] for sg, a, b in rs]
    entries = [
        ("rejoined", rootlog._payload(4, 0, None), {"kind": 4, "label": 0, "root": None}),
        ("promise", rootlog._payload(3, 7, None), {"kind": 3, "label": 7, "root": None}),
        ("accepted-records", rootlog.encode_records_entry(7, root, 1, (0, base), records),
         {"kind": 5, "label": 7, "root": root_desc(root), "seq": 1, "pred": [0, root_desc(base)],
          "records": items(records)}),
        ("committed", rootlog._payload(2, 7, root), {"kind": 2, "label": 7, "root": root_desc(root)}),
        ("accepted", rootlog._payload(1, 7, root), {"kind": 1, "label": 7, "root": root_desc(root)}),
        ("replica-accepted", rootlog.encode_replica_entry(7, root, 2, (7, root), [], ranges, accepted=True),
         {"kind": 6, "flags": 1, "label": 7, "root": root_desc(root), "seq": 2, "pred": [7, root_desc(root)],
          "ranges": ranges_desc(ranges), "records": []}),
        ("replica-stored", rootlog.encode_replica_entry(7, root, 3, (7, root), records, ranges, accepted=False),
         {"kind": 6, "flags": 0, "label": 7, "root": root_desc(root), "seq": 3, "pred": [7, root_desc(root)],
          "ranges": ranges_desc(ranges), "records": items(records)}),
    ]
    log = logfile.encode_header(rootlog.LOG_KIND) + b"".join(logfile.encode_frame(p) for _, p, _ in entries)
    last = logfile.encode_frame(entries[-1][1])
    torn = log[:-(len(last) // 2)]
    ranges_payload = tiered.TieredStorage._ranges_payload((root.segment, root.offset), (7, root.epoch), ranges)
    ranges_log = logfile.encode_header(tiered.RANGES_KIND) + logfile.encode_frame(ranges_payload)
    segs_log = logfile.encode_header(tiered.MANIFEST_KIND) + logfile.encode_frame(seg_uuid.bytes)

    manifest["cluster"] = {
        "segment": {"file": seg_file, "records": _records(VECTORS / seg_file)},
        "roots_log": {
            "file": _write("cluster/roots.log", log), "kind": "ROOT",
            "entries": [{"name": n, "size": len(p), "fields": f} for n, p, f in entries],
            "base": root_desc(base), "root": root_desc(root), "label": 7,
            "loaded": {"accepted": [7, root_desc(root)], "committed": [7, root_desc(root)], "promised": 7,
                       "top": 7, "empty": False},
        },
        "roots_log_torn": {
            "file": _write("cluster/roots-torn.log", torn), "kind": "ROOT", "valid_end": len(log) - len(last),
            "entries": len(entries) - 1,
            "note": "the last frame cut in its middle: a torn tail, dropped; every earlier entry is read "
                    "(compare invalid/log-mid-corruption: damaged bytes followed by a valid frame)"},
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
    gen_legacy_database(manifest)
    gen_cluster(manifest)
    MANIFEST.write_text(json.dumps(manifest, indent=1, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
