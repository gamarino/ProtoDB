"""The conformance vectors decode to their manifest and re-encode byte for byte (R83, R83a, D57).

Catches: any change of the storage, log or cluster wire format that the specification
(``docs/source/format-spec.rst``) and the committed vectors do not follow -- an encoder writing
other bytes, a decoder reading another meaning, a legacy form no longer read, an invalid input
no longer refused.  The vectors are the contract of other implementations (protoBaseST); this
test checks the committed files and never regenerates them (``generate.py`` does, on purpose).
"""
import json
import shutil
import uuid
from pathlib import Path

import msgpack
import pytest

from proto_db.cluster import protocol, rootlog
from proto_db.core import codec
from proto_db.core.atom import BUILTIN_TYPE_ALIASES, type_name_of
from proto_db.errors import ProtoCorruptionError
from proto_db.storage import cloud, format as fmt, logfile, tiered
from proto_db.storage.file import FileStorage
from proto_db.transactions import ObjectSpace
from tests.conformance import typed
from tests.conformance.generate import describe_database, saved_pointer, stub

HERE = Path(__file__).resolve().parent
MANIFEST = json.loads((HERE / "manifest.json").read_text())


def _bytes(rel: str) -> bytes:
    return (HERE / "vectors" / rel).read_bytes()


def _pack(data) -> bytes:
    return msgpack.packb(data, use_bin_type=True)


def _decode(data: bytes):
    return codec.decode_value(msgpack.unpackb(data, raw=False), stub)


def _reencode(value) -> bytes:
    return _pack(codec.encode_value(value, saved_pointer))


def test_versions_and_builtin_type_aliases():
    assert MANIFEST["versions"] == {"format": fmt.FORMAT_VERSION,
                                    "readable_formats": sorted(fmt.READABLE_FORMAT_VERSIONS),
                                    "log_format": logfile.LOG_FORMAT_VERSION,
                                    "protocol": protocol.PROTOCOL_VERSION}
    assert tuple(MANIFEST["builtin_type_aliases"]) == BUILTIN_TYPE_ALIASES


# -- canonical values ----------------------------------------------------------------------

@pytest.mark.parametrize("vector", MANIFEST["values"], ids=lambda v: v["id"])
def test_value_vector(vector):
    data = _bytes(vector["file"])
    value = _decode(data)
    assert typed.describe(value) == vector["value"]
    assert _reencode(value) == data
    assert _reencode(typed.build(vector["value"], stub)) == data      # the description fixes the bytes


@pytest.mark.parametrize("vector", MANIFEST["values_decode_only"], ids=lambda v: v["id"])
def test_legacy_value_vector_decodes_and_reencodes_canonically(vector):
    value = _decode(_bytes(vector["file"]))
    assert typed.describe(value) == vector["value"]
    assert _reencode(value) == _bytes(vector["canonical"])


@pytest.mark.parametrize("vector", MANIFEST["invalid"], ids=lambda v: v["id"])
def test_invalid_vector_is_refused(vector):
    data = _bytes(vector["file"])
    with pytest.raises(ProtoCorruptionError):
        if vector["layer"] == "value":
            _decode(data)
        elif vector["layer"] == "record":
            fmt.read_record(data, 0)
        else:
            fmt.decode_root(data)


# -- the value model ---------------------------------------------------------------------------

def test_order_vectors():
    from proto_db.values import order_key, is_orderable
    order = MANIFEST["order"]
    keys = [order_key(typed.build(d)) for d in order["ascending"]]
    assert all(a < b for a, b in zip(keys, keys[1:]))
    for group in order["equal"]:
        assert len({order_key(typed.build(d)) for d in group}) == 1
    for desc in order["unorderable"]:
        assert not is_orderable(typed.build(desc))


@pytest.mark.parametrize("vector", MANIFEST["hashes"], ids=lambda v: json.dumps(v["value"])[:40])
def test_hash_vector(vector):
    from proto_db.values import hash_key
    assert hash_key(typed.build(vector["value"])) == int(vector["hash_key"])


@pytest.mark.parametrize("vector", MANIFEST["exact_tokens"], ids=lambda v: json.dumps(v["value"])[:40])
def test_exact_token_vector(vector):
    from proto_db.values import canonical, exact_token
    assert typed.describe(exact_token(canonical(typed.build(vector["value"])))) == vector["token"]


# -- framing -------------------------------------------------------------------------------

def test_segment_headers():
    framing = MANIFEST["framing"]
    for name in ("segment_header", "segment_header_v2"):
        vector = framing[name]
        data = _bytes(vector["file"])
        assert len(data) == fmt.SEGMENT_HEADER_SIZE == 26
        assert fmt.decode_segment_header(data) == uuid.UUID(hex=vector["segment"])
        assert int.from_bytes(data[8:10], "little") == vector["version"]
    current = framing["segment_header"]
    assert fmt.encode_segment_header(uuid.UUID(hex=current["segment"])) == _bytes(current["file"])


def test_atom_and_blob_records():
    atom, blob = MANIFEST["framing"]["atom_record"], MANIFEST["framing"]["blob_record"]
    data = _bytes(atom["file"])
    kind, payload = fmt.read_record(data, 0)
    assert kind == atom["kind"] and fmt.record_size(data) == len(data)
    assert typed.describe_raw(msgpack.unpackb(payload, raw=False)) == atom["payload"]
    assert fmt.encode_record(kind, payload) == data
    data = _bytes(blob["file"])
    kind, payload = fmt.read_record(data, 0)
    assert kind == blob["kind"] and payload.hex() == blob["payload_hex"]
    assert fmt.encode_record(kind, payload) == data


def _root_fields(record) -> dict:
    return {"version": record.version, "segment": record.segment.hex, "offset": record.offset,
            "epoch": record.epoch}


def test_root_records():
    framing = MANIFEST["framing"]
    vector = framing["root"]
    record = fmt.decode_root(_bytes(vector["file"]))
    assert _root_fields(record) == vector["fields"]
    assert fmt.encode_root(record) == _bytes(vector["file"])
    legacy = framing["root_v2"]
    record = fmt.decode_root(_bytes(legacy["file"]))
    assert _root_fields(record) == dict(legacy["fields"], version=fmt.FORMAT_VERSION)   # read as current

    vector = framing["cloud_root"]
    record, recent = cloud._decode_root_object(_bytes(vector["file"]))
    assert _root_fields(record) == vector["fields"]
    assert [[s.hex, o, e] for s, o, e in recent] == vector["recent"]
    assert cloud._encode_root_object(record, recent) == _bytes(vector["file"])

    vector = framing["tiered_root"]
    label, record = tiered._decode_root_object(_bytes(vector["file"]))
    assert _root_fields(record) == vector["fields"] and label == vector["term"]
    assert tiered._encode_root_object(record, label) == _bytes(vector["file"])


# -- segments: every record of the database and the cluster commit ---------------------------

def _check_segment(rel: str, records: list) -> None:
    data = _bytes(rel)
    segment = fmt.decode_segment_header(data)
    assert fmt.encode_segment_header(segment) == data[:fmt.SEGMENT_HEADER_SIZE]
    assert Path(rel).name == segment.hex + ".seg"
    pos = fmt.SEGMENT_HEADER_SIZE
    for entry in records:
        assert entry["offset"] == pos
        kind, payload = fmt.read_record(data, pos)
        size = fmt.record_size(data[pos:pos + fmt.RECORD_HEADER_SIZE])
        assert (int(kind), size) == (entry["kind"], entry["size"])
        envelope = msgpack.unpackb(payload, raw=False)
        assert list(envelope) == ["t", "oid", "s"]
        assert envelope["t"] == entry["tag"] and type_name_of(entry["tag"]) == entry["type"]
        assert envelope["oid"].hex() == entry["oid"]
        state = codec.decode_state(envelope["s"], stub)
        assert [[k, typed.describe(v)] for k, v in state.items()] == entry["state"]
        again = _pack({"t": envelope["t"], "oid": envelope["oid"], "s": codec.encode_state(state, saved_pointer)})
        assert again == payload
        assert fmt.encode_record(kind, again) == data[pos:pos + size]
        pos += size
    assert pos == len(data), "the manifest lists every record of the segment"


def test_database_segment():
    db = MANIFEST["database"]
    _check_segment(f"{db['dir']}/{db['segment']}", db["records"])


def test_every_persistable_builtin_type_appears():
    seen = {r["type"] for r in MANIFEST["database"]["records"] + MANIFEST["cluster"]["segment"]["records"]}
    abstract = {"proto_db.collections.IndexedCollection", "proto_db.collections.sets._HashBuckets",
                "proto_db.indexing.Index", "proto_db.indexing.keyed.KeyGroupIndex", "proto_db.avl._Node"}
    assert set(BUILTIN_TYPE_ALIASES) - abstract <= seen
    assert "conformance.Invoice" in seen


def test_database_root():
    db = MANIFEST["database"]
    data = _bytes(f"{db['dir']}/space.root")
    record = fmt.decode_root(data)
    assert _root_fields(record) == db["root"]
    assert fmt.encode_root(record) == data


def test_database_contents_through_the_public_api(tmp_path):
    db = MANIFEST["database"]
    work = tmp_path / "db"
    shutil.copytree(HERE / "vectors" / db["dir"], work)
    space = ObjectSpace(FileStorage(str(work)))
    try:
        assert space.open_database("db").run(describe_database) == db["contents"]
    finally:
        space.close()


# -- the cluster: segment, logs and wire messages --------------------------------------------

def test_cluster_segment():
    _check_segment(MANIFEST["cluster"]["segment"]["file"], MANIFEST["cluster"]["segment"]["records"])


def _log(vector: dict, kind: bytes) -> list:
    data = _bytes(vector["file"])
    assert data[:logfile.HEADER_SIZE] == logfile.encode_header(kind)
    result = logfile.scan(data, kind, vector["file"])
    assert not result.torn and result.valid_end == len(data)
    assert logfile.log_contents(kind, result.payloads) == data
    return result.payloads


def _root(desc):
    return None if desc is None else fmt.RootRecord(fmt.FORMAT_VERSION, uuid.UUID(hex=desc["segment"]),
                                                    desc["offset"], desc["epoch"])


def test_roots_log():
    cluster = MANIFEST["cluster"]
    vector = cluster["roots_log"]
    payloads = _log(vector, rootlog.LOG_KIND)
    assert [len(p) for p in payloads] == [e["size"] for e in vector["entries"]]
    loaded = rootlog._decode(payloads, vector["file"])
    expected = vector["loaded"]
    assert loaded.accepted == (expected["accepted"][0], _root(expected["accepted"][1]))
    assert loaded.committed == (expected["committed"][0], _root(expected["committed"][1]))
    assert (loaded.promised, loaded.top, loaded.empty) == (expected["promised"], expected["top"], expected["empty"])
    segment = _bytes(cluster["segment"]["file"])
    seg_id = uuid.UUID(hex=Path(cluster["segment"]["file"]).stem)
    framed = [[seg_id.bytes, r["offset"], segment[r["offset"]:r["offset"] + r["size"]]]
              for r in cluster["segment"]["records"]]
    redo = [rootlog.decode_records_entry(p) for p in payloads if p[0] in rootlog.DATA_KINDS]
    assert [(e.label, e.root, e.accepted, e.records == framed) for e in redo] == [
        (7, _root(vector["root"]), True, True), (7, _root(vector["root"]), True, False),
        (7, _root(vector["root"]), False, True)]
    assert redo[0].pred == (0, _root(vector["base"])) and redo[0].ranges is None
    ranges = protocol.record_ranges(framed)
    assert redo[1].ranges == ranges and redo[2].ranges == ranges


def test_ranges_and_segments_logs():
    cluster = MANIFEST["cluster"]
    (payload,) = _log(cluster["ranges_log"], tiered.RANGES_KIND)
    entry = cluster["ranges_log"]["entries"][0]
    root = _root(entry["root"])
    ranges = [(uuid.UUID(hex=s), a, b) for s, a, b in entry["ranges"]]
    assert tiered.TieredStorage._ranges_payload((root.segment, root.offset), (entry["label"], root.epoch),
                                                ranges) == payload
    payloads = _log(cluster["segments_log"], tiered.MANIFEST_KIND)
    assert [p.hex() for p in payloads] == cluster["segments_log"]["entries"]


@pytest.mark.parametrize("vector", MANIFEST["cluster"]["messages"], ids=lambda v: v["id"])
def test_cluster_message(vector):
    frame = _bytes(vector["file"])
    assert int.from_bytes(frame[:4], "big") == len(frame) - 4
    (msg,) = protocol.FrameDecoder().feed(frame)
    assert msg["type"] == vector["type"] and msg["v"] == protocol.PROTOCOL_VERSION
    assert typed.describe_raw(msg) == vector["message"]
    assert protocol.encode_frame(msg) == frame


def test_every_message_type_has_a_vector():
    assert {v["type"] for v in MANIFEST["cluster"]["messages"]} == set(protocol.MESSAGE_TYPES)
