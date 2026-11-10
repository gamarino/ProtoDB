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
from proto_db.cluster.protocol import ClusterProtocolError
from proto_db.errors import ProtoCorruptionError, ProtoNotSupportedError, ProtoValidationError
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


@pytest.mark.parametrize("vector", MANIFEST["values_read_not_rewritten"], ids=lambda v: v["id"])
def test_format_3_map_keys_are_read_but_not_written_again(vector):
    value = _decode(_bytes(vector["file"]))
    assert typed.describe(value) == vector["value"]
    with pytest.raises(ProtoValidationError, match="map key"):
        _reencode(value)


_ERRORS = {"corruption": ProtoCorruptionError, "not_supported": ProtoNotSupportedError,
           "protocol": ClusterProtocolError}


def _read_layer(vector, data):
    layer = vector["layer"]
    if layer == "value":
        _decode(data)
    elif layer == "record":
        fmt.read_record(data, 0)
    elif layer == "root":
        fmt.decode_root(data)
    elif layer == "segment":
        fmt.decode_segment_header(data)
    elif layer == "atom":
        from proto_db.storage.memory import MemoryStorage
        from tests.support.persistence import DirectContext
        storage = MemoryStorage()
        ptr = storage.push_atom(msgpack.unpackb(data, raw=False, strict_map_key=False))
        stub = DirectContext(storage).load(ptr, vector["type"])
        stub._load()
    elif layer == "log":
        logfile.scan(data, vector["kind"].encode("ascii"), vector["file"])
    elif layer == "roots_log":
        rootlog._decode(logfile.scan(data, rootlog.LOG_KIND, vector["file"]).payloads, vector["file"])
    elif layer == "message":
        protocol.FrameDecoder().feed(data)
    else:
        raise AssertionError(f"unknown layer {layer!r}")


@pytest.mark.parametrize("vector", MANIFEST["invalid"], ids=lambda v: v["id"])
def test_invalid_vector_is_refused(vector):
    with pytest.raises(_ERRORS[vector["error"]]):
        _read_layer(vector, _bytes(vector["file"]))


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
    value = canonical(typed.build(vector["value"]))
    assert exact_token(value).hex() == vector["token"]
    # The token is the canonical encoding: what the value's own vector file would hold.
    assert exact_token(value) == _reencode(value)


def test_atom_hashes_and_tokens():
    from proto_db.core.dbobject import DBObject
    from proto_db.values import canonical, exact_token, hash_key
    for vector in MANIFEST["atom_hashes"]:
        atom = DBObject(x=1)
        object.__setattr__(atom, "oid", uuid.UUID(hex=vector["oid"]))
        assert hash_key(atom) == int(vector["hash_key"])
    atom = DBObject(x=1)
    tokens = [exact_token(atom), exact_token((atom, 1)), exact_token(canonical([1, {"k": atom}]))]
    assert [t.hex() for t in tokens] == [v["token"] for v in MANIFEST["atom_tokens"]]


# -- framing -------------------------------------------------------------------------------

def test_segment_headers():
    framing = MANIFEST["framing"]
    for name in ("segment_header", "segment_header_v3", "segment_header_v2"):
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
    for name in ("root_v3", "root_v2"):
        legacy = framing[name]
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
    header = fmt.encode_segment_header(segment)          # the version bytes [8:10] are checked by the caller
    assert header[:8] + header[10:] == data[:8] + data[10:fmt.SEGMENT_HEADER_SIZE]
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
        if entry["rewritable"]:
            again = _pack({"t": envelope["t"], "oid": envelope["oid"],
                           "s": codec.encode_state(state, saved_pointer)})
            # Records of the current format re-encode byte for byte; a legacy record (an alias, a
            # format-3 zone datetime or token) decodes to the listed state and re-encodes canonically.
            assert (again == payload) is entry["canonical"]
            if entry["canonical"]:
                assert fmt.encode_record(kind, again) == data[pos:pos + size]
        else:                                     # a format-3 map key form: read, refused at write
            with pytest.raises(ProtoValidationError, match="map key"):
                codec.encode_state(state, saved_pointer)
        pos += size
    assert pos == len(data), "the manifest lists every record of the segment"


@pytest.mark.parametrize("name", ["database", "legacy_database", "polyglot_database"])
def test_database_segment(name):
    db = MANIFEST[name]
    _check_segment(f"{db['dir']}/{db['segment']}", db["records"])
    assert int.from_bytes(_bytes(f"{db['dir']}/{db['segment']}")[8:10], "little") == db["format"]
    if name != "legacy_database":
        assert all(r["canonical"] for r in db["records"])
    else:
        assert not all(r["rewritable"] for r in db["records"])      # the format-3 maps are there


def test_every_persistable_builtin_type_appears():
    seen = {r["type"] for r in MANIFEST["database"]["records"] + MANIFEST["cluster"]["segment"]["records"]}
    abstract = {"proto_db.collections.IndexedCollection", "proto_db.collections.sets._HashBuckets",
                "proto_db.indexing.Index", "proto_db.indexing.keyed.KeyGroupIndex", "proto_db.avl._Node"}
    assert set(BUILTIN_TYPE_ALIASES) - abstract <= seen
    assert "conformance.Invoice" in seen


@pytest.mark.parametrize("name", ["database", "legacy_database", "polyglot_database"])
def test_database_root(name):
    db = MANIFEST[name]
    data = _bytes(f"{db['dir']}/space.root")
    record = fmt.decode_root(data)
    assert _root_fields(record) == dict(db["root"], version=fmt.FORMAT_VERSION)
    assert msgpack.unpackb(data[4:])["version"] == db["root"]["version"]
    if name != "legacy_database":
        assert fmt.encode_root(record) == data


@pytest.mark.parametrize("name", ["database", "legacy_database", "polyglot_database"])
def test_database_contents_through_the_public_api(tmp_path, name):
    db = MANIFEST[name]
    work = tmp_path / "db"
    shutil.copytree(HERE / "vectors" / db["dir"], work)
    space = ObjectSpace(FileStorage(str(work)))
    try:
        assert space.open_database("db").run(describe_database) == db["contents"]
    finally:
        space.close()


def test_an_opaque_atom_is_kept_as_written_when_what_references_it_changes(tmp_path):
    vector = MANIFEST["polyglot_database"]
    work = tmp_path / "db"
    shutil.copytree(HERE / "vectors" / vector["dir"], work)
    record = next(r for r in vector["records"] if r.get("type") == vector["opaque_type"])
    segment = _bytes(f"{vector['dir']}/{vector['segment']}")
    written = segment[record["offset"]:record["offset"] + record["size"]]
    space = ObjectSpace(FileStorage(str(work)))
    try:
        db = space.open_database("db")

        def change_holder(tx):
            holder = tx.get_root_object("holder")
            assert type(holder.order).__type_name__ == vector["opaque_type"]
            tx.set_root_object("holder", holder.with_(name="changed"))
        db.run(change_holder)

        def check(tx):
            order = tx.get_root_object("holder").order
            assert (order.atom_pointer.segment.hex, order.atom_pointer.offset) == \
                (Path(vector["segment"]).stem, record["offset"])
            assert order.number == 7 and order.customer.name == "ACME"
        db.run(check)
    finally:
        space.close()
    assert (work / vector["segment"]).read_bytes()[record["offset"]:record["offset"] + record["size"]] == written


def test_format_3_indexes_validate_before_their_first_change(tmp_path):
    work = tmp_path / "db"
    shutil.copytree(HERE / "vectors" / MANIFEST["legacy_database"]["dir"], work)
    space = ObjectSpace(FileStorage(str(work)))
    try:
        def check(tx):
            for root, name in (("numbers", "e"), ("vectors", "near")):
                collection = tx.get_root_object(root)
                index = collection.get_index(name)
                assert not index.has_current_tokens()
                index.validate(list(collection.elements()))      # stale, never corrupt
        space.open_database("db").run(check)
    finally:
        space.close()


def _holds_key(collection, key) -> bool:
    from proto_db.collections.lists import List
    from proto_db.collections.sets import CountedSet
    from proto_db.query.terms import EQ, Term
    if isinstance(collection, List):
        return list(collection.get_index("e").plan(Term(None, EQ, key)).execute()) == [key]
    if isinstance(collection, CountedSet):
        return collection.count_of(key) >= 1
    return collection.has(key)


def test_format_3_subclass_damage_is_detected_and_repaired(tmp_path):
    from proto_db.errors import ProtoMisfiledError
    misfiled = MANIFEST["legacy_database"]["misfiled"]
    work = tmp_path / "db"
    shutil.copytree(HERE / "vectors" / MANIFEST["legacy_database"]["dir"], work)
    space = ObjectSpace(FileStorage(str(work)))
    try:
        db = space.open_database("db")

        def detect_and_repair(tx):
            for root in misfiled["roots"]:
                damaged = tx.get_root_object(root)
                assert not all(_holds_key(damaged, k) for k in misfiled["keys"]), root
                with pytest.raises(ProtoMisfiledError):
                    damaged.validate()
                tx.set_root_object(root, damaged.rebuilt())
        db.run(detect_and_repair)
    finally:
        space.close()
    space = ObjectSpace(FileStorage(str(work)))
    try:
        def check(tx):
            for root in misfiled["roots"]:
                repaired = tx.get_root_object(root)
                repaired.validate()
                assert all(_holds_key(repaired, k) for k in misfiled["keys"]), root
        space.open_database("db").run(check)
    finally:
        space.close()


def _check_rebuilt(collection, name):
    index = collection.get_index(name)
    assert index.has_current_tokens()
    index.validate(list(collection.elements()))


def test_a_format_3_index_is_rebuilt_at_its_first_change(tmp_path):
    from proto_db.query.terms import EQ, Term
    work = tmp_path / "db"
    shutil.copytree(HERE / "vectors" / MANIFEST["legacy_database"]["dir"], work)
    space = ObjectSpace(FileStorage(str(work)))
    try:
        db = space.open_database("db")
        db.run(lambda tx: tx.set_root_object("numbers", tx.get_root_object("numbers").append_last(2)))

        def check(tx):
            numbers = tx.get_root_object("numbers")
            index = numbers.get_index("e")
            assert index.has_current_tokens()
            index.validate(list(numbers.elements()))
            assert list(index.plan(Term(None, EQ, 2)).execute()) == [2, 2, 2]
            vectors = tx.get_root_object("vectors")
            tx.set_root_object("vectors", vectors.remove_at(0))
        db.run(check)
        db.run(lambda tx: _check_rebuilt(tx.get_root_object("vectors"), "near"))
    finally:
        space.close()


# -- the cluster: segment, logs and wire messages --------------------------------------------

def test_cluster_segment():
    _check_segment(MANIFEST["cluster"]["segment"]["file"], MANIFEST["cluster"]["segment"]["records"])
    assert _bytes(MANIFEST["cluster"]["segment"]["file"])[8:10] == fmt.FORMAT_VERSION.to_bytes(2, "little")


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
    for entry, payload in zip(vector["entries"], payloads):
        assert _encode_entry(entry["fields"], segment_records()) == payload, entry["name"]
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


def segment_records() -> dict:
    """``{(segment hex, offset): framed record}`` of the cluster vector's segment."""
    cluster = MANIFEST["cluster"]
    segment = _bytes(cluster["segment"]["file"])
    seg_hex = Path(cluster["segment"]["file"]).stem
    return {(seg_hex, r["offset"]): segment[r["offset"]:r["offset"] + r["size"]]
            for r in cluster["segment"]["records"]}


def _encode_entry(fields: dict, framed: dict) -> bytes:
    """An acceptor-log entry rebuilt from its manifest fields with the entry encoders."""
    kind, label, root = fields["kind"], fields["label"], _root(fields["root"])
    if kind <= 4:
        return rootlog._payload(kind, label, root)
    pred = (fields["pred"][0], _root(fields["pred"][1]))
    records = []
    for seg_hex, offset, size in fields["records"]:
        data = framed[(seg_hex, offset)]
        assert len(data) == size
        records.append([bytes.fromhex(seg_hex), offset, data])
    if kind == 5:
        return rootlog.encode_records_entry(label, root, fields["seq"], pred, records)
    ranges = [(uuid.UUID(hex=s), a, b) for s, a, b in fields["ranges"]]
    return rootlog.encode_replica_entry(label, root, fields["seq"], pred, records, ranges,
                                        accepted=bool(fields["flags"] & rootlog.ACCEPTED))


def test_roots_log_torn_tail_is_dropped():
    vector = MANIFEST["cluster"]["roots_log_torn"]
    result = logfile.scan(_bytes(vector["file"]), rootlog.LOG_KIND, vector["file"])
    assert result.torn and result.valid_end == vector["valid_end"] and len(result.payloads) == vector["entries"]


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


# -- pinned vectors (format-spec.rst, "Format stability"; Task 10.P A6) ---------------------------

def test_the_committed_vectors_match_their_pinned_digests():
    from tests.conformance.generate import PINS, pin_lines
    assert pin_lines() == PINS.read_text(), "a vector changed: regenerate, and update the spec's pinned digest"


def test_the_spec_pins_the_digest_of_the_vector_manifest():
    import hashlib
    from tests.conformance.generate import PINS
    digest = hashlib.sha256(PINS.read_bytes()).hexdigest()
    spec = (HERE.parents[1] / "docs" / "source" / "format-spec.rst").read_text()
    assert f"``{digest}``" in spec, "the stability section must state the SHA-256 of vectors.sha256"
