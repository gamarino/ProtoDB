"""Record, segment header and root record format (spec section 6)."""
import io
import struct
import uuid
import zlib

import pytest

from proto_db.core.atom import AtomPointer
from proto_db.errors import ProtoCorruptionError, ProtoNotSupportedError, ProtoValidationError
from proto_db.storage import format as fmt
from proto_db.storage.format import (FORMAT_VERSION, SEGMENT_MAGIC, RecordKind, RootRecord,
                                     decode_root, decode_segment_header, encode_record,
                                     encode_root, encode_segment_header, read_record)


def test_constants():
    assert SEGMENT_MAGIC == b"PDB2SEG\0" and FORMAT_VERSION == 1
    assert RecordKind.ATOM == 1 and RecordKind.BLOB == 2


def test_segment_header_round_trip():
    seg = uuid.uuid4()
    raw = encode_segment_header(seg)
    assert raw[:8] == SEGMENT_MAGIC and len(raw) == fmt.SEGMENT_HEADER_SIZE == 26
    assert struct.unpack("<H", raw[8:10])[0] == 1 and raw[10:] == seg.bytes
    assert decode_segment_header(raw) == seg
    assert decode_segment_header(raw + b"extra") == seg


def test_segment_header_rejects_bad_input():
    raw = encode_segment_header(uuid.uuid4())
    with pytest.raises(ProtoCorruptionError):
        decode_segment_header(b"XXXXXXXX" + raw[8:])
    with pytest.raises(ProtoCorruptionError):
        decode_segment_header(raw[:-1])
    bad_version = raw[:8] + struct.pack("<H", 99) + raw[10:]
    with pytest.raises(ProtoNotSupportedError):
        decode_segment_header(bad_version)


@pytest.mark.parametrize("kind", [RecordKind.ATOM, RecordKind.BLOB])
@pytest.mark.parametrize("payload", [b"", b"x", bytes(range(256)) * 10])
def test_record_round_trip(kind, payload):
    rec = encode_record(kind, payload)
    assert len(rec) == fmt.RECORD_HEADER_SIZE + len(payload)
    length, k, crc = struct.unpack("<IBI", rec[:9])
    assert (length, k) == (len(payload), kind)
    assert crc == zlib.crc32(bytes([kind]) + payload)
    assert read_record(io.BytesIO(rec), 0) == (kind, payload)
    assert read_record(rec, 0) == (kind, payload)


def test_read_record_at_offset_among_others():
    a, b = encode_record(RecordKind.ATOM, b"first"), encode_record(RecordKind.BLOB, b"second")
    buf = b"HEADER" + a + b
    assert read_record(io.BytesIO(buf), 6) == (1, b"first")
    assert read_record(io.BytesIO(buf), 6 + len(a)) == (2, b"second")


def test_encode_record_validates():
    with pytest.raises(ProtoValidationError):
        encode_record(7, b"x")
    with pytest.raises(ProtoValidationError):
        encode_record(RecordKind.ATOM, "text")


def test_every_single_byte_corruption_detected():
    rec = encode_record(RecordKind.ATOM, b"the quick brown fox")
    for i in range(len(rec)):
        for flip in (0x01, 0x80, 0xFF):
            bad = bytearray(rec)
            bad[i] ^= flip
            with pytest.raises(ProtoCorruptionError):
                read_record(io.BytesIO(bytes(bad)), 0)


def test_corruption_detected_with_trailing_data():
    rec = encode_record(RecordKind.ATOM, b"payload")
    tail = encode_record(RecordKind.ATOM, b"next")
    for i in range(len(rec)):
        bad = bytearray(rec)
        bad[i] ^= 0x10
        with pytest.raises(ProtoCorruptionError):
            read_record(io.BytesIO(bytes(bad) + tail), 0)


def test_truncated_record_detected():
    rec = encode_record(RecordKind.BLOB, b"some payload bytes")
    for cut in range(len(rec)):
        with pytest.raises(ProtoCorruptionError):
            read_record(io.BytesIO(rec[:cut]), 0)
    with pytest.raises(ProtoCorruptionError):
        read_record(io.BytesIO(rec), len(rec))


def test_bad_kind_with_valid_crc_detected():
    payload = b"abc"
    rec = struct.pack("<IBI", 3, 9, zlib.crc32(bytes([9]) + payload)) + payload
    with pytest.raises(ProtoCorruptionError):
        read_record(io.BytesIO(rec), 0)


def test_root_round_trip():
    seg = uuid.uuid4()
    root = RootRecord(version=1, segment=seg, offset=12345, epoch=7)
    raw = encode_root(root)
    assert decode_root(raw) == root
    assert root.pointer == AtomPointer(seg, 12345)
    assert root.pointer.segment is seg


def test_root_every_byte_corruption_detected():
    raw = encode_root(RootRecord(1, uuid.uuid4(), 99, 3))
    for i in range(len(raw)):
        bad = bytearray(raw)
        bad[i] ^= 0x01
        with pytest.raises(ProtoCorruptionError):
            decode_root(bytes(bad))


def test_root_truncated_or_garbage():
    raw = encode_root(RootRecord(1, uuid.uuid4(), 99, 3))
    for data in (b"", raw[:3], raw[:-1], b"\x00" * 40):
        with pytest.raises(ProtoCorruptionError):
            decode_root(data)


def test_root_with_valid_crc_but_wrong_shape_is_corruption():
    import msgpack
    for body in (msgpack.packb([1, 2]), msgpack.packb({"version": 1}),
                 msgpack.packb({"version": 1, "segment": b"short", "offset": 0, "epoch": 0}),
                 b"\xc1"):
        raw = struct.pack("<I", zlib.crc32(body)) + body
        with pytest.raises(ProtoCorruptionError):
            decode_root(raw)
