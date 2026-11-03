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
    assert SEGMENT_MAGIC == b"PDB2SEG\0" and FORMAT_VERSION == 2
    assert fmt.RECORD_MAGIC == b"\xc1\xd2" and fmt.RECORD_HEADER_SIZE == 15
    assert RecordKind.ATOM == 1 and RecordKind.BLOB == 2


def test_segment_header_round_trip():
    seg = uuid.uuid4()
    raw = encode_segment_header(seg)
    assert raw[:8] == SEGMENT_MAGIC and len(raw) == fmt.SEGMENT_HEADER_SIZE == 26
    assert struct.unpack("<H", raw[8:10])[0] == 2 and raw[10:] == seg.bytes
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
    magic, k, length, crc, header_crc = struct.unpack("<2sBIII", rec[:15])
    assert (magic, length, k) == (fmt.RECORD_MAGIC, len(payload), kind)
    assert crc == zlib.crc32(payload) and header_crc == zlib.crc32(rec[:11])
    assert fmt.record_header(rec) == (kind, len(payload), crc)
    assert fmt.record_size(rec[:15]) == len(rec)
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
    body = struct.pack("<2sBII", fmt.RECORD_MAGIC, 9, 3, zlib.crc32(payload))
    rec = body + struct.pack("<I", zlib.crc32(body)) + payload
    with pytest.raises(ProtoCorruptionError):
        read_record(io.BytesIO(rec), 0)
    assert fmt.record_header(rec) is None
    with pytest.raises(ProtoCorruptionError):
        fmt.record_size(rec)


def test_version_1_records_are_not_readable():
    """The version 1 framing (length | kind | crc over kind + payload) is refused (D46 fix round 13)."""
    payload = b"abc"
    rec = struct.pack("<IBI", 3, 1, zlib.crc32(bytes([1]) + payload)) + payload + bytes(8)
    with pytest.raises(ProtoCorruptionError):
        read_record(io.BytesIO(rec), 0)


def test_record_header_is_self_validating():
    """Every single-byte change of a header is caught without reading the payload."""
    rec = encode_record(RecordKind.BLOB, b"payload" * 100)
    for i in range(fmt.RECORD_HEADER_SIZE):
        for flip in (0x01, 0x80, 0xFF):
            bad = bytearray(rec[:fmt.RECORD_HEADER_SIZE])
            bad[i] ^= flip
            assert fmt.record_header(bytes(bad)) is None
            with pytest.raises(ProtoCorruptionError):
                fmt.record_size(bytes(bad))


def test_root_round_trip():
    seg = uuid.uuid4()
    root = RootRecord(version=FORMAT_VERSION, segment=seg, offset=12345, epoch=7)
    raw = encode_root(root)
    assert decode_root(raw) == root
    assert root.pointer == AtomPointer(seg, 12345)
    assert root.pointer.segment is seg


def test_root_every_byte_corruption_detected():
    raw = encode_root(RootRecord(FORMAT_VERSION, uuid.uuid4(), 99, 3))
    for i in range(len(raw)):
        bad = bytearray(raw)
        bad[i] ^= 0x01
        with pytest.raises(ProtoCorruptionError):
            decode_root(bytes(bad))


def test_root_truncated_or_garbage():
    raw = encode_root(RootRecord(FORMAT_VERSION, uuid.uuid4(), 99, 3))
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
