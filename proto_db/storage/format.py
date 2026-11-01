"""On-disk byte formats of ProtoDB 2.0 (spec section 6).

Segment header: ``SEGMENT_MAGIC | format version (u16 LE) | segment UUID (16 bytes)``.
Record: ``length:u32 LE | kind:u8 | crc32:u32 LE | payload``; the CRC32 covers kind + payload.
Root record: ``crc32:u32 LE | msgpack{version, segment, offset, epoch}``; the CRC32 covers
the msgpack bytes.

Every integrity failure raises :class:`ProtoCorruptionError`.
"""
import enum
import io
import struct
import uuid
import zlib
from dataclasses import dataclass

import msgpack

from ..core.atom import AtomPointer
from ..errors import ProtoCorruptionError, ProtoNotSupportedError, ProtoValidationError

__all__ = ["SEGMENT_MAGIC", "FORMAT_VERSION", "SEGMENT_HEADER_SIZE", "RECORD_HEADER_SIZE", "RecordKind",
           "RootRecord", "encode_segment_header", "decode_segment_header", "encode_record",
           "read_record", "encode_root", "decode_root"]

SEGMENT_MAGIC = b"PDB2SEG\0"
FORMAT_VERSION = 1
_SEGMENT_HEADER = struct.Struct("<8sH16s")
_RECORD_HEADER = struct.Struct("<IBI")
_ROOT_CRC = struct.Struct("<I")
SEGMENT_HEADER_SIZE = _SEGMENT_HEADER.size
RECORD_HEADER_SIZE = _RECORD_HEADER.size


class RecordKind(enum.IntEnum):
    """Kind byte of a record."""

    ATOM = 1
    BLOB = 2


@dataclass(frozen=True)
class RootRecord:
    """The published root of a space: the pointer of the root atom and a CAS epoch."""

    version: int
    segment: uuid.UUID
    offset: int
    epoch: int

    @property
    def pointer(self) -> AtomPointer:
        """The root atom's pointer."""
        return AtomPointer(self.segment, self.offset)


def encode_segment_header(segment: uuid.UUID) -> bytes:
    """Return the header bytes that start a segment file."""
    return _SEGMENT_HEADER.pack(SEGMENT_MAGIC, FORMAT_VERSION, segment.bytes)


def decode_segment_header(data: bytes) -> uuid.UUID:
    """Validate a segment header (extra trailing bytes are ignored) and return its UUID."""
    if len(data) < SEGMENT_HEADER_SIZE:
        raise ProtoCorruptionError("Truncated segment header")
    magic, version, raw = _SEGMENT_HEADER.unpack_from(data)
    if magic != SEGMENT_MAGIC:
        raise ProtoCorruptionError("Bad segment magic")
    if version != FORMAT_VERSION:
        raise ProtoNotSupportedError(f"Unsupported segment format version {version}")
    return uuid.UUID(bytes=raw)


def encode_record(kind: int, payload: bytes) -> bytes:
    """Frame ``payload`` as a record of ``kind``."""
    if not isinstance(payload, (bytes, bytearray, memoryview)):
        raise ProtoValidationError("Record payload must be bytes")
    try:
        kind = RecordKind(kind)
    except ValueError as e:
        raise ProtoValidationError(f"Unknown record kind {kind!r}") from e
    payload = bytes(payload)
    if len(payload) > 0xFFFFFFFF:
        raise ProtoValidationError("Record payload too large")
    crc = zlib.crc32(payload, zlib.crc32(bytes([kind])))
    return _RECORD_HEADER.pack(len(payload), kind, crc) + payload


def read_record(stream, offset: int) -> tuple:
    """Read and verify the record at ``offset``; return ``(RecordKind, payload)``.

    ``stream`` is a seekable binary stream or a bytes-like object.  A short read, an
    unknown kind or a CRC mismatch raises :class:`ProtoCorruptionError`.
    """
    if isinstance(stream, (bytes, bytearray, memoryview)):
        stream = io.BytesIO(stream)
    stream.seek(offset)
    header = stream.read(RECORD_HEADER_SIZE)
    if len(header) < RECORD_HEADER_SIZE:
        raise ProtoCorruptionError(f"Truncated record header at offset {offset}")
    length, kind, crc = _RECORD_HEADER.unpack(header)
    payload = stream.read(length)
    if len(payload) < length:
        raise ProtoCorruptionError(f"Truncated record payload at offset {offset}")
    if zlib.crc32(payload, zlib.crc32(bytes([kind]))) != crc:
        raise ProtoCorruptionError(f"CRC mismatch in record at offset {offset}")
    try:
        return RecordKind(kind), payload
    except ValueError as e:
        raise ProtoCorruptionError(f"Unknown record kind {kind} at offset {offset}") from e


def encode_root(root: RootRecord) -> bytes:
    """Serialize a root record as ``crc32 | msgpack``."""
    body = msgpack.packb({"version": root.version, "segment": root.segment.bytes,
                          "offset": root.offset, "epoch": root.epoch}, use_bin_type=True)
    return _ROOT_CRC.pack(zlib.crc32(body)) + body


def decode_root(data: bytes) -> RootRecord:
    """Parse and verify a serialized root record."""
    if len(data) <= _ROOT_CRC.size:
        raise ProtoCorruptionError("Truncated root record")
    (crc,) = _ROOT_CRC.unpack_from(data)
    body = bytes(data[_ROOT_CRC.size:])
    if zlib.crc32(body) != crc:
        raise ProtoCorruptionError("CRC mismatch in root record")
    try:
        fields = msgpack.unpackb(body, raw=False)
    except (msgpack.exceptions.UnpackException, ValueError) as e:
        raise ProtoCorruptionError("Undecodable root record") from e
    try:
        version, raw, offset, epoch = (fields["version"], fields["segment"],
                                       fields["offset"], fields["epoch"])
    except (TypeError, KeyError) as e:
        raise ProtoCorruptionError("Malformed root record") from e
    if (type(version) is not int or type(offset) is not int or type(epoch) is not int
            or offset < 0 or epoch < 0 or not isinstance(raw, bytes) or len(raw) != 16):
        raise ProtoCorruptionError("Malformed root record")
    if version != FORMAT_VERSION:
        raise ProtoNotSupportedError(f"Unsupported root format version {version}")
    return RootRecord(version, uuid.UUID(bytes=raw), offset, epoch)
