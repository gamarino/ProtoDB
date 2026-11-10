"""On-disk byte formats of ProtoDB 2.0 (spec section 6).

Segment header: ``SEGMENT_MAGIC | format version (u16 LE) | segment UUID (16 bytes)``.
Record (format version 2, D46 fix round 13)::

    RECORD_MAGIC (2 bytes) | kind:u8 | length:u32 LE | payload_crc:u32 LE | header_crc:u32 LE | payload

``payload_crc`` is the CRC32 of the payload; ``header_crc`` is the CRC32 of the eleven bytes
before it (magic, kind, length and ``payload_crc``).  The header is thus self-validating: a
reader trusts a record's length -- its framing -- only when ``header_crc`` matches, without
reading the payload, and the magic lets a search for record headers in damaged data run in
the regular-expression engine (:func:`proto_db.storage.tiered.TieredStorage._repair_own_segment`).
Version 1 (``length | kind | crc32 over kind + payload``) did not protect the length on its
own, so the extent of an invalid record could not be trusted; it is not readable by this
version (there are no users to migrate).
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

__all__ = ["SEGMENT_MAGIC", "FORMAT_VERSION", "READABLE_FORMAT_VERSIONS", "SEGMENT_HEADER_SIZE", "RECORD_HEADER_SIZE", "RECORD_MAGIC",
           "RecordKind", "RootRecord", "encode_segment_header", "decode_segment_header", "encode_record",
           "record_header", "record_size", "read_record", "encode_root", "decode_root"]

SEGMENT_MAGIC = b"PDB2SEG\0"
#: Version of the segment, record and root formats; bump it when any of them changes.
#: 2: self-validating record headers (D46 fix round 13).
#: 3: compact atom references (the segment as 16 bytes, built-in types as small integers) and
#: large node values in atoms of their own (D51).  The root record's shape is unchanged.
FORMAT_VERSION = 4
#: Versions this code reads: a format-2 segment still decodes (its references are read by the
#: codec's format-2 branch); a root record of either version is read as the current version.
READABLE_FORMAT_VERSIONS = frozenset({2, 3, 4})
_SEGMENT_HEADER = struct.Struct("<8sH16s")
#: First two bytes of every record header.  ``0xC1`` is the one byte msgpack never emits as a
#: type byte, so the magic is rare in msgpack payloads (it only marks candidates: the header
#: CRC decides).
RECORD_MAGIC = b"\xc1\xd2"
_RECORD_HEADER = struct.Struct("<2sBIII")       # magic, kind, length, payload crc, header crc
_HEADER_BODY = _RECORD_HEADER.size - 4          # the bytes the header CRC covers
_ROOT_CRC = struct.Struct("<I")
SEGMENT_HEADER_SIZE = _SEGMENT_HEADER.size
RECORD_HEADER_SIZE = _RECORD_HEADER.size


class RecordKind(enum.IntEnum):
    """Kind byte of a record."""

    ATOM = 1
    BLOB = 2


_KINDS = frozenset(int(k) for k in RecordKind)


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
    if version not in READABLE_FORMAT_VERSIONS:
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
    body = _RECORD_HEADER.pack(RECORD_MAGIC, kind, len(payload), zlib.crc32(payload), 0)[:_HEADER_BODY]
    return body + zlib.crc32(body).to_bytes(4, "little") + payload


def record_header(data, pos: int = 0):
    """Decode the record header at ``pos`` of the bytes-like ``data``; return ``(kind, length,
    payload_crc)`` when it is a valid header (complete, magic, header CRC and kind all valid),
    else ``None``.  The payload is not read."""
    if pos < 0 or pos + RECORD_HEADER_SIZE > len(data):
        return None
    magic, kind, length, payload_crc, header_crc = _RECORD_HEADER.unpack_from(data, pos)
    if magic != RECORD_MAGIC or kind not in _KINDS or zlib.crc32(data[pos:pos + _HEADER_BODY]) != header_crc:
        return None
    return kind, length, payload_crc


def record_size(header, offset=None) -> int:
    """The framed size (header plus payload) of the record whose header is the first
    ``RECORD_HEADER_SIZE`` bytes of ``header``.  Raises :class:`ProtoCorruptionError` when the
    header is truncated or invalid (``offset`` only names the record in the message)."""
    where = "" if offset is None else f" at {offset}"
    if len(header) < RECORD_HEADER_SIZE:
        raise ProtoCorruptionError(f"Truncated record header{where}")
    decoded = record_header(header, 0)
    if decoded is None:
        raise ProtoCorruptionError(f"Invalid record header{where}")
    return RECORD_HEADER_SIZE + decoded[1]


def read_record(stream, offset: int) -> tuple:
    """Read and verify the record at ``offset``; return ``(RecordKind, payload)``.

    ``stream`` is a seekable binary stream or a bytes-like object.  A short read, an
    invalid header (magic, kind or header CRC) or a payload CRC mismatch raises
    :class:`ProtoCorruptionError`.
    """
    if isinstance(stream, (bytes, bytearray, memoryview)):
        stream = io.BytesIO(stream)
    stream.seek(offset)
    header = stream.read(RECORD_HEADER_SIZE)
    if len(header) < RECORD_HEADER_SIZE:
        raise ProtoCorruptionError(f"Truncated record header at offset {offset}")
    decoded = record_header(header, 0)
    if decoded is None:
        raise ProtoCorruptionError(f"Invalid record header at offset {offset}")
    kind, length, crc = decoded
    payload = stream.read(length)
    if len(payload) < length:
        raise ProtoCorruptionError(f"Truncated record payload at offset {offset}")
    if zlib.crc32(payload) != crc:
        raise ProtoCorruptionError(f"CRC mismatch in record at offset {offset}")
    return RecordKind(kind), payload


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
    if version not in READABLE_FORMAT_VERSIONS:
        raise ProtoNotSupportedError(f"Unsupported root format version {version}")
    return RootRecord(FORMAT_VERSION, uuid.UUID(bytes=raw), offset, epoch)
