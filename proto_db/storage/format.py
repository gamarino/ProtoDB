"""On-disk byte formats of proto_db 2.0 (spec section 6).

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
from dataclasses import dataclass, field

import msgpack

from ..core.atom import AtomPointer
from ..errors import ProtoCorruptionError, ProtoNotSupportedError, ProtoValidationError
from ..values import MAX_VALUE_DEPTH

__all__ = ["SEGMENT_MAGIC", "FORMAT_VERSION", "WRITE_FORMAT_VERSION", "FEATURE_VERSIONS", "ROOT_WRITE_VERSION",
           "write_version", "root_version",
           "payload_features", "atom_version", "next_root_version", "peek_segment_header", "record_version",
           "framed_version", "READABLE_FORMAT_VERSIONS",
           "SEGMENT_HEADER_SIZE", "RECORD_HEADER_SIZE", "RECORD_MAGIC", "RecordKind", "RootRecord",
           "encode_segment_header", "decode_segment_header", "segment_header_version", "encode_record",
           "record_header", "record_size", "read_record", "encode_root", "decode_root", "undecodable_atom"]

SEGMENT_MAGIC = b"PDB2SEG\0"
#: Version of the segment, record and root formats; bump it when any of them changes.
#: 2: self-validating record headers (D46 fix round 13).
#: 3: compact atom references (the segment as 16 bytes, built-in types as small integers) and
#: large node values in atoms of their own (D51).  The root record's shape is unchanged.
#: 4: exact tokens are canonical encodings and indexes persist ``member_tokens``, zone datetimes
#: are written as their UTC instant, one spelling per value, the map-key rule (D58).
#: 5: the feature ``parents``, the atom envelope key ``"p"`` (runtime registry, ruling R151), and the
#: feature ``registry``, records of the registry's built-in types.  Data that uses neither is still
#: written as version 4 (:data:`WRITE_FORMAT_VERSION`).
#: 6: the feature ``catalog``, records of the semantic catalog's built-in types (:data:`CATALOG_TYPES`).
#: Data that does not use it keeps version 4 or 5.
#: 7: the feature ``preconditions``, the mark of the precondition concurrency model (spec R50, R63):
#: every root record this revision publishes is version 7, whatever the data it reaches, so a
#: writer without the model (which would let the last writer win) refuses the space.  Segments
#: keep the lowest version sufficient for their records.
FORMAT_VERSION = 7
#: The version written in every segment header and root record: the lowest version sufficient
#: for what this revision can write.  It differs from :data:`FORMAT_VERSION` (the highest
#: version read) once a later revision adds a feature: data that does not use it keeps this
#: version, so older readers keep opening it, and data that does is written with
#: :func:`write_version` of its features ("Format stability" in format-spec.rst; pre-tag review I-3).
WRITE_FORMAT_VERSION = 4
#: The features added after the freeze, each with the format version that introduced it.
#: ``parents``: an atom whose envelope has the key ``"p"`` (its parents tuple, runtime registry).
#: ``registry``: a record of one of the runtime registry's built-in types (:data:`REGISTRY_TYPES`).
#: ``catalog``: a record of one of the semantic catalog's built-in types (:data:`CATALOG_TYPES`).
#: ``preconditions``: a root record published under the precondition concurrency model (R63); a
#: feature of root records only, never of an atom's payload.
FEATURE_VERSIONS: dict = {"parents": 5, "registry": 5, "catalog": 6, "preconditions": 7}
#: The lowest version of every root record this revision publishes (R63).  The upgrade is one-way:
#: a root is never lower than the root it replaces (:func:`next_root_version`).
ROOT_WRITE_VERSION = FEATURE_VERSIONS["preconditions"]

#: The runtime registry's built-in types (``proto_db.registry``): their type names and their
#: aliases (``proto_db.core.atom.BUILTIN_TYPE_ALIASES``; a test keeps the two in step).  A record
#: whose type tag is one of them uses the feature ``registry``.
REGISTRY_TYPES = {"proto_db.registry.ModuleList": 22, "proto_db.registry.Catalog": 23}
#: The semantic catalog's built-in types (``proto_db.catalog``): their type names and their aliases
#: (a test keeps them in step with ``BUILTIN_TYPE_ALIASES``).  A record whose type tag is one of
#: them uses the feature ``catalog``.
CATALOG_TYPES = {
    "proto_db.catalog.SemanticCatalog": 24, "proto_db.catalog.Module": 25, "proto_db.catalog.Extension": 26,
    "proto_db.catalog.Entity": 27, "proto_db.catalog.Relationship": 28, "proto_db.catalog.Attribute": 29,
    "proto_db.catalog.Role": 30, "proto_db.catalog.Intent": 31, "proto_db.catalog.Constraint": 32,
    "proto_db.catalog.Expression": 33, "proto_db.catalog.IntentRecord": 34,
}


def write_version(features=()) -> int:
    """The version to write for data using ``features`` (names of :data:`FEATURE_VERSIONS`)."""
    return max([WRITE_FORMAT_VERSION] + [FEATURE_VERSIONS[f] for f in features])


def payload_features(payload: dict) -> frozenset:
    """The features an atom ``payload`` (its envelope map) uses, read from the payload itself.

    Every feature is detectable from the bytes of the records that use it: a node that rebuilds a
    segment header from replicated records derives the version from the segment's first record
    (:func:`framed_version`), so the version of a record never depends on anything but its bytes."""
    tag = payload.get("t")
    used = _TAG_FEATURES.get(tag, _NONE) if type(tag) in (int, str) else _NONE
    return _WITH_PARENTS[used] if "p" in payload else used


_NONE = frozenset()
_PARENTS = frozenset({"parents"})
_REGISTRY = frozenset({"registry"})
_CATALOG = frozenset({"catalog"})
#: The feature a type tag (alias or name) uses, for the tags of a feature's built-in types.
_TAG_FEATURES = {tag: feature for types, feature in ((REGISTRY_TYPES, _REGISTRY), (CATALOG_TYPES, _CATALOG))
                 for name, alias in types.items() for tag in (name, alias)}
#: A payload's features once its envelope also has parents (the key ``"p"``).
_WITH_PARENTS = {used: used | _PARENTS for used in (_NONE, _REGISTRY, _CATALOG)}


def atom_version(payload: dict, features=frozenset()) -> int:
    """The version an atom ``payload`` needs: :func:`write_version` of the features it uses.

    ``features`` are the features its writer declares (``push_atom(..., features=...)``): they
    must be exactly the features the payload uses (:func:`payload_features`).  A name that is not
    in :data:`FEATURE_VERSIONS`, a declared feature the payload does not use, and a feature the
    payload uses without declaring it all raise ``ProtoValidationError``.  So a writer states
    which format features it writes, and the storage checks the statement against the bytes,
    whose version alone a receiver can derive (:func:`framed_version`)."""
    used = payload_features(payload)
    if isinstance(features, str):
        raise ProtoValidationError("features must be a collection of feature names, not a string")
    declared = frozenset(features)
    unknown = sorted(str(f) for f in declared if f not in FEATURE_VERSIONS)
    if unknown:
        raise ProtoValidationError(f"Unknown format features {unknown}; known: {sorted(FEATURE_VERSIONS)}")
    if declared != used:
        raise ProtoValidationError(
            f"The atom declares the format features {sorted(declared)} but its payload uses {sorted(used)}")
    return write_version(used)


def root_version(*versions: int) -> int:
    """The version of a root record this revision publishes over ``versions`` (the version of the
    root it replaces, the highest segment version its writer wrote, the committer's version of a
    cluster root): the highest of them, never below :data:`ROOT_WRITE_VERSION` (R63).  Reads the
    floor at call time, like :func:`write_version` reads :data:`WRITE_FORMAT_VERSION`."""
    return max((ROOT_WRITE_VERSION,) + versions)


def next_root_version(previous, storage) -> int:
    """The version of a root built on ``previous`` (a :class:`RootRecord` or ``None``) by a writer
    on ``storage``: the highest of :data:`ROOT_WRITE_VERSION`, ``previous``'s version and the
    highest segment version ``storage`` has written (``SharedStorage.written_version``).  A root
    is never lower than the root it replaces, so once a space holds data of a later version every
    later root of it is of that version, in cluster mode too, where the root's version travels on
    the wire and in the acceptor log ("Format stability" in format-spec.rst)."""
    return root_version(previous.version if previous is not None else 0,
                        getattr(storage, "written_version", WRITE_FORMAT_VERSION))


def record_version(kind: int, payload) -> int:
    """The version a record of ``kind`` with ``payload`` bytes needs.  A blob needs
    :data:`WRITE_FORMAT_VERSION`; an atom whose payload does not unpack to a map is answered
    with :data:`FORMAT_VERSION`, the highest version this revision knows (conservative: a reader
    of an earlier revision refuses rather than misreads)."""
    if kind != RecordKind.ATOM:
        return WRITE_FORMAT_VERSION
    try:
        decoded = msgpack.unpackb(bytes(payload), raw=False)
    except Exception:                 # noqa: BLE001 - any undecodable payload: the conservative answer
        return FORMAT_VERSION
    return write_version(payload_features(decoded)) if isinstance(decoded, dict) else FORMAT_VERSION


def framed_version(data, pos: int = 0) -> int:
    """The version the framed record at ``pos`` of ``data`` needs (:func:`record_version`).
    Raises :class:`ProtoCorruptionError` when no valid record is there."""
    kind, payload = read_record(memoryview(data)[pos:], 0)
    return record_version(kind, payload)


#: Versions this code reads: a format-2 segment still decodes (its references are read by the
#: codec's format-2 branch), a format-3 one too (its zone datetimes by the codec's wall-clock
#: branch, its indexes rebuilt at their first change); a root record of any of them is read as
#: the current version.  A format-5, format-6 or format-7 segment or root is read as such (their
#: features are additive; a version-7 root marks the concurrency model, its shape is unchanged).
READABLE_FORMAT_VERSIONS = frozenset({2, 3, 4, 5, 6, 7})
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
    """The published root of a space: the pointer of the root atom and a CAS epoch.

    ``version`` is the format version its bytes carry; it is not part of the root's identity
    (equality and hash): a cluster root travels on the wire and in the acceptor log as its
    pointer and epoch only, so the same root may be held with different versions."""

    version: int = field(compare=False)
    segment: uuid.UUID
    offset: int
    epoch: int

    @property
    def pointer(self) -> AtomPointer:
        """The root atom's pointer."""
        return AtomPointer(self.segment, self.offset)


def encode_segment_header(segment: uuid.UUID, version: int = None) -> bytes:
    """Return the header bytes that start a segment file of ``version`` (default
    :func:`write_version` with no feature).  The header is written before the segment's records,
    so its version is the version of the record that opens the segment (ruling R151)."""
    return _SEGMENT_HEADER.pack(SEGMENT_MAGIC, write_version() if version is None else version, segment.bytes)


def _decode_header(data) -> tuple:
    if len(data) < SEGMENT_HEADER_SIZE:
        raise ProtoCorruptionError("Truncated segment header")
    magic, version, raw = _SEGMENT_HEADER.unpack_from(data)
    if magic != SEGMENT_MAGIC:
        raise ProtoCorruptionError("Bad segment magic")
    if version not in READABLE_FORMAT_VERSIONS:
        raise ProtoNotSupportedError(f"Unsupported segment format version {version}")
    return version, uuid.UUID(bytes=raw)


def decode_segment_header(data: bytes) -> uuid.UUID:
    """Validate a segment header (extra trailing bytes are ignored) and return its UUID."""
    return _decode_header(data)[1]


def peek_segment_header(data) -> tuple:
    """``(magic_ok, version)`` of the possibly incomplete segment header at the start of ``data``:
    ``magic_ok`` tells that the bytes present agree with :data:`SEGMENT_MAGIC` (a torn header is a
    prefix of it), ``version`` is the header's version when its bytes are there, else ``None``.
    Nothing is validated beyond that; :func:`decode_segment_header` validates a complete header."""
    magic_size = len(SEGMENT_MAGIC)
    head = bytes(data[:magic_size])
    if not SEGMENT_MAGIC.startswith(head):
        return False, None
    if len(data) < magic_size + 2:
        return True, None
    return True, struct.unpack_from("<H", data, magic_size)[0]


def segment_header_version(data: bytes) -> int:
    """Validate a segment header like :func:`decode_segment_header` and return its version."""
    return _decode_header(data)[0]


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
    # A root of an earlier version is read as the current base version; a later one keeps its
    # version, so the root that replaces it is never written lower ("Format stability").
    return RootRecord(max(version, WRITE_FORMAT_VERSION), uuid.UUID(bytes=raw), offset, epoch)


def undecodable_atom(ptr, error: Exception) -> ProtoCorruptionError:
    """The error of an atom record at ``ptr`` whose msgpack payload ``error`` refused to unpack.

    MessagePack's unpacker refuses data nested deeper than 1024 containers (``StackError``): an
    atom payload nests two (its envelope and its state map) above its attribute values, so such
    a record holds a value deeper than ``MAX_VALUE_DEPTH`` (format spec, "Value nesting depth")."""
    if isinstance(error, msgpack.exceptions.StackError):
        return ProtoCorruptionError(
            f"Undecodable atom at {ptr}: it nests deeper than 1024 MessagePack containers, so a value "
            f"in it nests deeper than MAX_VALUE_DEPTH = {MAX_VALUE_DEPTH} (format spec, \"Value nesting depth\")")
    return ProtoCorruptionError(f"Undecodable atom at {ptr}")
