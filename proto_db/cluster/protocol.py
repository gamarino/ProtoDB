"""Wire protocol of the cluster coordinator (Tasks 8.3c and 8.3d, decisions D44, D45, D46).

Every message is a msgpack map with three common fields -- ``v`` (the protocol version,
:data:`PROTOCOL_VERSION`), ``type`` and ``from`` (the sender's node id) -- plus the
fields of its type:

``request``    ``term``, ``rid``, ``inc``: ask for the write lease for ``term``, a ballot
               qualified by the requester's rank (``rid`` matches
               the replies to one attempt; ``inc`` is the requester's incarnation, a random
               number drawn at each start, so a restarted node is never mistaken for its
               previous life).
``grant``      ``term``, ``rid``, ``root``, ``certain``: the sender grants the lease for ``term``.
``reject``     ``term``, ``rid``, ``root``, ``certain``, ``reason``, ``highest``, ``remaining``:
               the sender refuses; ``highest`` is the highest term it has seen and
               ``remaining`` the seconds left of the promise that blocks the grant.
``release``    ``term``, ``inc``, ``root``, ``committed``, ``certain``, ``held``: the sender gives
               up ``term``; ``held`` is True when it actually held the lease (a winner's
               release, which wakes waiting requesters) and False for the cancel of a lost
               attempt.
``records``    ``term``, ``rid``, ``index``, ``records``: one chunk of the records of the
               replication ``rid`` that do not fit in its ``replicate`` frame (sent before it,
               on the same ordered link; ``index`` counts from 0).
``replicate``  ``term``, ``rid``, ``root``, ``committed``, ``records``, ``chunks``: the lease
               holder of ``term`` replicates its new root (the commit point, D46) together
               with the records the receiver needs: ``[segment UUID bytes, offset, framed
               record bytes or None]`` (``None``: the records live in a shared storage; only
               prefetch), after ``chunks`` ``records`` messages carrying the rest.
``ack``        ``term``, ``rid``, ``ok``, ``root``: the receiver durably accepted (``ok``) or
               refused the replicated root; ``root`` is its accepted root.
``query``      ``rid``: ask a peer for the roots it knows (recovery, rejoin).
``answer``     ``rid``, ``root``, ``committed``, ``certain``, ``promised``: the peer's accepted and
               committed roots and the highest term it has seen or promised.
``fetch``      ``rid``, ``segment``, ``offset``: a peer page request for one record.
``fetched``    ``rid``, ``data``: the framed record bytes, or ``None`` when the peer lacks it.

``root`` and ``committed`` use :func:`encode_root`: a root with the term of the lease that
replicated it (its *label*); roots are ordered by
:func:`~proto_db.cluster.rootlog.root_key` (D46).  ``certain``
tells whether the sender knows of no grant whose outcome it could have missed.

On a stream, each message travels as one frame: a 4-byte big-endian length followed by
that many bytes of msgpack.  A frame longer than :data:`MAX_FRAME_BYTES` is a protocol
error (the connection is dropped before the frame is buffered); every transport refuses
to send one.  A replication splits its records into frames of at most
:data:`CHUNK_BYTES` of record data, and a cluster node refuses to store a record larger
than :data:`MAX_RECORD_BYTES`, so any commit fits.  A message of another
protocol version, an unknown type or a missing or ill-typed field is a
:class:`ClusterProtocolError`; receivers drop such messages.
"""
import struct
import uuid
from typing import Optional

import msgpack

from ..errors import ProtoCorruptionError
from ..storage.format import FORMAT_VERSION, RootRecord

__all__ = ["PROTOCOL_VERSION", "MAX_FRAME_BYTES", "CHUNK_BYTES", "MAX_RECORD_BYTES", "MESSAGE_TYPES",
           "ClusterProtocolError",
           "make_message", "validate_message", "encode_message", "decode_message", "check_frame_size",
           "encode_frame", "FrameDecoder", "encode_root", "decode_root", "decode_labeled_root"]

#: Version carried by every message; a receiver drops messages of any other version.
PROTOCOL_VERSION = 3
#: Largest accepted frame payload, in bytes (a replicate message carries a commit's records).
MAX_FRAME_BYTES = 64 * 1024 * 1024
#: Record bytes a replication puts in one frame at most (a single record may fill one alone).
CHUNK_BYTES = 16 * 1024 * 1024
#: Largest record payload a cluster node stores: a record always fits in one frame.
MAX_RECORD_BYTES = 16 * 1024 * 1024

_LENGTH = struct.Struct(">I")
_INT = "int"
_NUM = "number"
_STR = "str"
_BOOL = "bool"
_ROOT = "root"
_RECORDS = "records"
_UUID = "uuid"
_OPT_BYTES = "optional bytes"

#: Fields of each message type (besides ``v``, ``type`` and ``from``).
MESSAGE_TYPES = {
    "request": {"term": _INT, "rid": _INT, "inc": _INT},
    "grant": {"term": _INT, "rid": _INT, "root": _ROOT, "committed": _ROOT, "certain": _BOOL},
    "reject": {"term": _INT, "rid": _INT, "root": _ROOT, "certain": _BOOL, "reason": _STR,
               "highest": _INT, "remaining": _NUM},
    "release": {"term": _INT, "inc": _INT, "root": _ROOT, "committed": _ROOT, "certain": _BOOL,
                "held": _BOOL},
    "records": {"term": _INT, "rid": _INT, "index": _INT, "records": _RECORDS},
    "replicate": {"term": _INT, "rid": _INT, "root": _ROOT, "committed": _ROOT, "records": _RECORDS,
                  "chunks": _INT},
    "ack": {"term": _INT, "rid": _INT, "ok": _BOOL, "root": _ROOT},
    "query": {"rid": _INT},
    "answer": {"rid": _INT, "root": _ROOT, "committed": _ROOT, "certain": _BOOL, "promised": _INT},
    "fetch": {"rid": _INT, "segment": _UUID, "offset": _INT},
    "fetched": {"rid": _INT, "data": _OPT_BYTES},
}


class ClusterProtocolError(ProtoCorruptionError):
    """A malformed frame or message, or one of an unsupported protocol version."""


def _check_field(kind: str, name: str, value, expected: str) -> None:
    if expected == _INT:
        ok = type(value) is int and value >= 0
    elif expected == _NUM:
        ok = isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0
    elif expected == _STR:
        ok = isinstance(value, str)
    elif expected == _BOOL:
        ok = isinstance(value, bool)
    elif expected == _UUID:
        ok = isinstance(value, bytes) and len(value) == 16
    elif expected == _OPT_BYTES:
        ok = value is None or isinstance(value, bytes)
    elif expected == _RECORDS:
        ok = isinstance(value, list) and all(
            isinstance(r, (list, tuple)) and len(r) == 3 and isinstance(r[0], bytes) and len(r[0]) == 16
            and type(r[1]) is int and r[1] >= 0 and (r[2] is None or isinstance(r[2], bytes)) for r in value)
    else:
        decode_root(value)
        ok = True
    if not ok:
        raise ClusterProtocolError(f"Field {name!r} of a {kind!r} message must be a valid {expected}, got {value!r}")


def validate_message(msg) -> dict:
    """Return ``msg`` if it is a well-formed message of this protocol version; raise otherwise."""
    if not isinstance(msg, dict):
        raise ClusterProtocolError(f"A message must be a map, got {type(msg).__name__}")
    if msg.get("v") != PROTOCOL_VERSION:
        raise ClusterProtocolError(f"Unsupported protocol version {msg.get('v')!r} "
                                   f"(this node speaks version {PROTOCOL_VERSION})")
    kind = msg.get("type")
    schema = MESSAGE_TYPES.get(kind) if isinstance(kind, str) else None
    if schema is None:
        raise ClusterProtocolError(f"Unknown message type {kind!r}")
    if not isinstance(msg.get("from"), str):
        raise ClusterProtocolError("A message must name its sender in 'from'")
    for name, expected in schema.items():
        if name not in msg:
            raise ClusterProtocolError(f"A {kind!r} message lacks the field {name!r}")
        _check_field(kind, name, msg[name], expected)
    return msg


def make_message(kind: str, sender: str, **fields) -> dict:
    """Build and validate a message of type ``kind`` from ``sender``."""
    msg = {"v": PROTOCOL_VERSION, "type": kind, "from": sender}
    msg.update(fields)
    return validate_message(msg)


def encode_message(msg: dict) -> bytes:
    """msgpack bytes of a message (validated first)."""
    return msgpack.packb(validate_message(msg), use_bin_type=True)


def decode_message(data: bytes) -> dict:
    """Decode and validate msgpack bytes into a message."""
    try:
        msg = msgpack.unpackb(data, raw=False, strict_map_key=True)
    except (ValueError, TypeError, msgpack.ExtraData, msgpack.FormatError, msgpack.StackError) as e:
        raise ClusterProtocolError(f"Undecodable message: {e}") from e
    return validate_message(msg)


def check_frame_size(size: int) -> None:
    """Raise ``ClusterProtocolError`` when a message of ``size`` bytes exceeds :data:`MAX_FRAME_BYTES`."""
    if size > MAX_FRAME_BYTES:
        raise ClusterProtocolError(f"A message of {size} bytes exceeds the frame limit of {MAX_FRAME_BYTES}")


def encode_frame(msg: dict) -> bytes:
    """One frame: the 4-byte big-endian length, then the msgpack bytes of ``msg``."""
    payload = encode_message(msg)
    check_frame_size(len(payload))
    return _LENGTH.pack(len(payload)) + payload


class FrameDecoder:
    """Incremental decoder of a byte stream of frames (one per connection; not thread-safe)."""

    def __init__(self):
        self._buffer = bytearray()

    def feed(self, data: bytes) -> list:
        """Append ``data`` and return the complete messages it finishes, in order.

        Raises :class:`ClusterProtocolError` for an oversized frame or a bad message; the
        stream cannot be resynchronised after that, so the caller drops the connection.
        """
        self._buffer += data
        out = []
        while len(self._buffer) >= _LENGTH.size:
            (size,) = _LENGTH.unpack_from(self._buffer)
            if size > MAX_FRAME_BYTES:
                raise ClusterProtocolError(f"A frame of {size} bytes exceeds the limit of {MAX_FRAME_BYTES}")
            end = _LENGTH.size + size
            if len(self._buffer) < end:
                break
            payload = bytes(self._buffer[_LENGTH.size:end])
            del self._buffer[:end]
            out.append(decode_message(payload))
        return out


def encode_root(record: Optional[RootRecord], *, known: bool, term: int = 0):
    """Wire form of a root: ``None`` (unknown), ``[0, None, None, term]`` (known empty space)
    or ``[epoch, segment UUID bytes, offset, term]``; ``term`` is the root's label (D46)."""
    if not known:
        return None
    if record is None:
        return [0, None, None, term]
    return [record.epoch, record.segment.bytes, record.offset, term]


def decode_labeled_root(value) -> tuple:
    """Inverse of :func:`encode_root`: ``(known, RootRecord or None, term)``.

    A three-element value (no term) is accepted with term ``0``.
    """
    if value is None:
        return False, None, 0
    if not isinstance(value, (list, tuple)) or len(value) not in (3, 4):
        raise ClusterProtocolError(f"Malformed root {value!r}")
    epoch, segment, offset = value[:3]
    term = value[3] if len(value) == 4 else 0
    if type(epoch) is not int or epoch < 0 or type(term) is not int or term < 0:
        raise ClusterProtocolError(f"Malformed root epoch or term {value!r}")
    if epoch == 0:
        if segment is not None or offset is not None:
            raise ClusterProtocolError("The empty root (epoch 0) has no pointer")
        return True, None, term
    if not isinstance(segment, bytes) or len(segment) != 16 or type(offset) is not int or offset < 0:
        raise ClusterProtocolError(f"Malformed root pointer {value!r}")
    return True, RootRecord(FORMAT_VERSION, uuid.UUID(bytes=segment), offset, epoch), term


def decode_root(value) -> tuple:
    """``(known, RootRecord or None)`` of a wire root (its label is dropped)."""
    known, record, _ = decode_labeled_root(value)
    return known, record
