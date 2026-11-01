"""Wire protocol of the cluster coordinator (Task 8.3c, decisions D44 and D45).

Every message is a msgpack map with three common fields -- ``v`` (the protocol version,
:data:`PROTOCOL_VERSION`), ``type`` and ``from`` (the sender's node id) -- plus the
fields of its type:

``request``  ``term``, ``rid``: ask for the write lease for ``term`` (``rid`` matches the replies
             to one attempt).
``grant``    ``term``, ``rid``, ``root``, ``certain``: the sender grants the lease for ``term``.
``reject``   ``term``, ``rid``, ``root``, ``certain``, ``reason``, ``highest``, ``remaining``:
             the sender refuses; ``highest`` is the highest term it has seen and ``remaining``
             the seconds left of the promise that blocks the grant (``0.0`` when none).
``release``  ``term``, ``root``, ``certain``, ``held``: the sender gives up ``term``; ``held``
             is True when it actually held the lease (a winner's release, which wakes waiting
             requesters) and False for the cancel of a lost attempt.

``root`` is the sender's latest known storage root (see :func:`encode_root`) and
``certain`` tells whether the sender is sure no root newer than it was published (D45).

On a stream, each message travels as one frame: a 4-byte big-endian length followed by
that many bytes of msgpack.  A frame longer than :data:`MAX_FRAME_BYTES` is a protocol
error (the connection is dropped before the frame is buffered).  A message of another
protocol version, an unknown type or a missing or ill-typed field is a
:class:`ClusterProtocolError`; receivers drop such messages.
"""
import struct
import uuid
from typing import Optional

import msgpack

from ..errors import ProtoCorruptionError
from ..storage.format import FORMAT_VERSION, RootRecord

__all__ = ["PROTOCOL_VERSION", "MAX_FRAME_BYTES", "MESSAGE_TYPES", "ClusterProtocolError",
           "make_message", "validate_message", "encode_message", "decode_message",
           "encode_frame", "FrameDecoder", "encode_root", "decode_root"]

#: Version carried by every message; a receiver drops messages of any other version.
PROTOCOL_VERSION = 1
#: Largest accepted frame payload, in bytes.
MAX_FRAME_BYTES = 1024 * 1024

_LENGTH = struct.Struct(">I")
_INT = "int"
_NUM = "number"
_STR = "str"
_BOOL = "bool"
_ROOT = "root"

#: Fields of each message type (besides ``v``, ``type`` and ``from``).
MESSAGE_TYPES = {
    "request": {"term": _INT, "rid": _INT},
    "grant": {"term": _INT, "rid": _INT, "root": _ROOT, "certain": _BOOL},
    "reject": {"term": _INT, "rid": _INT, "root": _ROOT, "certain": _BOOL, "reason": _STR,
               "highest": _INT, "remaining": _NUM},
    "release": {"term": _INT, "root": _ROOT, "certain": _BOOL, "held": _BOOL},
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


def encode_frame(msg: dict) -> bytes:
    """One frame: the 4-byte big-endian length, then the msgpack bytes of ``msg``."""
    payload = encode_message(msg)
    if len(payload) > MAX_FRAME_BYTES:
        raise ClusterProtocolError(f"A message of {len(payload)} bytes exceeds the frame limit")
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


def encode_root(record: Optional[RootRecord], *, known: bool):
    """Wire form of a known root: ``None`` (unknown), ``[0, None, None]`` (known empty space)
    or ``[epoch, segment UUID bytes, offset]``."""
    if not known:
        return None
    if record is None:
        return [0, None, None]
    return [record.epoch, record.segment.bytes, record.offset]


def decode_root(value) -> tuple:
    """Inverse of :func:`encode_root`: ``(known, RootRecord or None)``."""
    if value is None:
        return False, None
    if not isinstance(value, (list, tuple)) or len(value) != 3:
        raise ClusterProtocolError(f"Malformed root {value!r}")
    epoch, segment, offset = value
    if type(epoch) is not int or epoch < 0:
        raise ClusterProtocolError(f"Malformed root epoch {epoch!r}")
    if epoch == 0:
        if segment is not None or offset is not None:
            raise ClusterProtocolError("The empty root (epoch 0) has no pointer")
        return True, None
    if not isinstance(segment, bytes) or len(segment) != 16 or type(offset) is not int or offset < 0:
        raise ClusterProtocolError(f"Malformed root pointer {value!r}")
    return True, RootRecord(FORMAT_VERSION, uuid.UUID(bytes=segment), offset, epoch)
