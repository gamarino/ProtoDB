"""Wire protocol of the cluster coordinator (Tasks 8.3c and 8.3d, decisions D44, D45, D46).

Every message is a msgpack map with three common fields -- ``v`` (the protocol version,
:data:`PROTOCOL_VERSION`), ``type`` and ``from`` (the sender's node id) -- plus the
fields of its type.  Since protocol 12 every request answered by a reply carries the
requester's incarnation ``inc`` and every reply echoes it (not repeated below for each type):
a reply reaching another incarnation of the requester is dropped, since request ids restart
at every start (Task 8.3g review, C1):

``request``    ``term``, ``rid``, ``inc``, ``vs``: ask for the write lease for ``term``, a ballot
               qualified by the requester's rank (``rid`` matches
               the replies to one attempt; ``inc`` is the requester's incarnation, a random
               number drawn at each start, so a restarted node is never mistaken for its
               previous life).
``grant``      ``term``, ``rid``, ``root``, ``certain``: the sender grants the lease for ``term``.
``reject``     ``term``, ``rid``, ``root``, ``certain``, ``reason``, ``highest``, ``remaining``:
               the sender refuses; ``highest`` is the highest term it has seen and
               ``remaining`` the seconds left of the promise that blocks the grant.
``release``    ``term``, ``inc``, ``root``, ``committed``, ``certain``, ``held``, ``next``,
               ``more``: the sender gives up ``term``; ``held`` is True when it actually held
               the lease (a winner's release, which wakes waiting requesters) and False for
               the cancel of a lost attempt.  ``next`` is the member whose turn the sender
               names (the one it has seen waiting longest, or ``None``) and ``more`` tells
               whether the sender still wants the lease: lease fairness hints (Task 8.3e),
               used only to time requests, never by a grant or an acceptance.
``want``       (no field): the sender wants the lease but defers to another member's turn; the
               receivers queue it (a fairness hint like ``next``/``more``, Task 8.3e).
``records``    ``term``, ``rid``, ``index``, ``records``: one chunk of the records of the
               replication ``rid`` that do not fit in its ``replicate`` frame (sent before it,
               on the same ordered link; ``index`` counts from 0).
``replicate``  ``term``, ``rid``, ``root``, ``committed``, ``records``, ``chunks``, ``base``,
               ``base_ranges``: the lease holder of ``term`` replicates its new root (the
               commit point, D46) together with the records the receiver needs: ``[segment
               UUID bytes, offset, framed record bytes or None]`` (``None``: the records live
               in a shared storage; only prefetch), after ``chunks`` ``records`` messages
               carrying the rest.  When the new root re-proposes a base nobody knew committed,
               ``base`` is that base and ``base_ranges`` the byte ranges of its commit, whose
               records ride with this replication (ruling R50); otherwise both are ``None``.
               Since protocol 8 (D50) it also carries ``seq`` (the write permission it runs
               under, ``0`` under a plain lease) and ``pred`` (the root it was built on): a root
               whose ``pred`` carries the root's own label is taken only when that ``pred`` is
               the acceptor's accepted root, which includes a root it learnt committed (the
               chain rule, Raft's log matching on roots; D55 rule V2', protocol 13 dropped the
               ``pred_committed`` catch-up).  Since protocol 11 (D54) it also
               carries ``vs`` (the fingerprint of the sender's voter set), ``ranges`` (the byte
               ranges of the commit's records) and ``copies`` (the members known to hold them
               durably, the sender included): a voter other than the sender accepts the root
               only when ``copies`` -- with itself, when the records ride in this message --
               counts a majority of the voters.  A message carrying records goes to the
               commit's data replicas, which store them durably whether or not they accept the
               root; the root alone goes to the other voters.
``propose``    ``term``, ``seq``, ``inc``, ``root``, ``pred``: the holder of permission ``seq`` of
               ``term`` built ``root`` on ``pred`` (the base its permit gave) in a round where no
               commit conflicted, and will send no other root under it; the owner may issue the
               next permission on ``root`` (D55).  No reply.
``renew``      ``term``, ``rid``, ``inc``: the owner of ``term`` asks its granters to extend the
               active grant they gave it (D50); answered by ``grant`` or ``reject``.
``permit_req`` ``term``, ``rid``, ``inc``: ask the owner of ``term`` for a write permission (D50).
``permit``     ``term``, ``rid``, ``seq``, ``inc``, ``root``, ``certain``, ``valid``, ``after``: write
               permission ``seq`` of ``term`` for the requester's incarnation ``inc`` (the one
               its ``permit_req`` carried; any other incarnation drops it unanswered, protocol
               9); ``root`` is the newest root the owner knows (committed unless ``certain`` is
               False, the first permission of an uncertain lease) and ``valid`` the seconds
               the permission may be used from the request's send.  ``after`` (protocol 13,
               D55) is ``None``, or ``[seq_j, holder_j]`` when ``root`` is the *proposal* of
               permission ``seq_j`` made by ``holder_j``: not known committed (``certain`` is
               then always False); ``holder_j`` is only a target of the gate's fallback query.
``permit_no``  ``term``, ``rid``, ``inc``, ``reason``: the sender does not grant (not the owner of that
               term, lease ended, term abandoned); ``term`` is the highest term it knows.
``done``       ``term``, ``seq``, ``inc``, ``outcome``, ``root``: the holder ends permission
               ``seq`` of ``term``: ``outcome`` is ``"committed"`` (``root`` is the last root it
               committed), ``"none"`` (nothing sent) or ``"unknown"``; broadcast, so every member
               learns the committed root (processed after the sender's replications on links
               that keep the sender's order).  ``inc`` is the sender's incarnation: the owner
               counts a ``done`` only from the incarnation the permission was issued to, and
               takes any other as an unknown outcome (8.3f review, C1).  ``copies`` and
               ``ranges`` locate the records of a committed ``root`` (D54; empty and ``None``
               otherwise): a member fetches a record it lacks from those members first.
``ack``        ``term``, ``rid``, ``ok``, ``root``, ``stored``: the receiver durably accepted
               (``ok``) or refused the replicated root; ``root`` is its accepted root;
               ``stored`` tells that the records the replication carried are durable here
               (a data replica, D54), sent only after they are.
``query``      ``rid``, ``ballot``: ask a peer for the roots it knows (recovery, rejoin).  With
               ``ballot > 0`` (a rejoin of an empty acceptor log, ruling R78a, protocol 10) a
               witness first makes a promise of ``ballot`` durable -- it then refuses every
               lower term and every root labelled below it -- and answers after it.
``answer``     ``rid``, ``root``, ``committed``, ``certain``, ``promised``, ``state``: the peer's
               accepted and committed roots and the highest lease term it has seen in a
               request or release, promised, or found as its accepted root's label (the
               committed root's label is not folded in: it travels in ``committed``);
               ``state`` is ``"ok"`` (a witness), ``"empty"`` (it is rejoining) or
               ``"unknown"`` (no acceptor log loaded: the roots are ``None`` and the answer
               never counts).  ``ballot`` echoes the query's when the answer follows a durable
               promise of it, else ``0``.
``fetch``      ``rid``, ``inc``, ``segment``, ``offset``: a peer page request for one record.
``fetched``    ``rid``, ``inc``, ``segment``, ``offset``, ``data``: the framed record bytes at
               ``segment``/``offset`` (echoed), or ``None`` when the peer lacks it.  The asker
               takes it only for the incarnation and the pointer it asked (protocol 12).
``warm``       ``term``, ``records``: records of a commit sent to a member that is not one of
               its data replicas, which keeps them in its record cache only: no append, no
               sync, no acknowledgement (ruling R89, protocol 12).  Best effort: a member that
               misses one fetches the records on demand.
``commit_query`` ``rid``, ``root``: where are the records of the commit of ``root``?
``commit_info``  ``rid``, ``persisted``, ``ranges``: ``persisted`` is True when the peer knows a
               persisted root ordering at or above ``root`` (that commit and every earlier one
               are in the object store); ``ranges`` lists ``[segment UUID bytes, start, end]``,
               the byte ranges of the records shipped with that commit, when the peer knows
               them (else ``None``).

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
           "encode_frame", "FrameDecoder", "encode_root", "decode_root", "decode_labeled_root",
           "encode_ranges", "decode_ranges", "record_ranges"]

#: Version carried by every message; a receiver drops messages of any other version.
#: 6: the framed record bytes carried by ``replicate``, ``records`` and ``fetched`` use the
#: version 2 record format (self-validating headers, D46 fix round 13).
#: 7: ``release`` carries the fairness hints ``next`` and ``more``, and the ``want`` message
#: (Task 8.3e, D46).
#: 8: write permissions (D50).
#: 9: ``permit`` carries the requester's incarnation ``inc`` (8.3f review, C1).
#: 10: ``query`` and ``answer`` carry ``ballot`` (clock-free rejoin, ruling R78a).
#: 11: bounded replication (Task 8.3g, D54): ``request``, ``renew``, ``query`` and ``replicate``
#: carry ``vs``, the fingerprint of the sender's voter set; ``replicate`` and ``done`` carry the
#: commit's ``copies`` (the members holding its records durably) and byte ``ranges``; ``ack``
#: carries ``stored``.
#: 12: every request a reply answers carries the requester's incarnation ``inc``
#: (``replicate``, ``records``, ``query``, ``fetch``, ``commit_query``; ``request`` and ``renew``
#: already did) and every reply echoes it (``grant``, ``reject``, ``ack``, ``answer``,
#: ``fetched``, ``commit_info``); ``fetched`` also echoes the ``segment`` and ``offset`` it
#: answers.  A reply to another incarnation, or about another record, is dropped: request ids
#: restart with every incarnation (Task 8.3g review, C1 and C2).
#: Also 12: ``warm`` carries a commit's records to the members that are not its data
#: replicas, for their record caches only (ruling R89).
#: 13: pipelined permissions (Task 8.3k, D55): the ``propose`` message, ``permit.after``;
#: ``replicate`` loses ``pred_committed`` (voters accept a same-term root only by extension of
#: what they hold, rule V2').
PROTOCOL_VERSION = 13
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
_OPT_STR = "optional str"
_RANGES = "ranges"
_STRS = "list of str"
_AFTER = "optional [seq, holder]"

#: Fields of each message type (besides ``v``, ``type`` and ``from``).
MESSAGE_TYPES = {
    "request": {"term": _INT, "rid": _INT, "inc": _INT, "vs": _INT},
    "grant": {"term": _INT, "rid": _INT, "inc": _INT, "root": _ROOT, "committed": _ROOT, "certain": _BOOL},
    "reject": {"term": _INT, "rid": _INT, "inc": _INT, "root": _ROOT, "certain": _BOOL, "reason": _STR,
               "highest": _INT, "remaining": _NUM},
    "release": {"term": _INT, "inc": _INT, "root": _ROOT, "committed": _ROOT, "certain": _BOOL,
                "held": _BOOL, "next": _OPT_STR, "more": _BOOL},
    "records": {"term": _INT, "rid": _INT, "inc": _INT, "index": _INT, "records": _RECORDS},
    "warm": {"term": _INT, "records": _RECORDS},
    "replicate": {"term": _INT, "rid": _INT, "inc": _INT, "root": _ROOT, "committed": _ROOT, "records": _RECORDS,
                  "chunks": _INT, "base": _ROOT, "base_ranges": _RANGES, "seq": _INT, "pred": _ROOT,
                  "vs": _INT, "copies": _STRS, "ranges": _RANGES},
    "propose": {"term": _INT, "seq": _INT, "inc": _INT, "root": _ROOT, "pred": _ROOT},
    "renew": {"term": _INT, "rid": _INT, "inc": _INT, "vs": _INT},
    "permit_req": {"term": _INT, "rid": _INT, "inc": _INT},
    "permit": {"term": _INT, "rid": _INT, "seq": _INT, "inc": _INT, "root": _ROOT, "certain": _BOOL,
               "valid": _NUM, "after": _AFTER},
    "permit_no": {"term": _INT, "rid": _INT, "inc": _INT, "reason": _STR},
    "done": {"term": _INT, "seq": _INT, "inc": _INT, "outcome": _STR, "root": _ROOT, "copies": _STRS,
             "ranges": _RANGES},
    "ack": {"term": _INT, "rid": _INT, "inc": _INT, "ok": _BOOL, "root": _ROOT, "stored": _BOOL},
    "want": {},
    "query": {"rid": _INT, "inc": _INT, "ballot": _INT, "vs": _INT},
    "answer": {"rid": _INT, "inc": _INT, "root": _ROOT, "committed": _ROOT, "certain": _BOOL, "promised": _INT,
               "state": _STR, "ballot": _INT},
    "fetch": {"rid": _INT, "inc": _INT, "segment": _UUID, "offset": _INT},
    "fetched": {"rid": _INT, "inc": _INT, "segment": _UUID, "offset": _INT, "data": _OPT_BYTES},
    "commit_query": {"rid": _INT, "inc": _INT, "root": _ROOT},
    "commit_info": {"rid": _INT, "inc": _INT, "persisted": _BOOL, "ranges": _RANGES},
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
    elif expected == _OPT_STR:
        ok = value is None or isinstance(value, str)
    elif expected == _AFTER:
        ok = value is None or (isinstance(value, (list, tuple)) and len(value) == 2 and type(value[0]) is int
                               and value[0] > 0 and isinstance(value[1], str))
    elif expected == _STRS:
        ok = isinstance(value, list) and all(isinstance(v, str) for v in value)
    elif expected == _RANGES:
        ok = value is None or (isinstance(value, list) and all(
            isinstance(r, (list, tuple)) and len(r) == 3 and isinstance(r[0], bytes) and len(r[0]) == 16
            and type(r[1]) is int and type(r[2]) is int and 0 <= r[1] <= r[2] for r in value))
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


def record_ranges(records: list) -> list:
    """The byte ranges ``[(segment UUID, start, end)]`` spanned, per segment, by ``records``
    (``[segment UUID bytes, offset, framed record or None]``; ``None`` items are skipped)."""
    spans = {}
    for raw, offset, data in records:
        if data is None:
            continue
        segment = uuid.UUID(bytes=raw)
        start, end = spans.get(segment, (offset, offset))
        spans[segment] = (min(start, offset), max(end, offset + len(data)))
    return [(segment, start, end) for segment, (start, end) in spans.items()]


def encode_ranges(ranges) -> Optional[list]:
    """Wire form of byte ranges ``[(segment UUID, start, end)]`` (``None`` stays ``None``)."""
    if ranges is None:
        return None
    return [[segment.bytes, start, end] for segment, start, end in ranges]


def decode_ranges(value) -> Optional[list]:
    """Inverse of :func:`encode_ranges` (the message was validated)."""
    if value is None:
        return None
    return [(uuid.UUID(bytes=segment), start, end) for segment, start, end in value]
