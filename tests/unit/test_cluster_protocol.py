"""Cluster protocol: versioned messages, root encoding and length-prefixed msgpack framing (Task 8.3c, D45)."""
import struct
import uuid

import msgpack
import pytest

from proto_db.cluster.protocol import (MAX_FRAME_BYTES, PROTOCOL_VERSION, ClusterProtocolError,
                                       FrameDecoder, decode_labeled_root, decode_message, decode_root, encode_frame,
                                       encode_root, make_message)
from proto_db.storage.format import FORMAT_VERSION, RootRecord


def _record(epoch=3):
    return RootRecord(FORMAT_VERSION, uuid.uuid4(), 64, epoch)


def test_every_message_carries_the_protocol_version_and_sender():
    msg = make_message("request", "n1", term=4, rid=7, inc=3, vs=0)
    assert msg["v"] == PROTOCOL_VERSION
    assert msg["type"] == "request" and msg["from"] == "n1"


@pytest.mark.parametrize("kind,fields", [
    ("request", dict(term=1, rid=2, inc=11, vs=7)),
    ("grant", dict(term=1, rid=2, inc=5, root=None, committed=[0, None, None, 1], certain=False)),
    ("reject", dict(term=1, rid=2, inc=5, root=[0, None, None], certain=True, reason="busy",
                    highest=5, remaining=0.25)),
    ("release", dict(term=9, inc=11, root=None, committed=[4, uuid.UUID(int=1).bytes, 64, 2], certain=False,
                     held=True, next="n2", more=True)),
    ("release", dict(term=9, inc=11, root=None, committed=None, certain=True, held=False, next=None,
                     more=False)),
    ("replicate", dict(term=3, rid=4, inc=5, root=[5, uuid.UUID(int=2).bytes, 80, 3], committed=None,
                       records=[[uuid.UUID(int=2).bytes, 80, b"framed"], [uuid.UUID(int=3).bytes, 26, None]],
                       chunks=1, base=None, base_ranges=None, seq=0, pred=None, pred_committed=False, vs=7,
                       copies=["n1"], ranges=[[uuid.UUID(int=2).bytes, 26, 90]])),
    ("replicate", dict(term=3, rid=4, inc=5, root=[5, uuid.UUID(int=2).bytes, 80, 3], committed=None, records=[],
                       chunks=0, base=[4, uuid.UUID(int=4).bytes, 64, 2],
                       base_ranges=[[uuid.UUID(int=4).bytes, 26, 90]], seq=3,
                       pred=[4, uuid.UUID(int=4).bytes, 64, 2], pred_committed=True, vs=7,
                       copies=["n1", "n4"], ranges=None)),
    ("renew", dict(term=9, rid=2, inc=11, vs=7)),
    ("permit_req", dict(term=9, rid=2, inc=11)),
    ("permit", dict(term=9, rid=2, seq=4, inc=7, root=[4, uuid.UUID(int=1).bytes, 64, 9], certain=True, valid=0.5)),
    ("permit_no", dict(term=9, rid=2, inc=5, reason="not_owner")),
    ("done", dict(term=9, seq=4, inc=11, outcome="committed", root=[5, uuid.UUID(int=1).bytes, 64, 9],
                  copies=["n1", "n2"], ranges=[[uuid.UUID(int=1).bytes, 26, 90]])),
    ("done", dict(term=9, seq=4, inc=11, outcome="none", root=None, copies=[], ranges=None)),
    ("records", dict(term=3, rid=4, inc=5, index=0, records=[[uuid.UUID(int=2).bytes, 40, b"framed"]])),
    ("ack", dict(term=3, rid=4, inc=5, ok=True, root=None, stored=False)),
    ("ack", dict(term=3, rid=4, inc=5, ok=False, root=None, stored=True)),
    ("query", dict(rid=1, inc=5, ballot=0, vs=7)),
    ("answer", dict(rid=1, inc=5, root=None, committed=[0, None, None, 0], certain=True, promised=4, state="ok", ballot=0)),
    ("fetch", dict(rid=2, inc=5, segment=uuid.UUID(int=9).bytes, offset=26)),
    ("fetched", dict(rid=2, inc=5, segment=uuid.UUID(int=9).bytes, offset=26, data=None)),
    ("fetched", dict(rid=2, inc=5, segment=uuid.UUID(int=9).bytes, offset=26, data=b"framed")),
    ("commit_query", dict(rid=3, inc=5, root=[4, uuid.UUID(int=4).bytes, 64, 2])),
    ("commit_info", dict(rid=3, inc=5, persisted=False, ranges=[[uuid.UUID(int=4).bytes, 26, 90]])),
    ("commit_info", dict(rid=3, inc=5, persisted=True, ranges=None)),
])
def test_message_roundtrip_through_a_frame(kind, fields):
    msg = make_message(kind, "peer-a", **fields)
    decoder = FrameDecoder()
    [out] = decoder.feed(encode_frame(msg))
    assert out == msg


def test_copies_must_be_a_list_of_member_ids():
    root = [4, uuid.UUID(int=4).bytes, 64, 2]
    for bad in (None, "n1", [1], [b"n1"]):
        with pytest.raises(ClusterProtocolError):
            make_message("done", "n1", term=1, seq=1, inc=1, outcome="committed", root=root, copies=bad, ranges=None)


def test_ranges_must_be_segment_start_end_triples():
    root = [4, uuid.UUID(int=4).bytes, 64, 2]
    for bad in ([[b"short", 1, 2]], [[uuid.UUID(int=4).bytes, 9, 2]], [[uuid.UUID(int=4).bytes, -1, 2]], "x"):
        with pytest.raises(ClusterProtocolError):
            make_message("commit_info", "n1", rid=1, persisted=False, ranges=bad)
    with pytest.raises(ClusterProtocolError):
        make_message("replicate", "n1", term=1, rid=1, root=root, committed=None, records=[], chunks=0,
                     base=root, base_ranges=[[b"short", 1, 2]])


def test_release_fairness_hints_are_typed():
    base = dict(term=9, inc=11, root=None, committed=None, certain=True, held=True)
    for bad in (dict(next=3, more=True), dict(next=b"n2", more=True), dict(next="n2", more=1), dict(next="n2")):
        with pytest.raises(ClusterProtocolError):
            make_message("release", "n1", **base, **bad)


def test_make_message_rejects_unknown_types_and_missing_fields():
    with pytest.raises(ClusterProtocolError):
        make_message("vote", "n1", term=1)
    with pytest.raises(ClusterProtocolError):
        make_message("request", "n1", term=1)          # rid missing
    with pytest.raises(ClusterProtocolError):
        make_message("request", "n1", term=-1, rid=1)  # negative term


def test_decode_rejects_another_protocol_version():
    msg = make_message("request", "n1", term=1, rid=1, inc=0, vs=0)
    msg["v"] = PROTOCOL_VERSION + 1
    with pytest.raises(ClusterProtocolError, match="version"):
        decode_message(msgpack.packb(msg, use_bin_type=True))


def test_decode_rejects_garbage_and_non_maps():
    with pytest.raises(ClusterProtocolError):
        decode_message(b"\xc1\xc1\xc1")
    with pytest.raises(ClusterProtocolError):
        decode_message(msgpack.packb([1, 2, 3]))


def test_frames_split_across_chunks_and_coalesced_in_one_chunk():
    msgs = [make_message("request", "n1", term=t, rid=t, inc=0, vs=0) for t in range(1, 6)]
    data = b"".join(encode_frame(m) for m in msgs)
    decoder = FrameDecoder()
    out = []
    for i in range(0, len(data), 3):     # three bytes at a time
        out.extend(decoder.feed(data[i:i + 3]))
    assert out == msgs
    assert FrameDecoder().feed(data) == msgs


def test_an_oversized_frame_is_rejected_before_it_is_buffered():
    decoder = FrameDecoder()
    with pytest.raises(ClusterProtocolError, match="frame"):
        decoder.feed(struct.pack(">I", MAX_FRAME_BYTES + 1))


def test_root_encoding_distinguishes_unknown_empty_and_a_record():
    assert encode_root(None, known=False) is None
    assert decode_root(None) == (False, None)
    assert decode_root(encode_root(None, known=True)) == (True, None)
    rec = _record()
    assert decode_root(encode_root(rec, known=True)) == (True, rec)


def test_roots_carry_their_label_term():
    rec = _record(7)
    assert decode_labeled_root(encode_root(rec, known=True, term=5)) == (True, rec, 5)
    assert decode_labeled_root(encode_root(None, known=True, term=2)) == (True, None, 2)
    assert decode_labeled_root([7, rec.segment.bytes, 64]) == (True, rec, 0)     # unlabelled: term 0
    with pytest.raises(ClusterProtocolError):
        decode_labeled_root([7, rec.segment.bytes, 64, -1])


def test_replicate_records_are_validated():
    with pytest.raises(ClusterProtocolError):
        make_message("replicate", "n1", term=1, rid=1, root=None, committed=None, records=[[b"short", 1, None]])
    with pytest.raises(ClusterProtocolError):
        make_message("fetch", "n1", rid=1, segment=b"x", offset=0)


def test_decode_root_rejects_malformed_values():
    with pytest.raises(ClusterProtocolError):
        decode_root([1, b"short", 3])
    with pytest.raises(ClusterProtocolError):
        decode_root([0, uuid.uuid4().bytes, 3])     # epoch 0 has no pointer
    with pytest.raises(ClusterProtocolError):
        decode_root("x")
