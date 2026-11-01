"""Cluster protocol: versioned messages, root encoding and length-prefixed msgpack framing (Task 8.3c, D45)."""
import struct
import uuid

import msgpack
import pytest

from proto_db.cluster.protocol import (MAX_FRAME_BYTES, PROTOCOL_VERSION, ClusterProtocolError,
                                       FrameDecoder, decode_message, decode_root, encode_frame,
                                       encode_root, make_message)
from proto_db.storage.format import FORMAT_VERSION, RootRecord


def _record(epoch=3):
    return RootRecord(FORMAT_VERSION, uuid.uuid4(), 64, epoch)


def test_every_message_carries_the_protocol_version_and_sender():
    msg = make_message("request", "n1", term=4, rid=7)
    assert msg["v"] == PROTOCOL_VERSION
    assert msg["type"] == "request" and msg["from"] == "n1"


@pytest.mark.parametrize("kind,fields", [
    ("request", dict(term=1, rid=2)),
    ("grant", dict(term=1, rid=2, root=None, certain=False)),
    ("reject", dict(term=1, rid=2, root=[0, None, None], certain=True, reason="busy",
                    highest=5, remaining=0.25)),
    ("release", dict(term=9, root=None, certain=False, held=True)),
])
def test_message_roundtrip_through_a_frame(kind, fields):
    msg = make_message(kind, "peer-a", **fields)
    decoder = FrameDecoder()
    [out] = decoder.feed(encode_frame(msg))
    assert out == msg


def test_make_message_rejects_unknown_types_and_missing_fields():
    with pytest.raises(ClusterProtocolError):
        make_message("vote", "n1", term=1)
    with pytest.raises(ClusterProtocolError):
        make_message("request", "n1", term=1)          # rid missing
    with pytest.raises(ClusterProtocolError):
        make_message("request", "n1", term=-1, rid=1)  # negative term


def test_decode_rejects_another_protocol_version():
    msg = make_message("request", "n1", term=1, rid=1)
    msg["v"] = PROTOCOL_VERSION + 1
    with pytest.raises(ClusterProtocolError, match="version"):
        decode_message(msgpack.packb(msg, use_bin_type=True))


def test_decode_rejects_garbage_and_non_maps():
    with pytest.raises(ClusterProtocolError):
        decode_message(b"\xc1\xc1\xc1")
    with pytest.raises(ClusterProtocolError):
        decode_message(msgpack.packb([1, 2, 3]))


def test_frames_split_across_chunks_and_coalesced_in_one_chunk():
    msgs = [make_message("request", "n1", term=t, rid=t) for t in range(1, 6)]
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


def test_decode_root_rejects_malformed_values():
    with pytest.raises(ClusterProtocolError):
        decode_root([1, b"short", 3])
    with pytest.raises(ClusterProtocolError):
        decode_root([0, uuid.uuid4().bytes, 3])     # epoch 0 has no pointer
    with pytest.raises(ClusterProtocolError):
        decode_root("x")
