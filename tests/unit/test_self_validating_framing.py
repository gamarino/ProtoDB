"""Self-validating record and frame headers; framing-driven, linear torn-tail classification
(8.3d re-review 12, I3; ruling R60).

Round 12 bounded the own-segment search by a candidate count, so an ordinary crash in the middle
of a blob of small integers (the reviewer's int32-label payload: almost one candidate per
element) exhausted the budget at 8 MiB and refused to open.  Record headers (format version 2)
and log frame headers (log format version 2) now carry a CRC over the header itself: a torn
record whose header is valid has a trustworthy extent, and the search -- run only when that
cannot settle the case -- is exact and linear with no cap.  An ordinary torn tail is always
trimmed; mid-log corruption still raises.
"""
import array
import os
import random
import struct
import time
import uuid
import zlib

import pytest

from proto_db.errors import ProtoCorruptionError, ProtoNotSupportedError
from proto_db.storage import crc as crc_module
from proto_db.storage import logfile
from proto_db.storage import tiered as tiered_module
from proto_db.storage.format import (RECORD_HEADER_SIZE, RECORD_MAGIC, encode_record, encode_segment_header,
                                     record_header)
from proto_db.storage.tiered import TieredStorage

pytestmark = [pytest.mark.timeout(300)]


def _forged_header(kind: int, length: int, payload_crc: int) -> bytes:
    """A record header with a valid header CRC (its payload need not match)."""
    body = struct.pack("<2sBII", RECORD_MAGIC, kind, length, payload_crc)
    return body + struct.pack("<I", zlib.crc32(body))


def _forged_frame_header(length: int, payload_crc: int) -> bytes:
    body = struct.pack("<2sII", logfile.FRAME_MAGIC, length, payload_crc)
    return body + struct.pack("<I", zlib.crc32(body))


def _segment(tmp_path, tail: bytes):
    """An own segment holding one valid record followed by ``tail``; returns ``(path, uuid,
    valid size, contents)``."""
    seg = uuid.uuid4()
    path = str(tmp_path / (seg.hex + ".seg"))
    good = encode_segment_header(seg) + encode_record(1, b"atom")
    data = good + tail
    with open(path, "wb") as f:
        f.write(data)
    return path, seg, len(good), data


def _assert_trimmed(path, seg, good, bound=None):
    t0 = time.monotonic()
    assert TieredStorage._repair_own_segment(path, seg) == good
    elapsed = time.monotonic() - t0
    assert os.path.getsize(path) == good
    if bound is not None:
        assert elapsed < bound, f"{elapsed:.1f} s"
    return elapsed


def _assert_refused(path, seg, data, match):
    with pytest.raises(ProtoCorruptionError, match=match):
        TieredStorage._repair_own_segment(path, seg)
    with open(path, "rb") as f:
        assert f.read() == data, "a refused repair changed the file"


# -- the exact search -----------------------------------------------------------------------------

def _brute_first_valid(data, start, size):
    for pos in range(start, size - RECORD_HEADER_SIZE + 1):
        if tiered_module._record_length(data, pos, size) is not None:
            return pos
    return None


def test_crc_shift_matches_zlib_crc32_combine():
    rng = random.Random(7)
    for _ in range(300):
        a = rng.randbytes(rng.randint(0, 200))
        b = rng.randbytes(rng.choice([0, 1, 5, rng.randint(0, 70000)]))
        assert crc_module.crc_shift(zlib.crc32(a), len(b)) ^ zlib.crc32(b) == zlib.crc32(a + b)


def test_the_candidate_pattern_matches_exactly_the_magic_before_a_kind_byte():
    probe = bytes(x for a in (0xC1, 0x00, 0xD2) for b in (0xD2, 0xC1, 0x01) for k in range(4)
                  for x in (a, b, k))
    hits = {m.start() for m in tiered_module._CANDIDATE.finditer(probe)}
    assert hits == {i for i in range(len(probe) - 2)
                    if probe[i:i + 2] == RECORD_MAGIC and probe[i + 2] in (1, 2)}
    assert hits


def _region(rng) -> bytes:
    parts = []
    for _ in range(rng.randint(0, 5)):
        c = rng.random()
        if c < 0.25:
            parts.append(encode_record(rng.choice([1, 2]), rng.randbytes(rng.randint(0, 2000))))
        elif c < 0.4:
            parts.append(encode_record(rng.choice([1, 2]), rng.randbytes(rng.randint(0, 3000)))[:-rng.randint(1, 50)])
        elif c < 0.55:
            parts.append((RECORD_MAGIC + bytes([rng.choice([1, 2])])) * rng.randint(1, 100))
        elif c < 0.7:
            parts.append(bytes([rng.choice([0, 1, 2, 0xC1, 0xD2, 255])]) * rng.randint(1, 400))
        elif c < 0.85:
            # header-valid candidates with a payload CRC that does not match (or by chance does)
            length = rng.randint(0, 5000)
            parts.append(_forged_header(rng.choice([1, 2]), length, rng.getrandbits(32)))
        else:
            parts.append(rng.randbytes(rng.randint(1, 2000)))
    data = b"".join(parts)
    if len(data) > 30 and rng.random() < 0.3:
        p = rng.randrange(len(data) - 20)
        rest = len(data) - p - RECORD_HEADER_SIZE
        payload = data[p + RECORD_HEADER_SIZE:p + RECORD_HEADER_SIZE + max(0, rest)]
        crc = zlib.crc32(payload) if rng.random() < 0.5 else rng.getrandbits(32)
        data = data[:p] + _forged_header(rng.choice([1, 2]), max(0, rest), crc) + data[p + RECORD_HEADER_SIZE:]
    return data


@pytest.mark.parametrize("checkpoint,direct", [(7, 0), (64, 16), (4096, 16384)])
def test_the_search_equals_trying_every_offset(monkeypatch, checkpoint, direct):
    """Fuzz: the linear search answers exactly as trying :func:`_record_length` at every offset,
    on regions mixing valid, torn and forged records, dense candidate markers and random bytes,
    with checkpoint settings that exercise the ``crc32_combine`` path."""
    monkeypatch.setattr(tiered_module, "_CRC_CHECKPOINT", checkpoint)
    monkeypatch.setattr(tiered_module, "_CRC_DIRECT", direct)
    rng = random.Random(checkpoint)
    found = 0
    for _ in range(400):
        data = _region(rng)
        start = rng.randint(0, len(data))
        expected = _brute_first_valid(data, start, len(data))
        found += expected is not None
        assert tiered_module._first_valid_record(data, start, len(data)) == expected
    assert found > 30


def _brute_classify(data: bytes, size_good: int):
    """Reference classification of an own segment's tail, by brute force (D46 item 6 as
    amended by fix round 13): ``("ok",)``, ``("trim", pos)`` or ``("corrupt", pos, later)``."""
    pos = size_good
    size = len(data)
    while pos < size:
        n = tiered_module._record_length(data, pos, size)
        if n is None:
            break
        pos += n
    if pos == size:
        return ("ok",)
    header = record_header(data, pos)
    if header is None:
        later = _brute_first_valid(data, pos + 1, size)
    elif pos + RECORD_HEADER_SIZE + header[1] > size:
        later = None
    else:
        later = _brute_first_valid(data, pos + RECORD_HEADER_SIZE + header[1], size)
    return ("trim", pos) if later is None else ("corrupt", pos, later)


def test_the_repair_classifies_like_the_brute_force_reference(tmp_path):
    rng = random.Random(13)
    outcomes = set()
    for i in range(300):
        tail = _region(rng)
        path, seg, good, data = _segment(tmp_path, tail)
        expected = _brute_classify(data, good)
        outcomes.add(expected[0])
        if expected[0] == "corrupt":
            _assert_refused(path, seg, data, f"offset {expected[1]} is invalid but a valid record follows at "
                                             f"offset {expected[2]}")
        else:
            end = len(data) if expected[0] == "ok" else expected[1]
            assert TieredStorage._repair_own_segment(path, seg) == end
            assert os.path.getsize(path) == end
        os.remove(path)
    assert outcomes == {"ok", "trim", "corrupt"}


# -- ordinary torn tails are always trimmed, fast -------------------------------------------------

def _int32_labels(mib: int) -> bytes:
    """A little-endian int32 array of ``mib`` MiB holding labels in {0, 1, 2}."""
    n = mib * 1024 * 1024 // 4
    labels = random.Random(1).randbytes(n).translate(bytes(i % 3 for i in range(256)))
    data = bytearray(4 * n)
    data[0::4] = labels
    assert array.array("i", data[:400]).tolist() == list(labels[:100])
    return bytes(data)


@pytest.mark.parametrize("mib", [8, 64])
def test_the_reviewers_int32_label_torn_blob_is_trimmed_automatically_and_fast(tmp_path, mib):
    """The reviewer's case (re-review 12, I3): a torn blob of little-endian int32 labels in
    {0, 1, 2}.  Round 12 gave up at 8 MiB and refused to open; the header is valid and the blob
    extends past the end of the file, so it is the writer's torn record."""
    payload = _int32_labels(mib)
    torn = encode_record(2, payload + payload)[:RECORD_HEADER_SIZE + len(payload)]
    path, seg, good, _ = _segment(tmp_path, torn)
    _assert_trimmed(path, seg, good, bound=30)


@pytest.mark.parametrize("mib", [8, 64])
def test_an_int32_label_blob_whose_header_is_torn_is_trimmed_after_a_linear_search(tmp_path, mib):
    """The same blob with its header lost (a power loss that kept later pages): the extent is
    unknown, so every later offset is searched -- linearly, with no cap."""
    payload = _int32_labels(mib)
    torn = bytes(RECORD_HEADER_SIZE) + payload
    path, seg, good, _ = _segment(tmp_path, torn)
    _assert_trimmed(path, seg, good, bound=60)


def test_a_torn_random_blob_tail_is_trimmed_quickly(tmp_path):
    torn = encode_record(2, os.urandom(64 << 20))[:-(1 << 20)]
    path, seg, good, _ = _segment(tmp_path, torn)
    _assert_trimmed(path, seg, good, bound=30)


def test_a_region_dense_with_candidate_markers_is_searched_linearly(tmp_path):
    """Worst case for the candidate filter: 8 MiB of record magic and kind bytes after a lost
    header; each candidate costs a header CRC, none has a valid header."""
    torn = bytes(RECORD_HEADER_SIZE) + (RECORD_MAGIC + b"\x01") * (8 * 1024 * 1024 // 3)
    path, seg, good, _ = _segment(tmp_path, torn)
    _assert_trimmed(path, seg, good, bound=120)


def test_forged_header_valid_candidates_cost_bounded_crc_work(tmp_path):
    """Worst case for the payload check: thousands of header-valid candidates each claiming the
    rest of a 16 MiB region, with payload CRCs that do not match.  Each costs at most two
    checkpoint intervals of CRC (``crc32_combine``), not its claimed payload."""
    filler = os.urandom(16 << 20)
    region = bytearray(filler)
    step = len(region) // 4000
    for i in range(4000):
        p = i * step
        region[p:p + RECORD_HEADER_SIZE] = _forged_header(2, len(region) - p - RECORD_HEADER_SIZE - 1, 0)
    torn = bytes(RECORD_HEADER_SIZE) + bytes(region)
    path, seg, good, _ = _segment(tmp_path, torn)
    _assert_trimmed(path, seg, good, bound=120)


def test_a_torn_blob_that_embeds_a_record_is_trimmed_when_its_header_is_valid(tmp_path):
    """Round 12 refused this case as ambiguous; the valid header proves the embedded record is
    the blob's content."""
    inner = encode_record(1, b"an embedded record")
    torn = encode_record(2, b"\x00" * 100 + inner + os.urandom(5000))[:-1000]
    path, seg, good, _ = _segment(tmp_path, torn)
    _assert_trimmed(path, seg, good)


def test_a_partial_record_header_is_trimmed(tmp_path):
    for cut in range(1, RECORD_HEADER_SIZE):
        path, seg, good, _ = _segment(tmp_path, encode_record(1, b"x" * 50)[:cut])
        _assert_trimmed(path, seg, good)
        os.remove(path)


def test_a_complete_record_with_a_damaged_payload_and_nothing_valid_after_is_trimmed(tmp_path):
    rec = bytearray(encode_record(2, os.urandom(3000)))
    rec[RECORD_HEADER_SIZE + 100] ^= 0x01
    path, seg, good, _ = _segment(tmp_path, bytes(rec) + bytes(4096) + encode_record(1, b"y" * 200)[:-5])
    _assert_trimmed(path, seg, good)


# -- mid-segment corruption still raises ---------------------------------------------------------

def test_a_damaged_payload_followed_by_a_valid_record_raises(tmp_path):
    rec = bytearray(encode_record(2, os.urandom(3000)))
    rec[RECORD_HEADER_SIZE + 100] ^= 0x01
    path, seg, good, data = _segment(tmp_path, bytes(rec) + encode_record(1, b"later"))
    _assert_refused(path, seg, data, r"valid record follows at offset \d+ \(not a torn tail; nothing was "
                                     r"truncated\)$")


@pytest.mark.parametrize("byte", range(RECORD_HEADER_SIZE))
def test_a_damaged_header_followed_by_a_valid_record_raises(tmp_path, byte):
    """Any damaged header byte -- magic, kind, length, either CRC -- makes the extent unknown:
    the later valid record is found, and the message says the case is ambiguous."""
    rec = bytearray(encode_record(2, os.urandom(3000)))
    rec[byte] ^= 0x10
    path, seg, good, data = _segment(tmp_path, bytes(rec) + encode_record(1, b"later"))
    _assert_refused(path, seg, data, "ambiguous")


def test_a_valid_record_inside_a_torn_record_with_a_damaged_header_is_ambiguous(tmp_path):
    inner = encode_record(1, b"an embedded record")
    torn = bytearray(encode_record(2, b"\x00" * 100 + inner + os.urandom(5000))[:-1000])
    torn[0:RECORD_HEADER_SIZE] = bytes(RECORD_HEADER_SIZE)
    path, seg, good, data = _segment(tmp_path, bytes(torn))
    _assert_refused(path, seg, data, "ambiguous")


# -- framed logs: the same framing -----------------------------------------------------------------

def _brute_first_frame(data, start, size):
    for pos in range(start, size - logfile.FRAME_OVERHEAD + 1):
        if logfile._frame_at(data, pos, size) is not None:
            return pos
    return None


@pytest.mark.parametrize("checkpoint,direct", [(7, 0), (64, 16), (4096, 16384)])
def test_the_frame_search_equals_trying_every_offset(monkeypatch, checkpoint, direct):
    """Fuzz: the linear frame search answers exactly as trying :func:`_frame_at` at every
    offset, with checkpoint settings that exercise the ``crc32_combine`` path (re-review 13, N1)."""
    monkeypatch.setattr(logfile, "_CRC_CHECKPOINT", checkpoint)
    monkeypatch.setattr(logfile, "_CRC_DIRECT", direct)
    rng = random.Random(5 + checkpoint)
    found = 0
    for _ in range(400):
        parts = []
        for _ in range(rng.randint(0, 5)):
            c = rng.random()
            if c < 0.3:
                parts.append(logfile.encode_frame(rng.randbytes(rng.randint(1, 500))))
            elif c < 0.45:
                parts.append(logfile.encode_frame(rng.randbytes(rng.randint(1, 500)))[:-rng.randint(1, 20)])
            elif c < 0.6:
                parts.append(logfile.FRAME_MAGIC * rng.randint(1, 100))
            elif c < 0.8:
                parts.append(_forged_frame_header(rng.randint(1, 3000), rng.getrandbits(32)))
            else:
                parts.append(rng.randbytes(rng.randint(1, 500)))
        data = b"".join(parts)
        if len(data) > 30 and rng.random() < 0.3:
            # a header-valid frame whose extent fits, with a matching or a wrong payload CRC
            p = rng.randrange(len(data) - 20)
            rest = len(data) - p - logfile.FRAME_OVERHEAD
            if rest > 0:
                payload = data[p + logfile.FRAME_OVERHEAD:]
                crc = zlib.crc32(payload) if rng.random() < 0.5 else rng.getrandbits(32)
                data = data[:p] + _forged_frame_header(rest, crc) + payload
        start = rng.randint(0, len(data))
        expected = _brute_first_frame(data, start, len(data))
        found += expected is not None
        assert logfile._first_valid_frame(data, start, len(data)) == expected
    assert found > 30


def test_forged_header_valid_frames_are_searched_linearly():
    """Re-review 13, N1: a torn tail of forged header-valid frames, each claiming half the tail
    (so every extent fits and every payload CRC must be checked), took 119 s at 4 MiB when each
    payload was CRC-ed in full.  With prefix CRCs it is linear; the classification is unchanged
    (a torn tail, trimmed to the last valid frame)."""
    for mib in (1, 4):
        size = mib * 1024 * 1024
        forged = _forged_frame_header(size // 2, 0)
        head = _log([b"x"])
        data = head + b"\x00" + forged * (size // len(forged))
        t0 = time.monotonic()
        result = logfile.scan(data, b"TEST", "x")
        elapsed = time.monotonic() - t0
        assert result.payloads == [b"x"] and result.torn and result.valid_end == len(head)
        assert elapsed < 20, f"{mib} MiB: {elapsed:.1f} s"


def _log(payloads, tail=b""):
    return logfile.log_contents(b"TEST", payloads) + tail


def test_a_frame_with_a_valid_header_cut_short_is_a_torn_tail():
    frame = logfile.encode_frame(b"p" * 400)
    for cut in range(1, len(frame)):
        data = _log([b"a", b"b"], frame[:cut])
        result = logfile.scan(data, b"TEST", "x")
        assert result.payloads == [b"a", b"b"] and result.torn
        assert result.valid_end == len(_log([b"a", b"b"]))


def test_a_torn_frame_that_embeds_a_frame_is_trimmed_when_its_header_is_valid():
    inner = logfile.encode_frame(b"embedded")
    data = _log([b"a"], logfile.encode_frame(b"\x00" * 20 + inner + b"z" * 300)[:-100])
    result = logfile.scan(data, b"TEST", "x")
    assert result.payloads == [b"a"] and result.torn


def test_a_damaged_frame_payload_followed_by_a_valid_frame_raises():
    frame = bytearray(logfile.encode_frame(b"p" * 400))
    frame[logfile.FRAME_OVERHEAD + 7] ^= 0x01
    data = _log([b"a"], bytes(frame) + logfile.encode_frame(b"later"))
    with pytest.raises(ProtoCorruptionError, match="not a torn tail"):
        logfile.scan(data, b"TEST", "x")


@pytest.mark.parametrize("byte", range(logfile.FRAME_OVERHEAD))
def test_a_damaged_frame_header_followed_by_a_valid_frame_raises(byte):
    frame = bytearray(logfile.encode_frame(b"p" * 400))
    frame[byte] ^= 0x10
    data = _log([b"a"], bytes(frame) + logfile.encode_frame(b"later"))
    with pytest.raises(ProtoCorruptionError, match="not a torn tail"):
        logfile.scan(data, b"TEST", "x")


def test_a_damaged_frame_payload_with_nothing_valid_after_is_a_torn_tail():
    frame = bytearray(logfile.encode_frame(b"p" * 400))
    frame[logfile.FRAME_OVERHEAD + 7] ^= 0x01
    result = logfile.scan(_log([b"a"], bytes(frame) + b"\x00" * 50), b"TEST", "x")
    assert result.payloads == [b"a"] and result.torn


def test_a_version_1_log_is_refused():
    header = logfile.LOG_MAGIC + (1).to_bytes(2, "little") + b"TEST"
    with pytest.raises(ProtoNotSupportedError):
        logfile.scan(header, b"TEST", "x")


def test_a_hard_linked_log_is_refused_before_it_is_repaired(tmp_path):
    from proto_db.errors import ProtoLockingError
    path = str(tmp_path / "a.log")
    fd, _ = logfile.open_log(path, b"TEST", durability="fsync")
    os.write(fd, logfile.encode_frame(b"entry") + b"\x01\x02")     # a torn tail
    os.close(fd)
    os.link(path, str(tmp_path / "b.log"))
    before = open(path, "rb").read()
    with pytest.raises(ProtoLockingError, match="hard links"):
        logfile.open_log(path, b"TEST", durability="fsync")
    assert open(path, "rb").read() == before
    os.remove(str(tmp_path / "b.log"))
    fd, payloads = logfile.open_log(path, b"TEST", durability="fsync")
    os.close(fd)
    assert payloads == [b"entry"]
