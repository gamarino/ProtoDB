"""The mark of the precondition concurrency model (spec R50-R52, R63; section 12 item 9).

Every root record this revision publishes is format version 7 (feature ``preconditions``), whatever
the data it reaches; segments keep the lowest version their records need.  A space written by the
previous software (format 6 and earlier) opens without migration, and its first commit under this
model marks it: the upgrade is one-way, since a reader limited to versions 2 to 6 refuses a
version-7 root.  The cluster protocol rises to 16 (R51).
"""
import hashlib
import json
import shutil
from pathlib import Path
from unittest import mock

import pytest

from proto_db.cluster import protocol
from proto_db.errors import ProtoNotSupportedError
from proto_db.storage import format as fmt
from proto_db.storage.file import FileStorage
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from tests.conformance.generate import describe_database

FROZEN = Path(__file__).resolve().parents[1] / "conformance" / "frozen"
V6 = json.loads((FROZEN / "v6-catalog.json").read_text())


def _root_version(directory: Path) -> int:
    """The version written in the bytes of ``space.root`` (not the decoded floor)."""
    import msgpack
    return msgpack.unpackb((directory / "space.root").read_bytes()[4:])["version"]


def _v6_copy(tmp_path) -> Path:
    work = tmp_path / "v6"
    shutil.copytree(FROZEN / "v6-catalog", work)
    return work


def test_constants():
    assert fmt.FORMAT_VERSION == 7
    assert fmt.FEATURE_VERSIONS["preconditions"] == 7
    assert fmt.ROOT_WRITE_VERSION == 7
    assert fmt.READABLE_FORMAT_VERSIONS == frozenset({2, 3, 4, 5, 6, 7})
    assert fmt.WRITE_FORMAT_VERSION == 4            # segments keep their lowest sufficient version


def test_new_root_is_version_7(tmp_path):
    """Every root is version 7, the first one included, on every storage that publishes roots;
    a space whose data uses no later feature keeps version-4 segments."""
    memory = MemoryStorage()
    space = ObjectSpace(memory)
    try:
        space.new_database("db").run(lambda tr: tr.set_root_object("n", 1))
        assert memory.read_root().version == 7
    finally:
        space.close()
    directory = tmp_path / "file"
    space = ObjectSpace(FileStorage(str(directory)))
    try:
        db = space.new_database("db")
        assert _root_version(directory) == 7
        db.run(lambda tr: tr.set_root_object("n", 1))
        assert _root_version(directory) == 7
    finally:
        space.close()
    headers = {int.from_bytes(p.read_bytes()[8:10], "little") for p in directory.glob("*.seg")}
    assert headers == {4}


def test_root_version_is_never_lower_than_7():
    """``next_root_version`` floors at 7 whatever the root it replaces (one-way upgrade)."""
    class Storage:
        written_version = 4
    old = fmt.RootRecord(4, fmt.uuid.UUID(int=1), 26, 3)
    assert fmt.next_root_version(None, Storage()) == 7
    assert fmt.next_root_version(old, Storage()) == 7


def test_frozen_v6_fixture_is_unchanged():
    """The frozen space is the bytes the previous software wrote (never regenerated)."""
    files = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((FROZEN / "v6-catalog").iterdir())}
    assert files == V6["files"]
    assert _root_version(FROZEN / "v6-catalog") == V6["root_version"] == 6


def test_space_written_by_rc4_opens_without_migration(tmp_path):
    """R52: a version-6 space opens and reads as written, and reading writes nothing; its first
    commit publishes a version-7 root and leaves every existing segment byte unchanged (no
    migration: the persisted content of collections and roots does not change)."""
    work = _v6_copy(tmp_path)
    before = {p.name: p.read_bytes() for p in work.glob("*.seg")}
    space = ObjectSpace(FileStorage(str(work)))
    try:
        db = space.open_database("db")
        assert db.run(describe_database) == V6["contents"]
        assert _root_version(work) == 6                     # a read publishes nothing
        db.run(lambda tr: tr.set_root_object("v7_mark", 1))
        assert _root_version(work) == 7
        assert db.run(lambda tr: tr.get_root_object("v7_mark")) == 1
    finally:
        space.close()
    for name, data in before.items():
        assert (work / name).read_bytes()[:len(data)] == data, f"segment {name} was rewritten"
    space = ObjectSpace(FileStorage(str(work)))
    try:
        mark, contents = space.open_database("db").run(
            lambda tr: (tr.get_root_object("v7_mark"), describe_database(tr)))
    finally:
        space.close()
    assert mark == 1
    # Every root the previous software wrote reads back as written; the commit dropped only the
    # leftover __intentLog of the removed intents mode (R55) and added the test's root.
    written = {name: desc for name, desc in V6["contents"]["roots"] if name != "__intentLog"}
    read = {name: desc for name, desc in contents["roots"] if name != "v7_mark"}
    assert read == written
    assert {k: v for k, v in contents.items() if k != "roots"} == \
        {k: v for k, v in V6["contents"].items() if k != "roots"}


def test_writer_without_version_7_rejected(tmp_path):
    """Section 12 item 9: software without the model (a reader of versions 2 to 6) refuses a
    version-7 root, as a decoded record and as a space it opens; a version-6 root still opens there."""
    directory = tmp_path / "space"
    space = ObjectSpace(FileStorage(str(directory)))
    try:
        space.new_database("db").run(lambda tr: tr.set_root_object("n", 1))
    finally:
        space.close()
    data = (directory / "space.root").read_bytes()
    with mock.patch.object(fmt, "READABLE_FORMAT_VERSIONS", frozenset({2, 3, 4, 5, 6})):
        with pytest.raises(ProtoNotSupportedError):
            fmt.decode_root(data)
        with pytest.raises(ProtoNotSupportedError):
            FileStorage(str(directory)).read_root()
        assert fmt.decode_root((FROZEN / "v6-catalog" / "space.root").read_bytes()).version == 6
    assert fmt.decode_root(data).version == 7


def test_v7_root_round_trips_on_the_wire():
    """A version-7 root travels with its version (the fifth element of a wire root)."""
    record = fmt.RootRecord(7, fmt.uuid.UUID(int=5), 26, 9)
    wire = protocol.encode_root(record, known=True, term=3)
    assert wire[4] == 7
    known, decoded, term = protocol.decode_labeled_root(wire)
    assert known and term == 3 and decoded == record and decoded.version == 7


def test_protocol_version_16():
    """R51: every node of a cluster applies the same validation; a protocol-15 message is refused."""
    assert protocol.PROTOCOL_VERSION == 16
    msg = protocol.make_message("query", "n0", rid=1, inc=1, ballot=1, vs=0)
    assert msg["v"] == 16
    with pytest.raises(protocol.ClusterProtocolError):
        protocol.validate_message(dict(msg, v=15))
