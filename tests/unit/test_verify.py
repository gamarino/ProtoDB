"""``python -m proto_db.tools.verify``: checks a space another implementation wrote (Task 10.P A2).

Catches: a verifier that skips atoms it cannot reach through the public API (history versions,
index nodes), that reports success on a damaged space, or whose dump drifts from the conformance
manifest's notation.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from proto_db.core.dbobject import DBObject
from proto_db.indexing import EqualityIndex
from proto_db.indexing.keyspec import KeySpec
from proto_db.storage.file import FileStorage
from proto_db.tools import verify
from proto_db.transactions import ObjectSpace
from tests.conformance import model  # noqa: F401 - registers conformance.Invoice in this process

ROOT = Path(__file__).resolve().parents[2]
VECTORS = ROOT / "tests" / "conformance" / "vectors"
MANIFEST = json.loads((ROOT / "tests" / "conformance" / "manifest.json").read_text())


def _without_ivf(contents: dict) -> dict:
    """The manifest's database contents without ``ivf``, the generator's own addition."""
    return {k: v for k, v in contents.items() if k != "ivf"}


def _copy(name: str, tmp_path) -> Path:
    target = tmp_path / name
    shutil.copytree(VECTORS / name, target)
    return target


def _run(directory, tmp_path, *extra):
    out = tmp_path / "dump.json"
    code = verify.main([str(directory), "--out", str(out), *extra])
    return code, (json.loads(out.read_text()) if out.exists() else None)


def test_the_conformance_database_verifies_and_dumps_its_manifest_contents(tmp_path, capsys):
    code, dump = _run(_copy("database", tmp_path), tmp_path)
    assert code == 0, capsys.readouterr().err
    assert dump["databases"]["db"] == _without_ivf(MANIFEST["database"]["contents"])
    assert dump["opaque_types"] == []
    assert dump["atoms"] > 0 and dump["collections"] > 0 and dump["root"]["epoch"] == MANIFEST["database"]["root"]["epoch"]


def test_the_polyglot_space_verifies_with_its_opaque_atom(tmp_path, capsys):
    code, dump = _run(_copy("polyglot", tmp_path), tmp_path)
    assert code == 0, capsys.readouterr().err
    assert dump["databases"]["db"] == MANIFEST["polyglot_database"]["contents"]
    assert dump["opaque_types"] == ["polyglot.st.Order"]


def test_a_space_this_repository_writes_verifies(tmp_path, capsys):
    path = tmp_path / "own"
    space = ObjectSpace(FileStorage(str(path)))
    db = space.new_database("d")
    with db.new_transaction() as tr:
        tr.set_root_object("d", tr.new_dictionary().set_at("a", DBObject(x=1)).set_at("b", 2))
        tr.set_root_object("l", tr.new_list().add_index(EqualityIndex("e", KeySpec.element()))
                           .append_last(1).append_last(2))
    with db.new_transaction() as tr:
        tr.set_root_object("d", tr.get_root_object("d").remove_at("b"))
    space.close()
    code, dump = _run(path, tmp_path)
    assert code == 0, capsys.readouterr().err
    assert [name for name, _ in dump["databases"]["d"]["roots"]] == ["d", "l"]


def test_the_misfiled_legacy_space_fails_naming_each_collection(tmp_path, capsys):
    code, _ = _run(_copy("legacy", tmp_path), tmp_path)
    err = capsys.readouterr().err
    assert code == 1
    assert "ProtoMisfiledError" in err
    for root in MANIFEST["legacy_database"]["misfiled"]["roots"]:
        assert root in err, root


def test_a_corrupted_copy_fails(tmp_path, capsys):
    target = _copy("database", tmp_path)
    segment = target / MANIFEST["database"]["segment"]
    data = bytearray(segment.read_bytes())
    record = MANIFEST["database"]["records"][len(MANIFEST["database"]["records"]) // 2]
    data[record["offset"] + record["size"] - 1] ^= 0x01         # a payload byte: its CRC fails
    segment.write_bytes(bytes(data))
    code, _ = _run(target, tmp_path)
    err = capsys.readouterr().err
    assert code == 1 and "ProtoCorruptionError" in err


def test_a_directory_without_a_space_fails(tmp_path, capsys):
    assert verify.main([str(tmp_path / "nothing")]) == 2
    assert "space.root" in capsys.readouterr().err


def test_the_module_runs_from_the_command_line(tmp_path):
    target = _copy("database", tmp_path)
    result = subprocess.run([sys.executable, "-m", "proto_db.tools.verify", str(target)], cwd=ROOT,
                            capture_output=True, text=True, env={**os.environ, "PYTHONPATH": str(ROOT)})
    assert result.returncode == 0, result.stderr
    dump = json.loads(result.stdout)
    # A fresh process has no class for the application type conformance.Invoice: its records are
    # read as opaque atoms (D59), described by type name and fields, and the space verifies.
    assert dump["opaque_types"] == ["conformance.Invoice"]
    expected = json.dumps(_without_ivf(MANIFEST["database"]["contents"])).replace(
        '"object": "conformance.Invoice"', '"opaque": "conformance.Invoice"')
    assert dump["databases"]["db"] == json.loads(expected)


# -- pre-tag review: I-6 (writer-forbidden encodings), m7 (exit codes) ----------------------------

def _space_with_raw_record(path, state_bytes: bytes):
    """A space whose root ``r`` is a DBObject record whose state map is ``state_bytes`` verbatim."""
    import uuid as _uuid

    import msgpack
    from proto_db.core.atom import type_tag
    from proto_db.storage.format import RecordKind
    space = ObjectSpace(FileStorage(str(path)))
    db = space.new_database("d")
    with db.new_transaction() as tr:
        tr.set_root_object("h", 0)
    with db.new_transaction() as tr:
        head = msgpack.packb({"t": type_tag("proto_db.core.dbobject.DBObject"), "oid": _uuid.uuid4().bytes},
                             use_bin_type=True)
        payload = bytes([head[0] + 1]) + head[1:] + msgpack.packb("s") + state_bytes
        ptr = space.storage.append_record(RecordKind.ATOM, payload)
        space.storage.flush()
        tr.set_root_object("r", tr.load(ptr, "proto_db.core.dbobject.DBObject"))
    space.close()
    return path


@pytest.mark.parametrize("name, value", [
    ("non-minimal int", b"\xcd\x00\x01"),                       # 1 as uint16
    ("float 32", b"\xca\x3f\xc0\x00\x00"),                     # 1.5 as float 32
    ("non-minimal str", b"\xd9\x01a"),                         # "a" as str 8
])
def test_encodings_writers_must_not_produce_fail_the_verifier(tmp_path, capsys, name, value):
    path = _space_with_raw_record(tmp_path / "db", b"\x81\xa1n" + value)
    code, dump = _run(path, tmp_path)
    err = capsys.readouterr().err
    assert code == 1, (name, err)
    assert "non-canonical" in err
    assert dump["databases"]["d"]["roots"]                      # the dump is still written


def test_a_canonical_raw_record_verifies(tmp_path, capsys):
    path = _space_with_raw_record(tmp_path / "db", b"\x81\xa1n\x01")
    code, _ = _run(path, tmp_path)
    assert code == 0, capsys.readouterr().err


def test_a_missing_segment_is_a_failure_of_the_space_not_exit_2(tmp_path, capsys):
    target = _copy("database", tmp_path)
    (target / MANIFEST["database"]["segment"]).unlink()
    code, _ = _run(target, tmp_path)
    assert code == 1, capsys.readouterr().err


# -- re-review: I-R1 (application aliases are stored spellings), n2 (root body) ------------------

def test_data_written_before_a_supported_rename_verifies(tmp_path, capsys):
    # The re-review's p4: a record of probe.Old written by an earlier process; the class is then
    # renamed probe.New with probe.Old as an alias (D57). Its t and the reference to it keep the old
    # name, a valid spelling: the verifier must not call it non-canonical.
    path = tmp_path / "renamed"
    script = (
        "import sys\n"
        "from proto_db.core.dbobject import DBObject\n"
        "from proto_db.storage.file import FileStorage\n"
        "from proto_db.transactions import ObjectSpace\n"
        "class OldT(DBObject):\n"
        "    __type_name__ = 'probe.Old'\n"
        "space = ObjectSpace(FileStorage(sys.argv[1]))\n"
        "with space.new_database('d').new_transaction() as tr:\n"
        "    tr.set_root_object('h', DBObject(name='h', t=OldT(v=1)))\n"
        "space.close()\n")
    subprocess.run([sys.executable, "-c", script, str(path)], cwd=ROOT, check=True,
                   env={**os.environ, "PYTHONPATH": str(ROOT)})

    class NewT(DBObject):
        __type_name__ = "tests.verify.RenamedNew"
        __type_aliases__ = ("probe.Old",)

    code, dump = _run(path, tmp_path)
    assert code == 0, capsys.readouterr().err
    assert dump["opaque_types"] == []
    holder = dump["databases"]["d"]["roots"][0][1]
    assert dict(holder["attrs"])["t"]["object"] == "tests.verify.RenamedNew"


def test_an_alias_less_built_in_name_is_still_non_canonical(tmp_path, capsys):
    import uuid as _uuid

    import msgpack
    from proto_db.storage.format import RecordKind
    path = tmp_path / "db"
    space = ObjectSpace(FileStorage(str(path)))
    db = space.new_database("d")
    with db.new_transaction() as tr:
        payload = msgpack.packb({"t": "proto_db.core.dbobject.DBObject", "oid": _uuid.uuid4().bytes,
                                 "s": {"n": 1}}, use_bin_type=True)
        ptr = space.storage.append_record(RecordKind.ATOM, payload)
        space.storage.flush()
        tr.set_root_object("r", tr.load(ptr, "proto_db.core.dbobject.DBObject"))
    space.close()
    code, _ = _run(path, tmp_path)
    assert code == 1 and "non-canonical" in capsys.readouterr().err


def test_a_root_record_with_a_wide_integer_is_non_canonical(tmp_path, capsys):
    import struct
    import zlib
    target = _copy("database", tmp_path)
    root = target / "space.root"
    data = root.read_bytes()
    fields = __import__("msgpack").unpackb(data[4:], raw=False)
    body = (b"\x84" + __import__("msgpack").packb("version") + __import__("msgpack").packb(fields["version"])
            + __import__("msgpack").packb("segment") + __import__("msgpack").packb(fields["segment"], use_bin_type=True)
            + __import__("msgpack").packb("offset") + __import__("msgpack").packb(fields["offset"])
            + __import__("msgpack").packb("epoch") + b"\xce" + struct.pack(">I", fields["epoch"]))
    root.write_bytes(struct.pack("<I", zlib.crc32(body)) + body)
    code, _ = _run(target, tmp_path)
    err = capsys.readouterr().err
    assert code == 1 and "non-canonical space.root" in err
