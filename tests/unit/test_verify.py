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
