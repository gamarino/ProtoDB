"""The step-6 acceptance of the protoBaseST handoff (Task 11; preflight F33).

protoBaseST passes when every catalog vector passes there (``README.md``, steps 13 and 14) and when this
file's two cross-language tests pass on its outputs.  They skip unless the environment names them:

* ``PROTODB_ST_STEP6_DB``: the directory of a ``FileStorage`` space written by the kit's demo (steps 1 to 3 at
  least; ``PROTODB_ST_STEP6_DATABASE`` names the database, default ``acme``);
* ``PROTODB_ST_STEP6_OUTPUT``: the file protoBaseST wrote for that database, in the form of
  ``python -m proto_db.agent.step6 --out`` (``describe`` of every entity and the rows of the step-6 queries);
* ``PROTODB_ST_CANONICAL``: the canonical form of that database's catalog protoBaseST computed.

The tests that always run prove the comparator: the expected output is stable, complete, and a one-field
difference (a changed value, a missing or an extra key) is reported, so a pass of the cross-language tests
means something.
"""
import functools
import importlib.util
import json
import os
import subprocess
import sys
import sysconfig
from pathlib import Path

import msgpack
import pytest

from proto_db.core import codec
from proto_db.agent import step6
from docs.kit.semantic import queries

REPO = Path(__file__).resolve().parents[2]


@functools.lru_cache(maxsize=None)
def _demo():
    """The kit's demo module, loaded once."""
    spec = importlib.util.spec_from_file_location("semantic_demo_t11", REPO / "docs" / "kit" / "semantic" /
                                                  "semantic_demo.py")
    demo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(demo)
    return demo


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    """A database of the kit's demo with its modules installed and its orders loaded (steps 1 to 3)."""
    demo = _demo()
    workdir = str(tmp_path_factory.mktemp("step6"))
    space = demo._space(workdir)
    try:
        space.new_database(queries.DATABASE)
    finally:
        space.close()
    demo._in_space(workdir, demo._install("acme_sales"))
    demo._in_space(workdir, demo._install("acme_delivery"))
    demo._in_space(workdir, lambda db: db.run(demo._load))
    return os.path.join(workdir, "space")


def _expected(path, out, canonical=None):
    return step6.write_expected(path, queries.DATABASE, str(out),
                                canonical_out=None if canonical is None else str(canonical))


# -- always on: the expected output and the comparator ----------------------------------------------------------

def test_step6_expected_output_is_stable(world, tmp_path):
    """Two runs over one database give identical bytes (and nothing in them depends on the run)."""
    _expected(world, tmp_path / "a.json", tmp_path / "a.mpk")
    _expected(world, tmp_path / "b.json", tmp_path / "b.mpk")
    assert (tmp_path / "a.json").read_bytes() == (tmp_path / "b.json").read_bytes()    # Python's own two runs
    assert (tmp_path / "a.mpk").read_bytes() == (tmp_path / "b.mpk").read_bytes()
    text = (tmp_path / "a.json").read_text(encoding="utf-8")
    assert json.dumps(json.loads(text), sort_keys=True, ensure_ascii=False, indent=2) + "\n" == text


def test_step6_expected_output_is_complete(world, tmp_path):
    """It describes every entity the catalog lists and answers every named step-6 query with rows."""
    out = _expected(world, tmp_path / "out.json")
    assert set(out) == {"describe", "queries"}
    assert set(out["describe"]) == {"acme.core.Company", "acme.sales.Customer", "acme.catalog.Product",
                                    "acme.sales.Order", "acme.sales.OrderLine"}
    assert all(d["type_name"] == name for name, d in out["describe"].items())
    assert set(out["queries"]) == set(queries.STEP6)
    for name, answer in out["queries"].items():
        assert answer["rows"], name
        # Only what the format specification defines is compared: no planner output, no terms.
        assert set(answer) == {"query", "rows", "version"}, name
        # The version's number is the database root's version; its ref is Python's private token.
        assert set(answer["version"]) == {"number"} and isinstance(answer["version"]["number"], int), name


def test_step6_canonical_form_is_the_catalogs(world, tmp_path):
    _expected(world, tmp_path / "out.json", tmp_path / "canonical.mpk")
    data = (tmp_path / "canonical.mpk").read_bytes()
    decoded = step6.decode_canonical(data)
    assert list(decoded) == ["entities", "module_set", "modules"]
    assert {"acme_sales", "acme_delivery"} <= set(decoded["modules"])
    assert data == step6.canonical_bytes(world, queries.DATABASE)


def test_the_comparator_accepts_equal_and_flags_a_planted_difference(world, tmp_path):
    out = _expected(world, tmp_path / "out.json")
    same = json.loads((tmp_path / "out.json").read_text(encoding="utf-8"))
    assert step6.differences(out, same) == []

    changed = json.loads(json.dumps(same))
    row = changed["queries"]["sold_in_june"]["rows"][0]["values"]
    row["quantity"] = str(int(float(row["quantity"])) + 1)
    found = step6.differences(out, changed)
    assert len(found) == 1 and found[0].startswith("queries.sold_in_june.rows[0].values.quantity: ")

    missing = json.loads(json.dumps(same))
    del missing["describe"]["acme.sales.Order"]["meaning"]
    assert step6.differences(out, missing) == ["describe.acme.sales.Order.meaning: missing in the actual output"]

    extra = json.loads(json.dumps(same))
    extra["describe"]["acme.sales.Order"]["extra"] = 1
    assert step6.differences(out, extra) == ["describe.acme.sales.Order.extra: not in the expected output"]

    shorter = json.loads(json.dumps(same))
    shorter["queries"]["customer_limits"]["rows"].pop()
    assert any("rows" in d and "length" in d for d in step6.differences(out, shorter))

    retyped = json.loads(json.dumps(same))
    retyped["describe"]["acme.sales.Order"]["kind"] = ["entity"]
    assert len(step6.differences(out, retyped)) == 1


def test_the_comparison_is_semantic_not_textual(world, tmp_path):
    """A file with other indentation, key order and escaping holds the same data: no difference; one planted
    value in such a file is found."""
    _expected(world, tmp_path / "out.json")
    data = json.loads((tmp_path / "out.json").read_text(encoding="utf-8"))
    other = tmp_path / "other.json"
    other.write_text(json.dumps(data, indent=4, ensure_ascii=True), encoding="utf-8")
    assert other.read_bytes() != (tmp_path / "out.json").read_bytes()
    assert step6.file_differences(data, str(other)) == []
    data["queries"]["widget"]["rows"][0]["type_name"] = "planted"
    assert len(step6.file_differences(data, str(other))) == 1


def test_the_canonical_comparator_flags_a_planted_difference(world, tmp_path):
    _expected(world, tmp_path / "out.json", tmp_path / "canonical.mpk")
    data = (tmp_path / "canonical.mpk").read_bytes()
    assert step6.canonical_differences(data, data) == []
    decoded = step6.decode_canonical(data)
    first = next(iter(decoded["entities"]))
    decoded["entities"][first]["meaning"] = "planted"
    planted = msgpack.packb(codec.encode_value(decoded, None), use_bin_type=True)
    found = step6.canonical_differences(data, planted)
    assert found and found[0].startswith(f"entities.{first}.meaning")
    assert step6.canonical_differences(data, data + b"\x00")       # trailing bytes are a difference


def test_the_command_line_writes_the_same_bytes(world, tmp_path):
    flags = ["-X", "gil=1"] if sysconfig.get_config_var("Py_GIL_DISABLED") and sys._is_gil_enabled() else []
    done = subprocess.run([sys.executable, *flags, "-m", "proto_db.agent.step6", "--path", world, "--database",
                           queries.DATABASE, "--out", str(tmp_path / "cli.json"), "--canonical",
                           str(tmp_path / "cli.mpk")],
                          cwd=REPO, capture_output=True, text=True, timeout=240)
    assert done.returncode == 0, done.stderr
    _expected(world, tmp_path / "api.json", tmp_path / "api.mpk")
    assert (tmp_path / "cli.json").read_bytes() == (tmp_path / "api.json").read_bytes()
    assert (tmp_path / "cli.mpk").read_bytes() == (tmp_path / "api.mpk").read_bytes()


def _python():
    flags = ["-X", "gil=1"] if sysconfig.get_config_var("Py_GIL_DISABLED") and sys._is_gil_enabled() else []
    return " ".join([sys.executable, *flags])


def test_demo_step6_without_a_command_is_external(world, tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("PROTOBASEST_STEP6_CMD", raising=False)
    demo = _demo()
    before = len(demo.CHECKS)
    demo.step_6(str(tmp_path), world)
    shown = capsys.readouterr().out
    assert "external" in shown and "protoBaseST-handoff.md" in shown and len(demo.CHECKS) == before


def test_demo_step6_runs_the_command_and_compares(world, tmp_path, monkeypatch, capsys):
    """The Python generator stands in for protoBaseST: equal output passes; a planted one-field difference
    in the command's output stops the demo."""
    demo = _demo()
    monkeypatch.setenv("PROTOBASEST_STEP6_CMD", f"{_python()} -m proto_db.agent.step6")
    demo.step_6(str(tmp_path), world)
    assert "protoBaseST's step-6 output equals" in capsys.readouterr().out
    plant = tmp_path / "plant.py"
    plant.write_text(
        f"import json, sys\nsys.path.insert(0, {str(REPO)!r})\n"
        "from proto_db.agent import step6\n"
        "out = sys.argv[sys.argv.index('--out') + 1]\n"
        "step6.main(sys.argv[1:])\n"
        "data = json.load(open(out, encoding='utf-8'))\n"
        "data['describe']['acme.sales.Order']['meaning'] += ' (planted)'\n"
        "open(out, 'w', encoding='utf-8').write(step6.text(data))\n", encoding="utf-8")
    monkeypatch.setenv("PROTOBASEST_STEP6_CMD", f"{_python()} {plant}")
    with pytest.raises(SystemExit, match="FAILED") as stopped:
        demo.step_6(str(tmp_path), world)
    assert str(stopped.value).startswith("[FAILED] protoBaseST's step-6 output equals")
    assert "describe.acme.sales.Order.meaning" in capsys.readouterr().out


# -- the cross-language acceptance: protoBaseST's outputs, skipped until they are given -------------------------

def _env(*names):
    values = [os.environ.get(name) for name in names]
    if not all(values):
        pytest.skip(f"{', '.join(names)} not set: the protoBaseST outputs are not given")
    return values


def test_protobasest_step6_output_equals_pythons(tmp_path):
    database, output = _env("PROTODB_ST_STEP6_DB", "PROTODB_ST_STEP6_OUTPUT")
    name = os.environ.get("PROTODB_ST_STEP6_DATABASE", queries.DATABASE)
    expected = step6.write_expected(database, name, str(tmp_path / "expected.json"))
    assert step6.file_differences(expected, output) == []


def test_protobasest_canonical_form_equals_pythons():
    database, canonical = _env("PROTODB_ST_STEP6_DB", "PROTODB_ST_CANONICAL")
    name = os.environ.get("PROTODB_ST_STEP6_DATABASE", queries.DATABASE)
    assert step6.canonical_differences(step6.canonical_bytes(database, name), Path(canonical).read_bytes()) == []
