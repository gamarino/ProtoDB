"""Provenance, parameter overrides and the inventory model of the performance suite (Task 9c).

* a clean ``git archive`` export (:mod:`benchmarks.export`) is recorded as clean with its commit,
  and editing or adding a file after the export makes it dirty;
* a directory that is not the top of a git work tree records no commit (an export inside the
  repository's tree must not report the repository's HEAD);
* ``--param NAME.KEY=JSON`` overrides one parameter of one workload;
* every results file records the stray-process check, and a session cell's figures are medians
  over its verified runs only (Task 9c review M9, I2);
* the inventory model is consistent: links are an involution between two different sites,
  every link id is its lower port, and a path never revisits a port;
* the cluster cache profile (R110) checks catalog values of any generation and nothing else, and
  the decode counter counts every atom a read decodes (Task 9c CARRY).
"""
import json
import os
import subprocess

import pytest

from benchmarks import harness
from benchmarks.workloads.inventory import MAX_HOPS, Model

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

pytestmark = pytest.mark.timeout(120)


def _git_ok() -> bool:
    """Whether REPO is the top level of a git work tree.  An export placed inside another work tree
    (review 8.3m M6) would otherwise run git against the enclosing repository."""
    try:
        top = subprocess.run(["git", "rev-parse", "--show-toplevel"], cwd=REPO, capture_output=True, text=True)
    except OSError:
        return False
    return top.returncode == 0 and os.path.realpath(top.stdout.strip()) == os.path.realpath(REPO)


@pytest.mark.skipif(not _git_ok(), reason="needs REPO to be a git repository's top level")
def test_an_export_is_clean_until_it_is_changed(tmp_path):
    from benchmarks.export import export
    dest = tmp_path / "export"
    record = export("HEAD", str(dest))
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, capture_output=True, text=True).stdout.strip()
    assert record["commit"] == head
    rev = harness.source_revision(str(dest))
    assert rev["clean"] is True and rev["commit_full"] == head and rev["dirty"] is False
    target = dest / "benchmarks" / "harness.py"
    target.write_text(target.read_text() + "\n# edited\n")
    rev = harness.source_revision(str(dest))
    assert rev["clean"] is False and rev["changed"] == ["benchmarks/harness.py"]
    target.write_text(target.read_text().replace("\n# edited\n", ""))
    assert harness.source_revision(str(dest))["clean"] is True
    (dest / "proto_db" / "planted.py").write_text("x = 1\n")
    rev = harness.source_revision(str(dest))
    assert rev["clean"] is False and rev["added"] == ["proto_db/planted.py"]


def test_no_source_record_means_no_export(tmp_path):
    assert harness.source_revision(str(tmp_path)) is None


def test_a_nested_directory_records_no_commit(tmp_path, monkeypatch):
    nested = os.path.join(REPO, "benchmarks")       # inside the work tree, not its top
    monkeypatch.setattr(harness, "REPO", nested)
    rev = harness._git_revision()
    assert rev["commit"] is None and rev["clean"] is False


def test_param_overrides_reach_the_workload(tmp_path):
    seen = {}

    def probe(ctx, p):
        """Records its parameters."""
        seen.update(p)
        result = harness.BenchResult({"n": p["n"]}, p["n"], p["n"])
        result.add_timing("noop", [0.1])
        return result

    harness.benchmark("__param_probe__", quick={"n": 1, "m": "a"}, full={"n": 2})(probe)
    try:
        out = tmp_path / "r.json"
        status = harness.main(["--quick", "--only", "__param_probe__", "--param", "__param_probe__.n=7",
                               "--param", "__param_probe__.m=text", "--workdir", str(tmp_path), "--json", str(out)])
        assert status == 0
        assert seen == {"n": 7, "m": "text"}
        doc = json.loads(out.read_text())
        assert doc["options"]["params"] == {"__param_probe__": {"n": 7, "m": "text"}}
        assert doc["results"]["__param_probe__"]["params"]["n"] == 7
        assert isinstance(doc["check"]["stray_processes"], list) and doc["check"]["checked_at"]
    finally:
        harness.REGISTRY.pop("__param_probe__", None)


def test_stray_check_excludes_this_process_and_its_ancestors():
    me = str(os.getpid())
    assert all(line.split()[0] != me for line in harness.stray_processes())


def test_session_summary_never_averages_failed_runs():
    from benchmarks.session import _summary
    ok = [{"ok": True, "commit_rate": r, "rep": i} for i, r in enumerate((12.8, 13.1))]
    bad = [{"ok": False, "commit_rate": r, "rep": 9, "thread_errors": [["ProtoOutcomeUnknownError"]],
            "ids_extra": 4, "ids_missing": 0} for r in (8.1, 10.6, 12.0)]
    out = _summary(ok + bad)
    assert (out["runs"], out["verified"], out["failed"]) == (5, 2, 3)
    assert out["commit_rate"] == {"median": 12.95, "min": 12.8, "max": 13.1}
    assert len(out["failed_runs"]) == 3 and out["failed_runs"][0]["ids_extra"] == 4
    nothing = _summary(bad)
    assert nothing["verified"] == 0 and "commit_rate" not in nothing


def test_param_needs_a_name_and_a_key():
    with pytest.raises(SystemExit):
        harness.parse_params(["objects=3"])


@pytest.mark.parametrize("sites", [2, 3, 28, 277])
def test_inventory_links_are_an_involution_between_sites(sites):
    m = Model(sites)
    linked = 0
    for s in range(min(sites, 6)):
        for p in m.ports_of_site(s):
            q = m.partner(p)
            if q is None:
                continue
            linked += 1
            assert m.partner(q) == p
            assert m.split(q // 64)[0] != s
        for lid, a, b in m.links_of_site(s):
            assert lid == a < b and m.partner(a) == b
    assert linked > 0


def test_inventory_paths_never_revisit_a_port():
    m = Model(277)
    for c in range(0, 27_700, 997):
        start = m.port_id(c, 0)
        path = m.path(start)
        assert len(path) == len(set(path)) <= 1 + 2 * MAX_HOPS
        for a, b in zip(path[::2], path[1::2]):
            assert m.partner(a) == b


def test_cache_profile_values_are_checked_for_any_generation_only():
    from benchmarks.cache_peer import catalog_value, category_ids, value_ok
    for key in (0, 7, 19_999):
        for gen in (0, 1, 12345):
            assert value_ok(key, catalog_value(key, gen))
    assert not value_ok(7, catalog_value(8, 3))                     # another key's value
    assert not value_ok(7, catalog_value(7, 3).replace(":3:", ":4:"))   # a generation with a wrong checksum
    assert not value_ok(7, None)
    assert category_ids(10, 4, 1) == [1, 5, 9]


def test_decode_counter_counts_every_decode_of_an_uncached_read():
    from benchmarks import decode_counter
    from proto_db import MemoryStorage, ObjectSpace
    decode_counter.install()
    storage = MemoryStorage()
    space = ObjectSpace(storage)
    db = space.new_database("d")
    db.run(lambda tr: tr.set_root_object("m", tr.new_dictionary().set_at(1, "a").set_at(2, "b")))
    cold = ObjectSpace(storage, atom_cache_entries=0)
    try:
        before = decode_counter.count()
        with cold.open_database("d").new_transaction() as tr:
            assert tr.get_root_object("m").get_at(2) == "b"
        assert decode_counter.count() > before
    finally:
        cold.close()


def test_the_session_runs_the_cache_profile_cells():
    from benchmarks import session
    plan = session.cache_cells(with_rqlite=True)
    labels = [session.label_of(k, a) for k, a in plan]
    assert len(plan) == 5 + 8 and len(set(labels)) == len(labels)
    assert "cache: protodb, point reads, atom cache off" in labels
    assert "cache: rqlite, query reads, level=strong" in labels
