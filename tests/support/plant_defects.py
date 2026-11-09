"""Planted defects: each one must make its target tests fail (Task 8.3n, review M6).

A plant is a few exact text edits applied to a copy of the repository: some plant a product
defect (a lost permission keep, a hole fill under the ship lock), the others make a test's own
workload or reader misbehave the way a broken cluster would (a lost acknowledged commit, an
unknown outcome resolved differently on one member, a commit removed after a member showed it,
a stray token).  Each plant names its target tests and the message the failure must carry.

Usage, from the repository root::

    python -m tests.support.plant_defects list
    python -m tests.support.plant_defects make <plant> <dest dir>
    python -m tests.support.plant_defects run <work dir> [--runs N] [--only P ...]

``run`` copies the tree once per plant into ``<work dir>`` (on a real disk, never a tmpfs),
runs the plant's targets ``--runs`` times per GIL mode (``-X gil=0`` and ``-X gil=1``) with the
interpreter running this script, and prints one line per run.  It exits 1 when a planted run
passes, or fails without the expected message: a plant that does not fail proves nothing.

The edits must match the current tree exactly once; a plant whose anchor moved fails loudly
(``make`` raises), so the plants are kept in step with the tests they target.
"""
import argparse
import os
import shutil
import subprocess
import sys
from typing import NamedTuple


class Plant(NamedTuple):
    edits: tuple            # ((path, old, new), ...)
    targets: tuple          # pytest node ids
    message: object         # a substring every failing run must print (a tuple: any one of them)
    what: str


_P = "tests/unit/test_cluster_persistence.py"
_STEADY_TARGET = (f"{_P}::test_steady_state_commits_write_nothing_in_the_object_store_under_the_lease",)
_BUMP = '''                def bump(tr):
                    pushed[_node].clear()               # this attempt's records only
                    tokens.observe(read_tokens(tr))     # P1: a committed base is history
                    incr(tr)
                    write_token(tr, token)
                go.wait()
                try:
                    tc.db(_node).run(bump, **RUN_KW)
                    tokens.acked(token)'''


def _bump(body: str, after_run: str = "") -> str:
    return ('''                idx = int(token.rsplit("-", 1)[1])

                def bump(tr):
                    pushed[_node].clear()               # this attempt's records only
                    tokens.observe(read_tokens(tr))     # P1: a committed base is history
''' + body + '''                go.wait()
                try:
                    tc.db(_node).run(bump, **RUN_KW)
''' + after_run + '''                    tokens.acked(token)''')


_REPORTED_UNKNOWN = '''                    if idx == 16:   # PLANT: applied, but reported unknown
                        raise ProtoOutcomeUnknownError("planted")
'''

PLANTS = {
    "fork": Plant(((_P, _BUMP, _bump('''                    d = tr.get_root_object("commit-tokens")
                    u = [k for k in d.keys() if k.endswith("-16")]
                    if idx == 17 and u:   # PLANT: commit 17 erases commit 16 from history
                        tr.set_root_object("n", tr.get_root_object("n") - 1)
                        tr.set_root_object("commit-tokens", d.remove_at(u[0]))
                    incr(tr)
                    write_token(tr, token)
''', _REPORTED_UNKNOWN)),), _STEADY_TARGET, "commits a member showed and history later lost",
        "an unknown commit that members showed is removed from history later"),
    "lost": Plant(((_P, _BUMP, _bump('''                    if idx == 17:   # PLANT: lost increment, token kept
                        tr.set_root_object("n", tr.get_root_object("n"))
                    else:
                        incr(tr)
                    write_token(tr, token)
''', _REPORTED_UNKNOWN)),), _STEADY_TARGET, "AssertionError",
        "an acknowledged increment is lost while an unknown one took effect (the counter alone balances)"),
    "lost_token": Plant(((_P, _BUMP, _bump('''                    if idx != 17:   # PLANT: commit 17 writes nothing of its own but is acknowledged
                        incr(tr)
                        write_token(tr, token)
                    else:
                        tr.set_root_object("other", 1)
''')),), _STEADY_TARGET, "acknowledged commits lost", "an acknowledged commit is lost"),
    "stray": Plant(((_P, _BUMP, _BUMP.replace(
        "                    write_token(tr, token)\n",
        "                    write_token(tr, token)\n"
        "                    if token.endswith('-12'):\n"
        "                        write_token(tr, 'stray-0')   # PLANT\n")),),
        _STEADY_TARGET, "tokens no acknowledged or unknown commit wrote", "a commit writes what no commit reported"),
    "split": Plant((
        (_P, '''            {n: (lambda n=n: snapshot_of(tc.db(n), lambda tr: tr.get_root_object("n"))) for n in tc.ids}, tokens)''',
         '''            {n: (lambda n=n: (lambda s: s._replace(tokens=s.tokens - {t for t in s.tokens if t.endswith("-16")})
                               if n == "n0" else s)(snapshot_of(tc.db(n), lambda tr: tr.get_root_object("n"))))
             for n in tc.ids}, tokens)'''),
        (_P, '''                    tc.db(_node).run(bump, **RUN_KW)
                    tokens.acked(token)''', '''                    tc.db(_node).run(bump, **RUN_KW)
                    if token.endswith("-16"):   # PLANT: applied, reported unknown; n0 will not show it
                        raise ProtoOutcomeUnknownError("planted")
                    tokens.acked(token)''')),
        _STEADY_TARGET, "unknown outcomes not resolved alike on every member",
        "an unknown outcome resolves present on some members and absent on others"),
    "check_lost": Plant((
        ("tests/concurrency/test_cluster_owner_failures.py",
         '''        {node: (lambda node=node: snapshot_of(tc.db(node), lambda tr: tr.get_root_object("n"))) for node in live},''',
         '''        {node: (lambda node=node: (lambda s: s._replace(tokens=s.tokens - {min(tokens.acked_tokens)}))(  # PLANT
            snapshot_of(tc.db(node), lambda tr: tr.get_root_object("n")))) for node in live},'''),),
        ("tests/concurrency/test_cluster_owner_failures.py::test_owner_crash_while_members_hold_or_await_permissions",),
        "acknowledged commits lost", "Load/_check: every member loses one acknowledged commit"),
    "readers_lost": Plant((
        ("tests/concurrency/test_history_readers.py",
         '''            {n: (lambda n=n: snapshot_of(cluster.db(n), lambda tr: tr.get_root_object("n"))) for n in cluster.ids},''',
         '''            {n: (lambda n=n: (lambda s: s._replace(tokens=s.tokens - {min(tokens.acked_tokens)}))(  # PLANT
                snapshot_of(cluster.db(n), lambda tr: tr.get_root_object("n")))) for n in cluster.ids},'''),),
        ("tests/concurrency/test_history_readers.py::test_cluster_readers_walk_both_links_while_two_nodes_commit",),
        "acknowledged commits lost", "history readers (review I1): every member loses one acknowledged commit"),
    "blind_lost": Plant((
        ("tests/support/outcomes.py", '''    return frozenset(counts)


def blind_snapshot_of''', '''    return frozenset(counts) - {min(counts, default=None)}   # PLANT: the oldest token is lost


def blind_snapshot_of'''),),
        ("tests/concurrency/test_cluster_contention.py::test_contending_peers_never_reach_the_storage",),
        "acknowledged commits lost", "blind tokens: every member loses one commit"),
    "forced_lost": Plant((
        ("tests/concurrency/test_cluster_contention.py",
         '''        assert_outcomes_resolved({n: (lambda n=n: blind_snapshot_of(tc.db(n), root="ids")) for n in tc.ids}, tokens)''',
         '''        assert_outcomes_resolved({n: (lambda n=n: (lambda s: s._replace(tokens=s.tokens - {min(tokens.acked_tokens)}))(
            blind_snapshot_of(tc.db(n), root="ids"))) for n in tc.ids}, tokens)   # PLANT'''),),
        ("tests/concurrency/test_cluster_contention.py::test_forced_replication_failures_surface_and_lose_nothing",),
        "acknowledged commits lost",
        "forced replication failures (fix round 2, item 3): every member loses one acknowledged commit"),
    "forced_old_bound": Plant((
        ("tests/concurrency/test_cluster_contention.py",
         '''    assert failures <= refused["unknown"] + refused["conflict"], (failures, refused)''',
         '''    assert failures <= unknown, (failures, unknown)     # PLANT: the bound before db68f49'''),),
        ("tests/concurrency/test_cluster_contention.py::test_forced_replication_failures_surface_and_lose_nothing",),
        "assert 2 <= 1", "forced replication failures: the bound before db68f49 (a retried conflict uncounted)"),
    "renew_stale": Plant((
        ("proto_db/cluster/lease.py",
         "            if renew and self._active and self._holder == holder and self._term == term and self._inc == inc:",
         "            if renew and term >= self._highest and self._active and self._holder == holder and self._term == term \\\n"
         "                    and self._inc == inc:     # PLANT: a refused higher request blocks every renewal"),),
        ("tests/unit/test_cluster_renewal.py::test_a_refused_higher_request_does_not_end_the_owners_term",
         "tests/unit/test_cluster_renewal.py::test_renewals_after_a_refused_higher_request_keep_the_term_deterministically"),
        ("a renewal of the owner's term was refused under an active grant", 'renewal_failures"] == 0'),
        "product: the renewal is decided after the stale-term rule (the 4c8c949 failure)"),
    "hint_forgotten": Plant((
        ("proto_db/cluster/coordinator.py",
         "                    if self._owner_hint[1] == sender and self._owner_hint[0] == wait.term:",
         "                    if self._owner_hint[1] == sender:     # PLANT: any refusal forgets the owner"),),
        ("tests/unit/test_cluster_permit_deferral.py::test_a_refusal_of_an_older_terms_request_keeps_the_owner_learnt_since",),
        "made n1 forget that n0 owns T2",
        "product: a refusal of an older term's request clears the owner learnt for a newer term"),
    "no_fallback": Plant((
        ("proto_db/cluster/coordinator.py",
         "                        if now >= fallback_at:\n                            if waiter is None:",
         "                        if False and now >= fallback_at:     # PLANT: no fallback query\n"
         "                            if waiter is None:"),),
        ("tests/concurrency/test_cluster_pipelined_permits.py::test_p2_a_closed_gate_after_a_store_is_a_truthful_conflict[False]",),
        "without its fallback query",
        "product: the gate never sends its fallback query (R94)"),
    "nokeep": Plant((
        ("proto_db/transactions.py",
         "                    keep = True         # this thread retries its own commit under the permission",
         "                    keep = False        # PLANT: the keep for the runner's own retry is lost"),),
        ("tests/concurrency/test_cluster_permit_hot_key.py::test_each_usable_permit_of_a_delayed_holder_commits",),
        "ended unretried with no cause", "product: a conflicted permission is never kept for its retry"),
    "nostore": Plant((
        ("proto_db/cluster/coordinator.py", "            use_store = bounded and not gate_open\n",
         "            use_store = False   # PLANT: no data stage leaves before the gate opens\n"),),
        # Without stores the pipeline also degrades: either assertion of the test may fire first.
        ("tests/concurrency/test_cluster_pipelined_permits.py::test_permits_overlap_the_predecessors_durability",),
        ("overlapped their data stage", "of the permits were issued on a proposal"), "product: a pipelined permit's data stage no longer overlaps (review I5)"),
    "shiplock": Plant((
        ("proto_db/storage/tiered.py", "            data = self._copy_prefix(segment, end)",
         "            with self._ship_lock:   # PLANT: the hole is filled under the ship lock\n"
         "                data = self._copy_prefix(segment, end)"),),
        ("tests/concurrency/test_cluster_bounded_replication.py::"
         "test_filling_a_hole_from_peers_never_blocks_the_nodes_own_shipping",),
        "ship() waits behind a fill", "product: filling a hole blocks the node's own shipping"),
}

_IGNORE = shutil.ignore_patterns("__pycache__", ".git", ".venv*", ".junie", "benchmarks", "docs", ".superpowers",
                                 ".hypothesis", "build", ".pytest_cache", "*.egg-info")
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def make(name: str, dest: str, src: str = ROOT) -> str:
    """Copy ``src`` to ``dest`` with plant ``name`` applied; return ``dest``."""
    plant = PLANTS[name]
    if os.path.exists(dest):
        shutil.rmtree(dest)
    shutil.copytree(src, dest, ignore=_IGNORE)
    for rel, old, new in plant.edits:
        path = os.path.join(dest, rel)
        with open(path) as f:
            text = f.read()
        count = text.count(old)
        if count != 1:
            raise SystemExit(f"plant {name}: the anchor occurs {count} times in {rel}, not once: {old[:70]!r}")
        with open(path, "w") as f:
            f.write(text.replace(old, new))
    return dest


def run(work: str, runs: int, only=None) -> int:
    bad = 0
    for name in only or PLANTS:
        plant = PLANTS[name]
        tree = make(name, os.path.join(work, f"plant-{name}"))
        for i in range(runs):
            for gil in (0, 1):
                base = os.path.join(work, f"bt-{name}-{i}-{gil}")
                env = dict(os.environ, TMPDIR=work)
                proc = subprocess.run([sys.executable, f"-X", f"gil={gil}", "-m", "pytest", "-q", "-p",
                                       "no:cacheprovider", f"--basetemp={base}", *plant.targets],
                                      cwd=tree, env=env, capture_output=True, text=True)
                shutil.rmtree(base, ignore_errors=True)
                out = proc.stdout + proc.stderr
                summary = next((ln for ln in reversed(out.splitlines()) if " in " in ln and ("passed" in ln or
                                                                                         "failed" in ln)), "")
                messages = plant.message if isinstance(plant.message, tuple) else (plant.message,)
                found = any(m in out for m in messages)
                ok = proc.returncode != 0 and found
                bad += not ok
                print(f"{name} run {i + 1} gil{gil}: {'FAILS as planted' if ok else 'NOT DETECTED'} "
                      f"(rc={proc.returncode}, message {'found' if found else 'missing'}) "
                      f"{summary.strip('= ')}", flush=True)
    print("every plant detected" if not bad else f"{bad} planted runs not detected")
    return 1 if bad else 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list")
    mk = sub.add_parser("make")
    mk.add_argument("plant", choices=sorted(PLANTS))
    mk.add_argument("dest")
    rn = sub.add_parser("run")
    rn.add_argument("work")
    rn.add_argument("--runs", type=int, default=3)
    rn.add_argument("--only", nargs="*", choices=sorted(PLANTS))
    args = ap.parse_args(argv)
    if args.cmd == "list":
        for name, plant in PLANTS.items():
            print(f"{name}: {plant.what}; targets {', '.join(plant.targets)}")
        return 0
    if args.cmd == "make":
        print(make(args.plant, args.dest))
        return 0
    return run(args.work, args.runs, args.only)


if __name__ == "__main__":
    sys.exit(main())
