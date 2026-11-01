"""SIGKILL: a child process commits in a loop and is killed at random moments.

Catches: acknowledged commits lost when the process dies (the reopened counter is below
the last value the child printed after ``publish_root`` returned), spaces left unopenable
or torn by a kill, and recovery that cannot continue from a killed writer's leftovers.
"""
import os
import random
import signal
import subprocess
import sys
import textwrap
import time

import pytest

from proto_db.storage.file import FileStorage

pytestmark = pytest.mark.skipif(not hasattr(signal, "SIGKILL"), reason="POSIX only (SIGKILL)")

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RUNS = 10
SEED = 20261101

CHILD = textwrap.dedent("""
    import sys
    from proto_db.storage.file import FileStorage
    s = FileStorage(sys.argv[1], segment_max_bytes=64 * 1024)
    root = s.read_root()
    i = s.get_atom(root.pointer)["i"] if root else 0
    epoch = root.epoch if root else 0
    while True:
        i += 1
        ptr = s.push_atom({"i": i, "pad": b"x" * (i % 200)})
        epoch = s.publish_root(ptr, epoch).epoch
        print(i, flush=True)
""")


def read_counter(path):
    s = FileStorage(path)
    try:
        root = s.read_root()
        if root is None:
            return 0
        atom = s.get_atom(root.pointer)
        assert atom["pad"] == b"x" * (atom["i"] % 200)
        assert root.epoch == atom["i"]
        return atom["i"]
    finally:
        s.close()


def test_kill_at_random_times_never_loses_acknowledged_commits(tmp_path):
    rng = random.Random(SEED)
    path = str(tmp_path / "space")
    env = dict(os.environ, PYTHONPATH=REPO)
    started = time.monotonic()
    killed_mid_run = 0
    for run in range(RUNS):
        before = read_counter(path)
        child = subprocess.Popen([sys.executable, "-c", CHILD, path], stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, text=True, env=env)
        try:
            first = child.stdout.readline()
            assert first, f"child produced no output: {child.stderr.read()}"
            time.sleep(rng.uniform(0.0, 0.4))
            child.send_signal(signal.SIGKILL)
            rest, err = child.communicate(timeout=30)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()
        assert child.returncode == -signal.SIGKILL, err
        lines = (first + rest).splitlines()
        complete = [int(x) for x in lines if x.strip().isdigit()]
        last_printed = complete[-1]
        assert complete[0] == before + 1
        visible = read_counter(path)
        assert last_printed <= visible <= last_printed + 1, (run, last_printed, visible)
        killed_mid_run += 1
    assert killed_mid_run == RUNS
    assert time.monotonic() - started < 60
