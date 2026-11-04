"""An owner whose own threads stopped asking ends its term (ruling R80, ``owner_idle_release``);
the member that keeps committing must then take the term and go on committing.

Measured before the fix (3 TCP peers, ``owner_idle_release`` 30 s): after the idle owner
ended its term, the busy member committed nothing for 27 s.  Its owner hint still named the
idle owner, whose refusals carried a term above the hint's (it had seen a higher ballot),
so the refusal never cleared the hint; and ``_become_owner`` defers to the member the hint
names.  Each permission attempt went: ask the old owner, refused, defer, ask again -- until
the permission timeout.
"""
import threading
import time

import pytest

import proto_db.transactions as tx
from proto_db.cluster.coordinator import ballot_after
from proto_db.cluster.protocol import make_message
from tests.support.tiered_cluster import RUN_KW, TieredCluster, incr

pytestmark = [pytest.mark.timeout(120)]


@pytest.mark.parametrize("round_", range(4))
def test_the_busy_member_keeps_committing_after_an_idle_owner_releases(tmp_path, round_):
    with TieredCluster(tmp_path, 3, storage_kw=dict(durability="fsync"), lease_duration=2.4,
                       request_timeout=0.1, owner_idle_release=0.5) as tc:
        tc.spaces["n0"].new_database("db")
        tc.db("n0").run(incr, **RUN_KW)                 # n0 owns a term
        n0, n2 = tc["n0"], tc["n2"]
        assert n0.owner == "n0"
        # n0 refuses a higher ballot (busy), as after any lost attempt of another member: its
        # later refusals of permit requests then carry a term above the one n1 knows n0 by.
        higher = ballot_after(n0._ownership.term, n0._ranks["n2"])
        n0._on_request("n2", make_message("request", "n2", term=higher, rid=10_000, inc=n2._inc))
        stop = threading.Event()
        stamps = []
        lock = threading.Lock()

        def work():
            db = tc.db("n1")
            while not stop.is_set():
                try:
                    db.run(incr, **RUN_KW)
                except (tx.ProtoConflictError, tx.ProtoTimeoutError):
                    continue
                with lock:
                    stamps.append(time.monotonic())
        threads = [threading.Thread(target=work, daemon=True) for _ in range(4)]
        start = time.monotonic()
        for t in threads:
            t.start()
        time.sleep(4.0)
        stop.set()
        for t in threads:
            t.join(30)
        released = tc["n0"].stats()["terms_released_idle_local"]
        gaps = [b - a for a, b in zip([start] + stamps, stamps)]
        print(f"released {released} commits {len(stamps)} longest gap {max(gaps):.2f}s owner {tc['n1'].owner}")
        assert released == 1
        assert max(gaps) < 1.0, f"n1 stalled {max(gaps):.2f} s"


@pytest.mark.parametrize("round_", range(2))
def test_a_member_whose_owner_ended_its_term_idle_takes_the_term(tmp_path, round_):
    """The same refusal without ``owner_idle_release``: n0 ends its term because nobody asks
    (a quarter of the lease, D50), and n1, whose hint still names n0, starts committing."""
    with TieredCluster(tmp_path, 3, storage_kw=dict(durability="fsync"), lease_duration=2.4,
                       request_timeout=0.1) as tc:
        tc.spaces["n0"].new_database("db")
        tc.db("n1").run(incr, **RUN_KW)                 # n1 learns n0 as the owner
        n0, n1, n2 = tc["n0"], tc["n1"], tc["n2"]
        assert n0.owner == "n0" and n1._owner_hint[1] == "n0"
        higher = ballot_after(n0._ownership.term, n0._ranks["n2"])
        n0._on_request("n2", make_message("request", "n2", term=higher, rid=10_000, inc=n2._inc))
        deadline = time.monotonic() + 5
        while n0._ownership is not None and time.monotonic() < deadline:
            time.sleep(0.02)                            # nobody asks: n0 ends its term
        assert n0._ownership is None and n1._owner_hint[1] == "n0"
        started = time.monotonic()
        tc.db("n1").run(incr, **RUN_KW)
        took = time.monotonic() - started
        print(f"n1's first commit after the end took {took:.2f}s")
        assert took < 1.0, f"n1 needed {took:.2f} s"
