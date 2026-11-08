"""Replies that reach another incarnation of their requester (Task 8.3g review, C1 and C2).

Request ids restart at 1 at every start of a node, so a reply delayed past its requester's
restart (P5: arbitrary delay) can carry the id of a new request.  Each case captures a real
reply sent to the previous incarnation and delivers it, with the colliding id, in place of
the reply to the new incarnation's request (which is lost).  Before protocol 12, replies were
matched by ``(rid[, term])`` only:

* C1: a stale ``ack`` counted as a durable copy and a vote, so a commit was acknowledged with
  its records on the committer alone, and ONE disk loss lost it;
* C2: a stale ``fetched`` was taken as the record at another pointer, cached under it and
  served to peers (wrong bytes for a pointer, P1).

Since protocol 12 every reply echoes the requester's incarnation (``fetched`` also the pointer)
and a mismatch is dropped.
"""
import time

import pytest

from proto_db.core.atom import AtomPointer
from tests.support.tiered_cluster import RUN_KW, TieredCluster, incr

pytestmark = [pytest.mark.timeout(120)]


def test_a_stale_ack_is_never_a_copy_or_a_vote_of_the_next_incarnation(tmp_path):
    with TieredCluster(tmp_path) as tc:
        tc.create_database("n0")
        tc.store.set_outage(True)                       # nothing is persisted: copies are all there is
        tc.commit("n0", incr)
        tc.kill("n2")
        tc.revive("n2")
        n0 = tc["n0"]
        send0 = n0._transport.send
        held = []

        def n0_send(to, msg):
            if not held and to == "n2" and msg["type"] == "ack" and msg["stored"]:
                held.append(msg)                        # delayed in the network
                return
            send0(to, msg)

        n0._transport.send = n0_send
        tc.commit("n2", incr)                 # incarnation A: n0 stores, its ack is delayed
        assert held
        n0._transport.send = send0
        tc.kill("n2")
        tc.revive("n2")                                 # incarnation B
        n2 = tc["n2"]
        send2 = n2._transport.send
        fired = []

        def n2_send(to, msg):
            if not fired and to == "n0" and msg["type"] == "replicate" and msg["records"]:
                stale = dict(held[0], rid=msg["rid"], term=msg["term"])    # the colliding id
                fired.append(stale)
                send0("n2", stale)                      # the delayed ack arrives now ...
                return                                  # ... and B's records never reach n0
            send2(to, msg)

        n2._transport.send = n2_send
        tc.commit("n2", incr)
        assert fired, "the schedule did not run"
        n2._transport.send = send2
        stale = tc["n2"].stats().get("stale_replies", 0)
        tc.wipe("n2")                                   # ONE disk lost (f = 1)
        for node in ("n0", "n1"):
            assert tc.eventually(node, 3), f"{node} lost an acknowledged commit"
        assert stale >= 1


def test_a_stale_fetched_reply_is_never_the_record_of_another_pointer(tmp_path):
    with TieredCluster(tmp_path) as tc:
        tc.create_database("n0")
        tc.store.set_outage(True)                       # a peer is the only source of a record
        tc.commit("n0", incr)
        r1 = tc["n0"].committed_root()[1]
        tc.commit("n0", incr)
        r2 = tc["n0"].committed_root()[1]
        p1, p2 = AtomPointer(r1.segment, r1.offset), AtomPointer(r2.segment, r2.offset)
        true1, true2 = tc.storages["n0"].local_record(p1), tc.storages["n0"].local_record(p2)
        assert true1 != true2
        tc.kill("n2")
        tc.revive("n2")                                 # incarnation A: an empty cache
        sends = {n: tc[n]._transport.send for n in ("n0", "n1")}
        held = []
        for node in ("n0", "n1"):
            def hook(to, msg, node=node):
                if not held and to == "n2" and msg["type"] == "fetched" and msg["data"] is not None:
                    held.append((node, msg))            # delayed in the network
                    return
                sends[node](to, msg)
            tc[node]._transport.send = hook
        tc["n2"].fetch_record(p1)
        assert held
        for node in ("n0", "n1"):
            tc[node]._transport.send = sends[node]
        tc.kill("n2")
        tc.revive("n2")                                 # incarnation B: request ids restart
        n2 = tc["n2"]
        send2 = n2._transport.send
        fired = []

        def n2_send(to, msg):
            if not fired and msg["type"] == "fetch":
                node, stale = held[0]
                fired.append(True)
                sends[node]("n2", dict(stale, rid=msg["rid"]))     # the colliding id
                return                                  # this request is lost
            send2(to, msg)

        n2._transport.send = n2_send
        st = tc.storages["n2"]
        if st.local_record(p2) is None:
            st.get_atom(p2)                             # reads p2 from a peer (or the store)
        assert fired, "the schedule did not run"
        n2._transport.send = send2
        time.sleep(0.05)
        assert st.local_record(p2) == true2, "n2 holds another record's bytes for p2"
