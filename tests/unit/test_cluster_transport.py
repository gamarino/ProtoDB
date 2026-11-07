"""Transports: the fault-injecting in-process network and TCP (Task 8.3c, D45)."""
import socket
import struct
import threading
import time

import pytest

from proto_db.cluster.protocol import ClusterProtocolError, make_message
from proto_db.cluster.transport import InProcessNetwork, TcpTransport

pytestmark = pytest.mark.timeout(60)


class Inbox:
    def __init__(self):
        self.cond = threading.Condition()
        self.items = []

    def __call__(self, msg):
        with self.cond:
            self.items.append(msg)
            self.cond.notify_all()

    def wait_for(self, n, timeout=5.0):
        with self.cond:
            self.cond.wait_for(lambda: len(self.items) >= n, timeout=timeout)
            return list(self.items)


def _req(sender, term):
    return make_message("request", sender, term=term, rid=term, inc=0, vs=0)


@pytest.fixture
def baseline_threads():
    """Fails a test that leaves a thread running: the threads alive after it (given 5 s to
    end) that were not alive before it.  Compared by identity, so a thread of an earlier test
    ending meanwhile can neither fail the check nor hide a leak (re-review 1, N3)."""
    before = set(threading.enumerate())
    yield before
    deadline = time.monotonic() + 5
    while set(threading.enumerate()) - before and time.monotonic() < deadline:
        time.sleep(0.01)
    leaked = set(threading.enumerate()) - before
    assert not leaked, f"threads left running: {sorted(t.name for t in leaked)}"


# -- in-process --------------------------------------------------------------------------------

def _pair(net):
    a, b = net.endpoint(), net.endpoint()
    ia, ib = Inbox(), Inbox()
    peers = {"a": None, "b": None}
    a.start("a", peers, ia)
    b.start("b", peers, ib)
    return a, b, ia, ib


def test_in_process_delivers_in_order_without_faults(baseline_threads):
    with InProcessNetwork(seed=1) as net:
        a, b, ia, ib = _pair(net)
        for t in range(1, 21):
            a.send("b", _req("a", t))
        got = ib.wait_for(20)
        assert [m["term"] for m in got] == list(range(1, 21))
        a.stop()
        b.stop()


def test_in_process_drop_and_duplicate(baseline_threads):
    with InProcessNetwork(seed=2) as net:
        a, b, ia, ib = _pair(net)
        net.set_faults(drop=1.0)
        a.send("b", _req("a", 1))
        net.set_faults(duplicate=1.0)
        a.send("b", _req("a", 2))
        got = ib.wait_for(2)
        time.sleep(0.05)
        assert [m["term"] for m in ib.items] == [2, 2]
        assert net.stats()["dropped"] == 1 and net.stats()["duplicated"] == 1
        a.stop()
        b.stop()


def test_in_process_delays_reorder_messages(baseline_threads):
    with InProcessNetwork(seed=3) as net:
        a, b, ia, ib = _pair(net)
        net.set_faults(delay=(0.0, 0.02))
        for t in range(1, 51):
            a.send("b", _req("a", t))
        got = ib.wait_for(50)
        terms = [m["term"] for m in got]
        assert sorted(terms) == list(range(1, 51)) and terms != sorted(terms)
        a.stop()
        b.stop()


def test_in_process_partition_and_heal(baseline_threads):
    with InProcessNetwork(seed=4) as net:
        a, b, ia, ib = _pair(net)
        net.partition({"a"}, {"b"})
        a.send("b", _req("a", 1))
        b.send("a", _req("b", 1))
        net.heal()
        a.send("b", _req("a", 2))
        got = ib.wait_for(1)
        time.sleep(0.05)
        assert [m["term"] for m in ib.items] == [2] and ia.items == []
        a.stop()
        b.stop()


def test_in_process_crashed_node_neither_sends_nor_receives(baseline_threads):
    with InProcessNetwork(seed=5) as net:
        a, b, ia, ib = _pair(net)
        b.stop()
        a.send("b", _req("a", 1))
        b.send("a", _req("b", 1))
        time.sleep(0.05)
        assert ia.items == [] and ib.items == []
        a.stop()


def test_in_process_messages_go_through_the_wire_encoding(baseline_threads):
    with InProcessNetwork(seed=6) as net:
        a, b, ia, ib = _pair(net)
        with pytest.raises(ClusterProtocolError):
            a.send("b", {"v": 1, "type": "nonsense", "from": "a"})
        a.stop()
        b.stop()


# -- TCP ---------------------------------------------------------------------------------------

def _tcp_pair():
    a, b = TcpTransport(), TcpTransport()
    peers = {"a": a.address, "b": b.address}
    ia, ib = Inbox(), Inbox()
    a.start("a", peers, ia)
    b.start("b", peers, ib)
    return a, b, ia, ib


def test_tcp_binds_port_zero_and_exchanges_frames(baseline_threads):
    a, b, ia, ib = _tcp_pair()
    try:
        assert a.address[1] != 0 and a.address != b.address
        for t in range(1, 101):
            a.send("b", _req("a", t))
            b.send("a", _req("b", t))
        assert [m["term"] for m in ib.wait_for(100)] == list(range(1, 101))
        assert [m["term"] for m in ia.wait_for(100)] == list(range(1, 101))
    finally:
        a.stop()
        b.stop()


def test_tcp_malformed_frame_drops_the_connection_not_the_listener(baseline_threads):
    a, b, ia, ib = _tcp_pair()
    try:
        with socket.create_connection(b.address, timeout=2) as s:
            s.sendall(struct.pack(">I", 3) + b"\xc1\xc1\xc1")
            s.settimeout(2)
            assert s.recv(1) == b""          # the listener closed this connection
        a.send("b", _req("a", 1))
        assert [m["term"] for m in ib.wait_for(1)] == [1]
    finally:
        a.stop()
        b.stop()


def test_tcp_send_to_a_peer_not_yet_listening_is_dropped_then_reconnects(baseline_threads):
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    a = TcpTransport(reconnect_backoff=0.01)
    ia = Inbox()
    a.start("a", {"a": a.address, "b": ("127.0.0.1", port)}, ia)
    b = None
    try:
        a.send("b", _req("a", 1))        # nobody listens: dropped, never raises
        time.sleep(0.05)
        b = TcpTransport(port=port)
        ib = Inbox()
        b.start("b", {"a": a.address, "b": b.address}, ib)
        deadline = time.monotonic() + 5
        while not ib.items and time.monotonic() < deadline:
            a.send("b", _req("a", 2))
            time.sleep(0.02)
        assert ib.items and all(m["term"] == 2 for m in ib.items)
    finally:
        a.stop()
        if b is not None:
            b.stop()


def test_tcp_send_after_stop_is_a_silent_drop(baseline_threads):
    a, b, ia, ib = _tcp_pair()
    a.stop()
    a.send("b", _req("a", 1))
    b.stop()


def test_tcp_best_effort_warm_frames_never_crowd_out_control_messages(baseline_threads):
    """Re-review N3: ``warm`` frames (cache-only records, ruling R89) toward a slow peer are
    dropped first and never fill the queue that carries control messages; control messages
    queued behind them are still delivered, ahead of the remaining warm frames."""
    import threading
    import uuid as _uuid
    from proto_db.cluster.protocol import make_message
    a = TcpTransport(max_queue=20, send_timeout=10.0)
    b = TcpTransport()
    peers = {"a": a.address, "b": b.address}
    release = threading.Event()
    got = []

    def slow(msg):                      # b stops reading its socket until released
        release.wait(10)
        got.append(msg["type"])

    a.start("a", peers, Inbox())
    b.start("b", peers, slow)
    try:
        record = [[_uuid.UUID(int=1).bytes, 26, b"x" * (256 * 1024)]]
        for _ in range(200):            # about 50 MB: far beyond the socket buffers
            a.send("b", make_message("warm", "a", term=1, records=record))
        time.sleep(0.5)
        for t in range(1, 6):
            a.send("b", _req("a", t))   # control traffic behind the stalled warm frames
        release.set()
        deadline = time.monotonic() + 10
        while got.count("request") < 5 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert got.count("request") == 5, f"control messages lost: {got.count('request')} of 5 delivered"
        assert a.dropped > 0            # warm frames beyond the bound were dropped
    finally:
        release.set()
        a.stop()
        b.stop()


@pytest.mark.protocol_errors_expected
def test_a_protocol_error_raised_by_a_handler_is_recorded_for_the_test(request):
    """Review M7: ``_deliver`` logs a handler's ``ClusterProtocolError`` and goes on; the suite's
    autouse fixture records it, and fails every test not marked ``protocol_errors_expected``."""
    from proto_db.cluster import transport
    from proto_db.cluster.protocol import ClusterProtocolError

    def handler(msg):
        raise ClusterProtocolError("the done message lacks its copies")
    transport._deliver(handler, {"type": "done"}, "test")
    assert len(request.node.delivered_protocol_errors) == 1
    assert "'done'" in request.node.delivered_protocol_errors[0]
