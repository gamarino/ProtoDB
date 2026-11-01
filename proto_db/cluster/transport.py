"""Message transports of the cluster coordinator (Task 8.3c, decision D45).

A :class:`Transport` delivers protocol messages (:mod:`proto_db.cluster.protocol`)
between peers, best effort: ``send`` never blocks on the network and never raises for a
network failure; a message may be lost, and the lease protocol tolerates loss,
duplication and reordering.  The coordinator never relies on delivery for safety.

Two implementations:

:class:`TcpTransport`
    Length-prefixed msgpack frames over TCP.  One listener thread multiplexes the
    listening socket and every accepted connection with a selector and decodes frames;
    one sender thread PER PEER drains that peer's outgoing queue over one outbound
    connection, connecting on demand with a timeout and reconnecting after a failure (a
    message that cannot be sent is dropped), so an unreachable or stalled peer never
    delays messages to the others.  There is no UDP.
:class:`InProcessNetwork` / :class:`InProcessTransport`
    Peers inside one process, for tests and simulations.  Every message goes through the
    wire encoding.  The network injects faults: drop and duplicate probabilities, random
    delays (which reorder messages), fixed per-link delays, partitions into groups, and crashes (a stopped
    endpoint neither sends nor receives).

Handlers are called on the transport's receiving thread, one message at a time per
endpoint; they must not block on the network (the coordinator only takes its lock,
updates state and enqueues replies).
"""
import heapq
import logging
import queue
import random
import selectors
import socket
import threading
import time
from typing import Callable, Optional, Protocol

from ..errors import ProtoDBError, ProtoValidationError
from .protocol import (ClusterProtocolError, FrameDecoder, check_frame_size, decode_message, encode_frame,
                       encode_message)

__all__ = ["Transport", "TcpTransport", "InProcessNetwork", "InProcessTransport"]

logger = logging.getLogger(__name__)

_STOP = object()


class Transport(Protocol):
    """What the coordinator needs from a transport.

    ``start(node_id, peers, handler)``: begin receiving; ``peers`` maps every node id to
    its address (``(host, port)``, or ``None`` for transports that need none); ``handler``
    is called with each received, validated message.  ``send(node_id, message)``: queue a
    message for a peer; best effort, never raises for a network failure, a silent no-op
    after ``stop``.  ``stop()``: stop receiving and sending and join every thread the
    transport started; idempotent.
    """

    def start(self, node_id: str, peers: dict, handler: Callable[[dict], None]) -> None:
        """Begin receiving messages for ``node_id``."""

    def send(self, node_id: str, message: dict) -> None:
        """Send ``message`` to ``node_id`` (best effort)."""

    def stop(self) -> None:
        """Stop and join the transport's threads."""


def _deliver(handler, msg: dict, where: str) -> None:
    """Call ``handler``; a ProtoDB error raised for one message is logged, not fatal to the thread."""
    try:
        handler(msg)
    except ProtoDBError as e:
        logger.error("%s: handling a %r message failed: %s", where, msg.get("type"), e)


# -- in-process --------------------------------------------------------------------------------

class InProcessNetwork:
    """A simulated network of :class:`InProcessTransport` endpoints with fault injection.

    Thread-safe.  Delayed messages are delivered by one dispatcher thread, started on the
    first delayed message and joined by :meth:`close`.  ``seed`` seeds the generator that
    decides drops, duplicates and delays.
    """

    def __init__(self, *, seed: Optional[int] = None):
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._rng = random.Random(seed)
        self._endpoints = {}
        self._groups = None
        self._drop = 0.0
        self._duplicate = 0.0
        self._delay = (0.0, 0.0)
        self._links = {}          # (src, dst) -> extra delay in seconds
        self._heap = []
        self._seq = 0
        self._closed = False
        self._dispatcher = None
        self._stats = {"sent": 0, "dropped": 0, "duplicated": 0, "delivered": 0}

    def endpoint(self) -> "InProcessTransport":
        """A new, unstarted endpoint on this network."""
        return InProcessTransport(self)

    def set_faults(self, *, drop: float = 0.0, duplicate: float = 0.0, delay: tuple = (0.0, 0.0)) -> None:
        """Set the drop and duplicate probabilities and the ``(min, max)`` delay in seconds."""
        low, high = delay
        if not (0.0 <= drop <= 1.0 and 0.0 <= duplicate <= 1.0 and 0.0 <= low <= high):
            raise ProtoValidationError("Faults need probabilities in [0, 1] and 0 <= min delay <= max delay")
        with self._lock:
            self._drop, self._duplicate, self._delay = drop, duplicate, (low, high)

    def set_link_delay(self, src: str, dst: str, seconds: float) -> None:
        """Add ``seconds`` of delay to every message from ``src`` to ``dst`` (``0`` removes it)."""
        if not isinstance(seconds, (int, float)) or seconds < 0:
            raise ProtoValidationError("A link delay must be a non-negative number of seconds")
        with self._lock:
            if seconds:
                self._links[(src, dst)] = float(seconds)
            else:
                self._links.pop((src, dst), None)

    def partition(self, *groups) -> None:
        """Split the network: messages flow only between nodes of the same group.

        A node in no group is isolated from every other node.
        """
        with self._lock:
            self._groups = [frozenset(g) for g in groups]

    def heal(self) -> None:
        """Remove the partition."""
        with self._lock:
            self._groups = None

    def stats(self) -> dict:
        """Counts of messages sent, dropped, duplicated and delivered."""
        with self._lock:
            return dict(self._stats)

    def _connected(self, a: str, b: str) -> bool:
        if self._groups is None or a == b:
            return True
        return any(a in g and b in g for g in self._groups)

    def _register(self, node_id: str, ep: "InProcessTransport") -> None:
        with self._lock:
            if self._closed:
                raise ProtoValidationError("The in-process network is closed")
            current = self._endpoints.get(node_id)
            if current is not None and current is not ep:
                raise ProtoValidationError(f"Node {node_id!r} is already running on this network")
            self._endpoints[node_id] = ep

    def _unregister(self, node_id: str, ep: "InProcessTransport") -> None:
        with self._lock:
            if self._endpoints.get(node_id) is ep:
                del self._endpoints[node_id]

    def _route(self, src: str, ep: "InProcessTransport", dst: str, data: bytes) -> None:
        with self._lock:
            self._stats["sent"] += 1
            target = self._endpoints.get(dst)
            if (self._closed or self._endpoints.get(src) is not ep or target is None
                    or not self._connected(src, dst) or self._rng.random() < self._drop):
                self._stats["dropped"] += 1
                return
            copies = 1
            if self._rng.random() < self._duplicate:
                copies = 2
                self._stats["duplicated"] += 1
            for _ in range(copies):
                delay = self._rng.uniform(*self._delay) if self._delay[1] > 0 else 0.0
                delay += self._links.get((src, dst), 0.0)
                if delay <= 0:
                    self._stats["delivered"] += 1
                    target._inbox.put(data)
                    continue
                self._seq += 1
                heapq.heappush(self._heap, (time.monotonic() + delay, self._seq, dst, data))
                if self._dispatcher is None:
                    self._dispatcher = threading.Thread(target=self._dispatch, name="proto-cluster-net",
                                                        daemon=True)
                    self._dispatcher.start()
                self._cond.notify()

    def _dispatch(self) -> None:
        with self._cond:
            while not self._closed:
                if not self._heap:
                    self._cond.wait()
                    continue
                due = self._heap[0][0] - time.monotonic()
                if due > 0:
                    self._cond.wait(due)
                    continue
                _, _, dst, data = heapq.heappop(self._heap)
                target = self._endpoints.get(dst)
                if target is None:
                    self._stats["dropped"] += 1
                else:
                    self._stats["delivered"] += 1
                    target._inbox.put(data)

    def close(self) -> None:
        """Drop undelivered messages and join the dispatcher.  Endpoints must be stopped separately."""
        with self._cond:
            self._closed = True
            self._heap = []
            dispatcher = self._dispatcher
            self._cond.notify_all()
        if dispatcher is not None:
            dispatcher.join()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


class InProcessTransport:
    """An endpoint of an :class:`InProcessNetwork` (see :class:`Transport`).

    One delivery thread per started endpoint.  ``stop`` is the crash of the node: in-flight
    messages to it are dropped and it sends nothing more.  A stopped endpoint cannot be
    restarted; a restarted node uses a new endpoint.
    """

    def __init__(self, network: InProcessNetwork):
        self._network = network
        self._inbox = queue.SimpleQueue()
        self._node = None
        self._thread = None
        self._state_lock = threading.Lock()
        self._running = False

    def start(self, node_id: str, peers: dict, handler: Callable[[dict], None]) -> None:
        """See :class:`Transport`."""
        with self._state_lock:
            if self._thread is not None:
                raise ProtoValidationError("This endpoint was already started")
            self._network._register(node_id, self)
            self._node = node_id
            self._running = True
            self._thread = threading.Thread(target=self._loop, args=(handler,),
                                            name=f"proto-cluster-inproc-{node_id}", daemon=True)
            self._thread.start()

    def send(self, node_id: str, message: dict) -> None:
        """See :class:`Transport`.  An invalid message, or one larger than a frame, raises
        ``ClusterProtocolError`` (as on TCP)."""
        data = encode_message(message)
        check_frame_size(len(data))
        if not self._running:
            return
        self._network._route(self._node, self, node_id, data)

    def _loop(self, handler) -> None:
        where = f"in-process transport {self._node!r}"
        while True:
            item = self._inbox.get()
            if item is _STOP:
                return
            try:
                msg = decode_message(item)
            except ClusterProtocolError as e:
                logger.warning("%s dropped a malformed message: %s", where, e)
                continue
            _deliver(handler, msg, where)

    def stop(self) -> None:
        """See :class:`Transport`."""
        with self._state_lock:
            if not self._running:
                return
            self._running = False
            self._network._unregister(self._node, self)
            thread = self._thread
        self._inbox.put(_STOP)
        if thread is not threading.current_thread():
            thread.join()


# -- TCP ---------------------------------------------------------------------------------------

class TcpTransport:
    """:class:`Transport` over TCP with length-prefixed msgpack frames (see the module docstring).

    The listening socket is bound at construction (``port=0`` picks a free port; read it
    back from :attr:`address`), so the address can be exchanged before :meth:`start`.

    Args:
        host, port: address to listen on.
        connect_timeout: seconds to establish an outbound connection.
        send_timeout: seconds a blocked send may take before the connection is dropped.
        reconnect_backoff: seconds after a failed connect during which messages to that
            peer are dropped without trying again.
        max_queue: outgoing messages buffered; beyond it, sends are dropped.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 0, *, connect_timeout: float = 1.0,
                 send_timeout: float = 1.0, reconnect_backoff: float = 0.1, max_queue: int = 10_000):
        for name, value in (("connect_timeout", connect_timeout), ("send_timeout", send_timeout)):
            if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
                raise ProtoValidationError(f"{name} must be a positive number of seconds")
        if not isinstance(reconnect_backoff, (int, float)) or reconnect_backoff < 0:
            raise ProtoValidationError("reconnect_backoff must be a non-negative number of seconds")
        if type(max_queue) is not int or max_queue <= 0:
            raise ProtoValidationError("max_queue must be a positive int")
        self._connect_timeout = connect_timeout
        self._send_timeout = send_timeout
        self._backoff = reconnect_backoff
        self._max_queue = max_queue
        self._queues = {}         # peer id -> SimpleQueue drained by that peer's sender thread
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((host, port))
            listener.listen(64)
            listener.setblocking(False)
        except OSError as e:
            listener.close()
            raise ProtoValidationError(f"Cannot listen on {host}:{port}: {e}") from e
        self._listener = listener
        self._address = listener.getsockname()[:2]
        self._state_lock = threading.Lock()
        self._running = False
        self._started = False
        self._threads = []
        self._wake_r = self._wake_w = None
        self._node = None
        self._peers = {}
        self._dropped = 0

    @property
    def address(self) -> tuple:
        """``(host, port)`` the transport listens on."""
        return self._address

    @property
    def dropped(self) -> int:
        """Outgoing messages dropped (queue full, unknown peer, connection failures)."""
        with self._state_lock:
            return self._dropped

    def _drop(self) -> None:
        with self._state_lock:
            self._dropped += 1

    def start(self, node_id: str, peers: dict, handler: Callable[[dict], None]) -> None:
        """See :class:`Transport`; ``peers`` maps node ids to ``(host, port)``."""
        with self._state_lock:
            if self._started:
                raise ProtoValidationError("This transport was already started")
            self._started = True
            self._node = node_id
            self._peers = {k: (tuple(v) if v is not None else None) for k, v in peers.items()}
            self._wake_r, self._wake_w = socket.socketpair()
            self._running = True
            self._threads = [threading.Thread(target=self._listen, args=(handler,),
                                              name=f"proto-cluster-tcp-in-{node_id}", daemon=True)]
            for peer, address in self._peers.items():
                if peer == node_id or address is None:
                    continue
                q = self._queues[peer] = queue.SimpleQueue()
                self._threads.append(threading.Thread(target=self._send_loop, args=(peer, address, q),
                                                      name=f"proto-cluster-tcp-out-{node_id}-{peer}", daemon=True))
            for t in self._threads:
                t.start()

    def send(self, node_id: str, message: dict) -> None:
        """See :class:`Transport`.  An invalid message raises ``ClusterProtocolError`` (a bug)."""
        frame = encode_frame(message)
        if not self._running:
            return
        q = self._queues.get(node_id)
        if q is None or q.qsize() >= self._max_queue:
            self._drop()
            return
        q.put(frame)

    # -- sending ---------------------------------------------------------------------------

    def _connect(self, address: tuple) -> socket.socket:
        sock = socket.create_connection(address, timeout=self._connect_timeout)
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.settimeout(self._send_timeout)
        except OSError:
            sock.close()
            raise
        return sock

    def _send_loop(self, node_id: str, address: tuple, q) -> None:
        """Drain ``q`` to ``node_id``; a blocking connect or send here delays only this peer."""
        sock = None
        failed_at = float("-inf")
        try:
            while True:
                frame = q.get()
                if frame is _STOP:
                    return
                for _ in range(2):  # one retry on a fresh connection
                    if sock is None:
                        if time.monotonic() - failed_at < self._backoff:
                            self._drop()
                            break
                        try:
                            sock = self._connect(address)
                        except OSError as e:
                            failed_at = time.monotonic()
                            logger.debug("Connecting to %s at %s failed: %s", node_id, address, e)
                            self._drop()
                            break
                    try:
                        sock.sendall(frame)
                        break
                    except OSError as e:
                        logger.debug("Sending to %s failed: %s; reconnecting", node_id, e)
                        sock.close()
                        sock = None
                else:
                    self._drop()
        finally:
            if sock is not None:
                sock.close()

    # -- receiving -------------------------------------------------------------------------

    def _listen(self, handler) -> None:
        where = f"TCP transport {self._node!r}"
        sel = selectors.DefaultSelector()
        sel.register(self._listener, selectors.EVENT_READ, None)
        sel.register(self._wake_r, selectors.EVENT_READ, None)
        accepted = []
        try:
            while True:
                for key, _ in sel.select():
                    sock = key.fileobj
                    if sock is self._wake_r:
                        return
                    if sock is self._listener:
                        self._accept(sel, accepted)
                        continue
                    self._read(sel, sock, key.data, accepted, handler, where)
        finally:
            for sock in accepted:
                sock.close()
            sel.close()

    def _accept(self, sel, accepted: list) -> None:
        while True:
            try:
                conn, _ = self._listener.accept()
            except (BlockingIOError, InterruptedError):
                return
            except OSError as e:
                logger.warning("Accepting a connection failed: %s", e)
                return
            conn.setblocking(False)
            accepted.append(conn)
            sel.register(conn, selectors.EVENT_READ, FrameDecoder())

    def _read(self, sel, sock, decoder: FrameDecoder, accepted: list, handler, where: str) -> None:
        try:
            data = sock.recv(65536)
        except (BlockingIOError, InterruptedError):
            return
        except OSError:
            data = b""
        msgs = []
        if data:
            try:
                msgs = decoder.feed(data)
            except ClusterProtocolError as e:
                logger.warning("%s dropped a connection after a malformed frame: %s", where, e)
                data = b""
        if not data:
            sel.unregister(sock)
            accepted.remove(sock)
            sock.close()
        for msg in msgs:
            _deliver(handler, msg, where)

    def stop(self) -> None:
        """See :class:`Transport`."""
        with self._state_lock:
            if not self._running:
                if not self._started:
                    self._started = True
                    self._listener.close()
                return
            self._running = False
            threads = self._threads
        self._wake_w.send(b"x")
        for q in self._queues.values():
            q.put(_STOP)
        for t in threads:
            if t is not threading.current_thread():
                t.join()
        self._listener.close()
        self._wake_r.close()
        self._wake_w.close()
