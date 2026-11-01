"""ClusterCoordinator: the majority-granted write lease and the cluster commit point
(Tasks 8.3c and 8.3d, decisions D44, D45, D46).

A cluster is a static list of peers ``(node_id, host, port)``; its size ``N`` is fixed at
construction and a majority is ``floor(N/2) + 1`` votes, the requester's own included.
Every process that writes the space runs one coordinator; transactions of a space opened
with a coordinator commit only while holding the lease (:mod:`proto_db.transactions`).

Acquiring
    The threads of one process queue on a local lock; the thread at its head asks every
    peer (itself included) to grant the lease for the next *ballot* above the highest term
    it knows.  A ballot is proposer-qualified: its low :data:`RANK_BITS` bits are the
    proposer's rank (its index among the sorted peer ids), so two proposers never ask for
    the same term, and a peer refuses a request whose term does not carry the requester's
    rank.  It
    holds the lease once a majority has granted, for ``lease_duration - drift_margin``
    measured on its own clock from the moment it SENT the request.  A lost attempt is
    cancelled with a ``release`` broadcast, and the requester backs off (jittered
    exponential delay, or up to a running lease's remaining time), woken early by any
    release.  No spinning, no queue among processes; fairness is not guaranteed (D45).
Granting
    See :class:`~proto_db.cluster.lease.PeerGrants`.  Requests and releases carry the
    sender's incarnation (a random number drawn at each start), so a restarted node is
    never confused with its previous life.  A grant's term is made durable in the node's
    acceptor log (:mod:`proto_db.cluster.rootlog`) before the grant is sent, and restored
    when the node restarts, so no term is ever granted twice (the acceptor rule of Paxos).
    A node grants, and acknowledges replications, only while a cluster tier (the storage
    of a space) is attached and while it is not fenced: a node without a durable place for
    its promises and roots never counts toward a majority.
Rejoining
    A node whose acceptor log is empty (a new node, or a disk lost or wiped) cannot tell
    which terms it promised or proposed in a previous life.  Until it has learnt the
    highest term promised or seen by ``N - majority + 1`` other members (``query``/
    ``answer``; for three nodes, both others), it neither grants, nor accepts replicated
    roots, nor proposes; it then records that term in its log, which also marks the rejoin
    as done.  Its own knowledge counts for nothing, so the answering members intersect,
    outside this node, every majority that granted a term: it never proposes a ballot of
    its own again, and never accepts a root below a ballot it granted.  With
    proposer-qualified ballots, two different roots therefore never share a label while at
    most one acceptor log is lost at a time (D46, ruling R47).  The price is availability:
    a node with an empty log -- every node on a cluster's first start -- needs those
    answers before it votes.
Roots and their labels (D46)
    Every root the cluster knows carries the term of the lease that replicated it, its
    *label*; roots are ordered by :func:`~proto_db.cluster.rootlog.root_key` everywhere, and
    two different roots with the same label and epoch are reported as a protocol violation
    (:func:`~proto_db.cluster.rootlog.compare_roots`).  Each node keeps its *accepted* root
    (the highest root it has durably accepted from a replication) and its *committed* root
    (the newest root it knows a majority acknowledged).  Transactions read committed roots
    only; the holder builds on the highest accepted root of its granting majority.
The commit point (D46)
    Under the lease the holder validates and merges against that root, then
    :meth:`ClusterCoordinator.replicate` sends ``(epoch, root pointer)`` with the records
    the commit added to every peer (split into several frames when they do not fit in
    one).  A peer accepts it -- stores the records and logs the root on its own durable
    tier (``fsync``), then acknowledges -- unless it is starting, has no tier, is fenced or
    has granted a higher term (the ballot rule).  The commit point is therefore the local
    WAL ``fsync`` of a majority.  A cluster over ``MemoryStorage`` keeps its acceptor logs
    in memory and is not durable.  The commit is acknowledged once a majority (the holder included) has
    acknowledged before the lease ends; the release then tells the peers it is committed.
    Every later lease's granting majority intersects that acknowledging majority, and a
    peer that acknowledged before granting reports the root (or a newer one) with its
    grant, so the next holder learns the newest committed root from its granters.  No
    storage root is written in the critical section: the persister
    (:mod:`proto_db.cluster.persister`) writes it later, coalesced.
Certainty
    A lease is certain when every granter knows a root and none reports a grant whose
    outcome it could not know (one that ended by expiry, by its holder's restart, or a
    release marked uncertain); such a suspicion is cleared when the node learns a root
    labelled with that term or later.  An uncertain lease runs a recovery round
    (:meth:`ClusterCoordinator.recover_root`): the roots of a majority of peers.
Peer services
    ``query``/``answer`` (recovery, joining peers) and ``fetch``/``fetched`` (peer page
    requests for records not yet in the object store).

Health
    An unexpected exception in the worker thread fences the node
    (:meth:`ClusterCoordinator.fence`): it is logged, the node stops granting, accepting
    and acquiring, drops (or refuses) the peer messages still queued and arriving,
    :attr:`ClusterCoordinator.healthy` and ``stats()["healthy"]`` turn False, and the
    exception is re-raised in its thread.  An unexpected exception in the persister thread
    only stops persisting (:meth:`ClusterCoordinator.persistence_failed`): persistence is
    off the commit path, so the node keeps granting and acknowledging, but it reports
    itself unhealthy.

Threads: the transport's own threads, plus one worker started by :meth:`start` that
applies replicated records, serves page requests and prefetches, and the persister
started by :meth:`attach`.  One lock guards the coordinator's state; it is never held
while sending, while doing I/O, or while calling user code.
"""
import collections
import itertools
import logging
import queue
import random
import threading
import time
import uuid
from typing import Callable, NamedTuple, Optional

from ..core.atom import AtomPointer
from ..errors import (ProtoConflictError, ProtoDBError, ProtoOutcomeUnknownError, ProtoTimeoutError,
                      ProtoUnexpectedError, ProtoUserError, ProtoValidationError)
from ..storage.format import RootRecord
from . import protocol
from .lease import Lease, LeaseAttempt, PeerGrants, majority
from .protocol import decode_labeled_root, encode_root, make_message
from .rootlog import compare_roots, root_key

__all__ = ["Peer", "ClusterCoordinator", "ballot_after", "ballot_rank", "RANK_BITS", "MAX_MEMBERS",
           "REFOUND_TERM_STEP"]

logger = logging.getLogger(__name__)

#: Once a majority has answered an undecided attempt, the remaining replies are awaited for
#: ``max(_MIN_STRAGGLE, _STRAGGLE_FACTOR * time those replies took)`` seconds at most.
_MIN_STRAGGLE = 0.005
_STRAGGLE_FACTOR = 4.0
#: Upper bound, in seconds, of the random spread a requester adds after being woken by a release.
_WAKE_SPREAD = 0.003
_STOP = object()
#: Roots whose records this node stored, remembered for :meth:`ClusterCoordinator.await_records`.
_STORED_MEMORY = 256
_CRITICAL_KEYS = ("validation", "replay", "path_copy", "replication")


#: Low bits of a term (a ballot) holding the proposer's rank; a cluster has at most
#: ``1 << RANK_BITS`` members.
RANK_BITS = 12
MAX_MEMBERS = 1 << RANK_BITS
#: Term increment when a cluster refounds from the persisted root (:meth:`ClusterCoordinator.refound`):
#: room for ``2**32`` ballots of every rank, and a multiple of :data:`MAX_MEMBERS`.
REFOUND_TERM_STEP = 1 << 44


def ballot_after(floor: int, rank: int) -> int:
    """The smallest ballot of proposer ``rank`` above ``floor``: a term whose low
    :data:`RANK_BITS` bits are ``rank``."""
    return ((floor - rank) // MAX_MEMBERS + 1) * MAX_MEMBERS + rank


def ballot_rank(term: int) -> int:
    """The rank of the proposer of ballot ``term``."""
    return term % MAX_MEMBERS


class Peer(NamedTuple):
    """A member of the cluster: its id and the address its transport listens on."""

    node_id: str
    host: str
    port: int


def _number(name: str, value, *, positive: bool = True) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value < 0 or (positive and value == 0):
        raise ProtoValidationError(f"{name} must be a {'positive' if positive else 'non-negative'} number")
    return float(value)


def _split_records(records: list, budget: int) -> list:
    """Split ``records`` into consecutive chunks of at most ``budget`` record bytes (a larger
    record forms a chunk alone); the last chunk rides in the ``replicate`` message."""
    chunks, current, size = [], [], 0
    for record in records:
        n = 32 + (len(record[2]) if record[2] is not None else 0)
        if current and size + n > budget:
            chunks.append(current)
            current, size = [], 0
        current.append(record)
        size += n
    chunks.append(current)
    return chunks


class _Waiter:
    """Replies gathered for one replicate, query or fetch (guarded by the coordinator lock)."""

    __slots__ = ("kind", "term", "acks", "nacks", "answers", "promised", "data", "missing")

    def __init__(self, kind: str, term: int = 0):
        self.kind = kind
        self.term = term
        self.acks = set()
        self.nacks = set()
        self.answers = {}
        self.promised = {}
        self.data = None
        self.missing = 0


class ClusterCoordinator:
    """A cluster member granting and acquiring the write lease (see the module docstring).

    Args:
        node_id: this node's id; must appear in ``peers``.
        peers: every member, this node included, as ``(node_id, host, port)``.
        transport: a :class:`~proto_db.cluster.transport.Transport`; default a
            :class:`~proto_db.cluster.transport.TcpTransport` listening on this node's
            peer address.
        lease_duration: seconds a grant blocks other holders on the granter's clock.
        drift_margin: seconds subtracted from the holder's view of its lease (default 10 %
            of ``lease_duration``); at least ``2 * rho * D / (1 + rho)`` for clock rates
            within ``1 +/- rho``.
        request_timeout: seconds an attempt (and a recovery query) waits for a majority.
        acquire_timeout: default seconds :meth:`acquire` tries before ``ProtoTimeoutError``.
        backoff: ``(min_seconds, max_seconds)`` of the jittered back-off between lost attempts.
        startup_grace: seconds after :meth:`start` during which this node grants nothing and
            accepts no replicated root (default ``lease_duration``).  ``0`` is safe only on a
            cluster's first start.
        fetch_timeout: seconds a peer page request waits for an answer; must be shorter than
            ``lease_duration - drift_margin`` (default a quarter of it), and a request made
            under the lease never waits beyond what remains of the lease.
        persist_interval: seconds between two passes of the persister (D46).
        persist_lag: seconds the persisted epoch may lag the committed one before this node
            persists a root committed by another node (covers a crashed committer).
        clock: testing seam; the monotonic clock (seconds) used for every lease decision.
        seed: testing seam; seed of the back-off jitter generator.
    """

    def __init__(self, node_id: str, peers, *, transport=None, lease_duration: float = 2.0,
                 drift_margin: Optional[float] = None, request_timeout: float = 0.25,
                 acquire_timeout: float = 30.0, backoff: tuple = (0.001, 0.05),
                 startup_grace: Optional[float] = None, fetch_timeout: Optional[float] = None,
                 persist_interval: float = 0.05, persist_lag: float = 1.0,
                 clock: Optional[Callable[[], float]] = None, seed: Optional[int] = None):
        members = []
        for entry in peers:
            try:
                peer = Peer(*entry)
            except TypeError as e:
                raise ProtoValidationError(f"A peer must be (node_id, host, port), got {entry!r}") from e
            if not isinstance(peer.node_id, str) or not peer.node_id or not isinstance(peer.host, str) \
                    or type(peer.port) is not int or not 0 <= peer.port <= 65535:
                raise ProtoValidationError(f"Invalid peer {entry!r}")
            members.append(peer)
        ids = [p.node_id for p in members]
        if len(set(ids)) != len(ids):
            raise ProtoValidationError("Peer ids must be unique")
        if node_id not in ids:
            raise ProtoValidationError(f"Node {node_id!r} is not in the peer list")
        if len(ids) > MAX_MEMBERS:
            raise ProtoValidationError(f"A cluster has at most {MAX_MEMBERS} members")
        self._duration = _number("lease_duration", lease_duration)
        margin = self._duration * 0.1 if drift_margin is None else _number("drift_margin", drift_margin,
                                                                             positive=False)
        if margin >= self._duration:
            raise ProtoValidationError("drift_margin must be smaller than lease_duration")
        self._margin = margin
        self._request_timeout = _number("request_timeout", request_timeout)
        if self._request_timeout >= self._duration - self._margin:
            raise ProtoValidationError("request_timeout must be shorter than lease_duration - drift_margin")
        self._acquire_timeout = _number("acquire_timeout", acquire_timeout, positive=False)
        try:
            low, high = backoff
        except (TypeError, ValueError) as e:
            raise ProtoValidationError("backoff must be a (min_seconds, max_seconds) pair") from e
        low = _number("backoff minimum", low, positive=False)
        high = _number("backoff maximum", high, positive=False)
        if high < low:
            raise ProtoValidationError("backoff must satisfy min_seconds <= max_seconds")
        self._backoff = (low, high)
        self._grace = self._duration if startup_grace is None else _number("startup_grace", startup_grace,
                                                                              positive=False)
        window = self._duration - self._margin
        self._fetch_timeout = window / 4 if fetch_timeout is None else _number("fetch_timeout", fetch_timeout)
        if self._fetch_timeout >= window:
            raise ProtoValidationError("fetch_timeout must be shorter than lease_duration - drift_margin, "
                                       "or a page request could consume the whole lease")
        self._persist_interval = _number("persist_interval", persist_interval)
        self._persist_lag = _number("persist_lag", persist_lag, positive=False)
        self._clock = clock if clock is not None else time.monotonic
        self._rng = random.Random(seed)

        self._node = node_id
        self._peers = {p.node_id: p for p in members}
        self._others = [i for i in ids if i != node_id]
        self._ranks = {peer: rank for rank, peer in enumerate(sorted(ids))}
        self._rank = self._ranks[node_id]
        self._majority = majority(len(members))
        if transport is None:
            from .transport import TcpTransport
            me = self._peers[node_id]
            transport = TcpTransport(me.host, me.port)
        self._transport = transport

        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._local = threading.Lock()      # queues this process's threads
        self._grants = None                 # PeerGrants, created by start()
        self._state = "new"
        self._inc = random.SystemRandom().getrandbits(62)
        self._attempt = None
        self._held = None
        self._hint = 0                      # highest term learnt from replies and roots
        self._rids = itertools.count(1)
        self._wakeups = 0                   # releases received
        self._accepted = (False, None, 0)   # (known, RootRecord or None, label)
        self._committed = (False, None, 0)
        self._committed_by = None           # node that committed the committed root, if known
        self._committed_at = 0.0            # time.monotonic() when it was learnt
        self._committed_version = 0
        self._suspect_term = 0              # a grant whose outcome this node could not know
        self._waiters = {}                  # rid -> _Waiter
        self._queued = 0                    # peer messages queued for the worker, not yet handled
        self._stored = collections.OrderedDict()   # root_key -> None: roots whose records are here
        self._chunks = {}                   # sender -> (rid, {index: records}) awaiting a replicate
        self._failure = None                # the exception that fenced this node, if any
        self._persist_failure = None        # the exception that stopped the persister, if any
        self._rejoining = False             # the acceptor log was empty: learning the promised terms
        self._rejoin_forced = False         # the operator accepted a rejoin from a bare majority
        self._rejoiner = None               # the thread running a rejoin, if any
        self._tier = None
        self._confirmed = True
        self._persister = None
        self._worker = None
        self._jobs = queue.Queue()
        self._stats = {"acquired": 0, "attempts": 0, "lost_attempts": 0, "grants_given": 0,
                       "grants_refused_no_storage": 0, "releases_received": 0, "storage_root_reads": 0,
                       "replications": 0, "replication_failures": 0, "accepted": 0, "refused": 0,
                       "recoveries": 0, "rejoins": 0, "fetches_requested": 0, "fetches_served": 0,
                       "critical_commits": 0}
        self._critical = {k: 0.0 for k in _CRITICAL_KEYS}

    # -- properties --------------------------------------------------------------------------

    @property
    def node_id(self) -> str:
        """This node's id."""
        return self._node

    @property
    def cluster_size(self) -> int:
        """Number of members, ``N``."""
        return len(self._peers)

    @property
    def majority(self) -> int:
        """Votes needed for the lease and acknowledgements needed for a commit: ``floor(N/2) + 1``."""
        return self._majority

    @property
    def transport(self):
        """The transport in use."""
        return self._transport

    @property
    def lease_duration(self) -> float:
        """Seconds a grant lasts on the granter's clock."""
        return self._duration

    @property
    def persister(self):
        """The :class:`~proto_db.cluster.persister.Persister` started by :meth:`attach`, or ``None``."""
        return self._persister

    @property
    def tier(self):
        """The attached cluster tier (see :mod:`proto_db.cluster.persister`), or ``None``."""
        return self._tier

    def stats(self) -> dict:
        """Counters: leases acquired, attempts, lost attempts, grants given (and requests
        refused for want of an attached tier), winners' releases received, storage root reads
        reported by the caller, replications and their failures, replicated roots accepted
        and refused, recovery rounds, rejoins completed (``rejoining`` while one runs), page
        requests sent and served, ``critical``: the summed seconds spent inside the critical
        section by stage (validation, intent replay, path copy, replication) over
        ``critical_commits`` commits, and the health metric: ``healthy``, ``fenced``,
        ``persisting`` and ``failure`` (the exception that fenced the node or stopped its
        persister, or ``None``)."""
        with self._lock:
            out = dict(self._stats)
            out["critical"] = dict(self._critical)
            failure = self._failure if self._failure is not None else self._persist_failure
            out["healthy"] = failure is None
            out["fenced"] = self._failure is not None
            out["persisting"] = self._persist_failure is None
            out["rejoining"] = self._rejoining
            out["failure"] = None if failure is None else repr(failure)
            return out

    @property
    def healthy(self) -> bool:
        """False once an internal failure fenced this node (see :meth:`fence`) or stopped its
        persister (see :meth:`persistence_failed`)."""
        with self._lock:
            return self._failure is None and self._persist_failure is None

    def persistence_failed(self, error: BaseException) -> None:
        """Record that the persister stopped after an unexpected internal failure ``error``:
        log it and report this node unhealthy.  Granting, acknowledging and acquiring go on
        (persistence is off the commit path); the backlog grows until the process restarts.
        Idempotent (the first failure is kept)."""
        with self._cond:
            first = self._persist_failure is None
            if first:
                self._persist_failure = error
            self._cond.notify_all()
        if first:
            logger.error("Node %s stopped persisting after an internal failure: %r", self._node, error,
                         exc_info=(type(error), error, error.__traceback__))

    def fence(self, error: BaseException) -> None:
        """Fence this node after an unexpected internal failure ``error`` (the worker thread
        crashed): log it, and stop granting, acknowledging and acquiring for good; queued and
        later peer messages are dropped (a replication is refused).  The rest of the cluster
        continues without this node; restart its process to bring it back.  Idempotent (the
        first failure is kept)."""
        with self._cond:
            first = self._failure is None
            if first:
                self._failure = error
            self._cond.notify_all()
        if first:
            logger.error("Node %s is fenced after an internal failure: %r", self._node, error,
                         exc_info=(type(error), error, error.__traceback__))

    def record_critical(self, timings: dict) -> None:
        """Add one commit's critical-section stage timings (seconds) to :meth:`stats`."""
        with self._lock:
            self._stats["critical_commits"] += 1
            for k in _CRITICAL_KEYS:
                self._critical[k] += float(timings.get(k, 0.0))

    # -- lifecycle ---------------------------------------------------------------------------

    def start(self) -> "ClusterCoordinator":
        """Start the transport and the worker; begin granting after the startup grace.  Returns ``self``."""
        with self._lock:
            if self._state != "new":
                raise ProtoValidationError(f"The coordinator is {self._state}; it cannot be started")
            self._grants = PeerGrants(self._duration, self._clock, startup_grace=self._grace)
            self._state = "running"
        self._worker = threading.Thread(target=self._work, name=f"proto-cluster-worker-{self._node}",
                                        daemon=True)
        self._worker.start()
        addresses = {i: ((p.host, p.port) if p.port else None) for i, p in self._peers.items()}
        try:
            self._transport.start(self._node, addresses, self._on_message)
        except BaseException:
            with self._lock:
                self._state = "stopped"
            self._jobs.put(_STOP)
            self._worker.join()
            raise
        return self

    def stop(self) -> None:
        """Stop the persister, the transport and the worker; wake every waiter.  Idempotent.

        A lease still held is revoked (its ``is_valid()`` turns False at once) and not
        released explicitly; the peers let it expire.
        """
        with self._cond:
            if self._state == "stopped":
                return
            started = self._state == "running"
            self._state = "stopped"
            if self._held is not None:
                self._held.revoke()
            persister, self._persister = self._persister, None
            self._cond.notify_all()
        if persister is not None:
            persister.stop()
        rejoiner = self._rejoiner
        if rejoiner is not None and rejoiner is not threading.current_thread() and rejoiner.ident is not None:
            rejoiner.join()             # woken by the notify above; it checks the state first
        if started:
            self._transport.stop()
        worker = self._worker
        if worker is not None:
            self._jobs.put(_STOP)
            if worker is not threading.current_thread():
                worker.join()

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc, tb):
        self.stop()
        return False

    def _check_running(self) -> None:
        if self._state != "running":
            raise ProtoValidationError(f"The coordinator is {self._state}")

    def _check_healthy(self) -> None:
        if self._failure is not None:
            raise ProtoUnexpectedError(f"Node {self._node} is fenced after an internal failure: "
                                       f"{self._failure!r}") from self._failure

    def attach(self, tier) -> None:
        """Attach the cluster tier of the space this node commits to and start its persister.

        ``tier`` provides the storage and the tier operations described in
        :mod:`proto_db.cluster.persister`.  The roots and the promise the tier logged (its
        acceptor log) are loaded, so a restarted node knows what it accepted and never
        grants a term it granted before; an empty log starts a rejoin (module docstring)
        in a background thread.  Attaching the same tier again is a no-op; another one
        raises ``ProtoValidationError``.
        """
        from .persister import Persister
        with self._lock:
            self._check_running()
            if self._tier is not None:
                if self._tier is tier or self._tier.storage is tier.storage:
                    return
                raise ProtoValidationError("This coordinator already serves another storage")
            self._tier = tier
        try:
            logged = tier.load_roots()
        except BaseException:
            with self._lock:
                self._tier = None
            raise
        accepted, committed, top, promised, empty = logged
        rejoin = empty and bool(self._others)
        with self._cond:
            if accepted is not None:
                self._observe_accepted(True, accepted[1], accepted[0])
            if committed is not None:
                self._observe_committed(True, committed[1], committed[0], None)
            self._hint = max(self._hint, top)
            self._grants.restore(max(promised, accepted[0] if accepted is not None else 0))
            tier.set_fetcher(self.fetch_record)
            self._confirmed = False
            self._rejoining = rejoin
            persister = Persister(self, tier, interval=self._persist_interval, lag=self._persist_lag)
            self._persister = persister
        persister.start()
        if rejoin:
            rejoiner = threading.Thread(target=self._rejoin, args=(tier,), name=f"proto-cluster-rejoin-{self._node}",
                                        daemon=True)
            with self._lock:
                self._rejoiner = rejoiner
            rejoiner.start()

    def _rejoin_answers(self) -> int:
        """Answers from other members a rejoin needs: ``N - majority + 1`` (every member when
        fewer).  Any majority that granted a term holds at least ``majority - 1`` members other
        than this node, and that many answers from the ``N - 1`` others always include one of
        them; this node's own (empty) knowledge counts for nothing."""
        if self._rejoin_forced:
            return self._majority - 1
        return min(len(self._others), self.cluster_size - self._majority + 1)

    def force_rejoin(self) -> None:
        """Disaster recovery: let this node's rejoin (now or at the next :meth:`attach`)
        complete with the answers of a bare majority counting itself, ``majority - 1`` other
        members, instead of ``N - majority + 1``.

        Only for an operator who knows that the members that cannot answer lost their acceptor
        logs too (a majority of disks lost: the cluster then refounds from the persisted root)
        or that the cluster is new.  Used otherwise, two leases may share a term and a
        committed root may be lost; that is why it is never automatic.
        """
        with self._cond:
            self._rejoin_forced = True
            self._cond.notify_all()
        logger.warning("Node %s: rejoin forced by the operator (bare majority)", self._node)

    def _rejoin(self, tier) -> None:
        """Learn the highest term promised or seen by a majority, record it, and start voting
        (see "Rejoining" in the module docstring).  Retries until it succeeds, the tier is
        detached or the coordinator stops."""
        while True:
            needed = self._rejoin_answers()
            with self._cond:
                if self._state != "running" or self._tier is not tier or not self._rejoining:
                    return
            try:
                waiter = self._ask("query", {}, lambda w: len(w.answers) >= len(self._others),
                                   self._request_timeout)
                with self._lock:
                    answers, promised = dict(waiter.answers), dict(waiter.promised)
                if len(answers) >= needed:
                    learnt = max([promised[p] for p in answers]
                                 + [max(a[2], c[2]) for a, c, _ in answers.values()])
                    tier.log_promise(learnt, force=True)        # also marks the rejoin as done
                    with self._cond:
                        if self._tier is not tier:
                            return
                        self._grants.restore(learnt)
                        self._hint = max(self._hint, learnt)
                        for _, (ck, croot, clabel), _ in answers.values():
                            self._observe_committed(ck, croot, clabel, None, accept=False)
                        self._rejoining = False
                        self._rejoin_forced = False
                        self._stats["rejoins"] += 1
                        self._cond.notify_all()
                    logger.info("Node %s rejoined with an empty acceptor log; highest term learnt %d",
                                self._node, learnt)
                    return
            except (ProtoDBError, OSError) as e:
                with self._lock:
                    if self._state != "running":
                        return
                logger.warning("Node %s: rejoin round failed (%s); retrying", self._node, e)
            except Exception as e:
                self.fence(e)
                raise
            with self._cond:
                self._cond.wait_for(lambda: self._state != "running", timeout=self._request_timeout)

    def wait_idle(self, timeout: Optional[float] = None) -> bool:
        """Wait until every queued peer message (replications, releases, fetches) has been
        handled; return False after ``timeout`` seconds.  Messages are handled in arrival order
        by one worker thread; tests use this to make a delivery synchronous."""
        with self._cond:
            return self._cond.wait_for(lambda: self._queued == 0, timeout=timeout)

    def await_records(self, root: Optional[RootRecord], label: int, lease: Lease) -> bool:
        """Wait (briefly, under the lease) until the records of the root ``root`` labelled
        ``label`` are stored here: the replication carrying them may still be in this node's
        queue when the lease is won.  Returns False after ``min(fetch_timeout, a quarter of
        the remaining lease)``; reads then fall back to peer page requests."""
        if root is None or label == 0:
            return True             # the empty space, or a root written outside the cluster
        wanted = root_key(label, root)
        timeout = min(self._fetch_timeout, max(0.0, lease.remaining()) / 4)
        with self._cond:
            return self._cond.wait_for(lambda: wanted in self._stored or self._state != "running",
                                       timeout=timeout) and wanted in self._stored

    def _note_stored(self, label: int, root: Optional[RootRecord]) -> None:
        """The records of the root ``root`` labelled ``label`` are stored here (lock held)."""
        key = root_key(label, root)
        self._stored[key] = None
        self._stored.move_to_end(key)
        while len(self._stored) > _STORED_MEMORY:
            self._stored.popitem(last=False)
        self._cond.notify_all()

    def refound(self, persisted_label: int) -> int:
        """A granting majority knows no root: the cluster restarts from the persisted root
        (labelled ``persisted_label``).  Raise this node's term floor to
        ``persisted_label + REFOUND_TERM_STEP`` and return it.

        A node outside that majority may hold roots accepted after the persisted one but lost
        with the majority's disks; their labels are below the persisted label plus the number
        of leases granted since, far below the step, so the refounded lineage supersedes
        them instead of being superseded when that node returns (D46).
        """
        with self._lock:
            self._hint = max(self._hint, persisted_label + REFOUND_TERM_STEP)
            return self._hint

    @property
    def needs_confirmation(self) -> bool:
        """True until a lease confirmed this node's committed root after :meth:`attach`.

        A node that (re)starts knows only the roots its log recorded; a commit made while it
        was down, or whose release it missed, may be known committed only by others.  The
        space confirms by an empty commit under the lease (which commits the newest root of
        the granting majority if needed) before its first snapshot, so reads never go
        backwards across a restart.
        """
        with self._lock:
            return not self._confirmed

    def confirmed(self) -> None:
        """Record that a lease confirmed the committed root (see :attr:`needs_confirmation`)."""
        with self._lock:
            self._confirmed = True

    def detach(self, tier) -> None:
        """Stop the persister of ``tier`` and forget the tier (the space is closing)."""
        with self._lock:
            if self._tier is None or (self._tier is not tier and self._tier.storage is not tier.storage):
                return
            persister, self._persister = self._persister, None
            self._tier = None
            self._rejoining = False
            self._cond.notify_all()
        if persister is not None:
            persister.stop()
        tier.set_fetcher(None)

    # -- known roots -------------------------------------------------------------------------

    def known_root(self) -> tuple:
        """``(known, RootRecord or None)``: the newest committed root this node knows (``None``
        with ``known`` True is the empty space).  Transactions start from it."""
        with self._lock:
            return self._committed[0], self._committed[1]

    def accepted_root(self) -> tuple:
        """``(known, RootRecord or None, label)``: the highest root this node accepted."""
        with self._lock:
            return self._accepted

    def committed_root(self) -> tuple:
        """``(known, RootRecord or None, label, committer, learnt_at, version)``."""
        with self._lock:
            known, record, label = self._committed
            return known, record, label, self._committed_by, self._committed_at, self._committed_version

    @property
    def committed_epoch(self) -> int:
        """Epoch of the newest committed root this node knows (``0`` when none)."""
        with self._lock:
            record = self._committed[1]
            return record.epoch if record is not None else 0

    def wait_committed_change(self, version: int, timeout: float) -> int:
        """Wait until the committed root changes from ``version`` (or ``timeout``); return the version."""
        with self._cond:
            self._cond.wait_for(lambda: self._committed_version != version or self._state != "running",
                                timeout=timeout)
            return self._committed_version

    def _learn_label(self, label: int) -> None:
        """A root labelled ``label`` is known: clear a suspicion it answers (lock held)."""
        self._hint = max(self._hint, label)
        if self._suspect_term and label >= self._suspect_term:
            self._suspect_term = 0

    def _observe_accepted(self, known: bool, root: Optional[RootRecord], label: int) -> bool:
        """Adopt ``root`` as accepted if it orders after the accepted root (lock held)."""
        if not known:
            return False
        self._learn_label(label)
        if self._accepted[0] and compare_roots(label, root, self._accepted[2], self._accepted[1]) <= 0:
            return False
        self._accepted = (True, root, label)
        return True

    def _observe_committed(self, known: bool, root: Optional[RootRecord], label: int, by, *,
                           accept: bool = True) -> bool:
        """Adopt ``root`` as committed if it orders after the committed root (lock held).  A root
        known committed through the cluster is also accepted knowledge (``accept``); one read
        from the persisted root is not (with ``accept=False``): a granting majority that knows
        nothing newer must refound (:meth:`refound`)."""
        if not known:
            return False
        if accept:
            self._observe_accepted(True, root, label)
        else:
            self._learn_label(label)
        if self._committed[0] and compare_roots(label, root, self._committed[2], self._committed[1]) <= 0:
            return False
        self._committed = (True, root, label)
        self._committed_by = by
        self._committed_at = time.monotonic()
        self._committed_version += 1
        self._cond.notify_all()
        return True

    def observe_root(self, root: Optional[RootRecord], *, from_storage: bool = False, term: int = 0,
                     accept: bool = True) -> None:
        """Learn ``root`` (``None``: the empty space), committed with label ``term``.

        ``from_storage=True`` counts a storage root read.  ``accept=False`` (the persisted
        root, D46) does not make it accepted knowledge (see :meth:`_observe_committed`).
        """
        with self._lock:
            self._observe_committed(True, root, term, None, accept=accept)
            if from_storage:
                self._stats["storage_root_reads"] += 1

    def _wire(self, triple) -> object:
        known, root, label = triple
        return encode_root(root, known=known, term=label)

    # -- message handling --------------------------------------------------------------------

    def _on_message(self, msg: dict) -> None:
        sender = msg["from"]
        if sender not in self._peers or sender == self._node:
            logger.warning("Node %s ignored a %r message from unknown sender %r", self._node, msg["type"], sender)
            return
        kind = msg["type"]
        if kind in ("records", "replicate", "release", "fetch"):
            # A release follows its replicate (and a replicate its record chunks) through the
            # same queue, so a root is never known committed here before its records are stored.
            with self._lock:
                fenced = self._failure is not None
                if not fenced:
                    self._queued += 1
                    self._jobs.put(msg)
            if fenced:
                self._refuse_queued(msg)
            return
        reply = None
        promise = None
        with self._cond:
            if self._state != "running":
                return
            if kind == "request":
                reply = self._decide(sender, msg["term"], msg["rid"], msg["inc"])
                if reply["type"] == "grant":
                    promise = self._tier
            elif kind in ("grant", "reject"):
                self._on_reply(sender, msg)
            elif kind == "query":
                reply = make_message("answer", self._node, rid=msg["rid"], root=self._wire(self._accepted),
                                     committed=self._wire(self._committed), certain=not self._suspect_term,
                                     promised=max(self._grants.highest_term, self._accepted[2],
                                                  self._committed[2]))
            else:
                self._on_answer(sender, kind, msg)
        if promise is not None and not self._promise(promise, msg["term"]):
            return                  # a promise that could not be made durable is not sent
        if reply is not None:
            self._transport.send(sender, reply)

    def _promise(self, tier, term: int) -> bool:
        """Make the promise of ``term`` durable in ``tier``'s acceptor log; False (logged) if it
        failed, in which case the grant must not be sent or counted."""
        try:
            tier.log_promise(term)
            return True
        except (ProtoDBError, OSError) as e:
            logger.error("Node %s could not record its promise of term %d; the grant is withheld: %s",
                         self._node, term, e)
            return False

    def _refusal(self) -> Optional[str]:
        """Why this node may not vote or accept right now (lock held), or ``None``."""
        if self._failure is not None:
            return "fenced"
        if self._tier is None:
            return "no_storage"
        if self._rejoining:
            return "rejoining"
        return None

    def _decide(self, requester: str, term: int, rid: int, inc: int) -> dict:
        """Apply the grant rules to a request (lock held); return the reply.  A node without
        an attached tier (nowhere durable to record the promise) or fenced refuses."""
        refusal = self._refusal()
        if refusal is not None:
            if refusal == "no_storage":
                self._stats["grants_refused_no_storage"] += 1
            return make_message("reject", self._node, term=term, rid=rid, root=self._wire(self._accepted),
                                certain=not self._suspect_term, reason=refusal,
                                highest=self._grants.highest_term, remaining=0.0)
        if ballot_rank(term) != self._ranks[requester]:
            # Not this requester's ballot: granting it could give one term to two proposers.
            logger.warning("Node %s refused term %d from %s: not a ballot of its rank", self._node, term,
                           requester)
            return make_message("reject", self._node, term=term, rid=rid, root=self._wire(self._accepted),
                                certain=not self._suspect_term, reason="bad_ballot",
                                highest=self._grants.highest_term, remaining=0.0)
        decision = self._grants.on_request(requester, term, inc)
        if decision.previous_expired:
            self._suspect_term = max(self._suspect_term, decision.ended_term)
        root = self._wire(self._accepted)
        certain = not self._suspect_term
        if decision.granted:
            self._stats["grants_given"] += 1
            return make_message("grant", self._node, term=term, rid=rid, root=root,
                                committed=self._wire(self._committed), certain=certain)
        return make_message("reject", self._node, term=term, rid=rid, root=root, certain=certain,
                            reason=decision.reason, highest=decision.highest,
                            remaining=max(0.0, decision.remaining))

    def _on_reply(self, sender: str, msg: dict) -> None:
        self._hint = max(self._hint, msg["term"], msg.get("highest", 0))
        known, root, label = decode_labeled_root(msg["root"])
        self._hint = max(self._hint, label)
        attempt = self._attempt
        if attempt is None or attempt.rid != msg["rid"] or attempt.term != msg["term"]:
            return
        if msg["type"] == "grant":
            attempt.on_grant(sender, known, root, msg["certain"], label)
            # The granter's committed root (it learnt it from the previous holder's release):
            # the next holder then knows its base is committed and needs no extra round.
            ck, croot, clabel = decode_labeled_root(msg["committed"])
            self._observe_committed(ck, croot, clabel, None, accept=False)
        else:
            attempt.on_reject(sender, msg["reason"], msg["highest"], msg["remaining"])
        self._cond.notify_all()   # decided, or a majority answered (straggler rule)

    def _on_release(self, sender: str, msg: dict) -> Optional[tuple]:
        """Handle a release (lock held); return ``(label, root)`` when it taught a newer
        committed root (logged by the caller, outside the lock)."""
        learnt = None
        ended = self._grants.on_release(sender, msg["term"], msg["inc"])
        if ended:
            self._suspect_term = max(self._suspect_term, ended)
        # The sender's accepted root is only a hint here: a root becomes accepted at this node
        # only with its records (a replication) or once known committed.
        known, root, label = decode_labeled_root(msg["root"])
        self._hint = max(self._hint, label)
        if msg["held"]:
            self._stats["releases_received"] += 1
            ck, croot, clabel = decode_labeled_root(msg["committed"])
            if self._observe_committed(ck, croot, clabel, sender):
                learnt = (clabel, croot)
            if not msg["certain"]:
                # The holder may have published something it could not replicate: whatever
                # its term decided is unknown here until a root of that term or later arrives.
                self._suspect_term = max(self._suspect_term, msg["term"])
        # Any release -- a winner's or a lost attempt's cancel -- may free the peer that
        # refused us, so it ends a waiting requester's back-off.
        self._wakeups += 1
        self._cond.notify_all()
        return learnt

    def _on_answer(self, sender: str, kind: str, msg: dict) -> None:
        waiter = self._waiters.get(msg["rid"])
        if waiter is None:
            return
        if kind == "ack":
            if waiter.kind != "replicate" or msg["term"] != waiter.term:
                return
            (waiter.acks if msg["ok"] else waiter.nacks).add(sender)
            known, root, label = decode_labeled_root(msg["root"])
            self._hint = max(self._hint, label)
        elif kind == "answer":
            if waiter.kind != "query":
                return
            waiter.answers[sender] = (decode_labeled_root(msg["root"]), decode_labeled_root(msg["committed"]),
                                      msg["certain"])
            waiter.promised[sender] = msg["promised"]
        elif kind == "fetched":
            if waiter.kind != "fetch":
                return
            if msg["data"] is not None and waiter.data is None:
                waiter.data = msg["data"]
            else:
                waiter.missing += 1
        self._cond.notify_all()

    def _broadcast(self, msg: dict) -> None:
        for peer in self._others:
            self._transport.send(peer, msg)

    # -- the worker: replicated records, page requests, prefetch ---------------------------

    def _work(self) -> None:
        while True:
            msg = self._jobs.get()
            if msg is _STOP:
                return
            try:
                kind = msg["type"]
                if kind == "replicate":
                    self._apply_replicate(msg)
                elif kind == "records":
                    self._buffer_records(msg)
                elif kind == "release":
                    with self._cond:
                        learnt = self._on_release(msg["from"], msg) if self._state == "running" else None
                        tier = self._tier
                    if learnt is not None and tier is not None:
                        # Logged so that a restarted node never serves an older root than it did.
                        tier.log_root("committed", learnt[0], learnt[1], sync=False)
                else:
                    self._serve_fetch(msg)
            except (ProtoDBError, OSError) as e:
                logger.error("Node %s: handling a %r message failed: %s", self._node, msg["type"], e)
            except Exception as e:
                # A bug or an unforeseen failure: never die silently, never keep acknowledging
                # with a broken worker.  Fence the node, refuse what is still queued (nothing
                # is enqueued once fenced), then let the thread fail loudly.
                self.fence(e)
                self._drain()
                raise
            finally:
                with self._cond:
                    self._queued -= 1
                    self._cond.notify_all()

    def _refuse_queued(self, msg: dict) -> None:
        """A fenced node's answer to a peer message it will not handle: a replication is
        refused (its holder need not wait for this node), anything else is dropped."""
        if msg["type"] == "replicate":
            self._transport.send(msg["from"], make_message("ack", self._node, term=msg["term"], rid=msg["rid"],
                                                           ok=False, root=self._wire(self._accepted)))

    def _drain(self) -> None:
        """Refuse every message still queued for the worker (fenced; worker thread)."""
        while True:
            try:
                msg = self._jobs.get_nowait()
            except queue.Empty:
                return
            if msg is _STOP:
                continue
            try:
                self._refuse_queued(msg)
            finally:
                with self._cond:
                    self._queued -= 1
                    self._cond.notify_all()

    def _buffer_records(self, msg: dict) -> None:
        """Keep a chunk of records until its replicate arrives (worker thread)."""
        with self._lock:
            if self._state != "running" or self._tier is None:
                return
            sender = msg["from"]
            pending = self._chunks.get(sender)
            if pending is None or pending[0] != msg["rid"]:
                pending = self._chunks[sender] = (msg["rid"], {})   # an older replication is void
            pending[1][msg["index"]] = msg["records"]

    def _may_accept(self, term: int, root: Optional[RootRecord], label: int) -> bool:
        """The acceptance rules of a replicated root (lock held): running, healthy, a tier
        attached, not starting, no higher term granted here (ballot rule), and not older
        than the accepted root."""
        if self._state != "running" or self._refusal() is not None or self._grants.starting \
                or self._grants.granted_top > term:
            return False
        return not self._accepted[0] or compare_roots(label, root, self._accepted[2], self._accepted[1]) >= 0

    def _apply_replicate(self, msg: dict) -> None:
        sender, term, rid = msg["from"], msg["term"], msg["rid"]
        known, root, label = decode_labeled_root(msg["root"])
        records = msg["records"]
        with self._lock:
            tier = self._tier
            pending = self._chunks.pop(sender, None)
        complete = True
        if msg["chunks"]:
            parts = pending[1] if pending is not None and pending[0] == rid else {}
            complete = all(i in parts for i in range(msg["chunks"]))
            if complete:
                records = [r for i in range(msg["chunks"]) for r in parts[i]] + records
        valid = known and label == term and root is not None and complete
        with self._lock:
            # Acknowledging needs a durable place for the records and the root: a node without
            # a tier never counts toward the majority.
            ok = valid and tier is not None and self._may_accept(term, root, label)
        if valid and tier is not None:
            # Records are stored even when the root is refused (a newer term was granted here
            # meanwhile): immutable data, and this node may be the next holder, whose base can
            # be this root (adopted from its granters); it then reads nothing under its lease.
            if records:
                tier.apply(records, root, label)      # durable before the ack
            with self._cond:
                self._note_stored(label, root)
        with self._lock:
            ok = ok and self._may_accept(term, root, label)
            if ok:
                self._observe_accepted(True, root, label)
                ck, croot, clabel = decode_labeled_root(msg["committed"])
                self._observe_committed(ck, croot, clabel, None)
                self._stats["accepted"] += 1
            else:
                self._stats["refused"] += 1
            reply_root = self._wire(self._accepted)
        if ok:
            tier.log_root("accepted", label, root, sync=True)     # durable before the ack
        self._transport.send(sender, make_message("ack", self._node, term=term, rid=rid, ok=ok, root=reply_root))
        if ok and records:
            tier.prefetch(records)

    def _serve_fetch(self, msg: dict) -> None:
        tier = self._tier
        data = None
        if tier is not None:
            data = tier.local_record(AtomPointer(uuid.UUID(bytes=msg["segment"]), msg["offset"]))
        with self._lock:
            if data is not None:
                self._stats["fetches_served"] += 1
        self._transport.send(msg["from"], make_message("fetched", self._node, rid=msg["rid"], data=data))

    # -- acquiring and releasing -------------------------------------------------------------

    def acquire(self, timeout: Optional[float] = None) -> Lease:
        """Acquire the write lease; raise ``ProtoTimeoutError`` after ``timeout`` seconds
        (default ``acquire_timeout``), ``ProtoValidationError`` if the coordinator stops and
        ``ProtoUnexpectedError`` if the node is fenced (:meth:`fence`).

        One thread of this process at a time holds or requests the lease; others wait here.
        Release with :meth:`release` (or use :meth:`lease`).
        """
        timeout = self._acquire_timeout if timeout is None else _number("timeout", timeout, positive=False)
        deadline = time.monotonic() + timeout
        with self._lock:
            self._check_running()
            self._check_healthy()
        if not self._local.acquire(timeout=timeout):
            raise ProtoTimeoutError(f"Node {self._node}: another thread held the cluster lease for {timeout} s")
        try:
            lease = self._acquire(deadline, timeout)
        except BaseException:
            self._local.release()
            raise
        return lease

    def _await_rejoin(self, deadline: float, timeout: float) -> None:
        """Wait until this node may propose: not while it rejoins (module docstring)."""
        with self._cond:
            self._cond.wait_for(lambda: not self._rejoining or self._state != "running"
                                or self._failure is not None, timeout=max(0.0, deadline - time.monotonic()))
            self._check_running()
            self._check_healthy()
            if self._rejoining:
                raise ProtoTimeoutError(f"Node {self._node} could not learn the promised terms from a majority "
                                        f"within {timeout} s (its acceptor log was empty)")

    def _acquire(self, deadline: float, timeout: float) -> Lease:
        failures = 0
        while True:
            self._await_rejoin(deadline, timeout)
            attempt, request, seen, promise = self._open_attempt()
            self._broadcast(request)
            if promise is not None and not self._promise(promise, attempt.term):
                with self._lock:
                    # Our own vote cannot be made durable: it does not count.
                    attempt.granted_by.pop(self._node, None)
                    attempt.on_reject(self._node, "promise_failed", attempt.term, 0.0)
            lease, cancel, wait = self._close_attempt(attempt)
            if lease is not None:
                return lease
            self._broadcast(cancel)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ProtoTimeoutError(
                    f"Node {self._node} could not obtain the cluster write lease from a majority "
                    f"({self._majority} of {self.cluster_size}) within {timeout} s")
            low, high = self._backoff
            delay = min(high, low * (2 ** min(failures, 30))) * (0.5 + self._rng.random() / 2)
            delay = min(max(delay, wait), remaining)
            failures += 1
            with self._cond:
                self._check_running()
                # ``seen`` dates from before the request: a release that arrived since then
                # (even before the refusals) ends the wait at once.
                self._cond.wait_for(lambda: self._wakeups != seen or self._state != "running", timeout=delay)
                self._check_running()
                woken = self._wakeups != seen
            if woken:
                time.sleep(self._rng.random() * _WAKE_SPREAD)

    def _open_attempt(self) -> tuple:
        """Open an attempt with this node's own vote decided; return ``(attempt, request,
        wakeups seen, tier whose log must record our own promise or None)``."""
        promise = None
        with self._lock:
            self._check_running()
            self._check_healthy()
            term = ballot_after(max(self._grants.highest_term, self._hint, self._accepted[2], self._committed[2]),
                                self._rank)
            attempt = LeaseAttempt(term=term, rid=next(self._rids), sent_at=self._clock(),
                                   cluster_size=self.cluster_size, lease_duration=self._duration,
                                   drift_margin=self._margin)
            self._attempt = attempt
            self._stats["attempts"] += 1
            refusal = self._refusal()
            if refusal is not None:
                attempt.on_reject(self._node, refusal, self._grants.highest_term, 0.0)
            else:
                decision = self._grants.on_request(self._node, term, self._inc)
                if decision.previous_expired:
                    self._suspect_term = max(self._suspect_term, decision.ended_term)
                if decision.granted:
                    known, root, label = self._accepted
                    attempt.on_grant(self._node, known, root, not self._suspect_term, label)
                    promise = self._tier
                else:
                    attempt.on_reject(self._node, decision.reason, decision.highest, decision.remaining)
            request = make_message("request", self._node, term=term, rid=attempt.rid, inc=self._inc)
            seen = self._wakeups
        return attempt, request, seen, promise

    def _close_attempt(self, attempt: LeaseAttempt) -> tuple:
        """Wait for the attempt's outcome; return ``(lease, None, 0)`` or ``(None, cancel, wait)``."""
        with self._cond:
            started = time.monotonic()
            deadline = started + self._request_timeout
            self._cond.wait_for(lambda: attempt.state != "pending" or self._state != "running"
                                or attempt.replies >= attempt.needed, timeout=self._request_timeout)
            if attempt.state == "pending" and self._state == "running":
                straggle = max(_MIN_STRAGGLE, _STRAGGLE_FACTOR * (time.monotonic() - started))
                self._cond.wait_for(lambda: attempt.state != "pending" or self._state != "running",
                                    timeout=max(0.0, min(straggle, deadline - time.monotonic())))
            self._attempt = None
            self._check_running()
            self._hint = max(self._hint, attempt.highest_seen)
            now = self._clock()
            if attempt.state == "won" and now < attempt.valid_until:
                known, root, certain, label = attempt.best_root(own=self._accepted)
                if label < attempt.term:
                    certain = certain and not self._suspect_term
                    lease = Lease(term=attempt.term, acquired_at=now, valid_until=attempt.valid_until,
                                  root=root if known else None, root_certain=certain and known,
                                  clock=self._clock, root_term=label)
                    self._held = lease
                    self._stats["acquired"] += 1
                    return lease, None, 0.0
                # A granter knows a root labelled with this term or later (a restarted cluster
                # reused low terms): a root of this lease would not order after it.  Retry higher.
                self._hint = max(self._hint, label)
            self._stats["lost_attempts"] += 1
            self._grants.on_release(self._node, attempt.term, self._inc)
            cancel = make_message("release", self._node, term=attempt.term, inc=self._inc,
                                  root=self._wire(self._accepted), committed=self._wire(self._committed),
                                  certain=False, held=False)
            return None, cancel, min(attempt.busy_remaining, self._duration)

    def release(self, lease: Lease) -> None:
        """Release ``lease`` and broadcast the accepted and committed roots.  Never raises for
        a lease this node holds; releasing twice is a no-op."""
        with self._lock:
            if lease is not self._held:
                if lease.released_at is not None:
                    return
                raise ProtoUserError("This lease is not held by this coordinator")
            lease._mark_released()
            self._held = None
            self._grants.on_release(self._node, lease.term, self._inc)
            running = self._state == "running"
            msg = make_message("release", self._node, term=lease.term, inc=self._inc,
                               root=self._wire(self._accepted), committed=self._wire(self._committed),
                               certain=not self._suspect_term, held=True)
        try:
            if running:
                self._broadcast(msg)
        finally:
            self._local.release()

    def lease(self, timeout: Optional[float] = None) -> "_LeaseContext":
        """Context manager: :meth:`acquire` on entry (yielding the lease), :meth:`release` on exit."""
        return _LeaseContext(self, timeout)

    # -- the commit point --------------------------------------------------------------------

    def _new_waiter(self, kind: str, term: int = 0) -> tuple:
        rid = next(self._rids)
        waiter = _Waiter(kind, term)
        self._waiters[rid] = waiter
        return rid, waiter

    def replicate(self, lease: Lease, root: RootRecord) -> float:
        """Commit ``root`` (built on ``lease.root``) by majority-acknowledged replication (D46).

        Sends ``(epoch, root pointer)`` labelled with the lease term, with the records the
        tier has not shipped yet, to every peer; makes them durable locally and accepts the
        root here; returns once a majority (this node included) has acknowledged, before the
        lease ends; then records the root as committed.  Returns the seconds spent.

        Raises ``ProtoConflictError`` when the lease ended before anything was sent (nothing
        was replicated: safe to retry), and ``ProtoOutcomeUnknownError`` when no majority
        acknowledged in time, or when anything fails once the root was sent (the space
        closing, an I/O error of the local log): peers may have acknowledged it, and a later
        holder may still adopt it, so the commit may or may not take effect.
        """
        started = time.perf_counter()
        tier = self._tier
        if tier is None:
            raise ProtoValidationError("replicate needs an attached tier (ObjectSpace attaches it)")
        if lease is not self._held:
            raise ProtoUserError("This lease is not held by this coordinator")
        records = tier.outgoing()
        label = lease.term
        # Records that ride with the replication are made durable by every receiver; records in
        # a shared storage must be durable there before any peer can acknowledge their root.
        riding = bool(getattr(tier, "cluster_tier", False))
        if not riding:
            tier.flush_local()
        chunks = _split_records(records, protocol.CHUNK_BYTES)
        with self._lock:
            self._note_stored(label, root)            # our own records
            self._check_running()
            if not lease.is_valid():
                raise ProtoConflictError("The cluster write lease expired before the root was replicated; "
                                         "nothing was replicated")
            rid, waiter = self._new_waiter("replicate", label)
            parts = [make_message("records", self._node, term=label, rid=rid, index=i, records=chunk)
                     for i, chunk in enumerate(chunks[:-1])]
            msg = make_message("replicate", self._node, term=label, rid=rid,
                               root=encode_root(root, known=True, term=label),
                               committed=self._wire(self._committed), records=chunks[-1], chunks=len(parts))
            self._stats["replications"] += 1
        failure = None
        try:
            for part in parts:                         # same ordered link, before the replicate
                self._broadcast(part)
            self._broadcast(msg)
            if riding:
                tier.flush_local()                     # our own records, durable before our own ack
            with self._lock:
                self_ok = self._may_accept(label, root, label)
                if self_ok:
                    self._observe_accepted(True, root, label)
            if self_ok:
                tier.log_root("accepted", label, root, sync=True)
                with self._lock:
                    waiter.acks.add(self._node)
            with self._cond:
                total = self.cluster_size

                def decided():
                    return (len(waiter.acks) >= self._majority or len(waiter.nacks) > total - self._majority
                            or self._state != "running" or not lease.is_valid())

                while not decided():
                    self._cond.wait(timeout=max(0.001, min(0.05, lease.remaining())))
                ok = len(waiter.acks) >= self._majority and lease.is_valid()
                if ok:
                    self._observe_committed(True, root, label, self._node)
                    if self._suspect_term and self._suspect_term <= label:
                        self._suspect_term = 0
                else:
                    self._stats["replication_failures"] += 1
                    self._suspect_term = max(self._suspect_term, label)
                acks = len(waiter.acks)
        except (ProtoDBError, OSError) as e:
            # The root left this node: peers may acknowledge it whatever happens here, so a
            # failure now (the space closing under the commit, a log write) leaves the outcome
            # unknown -- never a validation error, which would claim nothing took effect.
            failure, ok, acks = e, False, len(waiter.acks)
            with self._lock:
                self._stats["replication_failures"] += 1
                self._suspect_term = max(self._suspect_term, label)
        finally:
            with self._lock:
                self._waiters.pop(rid, None)
        if failure is not None:
            raise ProtoOutcomeUnknownError(
                f"Node {self._node}: the commit of root epoch {root.epoch} failed after the root was sent "
                f"({failure}); peers may have acknowledged it, so it may or may not take effect") from failure
        if not ok:
            raise ProtoOutcomeUnknownError(
                f"Node {self._node}: only {acks} of the {self._majority} acknowledgements needed arrived "
                f"before the lease ended; the root (epoch {root.epoch}) may or may not take effect")
        try:
            tier.shipped(records, root, label)
            tier.log_root("committed", label, root, sync=False)
        except (ProtoDBError, OSError) as e:
            # Committed already: the bookkeeping only speeds up a restart and local release,
            # never fail the commit for it (the space may be closing).
            logger.warning("Node %s could not record committed root epoch %d: %s", self._node, root.epoch, e)
        return time.perf_counter() - started

    # -- recovery and peer services ----------------------------------------------------------

    def _ask(self, kind: str, msg_fields: dict, done: Callable, timeout: float, targets=None) -> _Waiter:
        with self._lock:
            self._check_running()
            rid, waiter = self._new_waiter(kind)
        msg = make_message(kind, self._node, rid=rid, **msg_fields)
        try:
            for peer in (self._others if targets is None else targets):
                self._transport.send(peer, msg)
            with self._cond:
                self._cond.wait_for(lambda: done(waiter) or self._state != "running", timeout=timeout)
                return waiter
        finally:
            with self._lock:
                self._waiters.pop(rid, None)

    def recover_root(self) -> tuple:
        """Recovery round: ask every peer for its roots and wait for a majority (this node included).

        Returns ``(known, root, label)``: the highest accepted root among the answers and
        this node's.  Every committed root was acknowledged by a majority, which intersects
        the answering majority, so the result is the newest committed root or a root built on
        it.  Committed roots learnt are adopted and the suspicion is cleared; the accepted
        roots of others are not (a root is accepted here only with its records).
        Raises ``ProtoConflictError`` when no majority answers within ``request_timeout``.
        """
        needed = self._majority - 1
        waiter = self._ask("query", {}, lambda w: len(w.answers) >= len(self._others), self._request_timeout)
        with self._lock:
            answers = dict(waiter.answers)
            self._stats["recoveries"] += 1
            if len(answers) < needed:
                raise ProtoConflictError(f"Node {self._node}: only {len(answers) + 1} of the {self._majority} "
                                         f"members needed answered the recovery query")
            best = self._accepted
            for (ak, aroot, alabel), (ck, croot, clabel), _ in answers.values():
                # A peer's committed root may be the persisted one only: never accepted knowledge.
                self._observe_committed(ck, croot, clabel, None, accept=False)
                self._hint = max(self._hint, alabel)
                if ak and (not best[0] or compare_roots(alabel, aroot, best[2], best[1]) > 0):
                    best = (True, aroot, alabel)
            self._suspect_term = 0
            return best

    def current_root(self) -> tuple:
        """``(known, root)``: the committed root transactions start from, recovering it when
        unknown (a joining or restarted node): from a majority of peers, else from the tier's
        persisted root.  ``(False, None)`` when nothing can be learnt."""
        known, root = self.known_root()
        if known:
            return known, root
        try:
            self.recover_root()
        except ProtoConflictError as e:
            logger.info("Node %s: recovery from the peers failed (%s); reading the persisted root", self._node, e)
        known, root = self.known_root()
        if known:
            return known, root
        tier = self._tier
        if tier is not None:
            persisted = tier.read_persisted()
            with self._lock:
                self._stats["storage_root_reads"] += 1
                if persisted is None:
                    self._observe_committed(True, None, 0, None, accept=False)
                else:
                    self._observe_committed(True, persisted[1], persisted[0], None, accept=False)
        return self.known_root()

    def fetch_record(self, ptr) -> Optional[bytes]:
        """Peer page request: the framed record at ``ptr`` from the first peer that has it, or
        ``None`` (no peer answered with it within ``fetch_timeout``, nor within what remains
        of a lease this node holds)."""
        others = len(self._others)
        if not others:
            return None
        with self._lock:
            if self._state != "running":
                return None
            self._stats["fetches_requested"] += 1
            timeout = self._fetch_timeout
            if self._held is not None:
                timeout = min(timeout, self._held.remaining())
        waiter = self._ask("fetch", {"segment": ptr.segment.bytes, "offset": ptr.offset},
                           lambda w: w.data is not None or w.missing >= others, timeout)
        return waiter.data


class _LeaseContext:
    """``with coordinator.lease() as lease:`` (see :meth:`ClusterCoordinator.lease`)."""

    def __init__(self, coordinator: ClusterCoordinator, timeout: Optional[float]):
        self._coordinator = coordinator
        self._timeout = timeout
        self._lease = None

    def __enter__(self) -> Lease:
        self._lease = self._coordinator.acquire(self._timeout)
        return self._lease

    def __exit__(self, exc_type, exc, tb):
        self._coordinator.release(self._lease)
        return False
