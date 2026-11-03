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
    This holds across power loss only when the tier uses ``durability="fsync"``: with
    ``"flush"`` or ``"none"`` the promise is not synced, and a power loss on even one node
    can make it forget a promise (open defect D-SAFE-2, see "The commit point" below).
    A node grants, and acknowledges replications, only while a cluster tier (the storage
    of a space) is attached and while it is not fenced: a node without a durable place for
    its promises and roots never counts toward a majority.
Rejoining and witnesses
    A node is a *witness* -- its grants, acknowledgements and ``answer``s count -- only
    while its acceptor log is loaded (a space is open on it) and rejoined.  A node whose
    log has no rejoined marker (a new node, a disk lost or wiped, or an interrupted rejoin)
    cannot tell what it promised, proposed or accepted in a previous life.  Until it has
    learnt, from ``N - majority + 1`` other witnesses (``query``/``answer``; for three
    nodes, both others), the highest term they promised or saw AND the highest root they
    accepted, it neither grants, nor acknowledges, nor proposes, and its answers are marked
    ``empty``.  It then makes every record of that root durable on its own disk (copied from
    its peers, commit by commit back to the persisted root) or confirms that the object
    store holds it -- a root travels with its records (ruling R50) -- and only then logs that
    term and that root, then the rejoined marker; records it cannot obtain leave it
    rejoining, with the reason in ``stats()["rejoin_blocked"]``.  Its own
    knowledge counts for nothing, so the answering witnesses intersect, outside this node,
    every majority that granted a term or acknowledged a root: it never proposes one of
    its own ballots again, never accepts a root below a ballot it granted, and reports a
    root at or above every committed root, as it did before it lost its log.  A node without
    a loaded log answers ``unknown`` and is never counted.  ``empty`` answers count only
    for a cluster's first start: every other member answered, none has accepted any root,
    and none knows of any lease term (ruling R51: a lease granted before the first commit
    leaves its term at its granters and its holder).  Any other rejoin decides only on a
    round of answers sent at or after ``attach + restart_grace``
    (:attr:`ClusterCoordinator.restart_grace`), the instant the empty log was attached plus
    the safe grace, whatever the node's startup grace (rulings R53, R54): the node may have
    accepted and acknowledged a root just before it lost its log, under a lease whose holder
    has not counted that acknowledgement yet, and answers gathered while that lease runs may
    all miss the root (the other acceptors have not received it, the holder self-accepts
    after its broadcast or its answer is lost).  The lost log accepted nothing after it was
    detached (a node without an attached log neither grants nor accepts), so every lease it
    served was granted before the empty log was attached -- by a previous process, or by
    this one before :meth:`ClusterCoordinator.detach` when a space is closed and reopened
    over a replaced disk on a coordinator that keeps running -- and has ended on its
    holder's view by ``attach + restart_grace``.  A commit is reported only with
    acknowledgements counted within the lease, so every acceptor counted for it had durably
    accepted the root before it answered such a round.  Any other loss of a majority of logs
    needs the operator's :meth:`ClusterCoordinator.force_rejoin`.  With proposer-qualified ballots, two
    different roots never share a label, and no acknowledged root is lost, while at most
    one acceptor log is lost at a time, with its records, whether the object store is
    reachable or not (D46, rulings R47, R49 and R50).  The price is
    availability: a node with an empty log -- every node on a cluster's first start --
    needs those answers before it votes.
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
    WAL ``fsync`` of a majority.  **Only with** ``durability="fsync"``: TieredStorage in
    ``"flush"`` or ``"none"`` mode syncs nothing (promises, accepted roots and records stay
    in the OS cache), so the cluster is then process-crash safe only -- a power loss on a
    majority can lose acknowledged commits, and a power loss on even one node can make it
    forget a promise and void the one-holder-per-term argument (reasoned, not reproduced).
    Tracked as defect D-SAFE-2, to be fixed by Task 8.3e (acceptor metadata always fsynced).
    Use ``fsync`` for any cluster that must survive power loss.  A cluster over ``MemoryStorage`` keeps its acceptor logs
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
    ``query``/``answer`` (recovery, joining peers), ``fetch``/``fetched`` (peer page
    requests for records not yet in the object store) and ``commit_query``/``commit_info``
    (where the records of a commit are: covered by the persisted root, or their byte ranges).

Health
    An unexpected exception in the worker thread fences the node
    (:meth:`ClusterCoordinator.fence`): it is logged, the node stops granting, accepting
    and acquiring, drops (or refuses) the peer messages still queued and arriving,
    :attr:`ClusterCoordinator.healthy` and ``stats()["healthy"]`` turn False, and the
    exception is re-raised in its thread.  An unexpected exception in the persister thread
    only stops persisting (:meth:`ClusterCoordinator.persistence_failed`): persistence is
    off the commit path, so the node keeps granting and acknowledging, but it reports
    itself unhealthy.

    A failed write on the attached tier -- an acceptor-log write (an acceptance, a promise,
    a committed root, a rejoin's entries) or a write of the records an acceptance or a commit
    vouches for -- makes the tier *failed* (8.3d re-review 9, B1; ruling R57): what was
    being decided is never acknowledged nor granted, and from then on the node refuses every
    grant and replication (reason ``"tier_failed"``), answers ``unknown`` (never counted),
    does not rejoin, and :meth:`ClusterCoordinator.acquire` raises ``ProtoUnexpectedError``.
    It is not a fence: the coordinator is sound and the failure belongs to that tier (a
    storage poisons its handles after an I/O error, :mod:`proto_db.cluster.rootlog`,
    :mod:`proto_db.storage.tiered`).  :meth:`ClusterCoordinator.detach` clears it, so
    closing the space and reopening it on a healthy disk brings the node back (its log is
    loaded as after a restart).  ``healthy`` is False meanwhile and ``stats()["tier_failed"]``
    True.

Acceptor-log writes (8.3d re-review 7, N1; ruling R55)
    Every acceptor decision -- an acceptance (a peer's, or the holder's own), a grant and
    our own vote, a rejoin's adoption, a committed root learnt and logged -- runs under one
    acceptor-log lock together with its log write: decided on the tier the decision is
    about (for an acceptance, the tier its records went to) and only while that tier is
    attached, written to that tier's log, and, for an acceptance, published in memory only
    once written; the acknowledgement is sent after.  :meth:`ClusterCoordinator.attach` and
    :meth:`ClusterCoordinator.detach` change the tier under the same lock, so no write lands
    on a log after it was detached, a decision in flight finishes before the detach
    returns, and an attach never loads a log that a decision is still writing to.  Memory
    therefore never holds an acceptance its attached log lacks.  Grants are decided under
    that lock too because a grant reports the accepted root: an acceptance decided but not
    yet published would otherwise be missing from it.  A grant's promise is raised in memory
    when it is decided and logged before the grant is sent; a promise in memory that failed
    to log only refuses more.  A closed log handle refuses writes on every backend
    (:mod:`proto_db.cluster.rootlog`), as a second line of defence.  Committed roots learnt
    from the cluster are majority-accepted already; holding one in memory before (or
    without) logging it is safe, and a restart drops it as it drops any unlogged knowledge.

Threads: the transport's own threads, plus one worker started by :meth:`start` that
applies replicated records, serves page requests and prefetches, and the persister
started by :meth:`attach`.  One lock guards the coordinator's state; it is never held
while sending, while doing I/O, or while calling user code.  The acceptor-log lock is held
across acceptor-log writes (above) but never while sending or waiting; ``attach`` and
``detach`` are serialized by a third lock, taken first.
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
from ..errors import (ProtoConflictError, ProtoCorruptionError, ProtoDBError, ProtoOutcomeUnknownError, ProtoTimeoutError,
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
#: Seconds a member defers to the member whose turn a release named, if that member's request
#: has not been seen by then (lease fairness, Task 8.3e; capped by ``request_timeout``).
_TURN_SILENCE = 0.05
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

    __slots__ = ("kind", "term", "acks", "nacks", "answers", "promised", "states", "data", "missing")

    def __init__(self, kind: str, term: int = 0):
        self.kind = kind
        self.term = term
        self.acks = set()
        self.nacks = set()
        self.answers = {}
        self.promised = {}
        self.states = {}
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
            accepts no replicated root (default :attr:`restart_grace`, ``lease_duration +
            drift_margin``): a restarted node does not remember when the grants of its
            previous life end.  A shorter grace (``0`` included) is accepted only on a
            cluster's first start (rulings R50, R51): :meth:`attach` refuses it on an acceptor
            log that is not empty.  It never shortens a rejoin: whatever the grace, a rejoin
            with an empty log that is not a first start (some member has accepted a root or
            seen a lease term) decides only on a round of answers sent at or after the
            instant the log was attached plus :attr:`restart_grace` (rulings R53, R54;
            ``stats()["rejoin_blocked"]`` says so meanwhile).
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
        self._grace = self._duration + margin if startup_grace is None else _number(
            "startup_grace", startup_grace, positive=False)
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
        # Acceptor-log writes (module docstring, "Acceptor-log writes"; ruling R55).  Order:
        # _attach_lock, then _log_lock, then _lock; never send or wait on _cond holding either.
        self._attach_lock = threading.Lock()    # attach() and detach(), whole
        self._log_lock = threading.Lock()       # every acceptor decision with its log write
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
        self._tier_failure = None           # the write error that made the attached tier fail, if any
        self._rejoining = False             # the acceptor log was empty: learning the promised terms
        self._rejoin_forced = False         # the operator accepted a rejoin from a bare majority
        self._rejoin_blocked = None         # why the running rejoin cannot complete, if it cannot
        self._rejoiners = []                # every rejoin thread started (joined by stop)
        self._loaded = False                # the attached tier's acceptor log is loaded
        self._tier = None
        self._confirmed = True
        self._persister = None
        self._worker = None
        self._jobs = queue.Queue()
        self._stats = {"acquired": 0, "attempts": 0, "lost_attempts": 0, "grants_given": 0,
                       "grants_refused_no_storage": 0, "releases_received": 0, "storage_root_reads": 0,
                       "replications": 0, "replication_failures": 0, "accepted": 0, "refused": 0,
                       "recoveries": 0, "rejoins": 0, "fetches_requested": 0, "fetches_served": 0,
                       "critical_commits": 0, "turns_named": 0, "turns_taken": 0, "deferrals": 0,
                       "deferral_timeouts": 0}
        self._critical = {k: 0.0 for k in _CRITICAL_KEYS}
        # Lease fairness (module docstring, "Fairness"; Task 8.3e, D-LIVE-1): proposer-side
        # timing only, never consulted by a grant, an acceptance or a commit.
        self._turn_silence = min(_TURN_SILENCE, self._request_timeout)
        self._waiting = {}                  # member -> since: seen wanting the lease, turn not named yet
        self._wanting_since = {}            # member -> since: seen wanting the lease, not served since
        self._turn = None                   # (member, named at, its request seen at or None): defer to it
        self._my_turn = False               # the last winner's release named this node
        self._acquirers = 0                 # threads of this process inside acquire()
        self._registered = False            # the others queue this node: it asked, or released wanting more

    # -- properties --------------------------------------------------------------------------

    @property
    def node_id(self) -> str:
        """This node's id."""
        return self._node

    @property
    def restart_grace(self) -> float:
        """The shortest startup grace a node that served the cluster before may use:
        ``lease_duration + drift_margin`` seconds (ruling R51), also the default grace.

        A grant this node made in a previous life blocks other holders for ``lease_duration``
        on this node's clock from a moment before :meth:`start`, so ``lease_duration`` alone
        already outlasts it under the clock-rate bound ``drift_margin`` is sized for; the
        drift margin is a deliberate cushion on top (for example a disk moved to a host whose
        clock runs at another rate).  Only a cluster's first start may use less."""
        return self._duration + self._margin

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
        ``persisting``, ``tier_failed`` (a write on the attached tier failed: the node is no
        acceptor until the tier is detached) and ``failure`` (the exception that fenced the
        node, failed its tier or stopped its persister, or ``None``).  ``rejoin_blocked`` says why a rejoin that has its answers
        cannot complete (the records of the adopted root are unobtainable, or, outside a
        cluster's first start, its answers were gathered before the instant its empty log was
        attached plus ``restart_grace``),
        else ``None``."""
        with self._lock:
            out = dict(self._stats)
            out["critical"] = dict(self._critical)
            failure = next((f for f in (self._failure, self._tier_failure, self._persist_failure)
                            if f is not None), None)
            out["healthy"] = failure is None
            out["fenced"] = self._failure is not None
            out["tier_failed"] = self._tier_failure is not None
            out["persisting"] = self._persist_failure is None
            out["rejoining"] = self._rejoining
            out["rejoin_blocked"] = self._rejoin_blocked if self._rejoining else None
            out["failure"] = None if failure is None else repr(failure)
            return out

    @property
    def healthy(self) -> bool:
        """False once an internal failure fenced this node (see :meth:`fence`) or stopped its
        persister (see :meth:`persistence_failed`), and while the attached tier is failed
        (module docstring, "Health")."""
        with self._lock:
            return self._failure is None and self._persist_failure is None and self._tier_failure is None

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

    def _tier_failed(self, tier, error: BaseException, what: str) -> bool:
        """A write ``what`` on ``tier`` failed with ``error``.  When ``tier`` is still attached,
        record it as failed (module docstring, "Health"; the first failure is kept) and return
        True; return False when it was detached meanwhile (a closed space refuses writes: the
        tier is gone, not failed).  The caller never acknowledges nor grants what the write
        was for."""
        with self._cond:
            attached = self._tier is tier
            first = attached and self._tier_failure is None
            if first:
                self._tier_failure = error
            self._cond.notify_all()
        if first:
            logger.error("Node %s stops acting as an acceptor: %s failed on its tier (%r); close and reopen "
                         "the space on a healthy disk to bring it back", self._node, what, error,
                         exc_info=(type(error), error, error.__traceback__))
        return attached

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
        with self._lock:
            rejoiners = list(self._rejoiners)
        for rejoiner in rejoiners:      # woken by the notify above; each checks the state first
            if rejoiner is not threading.current_thread() and rejoiner.ident is not None:
                rejoiner.join()
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
        if self._tier_failure is not None:
            raise ProtoUnexpectedError(f"Node {self._node}: a write on its tier failed ({self._tier_failure!r}); "
                                       f"close and reopen the space on a healthy disk") from self._tier_failure

    def attach(self, tier) -> None:
        """Attach the cluster tier of the space this node commits to and start its persister.

        ``tier`` provides the storage and the tier operations described in
        :mod:`proto_db.cluster.persister`.  The roots and the promise the tier logged (its
        acceptor log) are loaded, so a restarted node knows what it accepted and never
        grants a term it granted before; they replace the accepted and committed roots held
        in memory, so a log attached to a coordinator that keeps running (a space closed and
        reopened) starts from what it holds, as after a restart.  An empty log starts a
        rejoin (module docstring) in a background thread; its freshness instant is this
        attach plus :attr:`restart_grace`.  Attaching the same tier again is a no-op; another
        one raises ``ProtoValidationError``.  Runs after any :meth:`detach` in progress has
        finished, and no acceptor decision runs while it loads the log (module docstring,
        "Acceptor-log writes").
        """
        from .persister import Persister
        with self._attach_lock:
            with self._log_lock:
                # No acceptor decision or log write runs while the tier changes: each one is
                # made, logged and published on the tier attached throughout (ruling R55).
                persister, rejoiner = self._attach_locked(tier, Persister)
            if persister is not None:
                persister.start()
            if rejoiner is not None:
                rejoiner.start()

    def _attach_locked(self, tier, persister_class) -> tuple:
        """The body of :meth:`attach` (``_attach_lock`` and ``_log_lock`` held); returns the
        persister and the rejoin thread to start, ``(None, None)`` for a no-op."""
        with self._lock:
            self._check_running()
            if self._tier is not None:
                if self._tier is tier or self._tier.storage is tier.storage:
                    return None, None
                raise ProtoValidationError("This coordinator already serves another storage")
            self._tier = tier
        try:
            logged = tier.load_roots()
        except BaseException:
            with self._lock:
                self._tier = None
            raise
        accepted, committed, top, promised, empty = logged
        if not empty and self._grace < self.restart_grace and self._others:
            with self._lock:
                self._tier = None
            raise ProtoValidationError(
                f"Node {self._node}: startup_grace={self._grace:g} is accepted only on a cluster's first "
                f"start; this node's acceptor log shows it has served the cluster before, so it may "
                f"have granted a lease that is still running.  Start it with a startup_grace of at "
                f"least lease_duration + drift_margin = {self.restart_grace:g} s (the default)")
        rejoin = empty and bool(self._others)
        if empty and not self._others:
            try:
                tier.log_rejoined()     # a one-member cluster has nobody to learn from
            except BaseException:
                with self._lock:
                    self._tier = None
                raise
        rejoiner = None
        with self._cond:
            # The acceptor state is what this log holds, exactly as after a restart (round 7
            # audit): a log attached to a coordinator that keeps running -- a space closed and
            # reopened, possibly over a replaced disk -- must not inherit the accepted root, the
            # committed root or the stored records of the log it replaces.  An accepted root
            # kept in memory without its records became the base of every lease this node
            # granted (ruling R50).  Lease activity (the grants running, the terms seen, the
            # suspicion, the term hint) stays: it is this process's own, and keeping it is
            # conservative.  No acceptance can be half done here (logged on the old tier but
            # not yet published, or the reverse): those hold _log_lock (ruling R55).
            self._accepted = (False, None, 0)
            if self._committed[0]:
                self._committed = (False, None, 0)
                self._committed_by = None
                self._committed_version += 1
            self._stored.clear()
            self._chunks.clear()
            if accepted is not None:
                self._observe_accepted(True, accepted[1], accepted[0])
            if committed is not None:
                self._observe_committed(True, committed[1], committed[0], None)
            self._hint = max(self._hint, top)
            self._grants.restore(max(promised, accepted[0] if accepted is not None else 0))
            tier.set_fetcher(self.fetch_record)
            self._confirmed = False
            self._rejoining = rejoin
            self._rejoin_blocked = None
            self._loaded = True
            persister = persister_class(self, tier, interval=self._persist_interval, lag=self._persist_lag)
            self._persister = persister
            if rejoin:
                # The freshness instant of this log's rejoin (rulings R53, R54): anchored here,
                # not at start(), because an empty log also arrives on a coordinator that keeps
                # running (a space closed and reopened over a replaced disk).
                fresh_from = self._clock() + self.restart_grace
                rejoiner = threading.Thread(target=self._rejoin, args=(tier, fresh_from),
                                            name=f"proto-cluster-rejoin-{self._node}", daemon=True)
                self._rejoiners = [t for t in self._rejoiners if t.is_alive()] + [rejoiner]
        return persister, rejoiner

    def _rejoin_answers(self) -> int:
        """Witness answers from other members a rejoin needs: ``N - majority + 1`` (every member
        when fewer).  Any majority that granted a term, or acknowledged a root, holds at least
        ``majority - 1`` members other than this node, and that many answers from the ``N - 1``
        others always include one of them; this node's own (empty) knowledge counts for
        nothing.  Forced (:meth:`force_rejoin`): ``majority - 1`` answers of any member that
        has its log loaded, witnesses or not."""
        if self._rejoin_forced:
            return self._majority - 1
        return min(len(self._others), self.cluster_size - self._majority + 1)

    def _witness_state(self) -> str:
        """What this node's answers are worth (lock held): ``"unknown"`` without a loaded
        acceptor log (no space opened, or stopping), ``"empty"`` while it rejoins, ``"ok"``
        for a witness (its log holds what it promised and accepted)."""
        if self._state != "running" or self._tier is None or not self._loaded or self._tier_failure is not None:
            return "unknown"
        return "empty" if self._rejoining else "ok"

    @staticmethod
    def _rootless(answer) -> bool:
        """True for an answer that has accepted no root (a committed root it read from the
        persisted root of the store is not acceptance)."""
        (ak, _, _), _, _ = answer
        return not ak

    def force_rejoin(self) -> None:
        """Disaster recovery after the loss of a majority of acceptor logs.

        On a node that has not rejoined yet (rejoining now, or not attached yet: the next
        :meth:`attach`): let its rejoin complete with the answers of a bare majority counting
        itself, ``majority - 1`` other members whose log is loaded (witnesses or rejoining),
        instead of ``N - majority + 1`` witnesses.  A forced rejoin adopts no committed root
        above the accepted root it adopts.

        On a node that has rejoined (a survivor): forget the committed root it knows when that
        root orders above every root it accepted; after a majority loss it names a commit that
        was lost with the other logs, and the commit guard refuses to build below it
        (``ProtoCorruptionError``).  The node then confirms the committed root under a lease
        before its next snapshot.  A rejoin it may need later is not forced.

        What persists across a restart, precisely.  Committed knowledge comes from two kinds of
        source.  (1) Knowledge that is not acceptance -- a grant reply, a recovery or rejoin
        answer, the persisted root of the object store -- is held in memory only, never logged.
        It is the only knowledge that can order above the accepted root, hence the only
        knowledge this call drops, and a restart does not bring it back.  (2) A committed root
        learnt from a release, or from this node's own commit, is logged in the acceptor log
        (``roots.log``); being known committed through the cluster, it is accepted knowledge as
        well, in memory and again when a restart reloads it, so it never orders above the
        accepted root: this call keeps it, and the guard never refuses to build below it,
        because this node's own accepted root takes part in its next base.  So after this call
        the operator has nothing more to do on that node, before or after a restart.

        Only for an operator who knows that the members that cannot answer lost their acceptor
        logs too (a majority of disks lost: the commits after the persisted root may be lost)
        or that the cluster is new.  Used otherwise, two leases may share a term and a
        committed root may be lost; that is why it is never automatic.
        """
        dropped = None
        with self._cond:
            survivor = self._loaded and not self._rejoining
            if survivor:
                ck, croot, clabel = self._committed
                ak, aroot, alabel = self._accepted
                if ck and (not ak or compare_roots(clabel, croot, alabel, aroot) > 0):
                    dropped = (clabel, croot.epoch if croot is not None else 0)
                    self._committed = (False, None, 0)
                    self._committed_by = None
                    self._committed_version += 1
                    self._confirmed = False
            else:
                self._rejoin_forced = True
            self._cond.notify_all()
        if not survivor:
            logger.warning("Node %s: rejoin forced by the operator (bare majority)", self._node)
        elif dropped is not None:
            logger.warning("Node %s: the operator dropped the committed root (label %d, epoch %d), which "
                           "orders above every root this node accepted", self._node, *dropped)

    def _rejoin_decision(self, answers: dict, states: dict, promised: dict) -> Optional[tuple]:
        """``(answering members, how)``: the answers a rejoin learns from and why they are
        enough -- ``"forced"``, ``"witnesses"``, ``"new"`` (a cluster's first start) or
        ``"rootless"`` -- or ``None`` when they are not enough (lock held).

        Witness answers (``"ok"``) count; ``N - majority + 1`` of them are needed.  Answers of
        empty members (rejoining) are counted only for a cluster's first start: when every
        other member answered, none of them has accepted any root (each is empty, or a
        witness that completed its own first start and has accepted nothing since) AND each
        reports a promised term of 0, i.e. no member has seen any lease term (ruling R51).  "No
        member accepted a root" alone does not mean "no member granted a lease": a lease
        granted before any commit leaves its term at its granters and at its holder, whose
        own grant goes through its :class:`~proto_db.cluster.lease.PeerGrants`, so at least
        one other member reports it (a lease needs a majority, at least two members, and
        every granter saw the request).  When every other member answered and none accepted a
        root but some term was seen, the answers are still enough -- they include every member
        that granted or requested a term, and no root had been committed when they were
        gathered -- but as ``"rootless"``, which is not a first start: like every rejoin but
        ``"new"``, it completes only on answers gathered at or after the attach of the empty
        log plus ``restart_grace`` (:meth:`_complete_rejoin`); a root in flight when the node
        lost its log is then visible in them.  Without it, a member that
        finishes its first start and requests a lease at once would leave the members still
        rejoining with too few witnesses for good.  Forced,
        any ``majority - 1`` answers of loaded members do.  ``"unknown"`` answers never count."""
        loaded = {p: a for p, a in answers.items() if states.get(p) in ("ok", "empty")}
        witnesses = {p: a for p, a in loaded.items() if states[p] == "ok"}
        if self._rejoin_forced:
            return (list(loaded), "forced") if len(loaded) >= self._majority - 1 else None
        if len(loaded) == len(self._others) and all(self._rootless(a) for a in loaded.values()):
            if not any(promised.get(p) for p in loaded):
                return list(loaded), "new"      # a new cluster: no root accepted, no term seen
            if len(witnesses) < self._rejoin_answers():
                return list(loaded), "rootless"     # waits, see above
        if len(witnesses) >= self._rejoin_answers():
            return list(witnesses), "witnesses"
        return None

    def _rejoin(self, tier, fresh_from: float) -> None:
        """Learn the highest term promised or seen by a majority, record it, and start voting
        (see "Rejoining" in the module docstring).  ``fresh_from`` is the instant, on this
        node's clock, from which a round counts outside a first start: when ``tier`` was
        attached plus :attr:`restart_grace`.  Retries until it succeeds, the tier is detached
        or the coordinator stops."""
        while True:
            with self._cond:
                if self._state != "running" or self._tier is not tier or not self._rejoining \
                        or self._tier_failure is not None:
                    return
            wait = self._request_timeout
            try:
                asked = self._clock()       # every answer below was gathered at or after this
                waiter = self._ask("query", {}, lambda w: len(w.answers) >= len(self._others),
                                   self._request_timeout)
                with self._lock:
                    answers, promised = dict(waiter.answers), dict(waiter.promised)
                    decision = self._rejoin_decision(answers, dict(waiter.states), promised)
                if decision is not None:
                    counted, how = decision
                    if self._complete_rejoin(tier, [answers[p] for p in counted], [promised[p] for p in counted],
                                             how, asked, fresh_from):
                        return
                    if how != "new" and asked < fresh_from:
                        # Too early a round: ask again as soon as a fresh one may be sent (at
                        # once if that instant has passed meanwhile; with a clock that does
                        # not follow real time, the wait is only an upper bound).
                        wait = min(wait, max(0.0, fresh_from - self._clock()))
            except (ProtoDBError, OSError) as e:
                with self._lock:
                    if self._state != "running":
                        return
                logger.warning("Node %s: rejoin round failed (%s); retrying", self._node, e)
            except Exception as e:
                self.fence(e)
                raise
            with self._cond:
                self._cond.wait_for(lambda: self._state != "running" or self._tier_failure is not None,
                                    timeout=wait)

    def _complete_rejoin(self, tier, answers: list, promised: list, how: str, asked: float,
                         fresh_from: float) -> bool:
        """Adopt what the counted ``answers`` teach -- the highest promise and label, and the
        highest accepted root, which orders at or above every committed root (each was
        accepted by a majority, which the counted answers intersect) -- make every record of
        that root durable here or confirm it is in the object store, log the promise and the
        root durably, then the rejoined marker, and only then act as an acceptor and a
        witness.  Return False, leaving the node rejoining with the reason in ``stats()``,
        when the records cannot be obtained now, or when ``how`` is not a cluster's first
        start and the round of ``answers`` was sent (at ``asked``, on this node's clock)
        before ``fresh_from`` -- the instant ``tier`` was attached plus :attr:`restart_grace`
        -- whatever this node's startup grace (rulings R53, R54).
        An empty acceptor log cannot tell a member that joins its cluster's first start late
        from a wiped one, and a wiped node may have accepted and acknowledged a root just
        before the loss, under a lease that is still running: answers gathered meanwhile may
        all miss that root (the other acceptors have not received it yet; the holder
        self-accepts after its broadcast, or its answer is lost), and its holder may still
        count the lost node's acknowledgement and report the commit.  The lost log accepted
        nothing once it was detached, so every lease it served was granted before ``tier`` was
        attached -- by a previous process, or by this one before :meth:`detach` -- and has
        ended on its holder's view by ``fresh_from`` (see :attr:`restart_grace`); anchoring
        at :meth:`start` missed a log replaced while the coordinator kept running.  A commit
        is reported only with acknowledgements counted within the lease, so each acceptor
        counted for it had durably accepted the root before it answered a round sent after
        that instant; the counted answers intersect those acceptors.  A ``"new"`` cluster cannot be stale this way: the holder
        of any lease records its own term before a grant exists, so its answer reports it.

        Re-proposing the highest accepted root is what Paxos prescribes, but only together
        with its value: adopted without its records, a root whose records lived on lost disks
        would become every later commit's base (ruling R50).  Committed roots the answers
        report are adopted as committed knowledge only (they may have been read from the
        persisted root of the store, which is never acceptance), are not logged, and, for a
        forced rejoin, only up to the adopted root (a majority loss may have lost them)."""
        safe = self.restart_grace
        if how != "new" and asked < fresh_from:
            return self._block_rejoin(
                tier, f"this is not a cluster's first start (some member has accepted a root or seen a "
                      f"lease term), and answers gathered before lease_duration + drift_margin = {safe:g} s "
                      f"from the attach of this node's empty acceptor log may miss a root it accepted "
                      f"before it lost its log; it asks again once that much time has passed (whatever "
                      f"its startup_grace={self._grace:g})")
        learnt = max(promised + [max(a[2], c[2]) for a, c, _ in answers] + [0])
        best = committed = None
        for (ak, aroot, alabel), (ck, croot, clabel), _ in answers:
            if ak and (best is None or compare_roots(alabel, aroot, best[0], best[1]) > 0):
                best = (alabel, aroot)
            if ck and (committed is None or compare_roots(clabel, croot, committed[0], committed[1]) > 0):
                committed = (clabel, croot)
        if how == "forced" and committed is not None and (
                best is None or compare_roots(committed[0], committed[1], best[0], best[1]) > 0):
            committed = None
        if best is not None:
            reason = self._secure_chain(tier, best[0], best[1])
            if reason is not None:
                return self._block_rejoin(tier, reason)
        with self._log_lock:
            # Logged and published only on the tier this rejoin serves, attached throughout
            # (ruling R55); an abandoned rejoin writes nothing.
            with self._lock:
                if self._tier is not tier:
                    return True
            try:
                if learnt:
                    tier.log_promise(learnt)
                if best is not None:
                    tier.log_root("accepted", best[0], best[1])
                tier.log_rejoined()             # last: the log now holds what the node must know
            except (ProtoDBError, OSError) as e:
                # Attached throughout (_log_lock): the tier failed.  Nothing is published; the
                # node stays rejoining, not a witness, until the tier is replaced (ruling R57).
                self._tier_failed(tier, e, "the rejoin's acceptor-log write")
                return self._block_rejoin(tier, f"writing its acceptor log failed ({e}); close and reopen the "
                                                f"space on a healthy disk")
            self._publish_rejoin(learnt, promised, best, committed)
        logger.info("Node %s rejoined with an empty acceptor log: highest term %d, accepted root %s",
                    self._node, learnt, None if best is None else root_key(*best)[:2])
        return True

    def _publish_rejoin(self, learnt: int, promised: list, best: Optional[tuple],
                        committed: Optional[tuple]) -> None:
        """Adopt in memory what :meth:`_complete_rejoin` logged (``_log_lock`` held)."""
        with self._cond:
            # The floor is every term and label learnt; the activity is the promises only.
            self._grants.restore(learnt, seen=max(promised + [0]))
            self._hint = max(self._hint, learnt)
            if best is not None:
                self._observe_accepted(True, best[1], best[0])
                self._note_stored(best[0], best[1])
            if committed is not None:
                self._observe_committed(True, committed[1], committed[0], None, accept=False)
            self._rejoining = False
            self._rejoin_forced = False
            self._rejoin_blocked = None
            self._stats["rejoins"] += 1
            self._cond.notify_all()

    def _block_rejoin(self, tier, reason: str) -> bool:
        """Record why the rejoin on ``tier`` cannot complete (logged when it changes); False."""
        with self._cond:
            changed = self._tier is tier and self._rejoin_blocked != reason
            if self._tier is tier:
                self._rejoin_blocked = reason
        if changed:
            logger.warning("Node %s stays unavailable (rejoining): %s", self._node, reason)
        return False

    def _secure_chain(self, tier, label: int, root: Optional[RootRecord]) -> Optional[str]:
        """Make every record of the root ``root`` labelled ``label`` durable on ``tier`` or
        confirm that the object store holds it; return ``None`` when done, else why not.

        Walks the chain of cluster roots from ``root`` back to the persisted root: every commit
        on it shipped records that only its acknowledgers hold until the store has them, and
        this node restores that copy (rejoin after a lost disk).  For each commit
        (:meth:`commit_records`): stop at a persisted one, skip one whose records the store
        holds, else store the records of its byte ranges here.  A storage shared by every node
        holds every record: nothing to do."""
        if root is None or not getattr(tier, "local_records", False):
            return None
        current = (label, root)
        while current is not None and current[1] is not None and current[0] > 0:
            with self._lock:
                if self._state != "running" or self._tier is not tier:
                    return "the node is stopping"
            where, records, ranges = self.commit_records(*current)
            if where is None:
                return records
            if where == "persisted":
                return None
            if where == "records":
                try:
                    tier.store_commit(records, current[1], current[0], ranges)
                except (ProtoDBError, OSError) as e:
                    if not self._tier_failed(tier, e, "storing the records of an adopted root"):
                        # Closed once detached: it refuses writes (ruling R56).
                        return "the tier was detached"
                    return (f"storing the records of root epoch {current[1].epoch} (label {current[0]}) failed "
                            f"({e}); the tier failed")
            current = tier.parent_root(current[1])
        return None

    def commit_records(self, label: int, root: Optional[RootRecord]) -> tuple:
        """Where the records of the commit of ``root`` labelled ``label`` are, gathering those
        this node must hold to adopt that root without its replication (ruling R50): a rejoin,
        or a lease holder re-proposing a base nobody knew committed.

        Returns ``("persisted", None, None)`` when the persisted root orders at or above it
        (this commit and every earlier one are in the object store; also for a storage shared
        by every node), ``("remote", None, None)`` when the store holds this commit's records,
        ``("records", records, ranges)`` with the records of the commit's byte ``ranges`` that
        the store is not known to hold (``[segment bytes, offset, framed record]``; read here
        or from the peers), or ``(None, reason, None)`` when some record is nowhere reachable.
        The ranges come from this node's own knowledge, else from the peers (``commit_query``).
        """
        with self._lock:
            tier, persister = self._tier, self._persister   # one attach's pair
        if root is None or tier is None or not getattr(tier, "local_records", False):
            return "persisted", None, None
        if persister is not None and persister.covers(label, root):
            return "persisted", None, None
        ranges = tier.commit_ranges(root)
        if ranges is None:
            persisted, ranges = self._ask_commit(label, root)
            if persisted:
                return "persisted", None, None
        what = f"root epoch {root.epoch} (label {label})"
        if ranges is None:
            try:
                # At least the root record's offset (a sealed object counts only up to its
                # recorded length, ruling R54).
                remote = tier.ensure_remote([(root.segment, root.offset + 1)])
            except (ProtoDBError, OSError) as e:
                logger.info("Node %s: the object store could not be checked for %s: %s", self._node, what, e)
                remote = False
            if remote:
                return "remote", None, None
            return None, (f"no reachable peer knows where the records of {what} are, and the object store "
                          f"does not hold them"), None
        records = tier.commit_records(ranges)
        if records is None:
            return None, (f"some records of {what} are on no reachable peer and not in the object store"), None
        return "records", records, ranges

    def _ask_commit(self, label: int, root: RootRecord) -> tuple:
        """``(persisted, ranges)`` for the commit of ``root`` from the first peer that knows
        (``commit_query``), within ``fetch_timeout`` (and what remains of a held lease)."""
        others = len(self._others)
        if not others:
            return False, None
        with self._lock:
            if self._state != "running":
                return False, None
            timeout = self._fetch_timeout
            if self._held is not None:
                timeout = min(timeout, max(0.0, self._held.remaining()))
        waiter = self._ask("commit_query", {"root": encode_root(root, known=True, term=label)},
                           lambda w: w.data is not None or w.missing >= others, timeout)
        if waiter.data is None:
            return False, None
        return waiter.data

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
        """Stop the persister of ``tier`` and forget the tier (the space is closing).  A
        rejoin forced for this tier (:meth:`force_rejoin`) is dropped; the acceptor state held
        in memory is replaced by the next :meth:`attach` with what that tier's log holds.
        An acceptor decision in flight on ``tier`` finishes (logged and published) before the
        tier is forgotten, and none is made on it afterwards, so the caller may close its log
        once this returns (module docstring, "Acceptor-log writes")."""
        with self._attach_lock:
            # Waits for an acceptor decision in flight (its log write and its publication)
            # to finish on this tier; none starts on it afterwards (ruling R55).
            with self._log_lock, self._lock:
                if self._tier is None or (self._tier is not tier and self._tier.storage is not tier.storage):
                    return
                persister, self._persister = self._persister, None
                self._tier = None
                self._loaded = False
                self._rejoining = False
                # A failure belongs to the tier that failed: the next attach starts healthy,
                # from what its log holds (ruling R57).
                self._tier_failure = None
                # A force was the operator's decision about the rejoin of this log; it does not
                # carry over to another one (nor does it survive a restart).  A force requested
                # while no log is attached applies to the next attach (force_rejoin).
                self._rejoin_forced = False
                self._cond.notify_all()
            # Still under _attach_lock: the next attach starts once this persister is gone.
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
                     accept: bool = True, tier=None) -> None:
        """Learn ``root`` (``None``: the empty space), committed with label ``term``.

        ``from_storage=True`` counts a storage root read.  ``accept=False`` (the persisted
        root, D46) does not make it accepted knowledge (see :meth:`_observe_committed`).
        ``tier``: the tier ``root`` was read from; the observation is dropped unless that
        tier is still attached (a root read from a detached tier's storage says nothing about
        the log attached since).
        """
        with self._lock:
            if tier is not None and self._tier is not tier:
                return
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
        if kind in ("records", "replicate", "release", "want", "fetch", "commit_query"):
            # A release follows its replicate (and a replicate its record chunks) through the
            # same queue, so a root is never known committed here before its records are stored;
            # a ``want`` follows the sender's release, which it must not precede (fairness).
            with self._lock:
                fenced = self._failure is not None
                if not fenced:
                    self._queued += 1
                    self._jobs.put(msg)
            if fenced:
                self._refuse_queued(msg)
            return
        if kind == "request":
            self._on_request(sender, msg)
            return
        reply = None
        with self._cond:
            if self._state != "running":
                return
            if kind in ("grant", "reject"):
                self._on_reply(sender, msg)
            elif kind == "query":
                state = self._witness_state()
                if state == "unknown":
                    # No acceptor log loaded: whatever is in memory is not what this node
                    # promised or accepted.  Never counted by the asker.
                    reply = make_message("answer", self._node, rid=msg["rid"], root=None, committed=None,
                                         certain=False, promised=0, state=state)
                else:
                    # ``promised``: the lease activity this node knows of (terms seen in requests
                    # and releases, promises) and the accepted root's label -- not the committed
                    # root's label, which rides in ``committed`` and may come from the persisted
                    # root of the store, nor a floor restored from such labels.  A rejoin tells a
                    # cluster's first start by ``promised == 0`` (ruling R51), and a new cluster
                    # over an existing object store has seen no lease term yet.
                    reply = make_message("answer", self._node, rid=msg["rid"], root=self._wire(self._accepted),
                                         committed=self._wire(self._committed), certain=not self._suspect_term,
                                         promised=max(self._grants.terms_seen, self._accepted[2]),
                                         state=state)
            else:
                self._on_answer(sender, kind, msg)
        if reply is not None:
            self._transport.send(sender, reply)

    def _on_request(self, sender: str, msg: dict) -> None:
        """Decide a lease request and make a grant's promise durable before it is sent.  The
        decision reports the accepted root and raises the promise in memory, so it is atomic
        with acceptances: under ``_log_lock``, no acceptance is decided but not yet published
        meanwhile (ruling R55; module docstring, "Acceptor-log writes")."""
        with self._log_lock:
            with self._cond:
                if self._state != "running":
                    return
                self._note_request(sender)
                reply = self._decide(sender, msg["term"], msg["rid"], msg["inc"])
                tier = self._tier if reply["type"] == "grant" else None
            # ``tier`` is attached until _log_lock is released: a grant needs one (_decide).
            if tier is not None and not self._log_promise(tier, msg["term"]):
                return              # a promise that could not be made durable is not sent
        self._transport.send(sender, reply)

    def _promise(self, tier, term: int) -> bool:
        """Make this node's promise of ``term`` to itself durable in ``tier``'s acceptor log;
        False if ``tier`` is no longer attached or the write failed (logged), in which case
        the vote must not be counted.  Checked and written under ``_log_lock``, so the
        promise lands only in the log attached when it is written (ruling R55)."""
        with self._log_lock:
            with self._lock:
                attached = self._tier is tier
            if not attached:
                logger.info("Node %s: its acceptor log was detached before it recorded its own promise "
                            "of term %d; its vote is withdrawn", self._node, term)
                return False
            return self._log_promise(tier, term)

    def _log_promise(self, tier, term: int) -> bool:
        """Write the promise of ``term`` to ``tier`` (``_log_lock`` held, ``tier`` attached);
        False if it failed: the grant must not be sent or counted, and the tier is failed
        (module docstring, "Health")."""
        try:
            tier.log_promise(term)
            return True
        except (ProtoDBError, OSError) as e:
            logger.error("Node %s could not record its promise of term %d; the grant is withheld: %s",
                         self._node, term, e)
            self._tier_failed(tier, e, f"recording the promise of term {term}")
            return False

    def _refusal(self) -> Optional[str]:
        """Why this node may not vote or accept right now (lock held), or ``None``."""
        if self._failure is not None:
            return "fenced"
        if self._tier is None or not self._loaded:
            return "no_storage"
        if self._tier_failure is not None:
            return "tier_failed"
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
        self._note_release(sender, msg)
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

    # -- lease fairness (module docstring, "Fairness"; Task 8.3e) ----------------------------

    def _note_wanting(self, member: str, now: float) -> None:
        """Queue ``member`` as wanting the lease (lock held), at the place it first wanted it
        since it was last served."""
        if member == self._node or member not in self._peers:
            return
        since = self._wanting_since.setdefault(member, now)
        self._waiting.setdefault(member, since)

    def _note_request(self, sender: str) -> None:
        """A lease request of ``sender`` arrived (lock held): it wants the lease, and if this node
        defers to it, its turn is being taken."""
        now = time.monotonic()
        self._note_wanting(sender, now)
        turn = self._turn
        if turn is not None and turn[0] == sender and turn[2] is None:
            self._turn = (sender, turn[1], now)

    def _note_release(self, sender: str, msg: dict) -> None:
        """Update the fairness state for a release or a cancel of ``sender`` (lock held)."""
        now = time.monotonic()
        if msg["held"]:
            self._waiting.pop(sender, None)             # served
            self._wanting_since.pop(sender, None)
            if msg["more"]:
                self._note_wanting(sender, now)         # it wants the lease again: the tail
        else:
            self._note_wanting(sender, now)             # a lost attempt still wants the lease
        if self._turn is not None and self._turn[0] == sender:
            self._turn = None                           # its attempt, or its lease, ended
        if msg["held"]:
            successor = msg["next"]
            if successor is not None and successor not in self._peers:
                successor = None
            if successor is not None:
                self._waiting.pop(successor, None)      # its turn is named (its place is kept)
            if successor == self._node:
                # Our turn: the others took us out of their queues (we are back in with the
                # request we send now; if we send none, we are no longer queued).
                self._turn, self._my_turn, self._registered = None, True, False
            elif successor is not None:
                self._turn = (successor, now, None)
            else:
                self._turn = None
        # The caller notifies the condition (a release wakes waiting requesters).

    def _next_turn(self) -> Optional[str]:
        """The member seen waiting longest (lock held), taken out of the queue (its place is
        kept until it is served, so a turn it does not take does not cost it its place), or
        ``None``."""
        if not self._waiting:
            return None
        member = min(self._waiting, key=lambda m: (self._waiting[m], m))
        del self._waiting[member]
        return member

    def _await_turn(self, deadline: float) -> None:
        """Wait while a release named another member's turn (module docstring, "Fairness"):
        until that member's request has been seen and its attempt or lease ended, at most
        ``lease_duration`` after its request, or :data:`_TURN_SILENCE` when its request is not
        seen.  A node the others do not queue (it neither asked, nor released wanting more,
        nor said ``want`` since it was last served or named) first broadcasts ``want``, so that
        a later release names it.  Timing only: it never decides a grant."""
        waited = False
        while True:
            with self._cond:
                while True:
                    self._check_running()
                    turn = self._turn
                    if turn is None:
                        return
                    member, named, seen = turn
                    now = time.monotonic()
                    end = seen + self._duration if seen is not None else named + self._turn_silence
                    if now >= end:
                        self._turn = None
                        self._stats["deferral_timeouts"] += 1
                        return
                    if now >= deadline:
                        return              # the attempt that follows reports the timeout
                    if not waited:
                        waited = True
                        self._stats["deferrals"] += 1
                    if not self._registered:
                        # Nobody queues this node: say so before deferring, or no release would
                        # ever name it (sent outside the lock, then the wait goes on).
                        self._registered = True
                        break
                    self._cond.wait(timeout=min(end, deadline) - now)
            self._broadcast(make_message("want", self._node))

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
            waiter.states[sender] = msg["state"]
        elif kind == "fetched":
            if waiter.kind != "fetch":
                return
            if msg["data"] is not None and waiter.data is None:
                waiter.data = msg["data"]
            else:
                waiter.missing += 1
        elif kind == "commit_info":
            if waiter.kind != "commit_query":
                return
            if waiter.data is not None:
                return
            if msg["persisted"]:
                waiter.data = (True, None)
            elif msg["ranges"] is not None:
                waiter.data = (False, protocol.decode_ranges(msg["ranges"]))
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
                    with self._log_lock:
                        with self._cond:
                            learnt = self._on_release(msg["from"], msg) if self._state == "running" else None
                            tier = self._tier
                        if learnt is not None and tier is not None:
                            # Logged so that a restarted node never serves an older root than it
                            # did; on the tier attached when it was learnt (ruling R55).  A failed
                            # write fails the tier (ruling R57); the root, majority-accepted
                            # already, may stay known in memory (module docstring).
                            try:
                                tier.log_root("committed", learnt[0], learnt[1])
                            except (ProtoDBError, OSError) as e:
                                self._tier_failed(tier, e, "logging a committed root")
                elif kind == "want":
                    with self._cond:
                        if self._state == "running":
                            self._note_wanting(msg["from"], time.monotonic())
                elif kind == "commit_query":
                    self._serve_commit(msg)
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
            failed = self._tier_failure is not None
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
        if valid and tier is not None and not failed:
            # Records are stored even when the root is refused (a newer term was granted here
            # meanwhile): immutable data, and this node may be the next holder, whose base can
            # be this root (adopted from its granters); it then reads nothing under its lease.
            try:
                if records:
                    tier.apply(records, root, label)      # durable before the ack
                bk, broot, blabel = decode_labeled_root(msg["base"])
                if bk and broot is not None and getattr(tier, "local_records", False):
                    # A re-proposed base: its records rode with this replication (ruling R50).
                    tier.store_commit([], broot, blabel, protocol.decode_ranges(msg["base_ranges"]) or [])
            except (ProtoDBError, OSError) as e:
                if self._tier_failed(tier, e, "storing a replication's records"):
                    # Still attached: the tier failed (ruling R57).  Refused below, never
                    # acknowledged; the node refuses everything until the tier is replaced.
                    ok = False
                else:
                    # The space closed under this replication: its tier refuses writes once
                    # closed (ruling R56), so the records may be missing there.  Refused below,
                    # never acknowledged, and not a fault of this node.
                    logger.info("Node %s: the tier was detached while storing a replication: %s", self._node, e)
                    ok = False
            else:
                with self._cond:
                    if self._tier is tier:
                        # Stored on the attached tier.  A tier detached meanwhile is not this
                        # node's store any more: a reattach starts from an empty set (N4).
                        self._note_stored(label, root)
        with self._log_lock:
            # The decision, the log write and the publication form one step against attach,
            # detach and every other acceptor decision (ruling R55): decided only on the tier
            # the records went to, logged on it while it is still attached, published in
            # memory only once logged; the acknowledgement follows.
            with self._lock:
                ok = ok and self._tier is tier and self._may_accept(term, root, label)
                if not ok:
                    self._stats["refused"] += 1
                    reply_root = self._wire(self._accepted)
            if ok:
                try:
                    tier.log_root("accepted", label, root)     # durable before the ack
                except (ProtoDBError, OSError) as e:
                    # Attached throughout (_log_lock): the tier failed (ruling R57).  Nothing is
                    # published and the replication is refused, never acknowledged.
                    self._tier_failed(tier, e, "logging an accepted root")
                    ok = False
                    with self._lock:
                        self._stats["refused"] += 1
                        reply_root = self._wire(self._accepted)
            if ok:
                with self._lock:
                    self._observe_accepted(True, root, label)
                    ck, croot, clabel = decode_labeled_root(msg["committed"])
                    self._observe_committed(ck, croot, clabel, None)
                    self._stats["accepted"] += 1
                    reply_root = self._wire(self._accepted)
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

    def _serve_commit(self, msg: dict) -> None:
        """Answer a ``commit_query``: whether the persisted root covers that commit, else the
        byte ranges of its records if this node knows them (worker thread)."""
        known, root, label = decode_labeled_root(msg["root"])
        with self._lock:
            tier, persister = self._tier, self._persister   # one attach's pair
        persisted = root is not None and persister is not None and persister.covers(label, root)
        ranges = None
        if not persisted and root is not None and tier is not None and getattr(tier, "local_records", False):
            ranges = tier.commit_ranges(root)
        self._transport.send(msg["from"], make_message("commit_info", self._node, rid=msg["rid"], persisted=persisted,
                                                       ranges=protocol.encode_ranges(ranges)))

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
            self._acquirers += 1        # tells a release of ours that this node still wants the lease
        try:
            if not self._local.acquire(timeout=timeout):
                raise ProtoTimeoutError(f"Node {self._node}: another thread held the cluster lease for {timeout} s")
            try:
                lease = self._acquire(deadline, timeout)
            except BaseException:
                self._local.release()
                raise
        finally:
            with self._lock:
                self._acquirers -= 1
        return lease

    def _await_rejoin(self, deadline: float, timeout: float) -> None:
        """Wait until this node may propose: not while it rejoins (module docstring)."""
        with self._cond:
            self._cond.wait_for(lambda: not self._rejoining or self._state != "running"
                                or self._failure is not None or self._tier_failure is not None,
                                timeout=max(0.0, deadline - time.monotonic()))
            self._check_running()
            self._check_healthy()
            if self._rejoining:
                raise ProtoTimeoutError(f"Node {self._node} could not learn the promised terms from a majority "
                                        f"within {timeout} s (its acceptor log was empty)")

    def _acquire(self, deadline: float, timeout: float) -> Lease:
        failures = 0
        while True:
            self._await_rejoin(deadline, timeout)
            self._await_turn(deadline)
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
                # The member whose turn a release named asks at once; the others spread out
                # (and defer to it, :meth:`_await_turn`).
                spread = woken and not self._my_turn
            if spread:
                time.sleep(self._rng.random() * _WAKE_SPREAD)

    def _open_attempt(self) -> tuple:
        """Open an attempt with this node's own vote decided; return ``(attempt, request,
        wakeups seen, tier whose log must record our own promise or None)``.  Our own vote reads
        the accepted root and raises the promise in memory, so it is decided under
        ``_log_lock``, atomically with acceptances (ruling R55); its promise is written by
        :meth:`_promise` after the broadcast."""
        promise = None
        with self._log_lock, self._lock:
            self._check_running()
            self._check_healthy()
            term = ballot_after(max(self._grants.highest_term, self._hint, self._accepted[2], self._committed[2]),
                                self._rank)
            attempt = LeaseAttempt(term=term, rid=next(self._rids), sent_at=self._clock(),
                                   cluster_size=self.cluster_size, lease_duration=self._duration,
                                   drift_margin=self._margin)
            self._attempt = attempt
            self._stats["attempts"] += 1
            if self._my_turn:
                self._my_turn = False
                self._stats["turns_taken"] += 1
            self._registered = True             # every member that receives the request queues us
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
                                  certain=False, held=False, next=None, more=True)
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
            # Fairness hints (module docstring, "Fairness"): the member seen waiting longest gets
            # the turn, and this node defers to it like every other member.
            more = self._acquirers > 0
            self._registered = more
            successor = self._next_turn()
            self._my_turn = False
            self._turn = None if successor is None else (successor, time.monotonic(), None)
            if successor is not None:
                self._stats["turns_named"] += 1
            msg = make_message("release", self._node, term=lease.term, inc=self._inc,
                               root=self._wire(self._accepted), committed=self._wire(self._committed),
                               certain=not self._suspect_term, held=True, next=successor,
                               more=more)
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

    def replicate(self, lease: Lease, root: RootRecord, *, base: Optional[tuple] = None, tier=None) -> float:
        """Commit ``root`` (built on ``lease.root``) by majority-acknowledged replication (D46).

        Sends ``(epoch, root pointer)`` labelled with the lease term, with the records the
        tier has not shipped yet, to every peer; makes them durable locally and accepts the
        root here; returns once a majority (this node included) has acknowledged, before the
        lease ends; then records the root as committed.  Returns the seconds spent.

        ``base`` is ``(label, record, records, ranges)`` when ``root`` re-proposes a base that
        nobody knew committed (``records`` and ``ranges`` from :meth:`commit_records`): the
        base's records the object store lacks are made durable here and ride with the
        replication, and every acknowledger remembers the base's byte ranges, so the base is
        re-proposed together with its records (ruling R50).

        ``tier`` is the tier the commit's records were written to (the space's, captured when
        the commit began); when it is no longer the attached tier (the space was closed, or
        closed and reopened, meanwhile) nothing is sent and ``ProtoValidationError`` is
        raised.  This node's own acceptance is in any case decided only on the tier attached
        when the replication started, and only while it stays attached (ruling R55).

        Raises ``ProtoConflictError`` when the lease ended before anything was sent (nothing
        was replicated: safe to retry), and ``ProtoOutcomeUnknownError`` when no majority
        acknowledged in time, or when anything fails once the root was sent (the space
        closing, an I/O error of the local log): peers may have acknowledged it, and a later
        holder may still adopt it, so the commit may or may not take effect.
        """
        started = time.perf_counter()
        attached = self._tier
        if attached is None:
            raise ProtoValidationError("replicate needs an attached tier (ObjectSpace attaches it)")
        if tier is not None and tier is not attached:
            raise ProtoValidationError("The space this commit belongs to was closed: its tier is no longer "
                                       "attached to the coordinator; nothing was replicated")
        tier = attached
        if lease is not self._held:
            raise ProtoUserError("This lease is not held by this coordinator")
        records = tier.outgoing()
        label = lease.term
        extra, base_wire, base_ranges = [], None, None
        if base is not None:
            base_label, base_root, extra, ranges = base
            extra = list(extra or ())
            base_wire = encode_root(base_root, known=True, term=base_label)
            base_ranges = protocol.encode_ranges(ranges)
            try:
                tier.store_commit(extra, base_root, base_label, ranges)     # durable here before our own ack
            except (ProtoDBError, OSError) as e:
                self._tier_failed(tier, e, "storing the records of a re-proposed base")
                raise
        # Our own records are durable here before any of them leaves this node.  In a shared
        # storage, no peer may acknowledge their root before they are durable there.  When they
        # ride with the replication, a holder that died between sending them and making them
        # durable would keep a strict prefix of its own segment while its peers hold the rest;
        # restarted, it would seal and upload that prefix as the whole segment, the store would
        # then "hold" records it lacks, and a root built on them would become unreadable.  So
        # the bytes of an own segment that any peer holds are always on this node's disk.
        try:
            tier.flush_local()
        except (ProtoDBError, OSError) as e:
            self._tier_failed(tier, e, "making the commit's records durable")
            raise
        chunks = _split_records(extra + records, protocol.CHUNK_BYTES)
        with self._lock:
            if self._tier is tier:
                self._note_stored(label, root)        # our own records, on the attached tier
            self._check_running()
            if not lease.is_valid():
                raise ProtoConflictError("The cluster write lease expired before the root was replicated; "
                                         "nothing was replicated")
            rid, waiter = self._new_waiter("replicate", label)
            parts = [make_message("records", self._node, term=label, rid=rid, index=i, records=chunk)
                     for i, chunk in enumerate(chunks[:-1])]
            msg = make_message("replicate", self._node, term=label, rid=rid,
                               root=encode_root(root, known=True, term=label),
                               committed=self._wire(self._committed), records=chunks[-1], chunks=len(parts),
                               base=base_wire, base_ranges=base_ranges)
            self._stats["replications"] += 1
        failure = None
        acks = nacks = 0
        stopped = False
        try:
            for part in parts:                         # same ordered link, before the replicate
                self._broadcast(part)
            self._broadcast(msg)
            with self._log_lock:
                # Our own acceptance follows the acceptor rule (ruling R55): decided only on
                # the tier this commit's records went to, logged on it while it is attached,
                # then published and counted.
                with self._lock:
                    self_ok = self._tier is tier and self._may_accept(label, root, label)
                if self_ok:
                    # Where the records of this root are, before its outcome is known: a node
                    # that adopts it later (a rejoin, a holder re-proposing it) copies them
                    # from here.
                    try:
                        tier.remember_commit(extra + records, root, label)
                        tier.log_root("accepted", label, root)
                    except (ProtoDBError, OSError) as e:
                        # Attached throughout (_log_lock): the tier failed (ruling R57); our own
                        # vote is not counted, and the outcome is unknown (below).
                        self._tier_failed(tier, e, "logging the holder's own acceptance")
                        raise
                    with self._lock:
                        self._observe_accepted(True, root, label)
                        waiter.acks.add(self._node)
            with self._cond:
                total = self.cluster_size

                def decided():
                    return (len(waiter.acks) >= self._majority or len(waiter.nacks) > total - self._majority
                            or self._state != "running" or not lease.is_valid())

                while not decided():
                    self._cond.wait(timeout=max(0.001, min(0.05, lease.remaining())))
                acks, nacks = len(waiter.acks), len(waiter.nacks)
                ok = acks >= self._majority and lease.is_valid()
                stopped = self._state != "running"
                if ok:
                    self._observe_committed(True, root, label, self._node)
                    if self._suspect_term and self._suspect_term <= label:
                        self._suspect_term = 0
                else:
                    self._stats["replication_failures"] += 1
                    self._suspect_term = max(self._suspect_term, label)
        except ProtoCorruptionError:
            # A protocol violation (two roots sharing a key) is an invariant breach, never an
            # unknown outcome: report it as it is.
            with self._lock:
                self._stats["replication_failures"] += 1
                self._suspect_term = max(self._suspect_term, label)
            raise
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
            if acks >= self._majority and stopped:
                why = (f"the coordinator stopped before this node recorded the outcome ({acks} "
                       f"acknowledgements, {self._majority} needed, had arrived)")
            elif acks >= self._majority:
                # A majority acknowledged, but this node observed it only after its lease had
                # ended (its thread was not scheduled in time, re-review 4 M2).  Reported as
                # unknown, conservatively: a commit is reported only when its majority is
                # observed within the lease.
                why = (f"the lease ended before this node observed the outcome ({acks} acknowledgements, "
                       f"{self._majority} needed, were counted only after the lease had ended)")
            elif stopped:
                why = (f"the coordinator stopped with {acks} of the {self._majority} acknowledgements "
                       f"needed")
            elif nacks > self.cluster_size - self._majority:
                why = (f"{nacks} members refused the root, and only {acks} of the {self._majority} "
                       f"acknowledgements needed arrived")
            else:
                why = f"only {acks} of the {self._majority} acknowledgements needed arrived before the lease ended"
            raise ProtoOutcomeUnknownError(
                f"Node {self._node}: {why}; the root (epoch {root.epoch}) may or may not take effect")
        try:
            tier.shipped(records, root, label)
            with self._log_lock:
                with self._lock:
                    attached = self._tier is tier
                if attached:            # never on a detached log (ruling R55)
                    tier.log_root("committed", label, root)
        except (ProtoDBError, OSError) as e:
            # Committed already (a majority acknowledged within the lease): never fail the
            # commit for this bookkeeping.  A write that failed on the attached tier fails it
            # (ruling R57); on a detached one (the space closing) it is only logged.
            if not self._tier_failed(tier, e, "recording a committed root"):
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
        roots of others are not (a root is accepted here only with its records).  Only
        witnesses count: members whose acceptor log is loaded and not rejoining (this node
        too: when it is not one, a majority of other witnesses is needed).
        Raises ``ProtoConflictError`` when no majority answers within ``request_timeout``.

        The suspicion cleared is only the one this node held when the round was sent: a term
        whose outcome became unknown while the round ran (its grant expired, or its holder
        released it uncertain) may have replicated a root after some answers were produced,
        so answers gathered before it cannot settle it (re-review 5, C1, applied here).
        """
        with self._lock:
            suspected = self._suspect_term      # what the answers below can settle
        waiter = self._ask("query", {}, lambda w: len(w.answers) >= len(self._others), self._request_timeout)
        with self._lock:
            answers = {p: a for p, a in waiter.answers.items() if waiter.states.get(p) == "ok"}
            own = self._witness_state() == "ok"
            needed = self._majority - 1 if own else self._majority
            self._stats["recoveries"] += 1
            if len(answers) < needed:
                raise ProtoConflictError(f"Node {self._node}: only {len(answers) + int(own)} of the "
                                         f"{self._majority} witnesses needed answered the recovery query")
            best = self._accepted if own else (False, None, 0)
            for (ak, aroot, alabel), (ck, croot, clabel), _ in answers.values():
                # A peer's committed root may be the persisted one only: never accepted knowledge.
                self._observe_committed(ck, croot, clabel, None, accept=False)
                self._hint = max(self._hint, alabel)
                if ak and (not best[0] or compare_roots(alabel, aroot, best[2], best[1]) > 0):
                    best = (True, aroot, alabel)
            if self._suspect_term <= suspected:
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
                if self._tier is not tier:
                    pass            # read from a tier detached meanwhile: says nothing now
                elif persisted is None:
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
