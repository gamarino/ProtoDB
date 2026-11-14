"""ClusterCoordinator: the majority-granted write lease and the cluster commit point
(Tasks 8.3c, 8.3d, 8.3f and 8.3k, decisions D44, D45, D46, D50, D55).

Write permissions (D50)
    Commits no longer acquire the lease each.  The member holding the lease is the *owner*
    of its term: it renews the lease (same term, no acceptor-log write) while there is
    demand and grants short, sequenced write permissions -- ``permit(term, seq, root,
    valid)`` -- in FIFO order, piggybacking the newest root it knows committed.  The holder of a permission does its own commit round (:meth:`permit`,
    :meth:`replicate`, :meth:`finish`): it replicates under the owner's term with the
    permission's ``seq`` and the root it built on (``pred``), and ends the permission with
    ``done``.  An acceptor takes a root of the same term only when it extends its accepted
    root, which includes a root it learnt committed (D55 rule V2', Raft's log matching on
    roots; protocol 13 removed the ``pred_committed`` catch-up), refuses siblings and stale
    sequence numbers, and keeps every D46 rule: two roots built on one base can never BOTH be
    committed, whatever the permissions (D50, S3).  Safety across terms DOES rest on the
    owner's sequencing (D50, S2 and S4; the reliance of S4 on the owner is accepted design under
    R79 and D50, not a pending deviation from R66d): the next phase 1 ranks the roots of one term by epoch, so a base
    issued twice could let an uncommitted sibling outrank a committed root.  The owner
    therefore ends a permission only through the incarnation it was issued to: a ``permit``
    names the requester's incarnation, any other incarnation drops it unanswered, and a
    ``done`` of another incarnation is an unknown outcome (8.3f review, C1).  An outcome the
    owner cannot know makes it abandon its term; the next owner's phase 1, raised by its
    recovery round, decides the root, as after any D46 lease.  The acquiring and lease rules
    below are the D45/D46 mechanics an owner uses to obtain and renew its term.

Pipelined permissions (Task 8.3k, D55; rulings R91, R93, R94)
    A holder whose round had no conflict proposes its root to the owner (``propose``) and runs
    no further round; the owner may then issue the next permission on that proposal (the
    permit names it in ``after`` and is never ``certain``), at most ``DEPTH`` (1) ahead.  The
    next holder builds on the proposal and runs its data stage at once -- as ``store``
    messages, which no code path accepts (V1), while its *gate* is closed -- but logs no own
    acceptance and sends no root until this node knows the proposal committed and holds it as
    its accepted root (the gate, H5; a plain ``query`` to the proposer and the voters after
    ``request_timeout`` is the fallback).  The gate is liveness and outcome quality only:
    safety rests on the voters, which accept a root of its predecessor's term only by
    extension of what they hold (V2').  A round that sent nothing acceptable ends with a
    retryable ``ProtoConflictError``; any other failure is an unknown outcome (H7).  The
    owner retires an entry and every older one on a committed ``done`` and abandons its term
    on any outcome it cannot sequence (O1-O8).

Bounded replication (Task 8.3g, D54; maintainer rulings R70, R70a and R89)
    A fixed set of ``V = 2f + 1`` *voters* (default the first three member ids in sorted
    order; every member of a cluster of fewer than three) orders roots: grants, renewals,
    ballots, rejoins, recovery rounds and root acceptance count only voters, and a majority
    means ``f + 1`` of them.  The other members are *learners* (Raft's non-voting members):
    they never grant, accept a root or count in a quorum, need no rejoin, learn committed
    roots from ``done`` and releases, and may own a term and commit like any member.  Losing
    ``f + 1`` voters stops writes (no quorum) while learners keep serving snapshot reads; the
    voter set is static configuration (no dynamic membership).

    A commit's records are made durable on the committer (its own append) and on ``f`` other
    members, its *data replicas*: the committer's successors in the sorted order of member
    ids, a member that did not answer within ``request_timeout`` being replaced by the next
    one (Dynamo's preference list; timing only).  A voter accepts the root only once ``f + 1``
    members hold the records durably: the replication names them (``copies``), and the root
    goes alone to the voters that are not data replicas after the replicas' ``stored``
    acknowledgements.  A voter that is a data replica stores and accepts in one append when
    that makes ``f + 1`` copies.  So a committed root -- accepted by ``f + 1`` voters, at most
    one of which is the committer -- never lacks ``f + 1`` durable copies of its records; the
    committer's own acceptance shares its disk with its own copy (each ``stored``
    acknowledgement is bound to the committer's incarnation, protocol 12).  Every other member
    receives the records in ``warm`` messages and keeps them only in its bounded record cache
    (R89: no append, no sync, never awaited; network bytes grow with ``N``, syncs and durable
    bytes do not).  A member that lacks a record fetches it on demand (:meth:`ClusterCoordinator.fetch_record`), from the members the locator
    names for the record's segment (learnt from ``replicate`` and ``done``), else from every
    member, else from the object store, and caches them by pointer.  Replicas keep their
    copies until the object store holds them (``release_local``); a persister that lags
    uploads the parts it holds, filling holes in a segment split across replicas from peers
    (repair through the object store).

    Limits (D54).  The voter set is static: no member is added or removed at run time.
    Losing ``f + 1`` voters stops every write until enough of them return.  No
    re-replication runs: once a data replica's disk is lost, a commit not yet in the object
    store has one copy fewer until the persister uploads it, and losing every one of its
    ``f + 1`` copies before that loses it.

A cluster is a static list of peers ``(node_id, host, port)``; its size ``N`` is fixed at
construction and a majority is ``floor(V/2) + 1`` votes of the ``V`` voters, the
requester's own included when it is a voter.
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
    release.  No spinning.  Before asking, the head thread defers to the member whose turn
    a release named (see "Fairness").
Fairness (Task 8.3e, defect D-LIVE-1; D46 Amendment (Task 8.3e))
    Without it the releasing process re-asked from its next queued thread a fraction of a
    millisecond after its release, long before a contender woken by that release could, and
    kept the lease (measured: the previous holder won 98.5 % of leases with two TCP peers in
    ``fsync`` mode).  Each member therefore queues the members it has seen wanting the
    lease -- a request or a cancel received, a ``want`` message, or a release saying
    ``more`` (the sender still has threads waiting) -- ordered by when it first saw each
    want.  A holder's release carries ``next``, the oldest member of its queue (served
    members leave the queues), and ``more``.  A member that receives a held release naming
    another member defers its own requests -- the releaser included -- until that member
    has been served (its release or cancel arrives), until ``_TURN_SILENCE`` (50 ms, capped
    by ``request_timeout``) passes without its request being seen, or until one lease
    duration after its request was seen; the named member skips the wake spread.  A member
    no peer queues (it released with ``more`` False, then wanted again) broadcasts ``want``
    once before deferring, so a later release names it.  Counters:
    ``stats()["turns_named"]``, ``["turns_taken"]``, ``["deferrals"]``,
    ``["deferral_timeouts"]``.

    The hints only time this node's own requests: grants, acceptances, ballots, terms,
    fencing and the acceptor log never read them, so no hint -- lost, late, duplicated,
    reordered or wrong -- can affect safety (D45: every request schedule is safe), and each
    deferral is bounded as above.  Fairness itself assumes honest hints, as every member runs
    this code (crash faults only): a member never queues itself, so an honest release never
    names its sender.  A member whose hints are wrong (for example naming itself, or always a
    member that never asks) only degrades the cluster to the pre-8.3e behaviour -- leases go
    to whoever asks first -- or costs one silence bound per turn.

    What it guarantees, measured: leases are served in turn, evenly, at 2-4 contending
    members (TCP, ``fsync``: per-member lease counts within a few percent).  At 8 members
    vote splitting is not solved: per-member commit ratios (min/max) of 0.12-0.85 with the
    GIL off and 0.55-0.77 with it on were measured, with up to 27 lease attempts per commit,
    though no member waited more than about 3 s.  Defect D-LIVE-2: turns were fair for
    leases, not for commits of conflicting transactions -- a read-modify-write of a hot key
    conflicted, retried with a fresh snapshot after the lease or permission had ended, and
    conflicted again, so whole members starved (measured: 2 members x 2 threads, 495 against
    0 commits in 3 s).  Task 8.3f phase A's retry-under-permission did not close it (a
    retry still arrived after the permission had ended).  Closed by the held retries of
    :mod:`proto_db.transactions` (D50 Amendment "held retries"): a commit of
    ``Database.run`` that conflicted under a permission retries at once, under the same
    permission, from the root it carries.
Granting
    See :class:`~proto_db.cluster.lease.PeerGrants`.  Requests and releases carry the
    sender's incarnation (a random number drawn at each start), so a restarted node is
    never confused with its previous life.  A grant's term is made durable in the node's
    acceptor log (:mod:`proto_db.cluster.rootlog`) before the grant is sent, and restored
    when the node restarts, so no term is ever granted twice (the acceptor rule of Paxos).
    The acceptor log is ``fsync``-ed before the grant whatever the tier's data durability
    mode (Task 8.3e, defect D-SAFE-2 fixed), so this holds across power loss in every mode.
    A node grants, and acknowledges replications, only while a cluster tier (the storage
    of a space) is attached and while it is not fenced: a node without a durable place for
    its promises and roots never counts toward a majority.
Rejoining and witnesses
    A node is a *witness* -- its grants, acknowledgements and ``answer``s count -- only
    while its acceptor log is loaded (a space is open on it) and rejoined.  A node whose
    log has no rejoined marker (a new node, a disk lost or wiped, or an interrupted rejoin)
    cannot tell what it promised, proposed or accepted in a previous life.  Until it has
    learnt, from ``V - majority + 1`` other voters that are witnesses (``query``/``answer``, ``V``
    the number of voters; for three voters, both others), the highest term they promised or saw AND the highest root they
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
    leaves its term at its granters and its holder).  Any other rejoin is a ballot, not a
    wait (ruling R78a, replacing the timed rule of rulings R53/R54): the node may have
    accepted and acknowledged a root just before it lost its log, under a lease whose holder
    has not counted that acknowledgement yet, and a plain round of answers may miss that
    root.  So, after a plain round that tells it the highest term anyone reported, it picks
    a fresh ballot above it and asks again with that ballot: each witness makes a promise of
    the ballot durable -- refusing from then on every lower term and every root labelled
    below it, which ends any lease the lost log served -- and answers only after it.  Only
    the answers that confirm the ballot count, and they intersect every majority that
    acknowledged a root (:meth:`ClusterCoordinator._complete_rejoin` gives the argument), so
    no elapsed time and no clock rate enters the decision; the price is one ballot per
    rejoin of an empty log, which ends the current owner's term.  Any other loss of a majority of logs
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
    tier (``fsync``), then acknowledges -- unless it has no tier, is fenced or
    has granted a higher term (the ballot rule).  The accepted root -- like every promise and
    the rejoined marker -- is ``fsync``-ed before the acknowledgement in every durability
    mode (Task 8.3e, defect D-SAFE-2 fixed).  On a :class:`~proto_db.storage.tiered.TieredStorage`
    the acceptance is ONE kind-5 entry of the acceptor log that holds the root together with
    every record its replication carried (Task 8.3f phase B, D50): the holder writes its own
    while its records travel to its data replicas (Task 8.3j, ruling R90; before it sends the
    replication when there are none) and sends the root alone to the other voters only after
    it, each follower writes its own before it acknowledges; data files are
    written without a sync and rebuilt from these entries when the storage opens.  So in
    every durability mode a root is durable exactly when its records are: an acknowledged
    commit survives power loss of any set of nodes, and D-AVAIL-1 (a durable root whose
    records were lost, in ``"flush"``/``"none"``) is closed by construction.  The mode now
    governs only writes outside the commit path (the persister's flush of an own segment
    before its upload, records of a refused replication).  On a shared-storage
    :class:`~proto_db.cluster.persister.PlainTier` the records live in the shared storage
    and the holder flushes them per that storage's mode before sending (unchanged).

    In every mode the lease's safety -- no term granted twice, no two roots with one label
    and epoch -- holds across power loss.  A cluster over ``MemoryStorage`` (Task 8.3e
    decision) is not durable in any sense: the storage, and so every node, lives in one
    process, each node's acceptor log is a :class:`~proto_db.cluster.rootlog.MemoryRootsLog`
    that survives a coordinator restart in that process (like a disk) and nothing else, and
    the space's data is in memory too.  Its lease safety holds against coordinator crashes
    and restarts within the process; the process's death ends the space, data and logs
    together, so there is nothing to recover and no power-loss guarantee to state.  It is for
    tests and experiments only.  The commit is acknowledged once a majority (the holder included) has
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
    (:meth:`ClusterCoordinator.recover_root`): the roots of a majority of peers.  Its base
    never orders below the lease's own root, the phase-1 value its granters reported after
    promising the term: the recovery answers promise nothing and can miss a root accepted
    after they were given (re-review 2 of 8.3n, C1).
Peer services
    ``query``/``answer`` (recovery, joining peers), ``fetch``/``fetched`` (peer page
    requests for records not yet in the object store) and ``commit_query``/``commit_info``
    (where the records of a commit are: covered by the persisted root, or their byte ranges).

Message lanes (Task 8.3f-B, ruling R81; D50 Amendment "Message lanes")
    The dispatcher -- the transport's receiving thread -- never waits on a disk.  It is an
    event-driven state machine: each message either changes memory and replies at once, or
    starts an operation on a lane and returns; the lane sends the reply when the operation
    completes.

    - **Dispatcher (control, memory only).**

      - ``permit_req``, ``permit``, ``permit_no``, ``grant``, ``reject``, ``answer`` and
        plain ``query``;
      - the fairness part of ``release`` and ``want``;
      - the owner's part of ``done``, which sequences the next permission.

    - **Acceptor lane (one thread, ``_jobs``, arrival order).** Every step that writes the
      acceptor log or reads its pending state:

      - ``request`` and ``renew``: the grant's promise;
      - a ballot ``query``: the ballot's promise;
      - ``records`` and ``replicate``: the acceptance;
      - the grant-ending and committed-root part of ``release``;
      - the committed-root part of ``done``.

      Each step decides, writes, publishes, then replies, exactly as the dispatcher and the
      worker did before, so the safety arguments of the sections below are unchanged:

      - a grant leaves only after its promise is durable, and an acknowledgement only after
        its acceptance is;
      - a request decided while an earlier write is pending cannot exist, because steps run
        one at a time;
      - a ``done`` or ``release`` teaches its committed root only after every replication the
        same sender sent earlier (FIFO per lane; on a link that keeps the sender's order).

    - **Read lane (one thread, ``_reads``).** ``fetch`` and ``commit_query`` read the local
      tier.

    The owner part of ``done`` reads only the message and the owner's own term state, never
    acceptor state, so the owner does not wait for its own copy of the holder's replication
    (head-of-line blocking).  ``stats()["dispatch_seconds_max"]`` exposes the dispatcher's
    longest message.

Health
    An unexpected exception in a lane thread fences the node
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
    For the same reason a logged committed root is not synced on its own (ruling R67): it
    becomes durable with the next synced acceptor entry, and a power loss may drop it.  Only
    :meth:`ClusterCoordinator.attach` reads it back, as committed knowledge and a term hint;
    grants, ballots, acceptances and rejoins read promises and accepted roots only (D46
    Amendment (Task 8.3e)).  Promises, accepted roots and the rejoined marker are synced
    before the reply they justify.

Threads: the transport's own threads, plus one worker started by :meth:`start` that
applies replicated records, serves page requests and prefetches, and the persister
started by :meth:`attach`.  One lock guards the coordinator's state; it is never held
while sending, while doing I/O, or while calling user code.  The acceptor-log lock is held
across acceptor-log writes (above) but never while sending or waiting; ``attach`` and
``detach`` are serialized by a third lock, taken first.
"""
import collections
import contextlib
import itertools
import logging
import queue
import random
import threading
import time
import uuid
import zlib
from typing import Callable, NamedTuple, Optional

from ..core.atom import AtomPointer
from ..errors import (ProtoConflictError, ProtoCorruptionError, ProtoDBError, ProtoOutcomeUnknownError, ProtoTimeoutError,
                      ProtoUnexpectedError, ProtoUserError, ProtoValidationError, ProtoWriteRefusedError)
from ..storage.format import RootRecord
from . import protocol
from .lease import Lease, LeaseAttempt, PeerGrants, Permission, majority
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
#: Seconds on one message above which the dispatcher counts it in ``stats()["dispatch_slow"]``.
#: The dispatcher never waits on a disk (ruling R81); this only exposes scheduling delays.
DISPATCH_SLOW = 0.01
#: Roots whose records this node stored, remembered for :meth:`ClusterCoordinator.await_records`.
_STORED_MEMORY = 256
#: Segments whose data replicas this node remembers (the locator of D54), most recent first.
_LOCATOR_MEMORY = 4096
#: Times a rejoin's query round asks a member whose answer has not arrived (see ``_gather``).
GATHER_ATTEMPTS = 3
_CRITICAL_KEYS = ("validation", "replay", "path_copy", "replication")
#: Messages carrying the fingerprint of the sender's voter set (D54).
_FINGERPRINTED = ("request", "renew", "query", "replicate", "store")


#: Smallest drift margin, as a fraction of ``lease_duration`` (ruling R78b): the holder believes
#: its lease for at most a third of the granters' promise.
MIN_MARGIN_FRACTION = 2.0 / 3.0
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


def _voter_set(ring: list, voters) -> tuple:
    """The sorted voter ids (D54): ``voters`` validated against the sorted member ids ``ring``,
    or the default (the first three, every member of a smaller cluster)."""
    if voters is None:
        return tuple(ring[:3])
    if isinstance(voters, (str, bytes)):
        raise ProtoValidationError("voters must be a collection of member ids")
    chosen = list(voters)
    if len(set(chosen)) != len(chosen) or any(v not in ring for v in chosen):
        raise ProtoValidationError(f"voters must be distinct member ids, got {chosen!r}")
    if len(ring) < 3:
        if len(chosen) != len(ring):
            raise ProtoValidationError("A cluster of fewer than three members has every member as a voter")
    elif len(chosen) < 3 or len(chosen) % 2 == 0:
        raise ProtoValidationError(f"The voter set must have an odd number of at least three members, "
                                   f"got {len(chosen)}")
    return tuple(sorted(chosen))


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

    __slots__ = ("kind", "term", "acks", "nacks", "answers", "promised", "states", "ballots", "data", "missing",
                 "stored", "unstored", "key")

    def __init__(self, kind: str, term: int = 0):
        self.kind = kind
        self.term = term
        self.acks = set()
        self.nacks = set()
        self.key = None             # a fetch: the (segment bytes, offset) asked (review C2)
        self.stored = set()         # members that hold the replication's records durably (D54)
        self.unstored = set()       # members that answered without storing them
        self.answers = {}
        self.promised = {}
        self.states = {}
        self.ballots = {}
        self.data = None
        self.missing = 0


class _PermitWait:
    """A request for a write permission awaiting the owner's answer (``reply``: a
    :class:`~proto_db.cluster.lease.Permission`, ``"refused"``, or ``"gone"`` once abandoned).
    ``term``: the owner's term the request was sent under (``0`` for this node's own
    ownership); a refusal or a silence then speaks for that term only."""

    def __init__(self, rid: int, sent_at: float, term: int = 0):
        self.rid = rid
        self.sent_at = sent_at
        self.term = term
        self.reply = None


def _siblings(label_a: int, a: Optional[RootRecord], label_b: int, b: Optional[RootRecord]) -> bool:
    """True when ``a`` and ``b`` are different roots with the same label and epoch (D50: two
    roots built on one base in one term, which the chain rule refuses)."""
    return a is not None and b is not None and label_a == label_b and a.epoch == b.epoch \
        and (a.segment, a.offset) != (b.segment, b.offset)


#: Pipelined permissions (Task 8.3k, D55): at most ``DEPTH + 1`` permissions of a term are
#: outstanding, and at most ``DEPTH`` of them past their proposal and not done.  Every owner
#: rule (O1-O8) is written for a general depth; 1 is the measured choice (D55, section 4.1).
DEPTH = 1


class _Issued:
    """One permission an owner issued and has not retired (D55): to ``node``'s incarnation
    ``inc`` for its request ``rid``, number ``seq``, ``deadline`` on the owner's clock, the
    ``permit`` message sent (``None`` for this node's own threads), the ``base`` it carries
    ``(known, root, label)``, and the ``proposal`` ``(root, label)`` its holder made, if any.
    ``consumed`` is True once the next permission was issued on that proposal (rule O2)."""

    __slots__ = ("node", "seq", "inc", "rid", "deadline", "permit", "base", "proposal", "consumed")

    def __init__(self, node: str, seq: int, inc: int, rid: int, deadline: float, permit, base: tuple):
        self.node, self.seq, self.inc, self.rid = node, seq, inc, rid
        self.deadline, self.permit, self.base = deadline, permit, base
        self.proposal = None
        self.consumed = False


class _Ownership:
    """What this node does as the owner of its lease's term (D50, D55): a FIFO of permission
    requests ``(node, rid, inc, local wait or None)``; the ``pipeline`` of issued permissions
    not yet retired (:class:`_Issued`, oldest first, at most ``DEPTH + 1``); and ``base``, the
    newest root it knows committed in its term ``(known, root, label)`` (known committed when
    ``committed``), which a permit carries when no proposal is waiting to be issued on.  Only
    the incarnation a permission was issued to can end it with ``"none"`` or ``"committed"``
    (8.3f review, C1)."""

    def __init__(self, lease: Lease, base: tuple, committed: bool, now: float):
        self.lease = lease
        self.term = lease.term
        self.seq = 0
        self.pipeline = []
        self.base = base
        self.committed = committed
        self.queue = collections.deque()
        self.last_demand = now
        self.last_local = now           # the last permission request of this node's own threads
        self.abandoned = False
        self.ended = False

    def serving(self, now: float) -> bool:
        """True while permissions may be issued: neither abandoned nor ended, lease valid."""
        return not self.abandoned and not self.ended and self.lease.is_valid()

    def entry(self, node: str, seq: int) -> Optional[_Issued]:
        """The issued permission ``seq`` of ``node``, if it is in the pipeline."""
        for issued in self.pipeline:
            if issued.node == node and issued.seq == seq:
                return issued
        return None


class ClusterCoordinator:
    """A cluster member granting and acquiring the write lease (see the module docstring).

    Args:
        node_id: this node's id; must appear in ``peers``.
        peers: every member, this node included, as ``(node_id, host, port)``.
        transport: a :class:`~proto_db.cluster.transport.Transport`; default a
            :class:`~proto_db.cluster.transport.TcpTransport` listening on this node's
            peer address.
        lease_duration: seconds a grant blocks other holders on the granter's clock.
        drift_margin: seconds subtracted from the holder's view of its lease: at least, and by
            default, two thirds of ``lease_duration`` (ruling R78b), so the holder believes
            its lease for at most ``lease_duration / 3`` on its own timer, while each granter
            keeps its promise for ``lease_duration`` on its timer.  Exclusion then holds for
            timer rates up to 3x apart (``m >= 2 * rho * D / (1 + rho)`` with ``rho = 0.5``),
            far beyond real oscillators; it is a liveness and efficiency aid, never a premise
            of commit safety (D46, D50).
        request_timeout: seconds an attempt (and a recovery query) waits for a majority.
        send_reserve: seconds of validity a lease or permission must have left before its
            holder sends anything a voter may accept (Task 8.3n fix round 3, ruling R121).  With
            less, nothing leaves and the commit ends with a retryable ``ProtoConflictError``
            (rule H7).  A send that cannot finish in what is left ends unknown (P5), and an
            unknown outcome abandons the owner's whole term (O6): the reserve turns that doomed
            send into the truthful conflict it would have been.  Default ``request_timeout`` (the
            time this node already allows a majority to answer), capped at a quarter of the
            belief, ``(lease_duration - drift_margin) / 4``: an owner issues a permission with as
            little as that left (D50's D/4 rule) and renews its own lease at half the belief, so a
            larger reserve would make permissions the owner may issue unusable by rule (measured
            in fix round 3: with a 1 s lease and a 0.25 s ``request_timeout``, a 0.25 s reserve
            refused about 1,090 sends in 4 s and halved the commits).  ``0`` keeps the old rule (any
            validity left); at most a quarter of the belief.  Liveness only: it never decides an
            acceptance or an acknowledgement.
        acquire_timeout: default seconds :meth:`acquire` tries before ``ProtoTimeoutError``.
        backoff: ``(min_seconds, max_seconds)`` of the jittered back-off between lost attempts.
        startup_grace: seconds after :meth:`start` during which this node grants nothing
            (default :attr:`restart_grace`, ``lease_duration + drift_margin``): a restarted node
            does not remember when the grants of its previous life end.  The grace does not
            apply to acceptances (D50): ``_may_accept`` has no grace check, because accepting reads
            only the promise and the accepted root, both restored from the log at attach.  A shorter grace (``0`` included) is accepted only on a
            cluster's first start (rulings R50, R51): :meth:`attach` refuses it on an acceptor
            log that is not empty.  A rejoin of an empty log does not depend on it: outside a
            first start it decides on the answers of a ballot round (ruling R78a, module
            docstring "Rejoining").
        fetch_timeout: seconds a peer page request waits for an answer; must be shorter than
            ``lease_duration - drift_margin`` (default a quarter of it), and a request made
            under the lease never waits beyond what remains of the lease.
        persist_interval: seconds between two passes of the persister (D46).
        persist_lag: seconds the persisted epoch may lag the committed one before this node
            persists a root committed by another node (covers a crashed committer).
        owner_idle_release: seconds after which an owner whose own threads asked for no
            permission ends its term, even while other members ask (ruling R80); a member
            with load then takes the ownership through an ordinary grant round.  ``None``
            (the default) keeps only the rule that ends a term nobody asks for.  Liveness
            and efficiency only (D50 Amendment "owner_idle_release"): measured, it did not
            raise throughput, so it is off by default.
        voters: the ids of the members that order roots (Task 8.3g, D54): grants, renewals,
            ballots, recovery rounds and root acceptance count only their votes, a majority
            of them.  Default: the first three member ids in sorted order, or every member
            when the cluster has fewer than three.  An odd number of at least three members
            (3 or 5 recommended); every member of a smaller cluster.  Static configuration:
            every member must be given the same set (requests, renewals, ballots and
            replications carry a fingerprint of it, and a member configured differently
            drops them).  The other members are learners.
        clock: testing seam; the monotonic clock (seconds) used for every lease decision.
        seed: testing seam; seed of the back-off jitter generator.
    """

    def __init__(self, node_id: str, peers, *, transport=None, lease_duration: float = 2.0,
                 drift_margin: Optional[float] = None, request_timeout: float = 0.25,
                 send_reserve: Optional[float] = None,
                 acquire_timeout: float = 30.0, backoff: tuple = (0.001, 0.05),
                 startup_grace: Optional[float] = None, fetch_timeout: Optional[float] = None,
                 persist_interval: float = 0.05, persist_lag: float = 1.0,
                 owner_idle_release: Optional[float] = None, voters=None,
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
        margin = self._duration * MIN_MARGIN_FRACTION if drift_margin is None else _number(
            "drift_margin", drift_margin, positive=False)
        if margin >= self._duration:
            raise ProtoValidationError("drift_margin must be smaller than lease_duration")
        if margin < self._duration * MIN_MARGIN_FRACTION * (1 - 1e-9):
            raise ProtoValidationError(
                f"drift_margin must be at least {MIN_MARGIN_FRACTION:.3g} x lease_duration "
                f"({self._duration * MIN_MARGIN_FRACTION:g} s): a holder believes its lease for at most a third "
                f"of the granters' promise, so timers may run at rates up to 3x apart (ruling R78b)")
        self._margin = margin
        self._belief = self._duration - margin
        self._request_timeout = _number("request_timeout", request_timeout)
        self._idle_release = None if owner_idle_release is None else _number("owner_idle_release",
                                                                              owner_idle_release)
        if self._request_timeout >= self._duration - self._margin:
            raise ProtoValidationError("request_timeout must be shorter than lease_duration - drift_margin")
        quarter = (self._duration - self._margin) / 4
        self._send_reserve = min(self._request_timeout, quarter) if send_reserve is None else _number(
            "send_reserve", send_reserve, positive=False)
        if self._send_reserve > quarter * (1 + 1e-9):
            raise ProtoValidationError(
                f"send_reserve must be at most (lease_duration - drift_margin) / 4 ({quarter:g} s): an owner "
                f"issues a permission with as little as that left (the D/4 rule), and a larger reserve would "
                f"refuse it by rule")
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
        # Voters and learners (Task 8.3g, D54).
        self._ring = sorted(ids)            # data-replica placement: successors in this order
        self._voters = _voter_set(self._ring, voters)
        self._is_voter = node_id in self._voters
        self._other_voters = [i for i in self._voters if i != node_id]
        self._fingerprint = zlib.crc32(",".join(self._voters).encode("utf-8"))
        self._majority = majority(len(self._voters))
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
        self._progress = 0                  # winners' releases and dones received (permit back-off)
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
        # The ballot of this incarnation's last rejoin: V - majority + 1 voters promised it, so
        # no lower term can gain a majority again; this node never replicates under one.
        self._rejoin_ballot = 0
        self._rejoiners = []                # every rejoin thread started (joined by stop)
        self._loaded = False                # the attached tier's acceptor log is loaded
        self._tier = None
        self._confirmed = True
        self._persister = None
        self._worker = None
        self._jobs = queue.Queue()          # the acceptor lane: every durable acceptor step (D50, R81)
        self._reader = None
        self._reads = queue.Queue()         # the read lane: records and commits served to peers
        self._stats = {"acquired": 0, "attempts": 0, "lost_attempts": 0, "grants_given": 0,
                       "grants_refused_no_storage": 0, "releases_received": 0, "storage_root_reads": 0,
                       "replications": 0, "replication_failures": 0, "accepted": 0, "refused": 0,
                       "recoveries": 0, "rejoins": 0, "rejoin_ballots": 0, "fetches_requested": 0, "fetches_served": 0,
                       "critical_commits": 0, "turns_named": 0, "turns_taken": 0, "deferrals": 0,
                       "deferral_timeouts": 0, "dispatch_seconds_max": 0.0, "dispatch_slow": 0,
                       "terms_released_idle_local": 0, "config_mismatches": 0, "stale_replies": 0, "replica_hedges": 0,
                       "fetches_located": 0, "fetch_fallbacks": 0, "record_fetches": 0,
                       "record_fetches_permitted": 0, "records_warmed": 0}
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
        # Write permissions (D50, Task 8.3f).
        self._by_rank = {rank: peer for peer, rank in self._ranks.items()}
        self._logged_promise = (None, 0)    # (tier, highest promise logged on it): re-grants write nothing
        self._ownership = None              # _Ownership while this node owns its lease's term
        self._owner_threads = []            # every ownership thread started (joined by stop)
        self._owner_hint = (0, None)        # (term, node): the newest owner this node learnt of
        self._permit = None                 # the Permission this node holds
        self._permit_waits = {}             # rid -> _PermitWait of a request sent to an owner
        # (term, seq) of the permits received lately: a duplicated permit (the transport may
        # deliver a message twice) is ignored; answering it with done("none") would end, at
        # the owner, a permission this node is still using.
        self._permits_seen = collections.deque(maxlen=64)
        # (term, seq, holder) of permissions that ended without committing (a ``done`` saying
        # ``unknown`` or ``none``): a successor's gate on such a predecessor closes at once
        # (review 8.3k, M4; liveness only -- a closed gate is always truthful).
        self._failed_permits = collections.deque(maxlen=64)
        self._seq_seen = (0, 0)             # (term, seq): highest permission accepted here
        self._mismatched = set()            # members seen with another voter set (logged once each)
        # Bounded replication (D54): where records are, and which members answered lately.
        self._locator = collections.OrderedDict()       # segment UUID -> [members holding its records]
        self._placements = collections.OrderedDict()    # root_key -> (copies, ranges) of a commit
        self._replica_suspects = {}         # member -> monotonic time until which it is tried last
        for key in ("permits_issued", "permits_held", "permit_requests_sent", "permits_refused", "renewals",
                    "renewal_failures", "ownerships", "terms_abandoned", "permit_rounds", "permits_foreign_dropped",
                    "permit_requests_retried", "pipelined_permits", "proposals", "proposals_received",
                    "foreign_proposals", "gate_open_at_send", "gate_waits", "gate_fallbacks", "gate_closures",
                    "stores_sent", "stores_applied", "records_cached_on_receipt", "sends_refused_reserve"):
            self._stats[key] = 0

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
    def voters(self) -> tuple:
        """The ids of the voters, sorted (Task 8.3g, D54)."""
        return self._voters

    @property
    def is_voter(self) -> bool:
        """True when this node is a voter, False for a learner."""
        return self._is_voter

    @property
    def majority(self) -> int:
        """Votes needed for the lease and acknowledgements needed for a commit: a majority of
        the voters, ``floor(V/2) + 1``; also the number of nodes that hold every commit's
        records durably before any other voter accepts its root (D54)."""
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
    def acquire_timeout(self) -> float:
        """Default seconds :meth:`acquire` and :meth:`permit` try before ``ProtoTimeoutError``."""
        return self._acquire_timeout

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
        cannot complete (the records of the adopted root are unobtainable, or too few
        witnesses confirmed its ballot durably), else ``None``; ``rejoin_ballots`` counts the
        ballot rounds that completed a rejoin (ruling R78a).  ``dispatch_seconds_max``: the
        longest the dispatcher spent on one message; ``dispatch_slow``: messages it spent
        more than :data:`DISPATCH_SLOW` on (it never waits on a disk, ruling R81).
        ``terms_released_idle_local``: terms this node ended as owner because its own threads
        asked for nothing for ``owner_idle_release`` seconds (ruling R80)."""
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
        was for.  A :class:`ProtoWriteRefusedError` refuses that one write and leaves the tier
        healthy: it is logged and not recorded as a failure (Task 8.3j re-review N1)."""
        if isinstance(error, ProtoWriteRefusedError):
            with self._cond:
                attached = self._tier is tier
            logger.warning("Node %s: %s was refused by its tier (%s); the tier stays in service",
                           self._node, what, error)
            return attached
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
        self._worker = threading.Thread(target=self._work, args=(self._jobs,),
                                        name=f"proto-cluster-worker-{self._node}", daemon=True)
        self._reader = threading.Thread(target=self._work, args=(self._reads,),
                                        name=f"proto-cluster-reader-{self._node}", daemon=True)
        self._worker.start()
        self._reader.start()
        addresses = {i: ((p.host, p.port) if p.port else None) for i, p in self._peers.items()}
        try:
            self._transport.start(self._node, addresses, self._on_message)
        except BaseException:
            with self._lock:
                self._state = "stopped"
            for lane, thread in ((self._jobs, self._worker), (self._reads, self._reader)):
                lane.put(_STOP)
                thread.join()
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
        with self._lock:
            owners = list(self._owner_threads)
        for owner in owners:            # woken by the notify above; each checks the state first
            if owner is not threading.current_thread() and owner.ident is not None:
                owner.join()
        if started:
            self._transport.stop()
        for lane, thread in ((self._jobs, self._worker), (self._reads, self._reader)):
            if thread is not None:
                lane.put(_STOP)
                if thread is not threading.current_thread():
                    thread.join()

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
        rejoin (module docstring) in a background thread.  Attaching the same tier again is a no-op; another
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
        # A learner neither grants nor accepts roots: there is nothing it must learn before it
        # serves (D54).  A voter without another voter has nobody to learn from.
        rejoin = empty and self._is_voter and bool(self._other_voters)
        if empty and not rejoin:
            try:
                tier.log_rejoined()     # a one-voter cluster or a learner: nothing to learn
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
            self._logged_promise = (tier, promised)
            tier.set_fetcher(self.fetch_record)
            self._confirmed = False
            self._rejoining = rejoin
            self._rejoin_blocked = None
            self._loaded = True
            persister = persister_class(self, tier, interval=self._persist_interval, lag=self._persist_lag)
            self._persister = persister
            if rejoin:
                rejoiner = threading.Thread(target=self._rejoin, args=(tier,),
                                            name=f"proto-cluster-rejoin-{self._node}", daemon=True)
                self._rejoiners = [t for t in self._rejoiners if t.is_alive()] + [rejoiner]
        return persister, rejoiner

    def _rejoin_answers(self) -> int:
        """Witness answers from other voters a rejoin needs: ``V - majority + 1`` with ``V`` the
        number of voters, learners excluded (every other voter when fewer, ``min(V - 1, V -
        majority + 1)``).  Any majority of voters that granted a term, or acknowledged a root,
        holds at least ``majority - 1`` voters other than this node, and that many answers from
        the ``V - 1`` other voters always include one of them; this node's own (empty) knowledge counts for
        nothing.  Forced (:meth:`force_rejoin`): ``majority - 1`` answers of any member that
        has its log loaded, witnesses or not."""
        if self._rejoin_forced:
            return self._majority - 1
        return min(len(self._other_voters), len(self._voters) - self._majority + 1)

    def _witness_state(self) -> str:
        """What this node's answers are worth (lock held): ``"unknown"`` without a loaded
        acceptor log (no space opened, or stopping), ``"empty"`` while it rejoins, ``"ok"``
        for a witness (its log holds what it promised and accepted)."""
        if self._state != "running" or self._tier is None or not self._loaded or self._tier_failure is not None:
            return "unknown"
        if not self._is_voter:
            return "learner"            # never counted (D54)
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
        instead of ``V - majority + 1`` witnesses.  A forced rejoin adopts no committed root
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

        Witness answers (``"ok"``) count; ``V - majority + 1`` of them are needed.  Answers of
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
        ``"new"`` and ``"forced"``, it completes only on the answers of a ballot round, whose
        witnesses promised the ballot durably before answering (ruling R78a,
        :meth:`_complete_rejoin`); a root in flight when the node lost its log is then
        visible in them or never accepted again.  Without it, a member that
        finishes its first start and requests a lease at once would leave the members still
        rejoining with too few witnesses for good.  Forced,
        any ``majority - 1`` answers of loaded members do.  ``"unknown"`` answers never count."""
        loaded = {p: a for p, a in answers.items() if states.get(p) in ("ok", "empty")}
        witnesses = {p: a for p, a in loaded.items() if states[p] == "ok"}
        if self._rejoin_forced:
            return (list(loaded), "forced") if len(loaded) >= self._majority - 1 else None
        if len(loaded) == len(self._other_voters) and all(self._rootless(a) for a in loaded.values()):
            if not any(promised.get(p) for p in loaded):
                return list(loaded), "new"      # a new cluster: no root accepted, no term seen
            if len(witnesses) < self._rejoin_answers():
                return list(loaded), "rootless"     # waits, see above
        if len(witnesses) >= self._rejoin_answers():
            return list(witnesses), "witnesses"
        return None

    def _rejoin(self, tier) -> None:
        """Learn the highest term promised or seen by a majority and the highest accepted root,
        record them, and start voting (see "Rejoining" in the module docstring).  Outside a
        cluster's first start or a forced rejoin, the answers that count are those of a round
        that asked the witnesses to promise a fresh ballot durably first (ruling R78a): no
        elapsed time enters the decision.  Retries until it succeeds, the tier is detached or
        the coordinator stops."""
        while True:
            with self._cond:
                if self._state != "running" or self._tier is not tier or not self._rejoining \
                        or self._tier_failure is not None:
                    return
            try:
                if self._rejoin_round(tier):
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
                self._cond.wait_for(lambda: self._state != "running" or self._tier_failure is not None,
                                    timeout=self._request_timeout)

    def _gather(self, ballot: int) -> tuple:
        """One ``query`` round to every other member; ``(answers, states, promised)`` with the
        state of a witness that did not confirm ``ballot`` (when non-zero) demoted to
        ``"unknown"``: its answer did not follow a durable promise of the ballot.

        A member whose answer has not arrived within ``request_timeout`` is asked again, with
        the same ballot, up to :data:`GATHER_ATTEMPTS` times in all: on a lossy network one
        lost query or answer would otherwise fail the whole round, and a rejoin that needs
        every witness (two members rejoining at once in a 5-member cluster) would rarely
        complete (measured: 20 % loss, 10 s without a rejoin).  Merging is sound: each answer
        is counted on its own merits -- a ballot answer is sent only after the witness made
        its promise of ``ballot`` durable, and asking again for the same ballot needs no new
        promise -- and the answers of one round already arrive at different moments."""
        answers, promised, states, ballots = {}, {}, {}, {}
        for _ in range(GATHER_ATTEMPTS):
            missing = [p for p in self._other_voters if p not in answers]
            if not missing:
                break
            waiter = self._ask("query", {"ballot": ballot, "vs": self._fingerprint},
                               lambda w: len(w.answers) >= len(missing), self._request_timeout, targets=missing)
            with self._lock:
                answers.update(waiter.answers)
                promised.update(waiter.promised)
                states.update(waiter.states)
                ballots.update(waiter.ballots)
                if self._state != "running":
                    break
        if ballot:
            states = {p: (st if st != "ok" or ballots.get(p) == ballot else "unknown")
                      for p, st in states.items()}
        return answers, states, promised

    def _rejoin_round(self, tier) -> bool:
        """One attempt of :meth:`_rejoin`; True when the rejoin completed."""
        answers, states, promised = self._gather(0)
        with self._lock:
            decision = self._rejoin_decision(answers, states, promised)
        if decision is None:
            return False
        counted, how = decision
        ballot = 0
        if how in ("witnesses", "rootless"):
            # The ballot round (ruling R78a): above every term the plain round taught, so each
            # witness that confirms it has durably promised to refuse every lower term, ending
            # any lease this node's lost log may have served.  Answers read after that promise
            # report every root they accepted before it, and no root of a lower term is
            # accepted after it.
            learnt = max(list(promised.values()) + [max(a[2], c[2]) for a, c, _ in answers.values()] + [0])
            with self._lock:
                floor = max(learnt, self._grants.highest_term, self._hint)
            ballot = ballot_after(floor, self._rank)
            answers, states, promised = self._gather(ballot)
            with self._lock:
                decision = self._rejoin_decision(answers, states, promised)
            if decision is None or decision[1] not in ("witnesses", "rootless"):
                self._block_rejoin(tier, f"too few members confirmed the rejoin ballot {ballot} durably; "
                                         f"it asks again")
                return False
            counted, how = decision
            self._stats["rejoin_ballots"] += 1
        return self._complete_rejoin(tier, [answers[p] for p in counted], [promised[p] for p in counted], how,
                                     ballot)

    def _complete_rejoin(self, tier, answers: list, promised: list, how: str, ballot: int) -> bool:
        """Adopt what the counted ``answers`` teach -- the highest promise and label (``ballot``
        included), and the highest accepted root, which orders at or above every committed
        root -- make every record of that root durable here or confirm it is in the object
        store, log the promise and the root durably, then the rejoined marker, and only then
        act as an acceptor and a witness.  Return False, leaving the node rejoining with the
        reason in ``stats()``, when the records cannot be obtained now.

        Why the counted answers are enough without any wait (ruling R78a; the 8.3f review's
        clock-free rejoin).  An empty acceptor log cannot tell a member that joins its
        cluster's first start late from a wiped one, and a wiped node may have accepted and
        acknowledged a root R just before the loss, under a lease whose holder may still count
        that acknowledgement.  Outside a first start or a forced rejoin, the counted answers
        come from ``V - majority + 1`` witnesses that each made a promise of ``ballot`` durable
        before answering; ``ballot`` is above every term they reported in the plain round.  Let
        M be a majority that acknowledged R (old incarnation of this node included); ``M``
        minus this node has at least ``majority - 1`` members among the ``V - 1`` others, and
        ``(majority - 1) + (V - majority + 1) = V > V - 1``, so some counted witness x is in
        M.  Either x accepted R before it promised ``ballot`` -- its answer, read after the
        promise and atomically with acceptances (ruling R55), reports R or a root above it --
        or it never accepts R (R's label is below ``ballot``, the ballot rule), and then R has
        no majority with x in it: a contradiction.  So the adopted root orders at or above
        every committed root, whatever the timing of leases or clock rates.  A ``"new"``
        cluster cannot be stale this way: the holder of any lease records its own term before a
        grant exists, so its answer reports it; a ``"rootless"`` rejoin counts every loaded
        member, and the witnesses among them confirmed the ballot.

        Re-proposing the highest accepted root is what Paxos prescribes, but only together
        with its value: adopted without its records, a root whose records lived on lost disks
        would become every later commit's base (ruling R50).  Committed roots the answers
        report are adopted as committed knowledge only (they may have been read from the
        persisted root of the store, which is never acceptance), are not logged, and, for a
        forced rejoin, only up to the adopted root (a majority loss may have lost them)."""
        learnt = max(promised + [max(a[2], c[2]) for a, c, _ in answers] + [ballot, 0])
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
            self._publish_rejoin(learnt, promised, best, committed, ballot)
        logger.info("Node %s rejoined with an empty acceptor log: highest term %d, accepted root %s",
                    self._node, learnt, None if best is None else root_key(*best)[:2])
        return True

    def _publish_rejoin(self, learnt: int, promised: list, best: Optional[tuple],
                        committed: Optional[tuple], ballot: int = 0) -> None:
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
            self._rejoin_ballot = max(self._rejoin_ballot, ballot)
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
                            f"({e})" + ("" if isinstance(e, ProtoWriteRefusedError) else "; the tier failed"))
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
        """Wait until every queued peer message (on the acceptor and read lanes) has been
        handled; return False after ``timeout`` seconds.  Each lane handles its messages in
        arrival order on one thread; tests use this to make a delivery synchronous."""
        with self._cond:
            return self._cond.wait_for(lambda: self._queued == 0, timeout=timeout)

    def await_records(self, root: Optional[RootRecord], label: int, lease: Lease) -> bool:
        """Wait (briefly, under the lease) until the records of the root ``root`` labelled
        ``label`` are stored here: the replication carrying them may still be in this node's
        queue when the lease is won.  Returns False after ``min(fetch_timeout, a quarter of
        the remaining lease)``; reads then fall back to peer page requests.  Returns False at
        once when this node is not known to be one of that commit's data replicas (D54)."""
        if root is None or label == 0:
            return True             # the empty space, or a root written outside the cluster
        wanted = root_key(label, root)
        timeout = min(self._fetch_timeout, max(0.0, lease.remaining()) / 4)
        with self._cond:
            if wanted in self._stored:
                return True
            placed = self._placements.get(wanted)
            if placed is None or self._node not in placed[0]:
                # Not one of this commit's data replicas, as far as this node knows (D54): the
                # records come on demand from those that are; waiting would gain nothing.
                return False
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
        """The dispatcher (the transport's receiving thread): handle ``msg`` without waiting on
        a disk (ruling R81, D50 "Message lanes").  Its duration is recorded in
        ``stats()["dispatch_seconds_max"]`` and ``["dispatch_slow"]`` (over 10 ms)."""
        started = time.perf_counter()
        try:
            self._route(msg)
        finally:
            spent = time.perf_counter() - started
            with self._lock:
                if spent > self._stats["dispatch_seconds_max"]:
                    self._stats["dispatch_seconds_max"] = spent
                if spent > DISPATCH_SLOW:
                    self._stats["dispatch_slow"] += 1

    def _enqueue(self, lane: queue.Queue, msg: dict) -> None:
        """Queue ``msg`` for a lane's thread, or refuse it at once when this node is fenced."""
        with self._lock:
            fenced = self._failure is not None
            if not fenced:
                self._queued += 1
                lane.put(msg)
        if fenced:
            self._refuse_queued(msg)

    def _route(self, msg: dict) -> None:
        sender = msg["from"]
        if sender not in self._peers or sender == self._node:
            logger.warning("Node %s ignored a %r message from unknown sender %r", self._node, msg["type"], sender)
            return
        kind = msg["type"]
        if kind in _FINGERPRINTED and msg["vs"] != self._fingerprint:
            # Configured with another voter set (D54): two sets could form two disjoint
            # majorities, so nothing it asks for is granted, answered or accepted here.
            with self._lock:
                self._stats["config_mismatches"] += 1
                first = sender not in self._mismatched
                self._mismatched.add(sender)
            if first:
                logger.error("Node %s drops %r messages from %s: it is configured with another voter set "
                             "(every member must be given the same voters)", self._node, kind, sender)
            return
        if kind in ("records", "replicate", "store") and msg["records"]:
            # D55 rule H3: the next holder may build on this commit's root before the lane has
            # made these records durable, so they are cached at once (memory only, as ``warm``).
            self._cache_on_receipt(msg["records"])
        if kind in ("records", "replicate", "store", "request", "renew") or (kind == "query" and msg["ballot"]):
            # The acceptor lane: each step decides, makes its write durable, publishes, then
            # replies, in arrival order, exactly as when the dispatcher ran the grants inline
            # (D50 "Message lanes").  A replicate follows its record chunks.
            self._enqueue(self._jobs, msg)
            return
        if kind in ("fetch", "commit_query", "warm"):
            self._enqueue(self._reads, msg)
            return
        if kind == "release":
            # Fairness at once (timing only); the grant it ends and the committed root it
            # teaches go through the acceptor lane, behind the sender's earlier replications.
            with self._cond:
                if self._state == "running":
                    self._note_release(sender, msg)
            self._enqueue(self._jobs, msg)
            return
        if kind == "want":
            # After the sender's release, whose fairness part the dispatcher handled already.
            with self._cond:
                if self._state == "running":
                    self._note_wanting(sender, time.monotonic())
            return
        if kind == "done":
            # The owner sequences its next permission at once: ``done`` never waits behind this
            # node's own copy of the sender's replication (head-of-line blocking, R81).  The
            # committed root it teaches is learnt and logged on the acceptor lane.
            self._on_done_owner(msg)
            self._enqueue(self._jobs, msg)
            return
        if kind == "permit_req":
            self._on_permit_req(sender, msg)
            return
        if kind == "propose":
            self._on_propose(sender, msg)
            return
        if kind in ("permit", "permit_no"):
            self._on_permit_reply(sender, msg)
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
                    reply = make_message("answer", self._node, rid=msg["rid"], inc=msg["inc"], root=None, committed=None,
                                         certain=False, promised=0, state=state, ballot=0)
                else:
                    # ``promised``: the lease activity this node knows of (terms seen in requests
                    # and releases, promises) and the accepted root's label -- not the committed
                    # root's label, which rides in ``committed`` and may come from the persisted
                    # root of the store, nor a floor restored from such labels.  A rejoin tells a
                    # cluster's first start by ``promised == 0`` (ruling R51), and a new cluster
                    # over an existing object store has seen no lease term yet.
                    reply = make_message("answer", self._node, rid=msg["rid"], inc=msg["inc"],
                                         root=self._wire(self._accepted),
                                         committed=self._wire(self._committed), certain=not self._suspect_term,
                                         promised=max(self._grants.terms_seen, self._accepted[2]),
                                         state=state, ballot=0)
            else:
                self._on_answer(sender, kind, msg)
        if reply is not None:
            self._transport.send(sender, reply)

    def _on_ballot_query(self, sender: str, msg: dict) -> None:
        """A rejoining member asks for a durable promise of its ballot before this witness
        answers (ruling R78a, "Rejoining" in the module docstring).  The promise is raised in
        memory and the answer read under ``_log_lock``, so no acceptance falls between them
        (ruling R55); the answer is sent only once the promise is durable.  Answered as a
        plain query (``ballot`` 0, not counted by the rejoin) when this node is not a
        witness by then."""
        ballot = msg["ballot"]
        with self._log_lock:
            with self._cond:
                if self._state != "running":
                    return
                state = self._witness_state()
                tier = self._tier if state == "ok" else None
                if tier is not None:
                    ended = self._grants.promise(ballot)
                    if ended:
                        # A grant ended before its time: its holder's outcome is not known here.
                        self._suspect_term = max(self._suspect_term, ended)
                    self._hint = max(self._hint, ballot)
                    self._cond.notify_all()
                reply = make_message("answer", self._node, rid=msg["rid"], inc=msg["inc"],
                                     root=self._wire(self._accepted) if state != "unknown" else None,
                                     committed=self._wire(self._committed) if state != "unknown" else None,
                                     certain=state != "unknown" and not self._suspect_term,
                                     promised=max(self._grants.terms_seen, self._accepted[2]) if state != "unknown" else 0,
                                     state=state, ballot=ballot if tier is not None else 0)
            if tier is not None and not (self._logged_promise[0] is tier and ballot <= self._logged_promise[1]) \
                    and not self._log_promise(tier, ballot):
                return              # a promise that could not be made durable is not answered
        self._transport.send(sender, reply)

    def _on_request(self, sender: str, msg: dict, *, renew: bool = False) -> None:
        """Decide a lease request and make a grant's promise durable before it is sent.  The
        decision reports the accepted root and raises the promise in memory, so it is atomic
        with acceptances: under ``_log_lock``, no acceptance is decided but not yet published
        meanwhile (ruling R55; module docstring, "Acceptor-log writes")."""
        with self._log_lock:
            with self._cond:
                if self._state != "running":
                    return
                if not renew:
                    self._note_request(sender)
                else:
                    # Only a renewal proves its term was won: a plain request may lose, and
                    # taking it as the owner's would send every permission request to a
                    # loser (measured: 8 TCP peers, members starved for seconds).
                    self._note_owner(msg["term"])
                reply = self._decide(sender, msg["term"], msg["rid"], msg["inc"], renew=renew)
                tier = self._tier if reply["type"] == "grant" else None
            # ``tier`` is attached until _log_lock is released: a grant needs one (_decide).  A
            # term at or below the promise already logged on that tier needs no new write (a
            # renewal or a duplicate; D50): the log keeps only the highest promise.
            if tier is not None and not (self._logged_promise[0] is tier and msg["term"] <= self._logged_promise[1]) \
                    and not self._log_promise(tier, msg["term"]):
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
            with self._lock:
                if self._logged_promise[0] is tier:
                    self._logged_promise = (tier, max(self._logged_promise[1], term))
                else:
                    self._logged_promise = (tier, term)
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
        if not self._is_voter:
            return "learner"
        if self._tier is None or not self._loaded:
            return "no_storage"
        if self._tier_failure is not None:
            return "tier_failed"
        if self._rejoining:
            return "rejoining"
        return None

    def _decide(self, requester: str, term: int, rid: int, inc: int, *, renew: bool = False) -> dict:
        """Apply the grant rules to a request (lock held); return the reply.  A node without
        an attached tier (nowhere durable to record the promise) or fenced refuses."""
        refusal = self._refusal()
        if refusal is not None:
            if refusal == "no_storage":
                self._stats["grants_refused_no_storage"] += 1
            return make_message("reject", self._node, term=term, rid=rid, inc=inc, root=self._wire(self._accepted),
                                certain=not self._suspect_term, reason=refusal,
                                highest=self._grants.highest_term, remaining=0.0)
        if ballot_rank(term) != self._ranks[requester]:
            # Not this requester's ballot: granting it could give one term to two proposers.
            logger.warning("Node %s refused term %d from %s: not a ballot of its rank", self._node, term,
                           requester)
            return make_message("reject", self._node, term=term, rid=rid, inc=inc, root=self._wire(self._accepted),
                                certain=not self._suspect_term, reason="bad_ballot",
                                highest=self._grants.highest_term, remaining=0.0)
        decision = self._grants.on_request(requester, term, inc, renew=renew)
        if decision.previous_expired:
            self._suspect_term = max(self._suspect_term, decision.ended_term)
        root = self._wire(self._accepted)
        certain = not self._suspect_term
        if decision.granted:
            self._stats["grants_given"] += 1
            return make_message("grant", self._node, term=term, rid=rid, inc=inc, root=root,
                                committed=self._wire(self._committed), certain=certain)
        return make_message("reject", self._node, term=term, rid=rid, inc=inc, root=root, certain=certain,
                            reason=decision.reason, highest=decision.highest,
                            remaining=max(0.0, decision.remaining))

    def _on_reply(self, sender: str, msg: dict) -> None:
        self._hint = max(self._hint, msg["term"], msg.get("highest", 0))
        if msg["inc"] != self._inc:
            self._stats["stale_replies"] += 1
            return                  # a reply to a previous incarnation of this node (review C1)
        known, root, label = decode_labeled_root(msg["root"])
        self._hint = max(self._hint, label)
        if sender not in self._voters:
            return                  # a learner's reply is never a vote (D54)
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
        # refused us, so it wakes a waiting requester: a cancel ends only the busy extension
        # of its back-off, a winner's release ends it all (:meth:`_back_off`; N7, M2).
        self._wakeups += 1
        if msg["held"]:
            self._progress += 1
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

    def _await_turn(self, deadline: float, *, yield_to_owner: bool = False) -> bool:
        """Wait while a release named another member's turn (module docstring, "Fairness"):
        until that member's request has been seen and its attempt or lease ended, at most
        ``lease_duration`` after its request, or :data:`_TURN_SILENCE` when its request is not
        seen.  A node the others do not queue (it neither asked, nor released wanting more,
        nor said ``want`` since it was last served or named) first broadcasts ``want``, so that
        a later release names it.  Timing only: it never decides a grant.

        ``yield_to_owner`` (a permission request, D50): stop waiting, returning False, as soon
        as another member is known to own a term -- the caller asks that owner for a
        permission instead of waiting to acquire the lease.  Without it, a named member that
        became owner and renews under load kept this node waiting for ``lease_duration``
        without a single commit (Task 8.3f-B, measured: 4.6-4.7 s with a 6 s lease).  Returns
        True when the caller may attempt the lease."""
        waited = False
        while True:
            with self._cond:
                while True:
                    self._check_running()
                    if yield_to_owner and self._owner_hint[1] not in (None, self._node):
                        return False
                    turn = self._turn
                    if turn is None:
                        return True
                    member, named, seen = turn
                    now = time.monotonic()
                    end = seen + self._duration if seen is not None else named + self._turn_silence
                    if now >= end:
                        self._turn = None
                        self._stats["deferral_timeouts"] += 1
                        return True
                    if now >= deadline:
                        return True         # the attempt that follows reports the timeout
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
        if msg["inc"] != self._inc:
            # A reply to a previous incarnation of this node, whose request ids restarted at 1:
            # never an acknowledgement, a copy, an answer or a record of this one (review C1).
            self._stats["stale_replies"] += 1
            return
        waiter = self._waiters.get(msg["rid"])
        if waiter is None:
            return
        if kind == "ack":
            if waiter.kind != "replicate" or msg["term"] != waiter.term:
                return
            if sender in self._voters:          # only a voter's answer decides the root (D54)
                if msg["ok"]:
                    waiter.acks.add(sender)
                    waiter.nacks.discard(sender)
                elif sender not in waiter.acks:
                    waiter.nacks.add(sender)
            if msg["stored"]:
                waiter.stored.add(sender)
                self._replica_suspects.pop(sender, None)
            else:
                waiter.unstored.add(sender)
            known, root, label = decode_labeled_root(msg["root"])
            self._hint = max(self._hint, label)
        elif kind == "answer":
            if waiter.kind != "query":
                return
            waiter.answers[sender] = (decode_labeled_root(msg["root"]), decode_labeled_root(msg["committed"]),
                                      msg["certain"])
            waiter.promised[sender] = msg["promised"]
            waiter.states[sender] = msg["state"]
            waiter.ballots[sender] = msg["ballot"]
        elif kind == "fetched":
            if waiter.kind != "fetch" or waiter.key != (msg["segment"], msg["offset"]):
                return              # the bytes of another record are never this one's (review C2)
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

    def _send_voters(self, msg: dict) -> None:
        """Send ``msg`` to every other voter (a request, a renewal or a root: only votes count,
        D54); a broadcast when every member is a voter."""
        if len(self._other_voters) == len(self._others):
            self._broadcast(msg)
            return
        for peer in self._other_voters:
            self._transport.send(peer, msg)

    # -- bounded replication: placement and locator (D54) -----------------------------------

    def _replica_preference(self) -> list:
        """The other members in the order this node asks them to hold its records (lock
        held): its successors in the sorted order of member ids (the deterministic placement
        of D54), members that did not answer lately last.  Timing only: which members are
        asked first never decides an acceptance."""
        i = self._ring.index(self._node)
        order = self._ring[i + 1:] + self._ring[:i]
        now = time.monotonic()
        for member, until in list(self._replica_suspects.items()):
            if until <= now:
                del self._replica_suspects[member]
        return [m for m in order if m not in self._replica_suspects] + \
            [m for m in order if m in self._replica_suspects]

    def _learn_locator(self, label: int, root: Optional[RootRecord], copies, ranges) -> None:
        """Remember that ``copies`` hold the records of the commit of ``root`` labelled
        ``label``, spanning ``ranges`` (lock held; bounded, most recent first)."""
        copies = list(dict.fromkeys(c for c in copies if c in self._peers))
        if not copies:
            return
        if root is not None:
            key = root_key(label, root)
            self._placements[key] = (copies, list(ranges) if ranges is not None else None)
            self._placements.move_to_end(key)
            while len(self._placements) > _STORED_MEMORY:
                self._placements.popitem(last=False)
        for segment, _, _ in ranges or ():
            known = self._locator.get(segment, [])
            self._locator[segment] = copies + [c for c in known if c not in copies]
            self._locator.move_to_end(segment)
        while len(self._locator) > _LOCATOR_MEMORY:
            self._locator.popitem(last=False)

    def _place_records(self, waiter: "_Waiter", lease, parts: list, data_msg: dict, preference: list,
                       meanwhile: Optional[Callable[[], None]]) -> Optional[list]:
        """The data phase of a replication (D54): send the records to ``majority - 1`` members
        of ``preference`` and wait until that many hold them durably; return the copies (this
        node first), or ``None`` when the lease ended or the coordinator stopped first.

        ``meanwhile()`` runs once the records have left for the first members asked and before
        waiting for any acknowledgement: the holder's own synced append (Task 8.3j, ruling R90),
        whose sync overlaps the replicas'.  This method returns only after it, so the copies
        returned include this node's durable one; an exception it raises propagates (the
        records have left: the caller reports the outcome unknown).

        A member that answers without storing them, or does not answer within
        ``request_timeout`` (counted from the end of ``meanwhile()``), is replaced by the next
        member of the preference list (and tried last for one lease duration).  Timing only: the copies are those whose ``stored``
        acknowledgement arrived, each sent after the records were durable there."""
        wanted = self._majority - 1
        asked = []
        send = preference[:wanted]
        hedge_at = time.monotonic() + self._request_timeout
        while True:
            for peer in send:
                for part in parts:
                    self._transport.send(peer, part)
                self._transport.send(peer, data_msg)
            asked.extend(send)
            send = []
            if meanwhile is not None:
                meanwhile()
                meanwhile = None
                # The hedge deadline counts from here: a slow own sync must not make a replica
                # that is still syncing look unresponsive (Task 8.3j review M1).
                hedge_at = time.monotonic() + self._request_timeout
            with self._cond:
                while not send:
                    stored = waiter.stored
                    if len(stored) >= wanted:
                        return [self._node] + [p for p in asked if p in stored]
                    if self._state != "running" or not lease.is_valid():
                        return None
                    left = [p for p in preference if p not in asked]
                    waiting = [p for p in asked if p not in stored and p not in waiter.unstored]
                    now = time.monotonic()
                    short = wanted - len(stored) - len(waiting)
                    if short > 0 and left:
                        send = left[:short]
                    elif now >= hedge_at:
                        for peer in waiting:
                            self._replica_suspects[peer] = now + self._duration
                        self._stats["replica_hedges"] += 1
                        hedge_at = now + self._request_timeout
                        send = left[:1]
                    else:
                        self._cond.wait(timeout=max(0.001, min(hedge_at - now, lease.remaining())))

    # -- the worker: replicated records, page requests, prefetch ---------------------------

    def _work(self, lane: queue.Queue) -> None:
        """A lane's thread: the acceptor lane (``_jobs``) or the read lane (``_reads``)."""
        while True:
            msg = lane.get()
            if msg is _STOP:
                return
            try:
                kind = msg["type"]
                if kind in ("request", "renew"):
                    self._on_request(msg["from"], msg, renew=kind == "renew")
                elif kind == "query":
                    self._on_ballot_query(msg["from"], msg)
                elif kind == "replicate":
                    self._apply_replicate(msg)
                elif kind == "store":
                    self._apply_store(msg)
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
                elif kind == "done":
                    self._on_done(msg)
                elif kind == "commit_query":
                    self._serve_commit(msg)
                elif kind == "warm":
                    self._warm(msg)
                else:
                    self._serve_fetch(msg)
            except (ProtoDBError, OSError) as e:
                logger.error("Node %s: handling a %r message failed: %s", self._node, msg["type"], e)
            except Exception as e:
                # A bug or an unforeseen failure: never die silently, never keep acknowledging
                # with a broken worker.  Fence the node, refuse what is still queued (nothing
                # is enqueued once fenced), then let the thread fail loudly.
                self.fence(e)
                self._drain(lane)
                raise
            finally:
                with self._cond:
                    self._queued -= 1
                    self._cond.notify_all()

    def _refuse_queued(self, msg: dict) -> None:
        """A fenced node's answer to a peer message it will not handle: a replication is
        refused (its holder need not wait for this node), anything else is dropped."""
        if msg["type"] in ("replicate", "store"):
            self._transport.send(msg["from"], make_message("ack", self._node, term=msg["term"], rid=msg["rid"],
                                                           inc=msg["inc"],
                                                           ok=False, root=self._wire(self._accepted), stored=False))

    def _drain(self, lane: queue.Queue) -> None:
        """Refuse every message still queued on ``lane`` (fenced; that lane's thread)."""
        while True:
            try:
                msg = lane.get_nowait()
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
            if pending is None or pending[0] != (msg["rid"], msg["inc"]):
                # An older replication, or one of another incarnation of the sender, is void.
                pending = self._chunks[sender] = ((msg["rid"], msg["inc"]), {})
            pending[1][msg["index"]] = msg["records"]

    def _may_accept(self, term: int, root: Optional[RootRecord], label: int, pred: Optional[tuple] = None,
                    seq: int = 0) -> bool:
        """The acceptance rules of a replicated root (lock held): running, healthy, a tier
        attached (its acceptor log loaded), no higher term granted here (ballot rule), not older
        than the accepted root, and the chain rule (D50; D55 rule V2').  The startup grace does
        not apply (D50): it keeps a restarted node from granting while a lease it may have
        granted before the restart could still run, a timing promise; an acceptance reads only
        the promise and the accepted root, both restored from the log at attach.

        The chain rule is Raft's log matching applied to roots (D55, V2'): a root whose ``pred``
        (the ``(known, root, label)`` it was built on) carries the root's own label is accepted
        only when it is the accepted root again (a duplicate) or its ``pred`` IS the accepted
        root -- which includes a root this node learnt committed (:meth:`_observe_committed`
        makes it the accepted root).  Nothing a message claims about ``pred`` counts.  Only the
        first root of a term -- its ``pred`` labelled below it, a base that came out of phase
        1 -- may follow an accepted root of an older term, or arrive at a node that accepted
        nothing yet, under the ballot rule alone.  A permission ``seq`` below the highest
        accepted in this term is refused (a stale permission's late message), and a sibling
        (another root of the same term and epoch) is refused, never a protocol violation."""
        if self._state != "running" or self._refusal() is not None or self._grants.granted_top > term:
            return False
        first_of_term = pred is not None and pred[0] and pred[2] < label
        if not self._accepted[0]:
            return first_of_term
        _, accepted, accepted_label = self._accepted
        if _siblings(label, root, accepted_label, accepted):
            # Two roots of one term on one base: two holders of one term (the owner's
            # sequencing broke).  Refused, never a protocol violation: D50, S3.
            return False
        order = compare_roots(label, root, accepted_label, accepted)
        if order <= 0:
            return order == 0
        if accepted_label != label:
            # A root of a newer term: only its first root, built on a base out of phase 1.  A
            # later root of that term extends a root this node does not hold (V2').
            return first_of_term
        if self._seq_seen[0] == label and seq < self._seq_seen[1]:
            return False
        if pred is None or not pred[0] or _siblings(pred[2], pred[1], accepted_label, accepted):
            return False
        return compare_roots(pred[2], pred[1], accepted_label, accepted) == 0

    def _apply_replicate(self, msg: dict) -> None:
        sender, term, rid = msg["from"], msg["term"], msg["rid"]
        known, root, label = decode_labeled_root(msg["root"])
        pred, seq = decode_labeled_root(msg["pred"]), msg["seq"]
        records = msg["records"]
        with self._lock:
            tier = self._tier
            failed = self._tier_failure is not None
            pending = self._chunks.pop(sender, None)
        complete = True
        if msg["chunks"]:
            parts = pending[1] if pending is not None and pending[0] == (rid, msg["inc"]) else {}
            complete = all(i in parts for i in range(msg["chunks"]))
            if complete:
                records = [r for i in range(msg["chunks"]) for r in parts[i]] + records
        valid = known and label == term and root is not None and complete
        with self._lock:
            # Acknowledging needs a durable place for the records and the root: a node without
            # a tier never counts toward the majority.
            self._note_owner(term)
            ok = valid and tier is not None and self._may_accept(term, root, label, pred, seq)
        if tier is not None and callable(getattr(tier, "accept_replication", None)):
            self._accept_with_records(msg, tier, failed, valid, ok, root, label, pred, seq, records)
            return
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
                ok = ok and self._tier is tier and self._may_accept(term, root, label, pred, seq)
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
                    self._note_seq(label, seq)
                    ck, croot, clabel = decode_labeled_root(msg["committed"])
                    self._observe_committed(ck, croot, clabel, None)
                    self._stats["accepted"] += 1
                    reply_root = self._wire(self._accepted)
        self._transport.send(sender, make_message("ack", self._node, term=term, rid=rid, inc=msg["inc"], ok=ok,
                                                  root=reply_root,
                                                  # Never a durable copy here: ``apply`` honours the
                                                  # durability mode, so no ``stored`` is claimed (review
                                                  # M3); bounded replication needs ``accept_replication``.
                                                  stored=False))
        if ok and records:
            tier.prefetch(records)

    def _accept_with_records(self, msg: dict, tier, failed: bool, valid: bool, ok: bool, root, label: int,
                             pred, seq: int, records: list) -> None:
        """The acceptor side of a replication on a tier that accepts with its records (Task
        8.3f phase B, D50; bounded replication, Task 8.3g, D54).

        A voter accepts the root only when ``majority`` members hold the commit's records
        durably: the ``copies`` the message names, plus this node when the records ride in it
        (it is then one of the commit's data replicas, and its acceptance and its copy are one
        append).  The decision, ONE ``fsync``-ed entry holding the root and the records it
        carried (none for a root sent alone, which names the commit's ranges instead), the
        publication, then the acknowledgement.  A data replica that does not accept the root
        (a learner, or a voter refusing it) still stores the records durably, as one synced
        entry of their own, before it acknowledges them as ``stored``."""
        sender, term, rid = msg["from"], msg["term"], msg["rid"]
        copies = set(msg["copies"])
        ranges = protocol.decode_ranges(msg["ranges"])
        carried = any(r[2] is not None for r in records)
        local = bool(getattr(tier, "local_records", False))
        holders = copies | ({self._node} if carried else set())
        enough = not local or ranges == [] or len(holders) >= self._majority
        if valid and not failed:
            bk, broot, blabel = decode_labeled_root(msg["base"])
            if bk and broot is not None and local:
                # A re-proposed base: its records rode with this replication (ruling R50); its
                # ranges are remembered (synced) before the root is accepted.
                try:
                    tier.store_commit([], broot, blabel, protocol.decode_ranges(msg["base_ranges"]) or [])
                except (ProtoDBError, OSError) as e:
                    if not self._tier_failed(tier, e, "remembering a re-proposed base"):
                        logger.info("Node %s: the tier was detached while storing a replication: %s", self._node, e)
                    ok = False
        with self._log_lock:
            # The decision, the log write and the publication form one step against attach,
            # detach and every other acceptor decision (ruling R55).
            with self._lock:
                ok = ok and enough and not failed and self._tier is tier and self._may_accept(
                    term, root, label, pred, seq)
            if ok:
                try:
                    tier.accept_replication(label, root, records, seq=seq,
                                            pred=(pred[2], pred[1]) if pred is not None and pred[0] else None,
                                            ranges=None if carried else ranges)
                except (ProtoDBError, OSError) as e:
                    # Attached throughout (_log_lock): the tier failed (ruling R57), or the space
                    # closed under it.  Nothing is published; never acknowledged.
                    if not self._tier_failed(tier, e, "logging an accepted root with its records"):
                        logger.info("Node %s: the tier was detached while accepting a replication: %s",
                                    self._node, e)
                    ok = False
            with self._lock:
                if ok:
                    self._observe_accepted(True, root, label)
                    self._note_seq(label, seq)
                    ck, croot, clabel = decode_labeled_root(msg["committed"])
                    self._observe_committed(ck, croot, clabel, None)
                    self._stats["accepted"] += 1
                    if self._tier is tier and carried:
                        self._note_stored(label, root)
                else:
                    self._stats["refused"] += 1
                reply_root = self._wire(self._accepted)
        stored = ok and carried
        if not ok and valid and not failed and carried:
            # A data replica that does not accept the root: the records are made durable here
            # all the same (one synced entry) before they are acknowledged as stored.
            try:
                tier.accept_replication(label, root, records, seq=seq,
                                        pred=(pred[2], pred[1]) if pred is not None and pred[0] else None,
                                        ranges=ranges if ranges is not None else protocol.record_ranges(records),
                                        accepted=False)
                stored = True
            except (ProtoDBError, OSError) as e:
                if not self._tier_failed(tier, e, "storing a replication's records"):
                    logger.info("Node %s: the tier was detached while storing a replication: %s", self._node, e)
            if stored:
                with self._cond:
                    if self._tier is tier:
                        self._note_stored(label, root)
        if valid:
            with self._lock:
                self._learn_locator(label, root, (list(msg["copies"]) + ([self._node] if stored else [])),
                                    ranges)
        self._transport.send(sender, make_message("ack", self._node, term=term, rid=rid, inc=msg["inc"], ok=ok,
                                                  root=reply_root,
                                                  stored=stored))

    def _apply_store(self, msg: dict) -> None:
        """The data stage of a gated pipelined round (acceptor lane; D55 rule V1): make the
        records durable with ONE synced entry that does not accept the root, learn where they
        are, then answer ``stored``.  Never an acceptance, by construction: this path does not
        call :meth:`_may_accept`, :meth:`_note_seq`, :meth:`_observe_accepted` nor
        :meth:`_observe_committed`, and never answers ``ok`` True; replay counts only accepted
        entries, so the entry never appears in an answer, a ballot or after a restart."""
        sender, term, rid = msg["from"], msg["term"], msg["rid"]
        known, root, label = decode_labeled_root(msg["root"])
        records = msg["records"]
        with self._lock:
            tier = self._tier
            failed = self._tier_failure is not None
            running = self._state == "running"
            pending = self._chunks.pop(sender, None)
        complete = True
        if msg["chunks"]:
            parts = pending[1] if pending is not None and pending[0] == (rid, msg["inc"]) else {}
            complete = all(i in parts for i in range(msg["chunks"]))
            if complete:
                records = [r for i in range(msg["chunks"]) for r in parts[i]] + records
        valid = (known and label == term and root is not None and complete and running
                 and any(r[2] is not None for r in records))
        stored = False
        ranges = protocol.decode_ranges(msg["ranges"])
        if valid and not failed and tier is not None and callable(getattr(tier, "accept_replication", None)):
            try:
                self._store_entry(tier, msg, label, root, records,
                                  ranges if ranges is not None else protocol.record_ranges(records))
                stored = True
            except (ProtoDBError, OSError) as e:
                if not self._tier_failed(tier, e, "storing a pipelined round's records"):
                    logger.info("Node %s: the tier was detached while storing records: %s", self._node, e)
        with self._lock:
            if stored:
                self._stats["stores_applied"] += 1
                if self._tier is tier:
                    self._note_stored(label, root)
            if valid:
                self._learn_locator(label, root, list(msg["copies"]) + ([self._node] if stored else []), ranges)
            reply_root = self._wire(self._accepted)
        self._transport.send(sender, make_message("ack", self._node, term=term, rid=rid, inc=msg["inc"], ok=False,
                                                  root=reply_root, stored=stored))

    def _store_entry(self, tier, msg: dict, label: int, root: RootRecord, records: list, ranges: list) -> None:
        """The one synced entry of a ``store``: the records, never an acceptance (V1).
        Planted test P2 breaks this seam."""
        tier.accept_replication(label, root, records, ranges=ranges, accepted=False)

    def _cache_on_receipt(self, records: list) -> None:
        """Cache records of another member's commit as they arrive (dispatcher, memory only)."""
        tier = self._tier
        cache = getattr(tier, "cache_records", None) if tier is not None else None
        if cache is not None:
            cached = cache(records)
            with self._lock:
                self._stats["records_cached_on_receipt"] += cached

    def _serve_fetch(self, msg: dict) -> None:
        tier = self._tier
        data = None
        if tier is not None:
            data = tier.local_record(AtomPointer(uuid.UUID(bytes=msg["segment"]), msg["offset"]))
        with self._lock:
            if data is not None:
                self._stats["fetches_served"] += 1
        self._transport.send(msg["from"], make_message("fetched", self._node, rid=msg["rid"], inc=msg["inc"],
                                                       segment=msg["segment"], offset=msg["offset"], data=data))

    def _warm(self, msg: dict) -> None:
        """Cache the records of another member's commit (ruling R89): memory only, never
        acknowledged; a tier without a record cache ignores them."""
        tier = self._tier
        cache = getattr(tier, "cache_records", None) if tier is not None else None
        if cache is None:
            return
        cached = cache(msg["records"])
        with self._lock:
            self._stats["records_warmed"] += cached

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
        self._transport.send(msg["from"], make_message("commit_info", self._node, rid=msg["rid"], inc=msg["inc"],
                                                       persisted=persisted,
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
            self._send_voters(request)
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
            # ``seen`` dates from before the request: what arrived since then counts.
            self._back_off(failures, wait, remaining, seen)
            failures += 1

    def _wake_marks(self) -> tuple:
        """``(wakeups, progress, owner hint)`` now (lock held): what :meth:`_back_off` compares
        against to tell what arrived since."""
        return self._wakeups, self._progress, self._owner_hint

    def _back_off(self, failures: int, busy: float, remaining: float, seen: tuple) -> None:
        """The wait after a lost lease attempt, shared by :meth:`_acquire` and :meth:`permit`
        (re-review 1 of 8.3n, N7; re-review 2, M2): the jittered exponential delay, extended
        to the refusing granters' ``busy`` remaining, never past ``remaining``.  ``seen`` is
        :meth:`_wake_marks` from before the attempt.  Progress -- a winner's release, a done,
        a newly learnt owner -- ends the whole wait.  Any other release (a lost attempt's
        cancel) may have freed a granter, so it ends the busy extension, never the back-off
        itself: every lost attempt broadcasts a cancel, and when each one ended every waiter's
        back-off, members retried every 2-4 ms while no majority could grant.  A woken member
        that the release did not name for the turn adds a small random spread (and defers to
        the named one, :meth:`_await_turn`)."""
        low, high = self._backoff
        delay = min(high, low * (2 ** min(failures, 30))) * (0.5 + self._rng.random() / 2)
        now = time.monotonic()
        backoff_end = now + min(delay, remaining)
        busy_end = now + min(max(delay, busy), remaining)
        with self._cond:
            self._check_running()

            def progress():
                return self._progress != seen[1] or self._owner_hint != seen[2] or self._state != "running"
            self._cond.wait_for(progress, timeout=max(0.0, backoff_end - time.monotonic()))
            if not progress():
                self._cond.wait_for(lambda: progress() or self._wakeups != seen[0],
                                    timeout=max(0.0, busy_end - time.monotonic()))
            self._check_running()
            woken = progress() or self._wakeups != seen[0]
            spread = woken and not self._my_turn
        if spread:
            time.sleep(self._rng.random() * _WAKE_SPREAD)

    def _open_attempt(self) -> tuple:
        """Open an attempt with this node's own vote decided; return ``(attempt, request,
        wake marks seen (:meth:`_wake_marks`), tier whose log must record our own promise or
        None)``.  Our own vote reads the accepted root and raises the promise in memory, so it is decided under
        ``_log_lock``, atomically with acceptances (ruling R55); its promise is written by
        :meth:`_promise` after the broadcast."""
        promise = None
        with self._log_lock, self._lock:
            self._check_running()
            self._check_healthy()
            term = ballot_after(max(self._grants.highest_term, self._hint, self._accepted[2], self._committed[2]),
                                self._rank)
            attempt = LeaseAttempt(term=term, rid=next(self._rids), sent_at=self._clock(),
                                   cluster_size=len(self._voters), lease_duration=self._duration,
                                   drift_margin=self._margin)
            self._attempt = attempt
            self._stats["attempts"] += 1
            if self._my_turn:
                self._my_turn = False
                self._stats["turns_taken"] += 1
            self._registered = True             # every member that receives the request queues us
            refusal = self._refusal()
            if refusal == "learner":
                pass                            # no vote of its own (D54)
            elif refusal is not None:
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
            request = make_message("request", self._node, term=term, rid=attempt.rid, inc=self._inc,
                                   vs=self._fingerprint)
            seen = self._wake_marks()
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
                known, root, certain, label = attempt.best_root(own=self._accepted if self._is_voter else None)
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

    # -- write permissions (D50, Task 8.3f) ---------------------------------------------------

    def _note_owner(self, term: int) -> None:
        """A message of the lease term ``term`` was seen (lock held): remember the newest owner."""
        if term > self._owner_hint[0]:
            self._owner_hint = (term, self._by_rank.get(ballot_rank(term)))
            self._cond.notify_all()         # a deferred permit request may ask this owner now

    def _note_seq(self, label: int, seq: int) -> None:
        """A root of term ``label`` under permission ``seq`` was accepted here (lock held)."""
        if (label, seq) > self._seq_seen:
            self._seq_seen = (label, seq)

    @property
    def owner(self) -> Optional[str]:
        """The node this node believes owns the current lease term (D50), or ``None``."""
        with self._lock:
            if self._ownership is not None and not self._ownership.ended:
                return self._node
            return self._owner_hint[1]

    def permit(self, timeout: Optional[float] = None) -> Permission:
        """Obtain a write permission (D50): from this node's own ownership, from the owner this
        node knows of, or by becoming the owner (acquiring the lease).  Raises
        ``ProtoTimeoutError`` after ``timeout`` seconds (default ``acquire_timeout``),
        ``ProtoValidationError`` if the coordinator stops and ``ProtoUnexpectedError`` if the
        node is fenced.  One permission at a time per node (``ProtoUserError`` otherwise); end
        it with :meth:`finish`."""
        timeout = self._acquire_timeout if timeout is None else _number("timeout", timeout, positive=False)
        deadline = time.monotonic() + timeout
        with self._lock:
            self._check_running()
            self._check_healthy()
            if self._permit is not None:
                raise ProtoUserError(f"Node {self._node} already holds a write permission")
            self._acquirers += 1
        try:
            failures = 0
            while True:
                self._await_rejoin(deadline, timeout)
                permission = self._permission_from_owner(deadline)
                if permission is not None:
                    return permission
                with self._lock:
                    # Dated from before the attempt (as in _acquire): what arrives meanwhile counts.
                    seen = self._wake_marks()
                owned, busy = self._become_owner(deadline)
                if owned:
                    continue
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ProtoTimeoutError(
                        f"Node {self._node} could not obtain a write permission within {timeout} s")
                self._back_off(failures, busy, remaining, seen)
                failures += 1
        finally:
            with self._lock:
                self._acquirers -= 1

    def _permission_from_owner(self, deadline: float) -> Optional[Permission]:
        """A permission from this node's ownership or from the owner it knows of, or ``None``
        (no owner known, refused, or no answer in time)."""
        sends = []
        with self._cond:
            self._check_running()
            self._check_healthy()
            own = self._ownership
            if own is not None and own.serving(self._clock()):
                wait = _PermitWait(next(self._rids), self._clock())
                own.queue.append((self._node, wait.rid, self._inc, wait))
                own.last_demand = own.last_local = time.monotonic()
                sends = self._dispatch(own)
                target = None
            else:
                term, target = self._owner_hint
                if target is None or target == self._node or term == 0:
                    return None
                wait = _PermitWait(next(self._rids), self._clock(), term)
                self._permit_waits[wait.rid] = wait
                self._stats["permit_requests_sent"] += 1
                request = make_message("permit_req", self._node, term=term, rid=wait.rid, inc=self._inc)
        self._send_all(sends)
        if target is not None:
            self._transport.send(target, request)
        # A remote owner answers when this request reaches the head of its queue; one lease
        # duration without an answer means it is gone or busy elsewhere: look again.
        limit = time.monotonic() + (self._duration if target is not None else self._duration * 2)
        # A lost request or permit is retried after request_timeout (review M6): the owner
        # answers a retry of an outstanding permission with the same permit, never a second.
        retry_at = time.monotonic() + self._request_timeout
        with self._cond:
            try:
                while wait.reply is None:
                    if self._state != "running" or self._failure is not None or self._tier_failure is not None:
                        break
                    if target is None and (self._ownership is not own or own.ended):
                        break
                    now = time.monotonic()
                    if now >= min(limit, deadline):
                        break
                    if target is not None and now >= retry_at:
                        retry_at = now + self._request_timeout
                        self._stats["permit_requests_retried"] += 1
                        self._cond.release()
                        try:
                            self._transport.send(target, request)
                        finally:
                            self._cond.acquire()
                        continue
                    wake = min(limit, deadline) if target is None else min(limit, deadline, retry_at)
                    self._cond.wait(timeout=wake - now)
                reply = wait.reply
                wait.reply = wait.reply or "gone"   # a permit arriving later is handed back
            finally:
                self._permit_waits.pop(wait.rid, None)
            if isinstance(reply, Permission):
                self._permit = reply
                self._stats["permits_held"] += 1
                if reply.root_certain:
                    # The owner knows its base committed: so does this node now.
                    self._observe_committed(True, reply.root, reply.root_term, reply.owner)
                return reply
            if target is None and own is not None and wait in [q[3] for q in own.queue]:
                own.queue = collections.deque(q for q in own.queue if q[3] is not wait)
        if reply is None and target is not None:
            with self._lock:
                # Unresponsive: try to own -- unless this node learnt a newer term of that
                # member meanwhile (its renewals reached here), which the silence says
                # nothing about (Task 8.3n fix round 2, as for a refusal).
                if self._owner_hint == (wait.term, target):
                    self._owner_hint = (self._owner_hint[0], None)
        self._check_running()
        self._check_healthy()
        return None

    def _send_all(self, sends: list) -> None:
        for target, msg in sends:
            if target is None:
                self._broadcast(msg)
            else:
                self._transport.send(target, msg)

    def _become_owner(self, deadline: float) -> tuple:
        """Try once to acquire the lease and own its term; return ``(owned, busy)``: True when
        this node now owns it, and the refusing granters' ``busy_remaining`` after a lost
        attempt (0 otherwise)."""
        if not self._local.acquire(timeout=max(0.0, deadline - time.monotonic())):
            return False, 0.0
        try:
            with self._lock:
                if self._ownership is not None:
                    self._local.release()
                    return True, 0.0
            if not self._await_turn(deadline, yield_to_owner=True):
                self._local.release()       # an owner is known: permit() asks it
                return False, 0.0
            attempt, request, seen, promise = self._open_attempt()
            self._send_voters(request)
            if promise is not None and not self._promise(promise, attempt.term):
                with self._lock:
                    attempt.granted_by.pop(self._node, None)
                    attempt.on_reject(self._node, "promise_failed", attempt.term, 0.0)
            lease, cancel, wait = self._close_attempt(attempt)
        except BaseException:
            self._local.release()
            raise
        if lease is None:
            self._broadcast(cancel)
            # A "busy" rejection names the granter's highest REQUESTED term, not the owner's:
            # the owner is learnt from what only an owner's term carries (renew, permit,
            # replicate, done), all broadcast or sent within one round.
            self._local.release()
            return False, wait
        with self._cond:
            stopped = self._state != "running"
            if not stopped:
                known = lease.root is not None or lease.root_certain
                committed = lease.root_certain and self._committed == (True, lease.root, lease.root_term)
                own = _Ownership(lease, (known, lease.root, lease.root_term), committed, time.monotonic())
                self._ownership = own
                self._owner_hint = (lease.term, self._node)
                self._stats["ownerships"] += 1
                thread = threading.Thread(target=self._own_loop, args=(own,), name=f"proto-cluster-owner-{self._node}",
                                          daemon=True)
                self._owner_threads = [t for t in self._owner_threads if t.is_alive()] + [thread]
        if stopped:
            self.release(lease)         # review M2: every exit gives the lease and local lock back
            return False, 0.0
        thread.start()
        return True, 0.0

    def _dispatch(self, own: "_Ownership") -> list:
        """Issue the next permission of ``own`` when rule O1 allows it (lock held); return the
        messages to send.

        O1 (D55): the queue is not empty, the term is serving, the R80 drain and the D/4 rule
        allow it, the pipeline holds at most ``DEPTH`` entries, and its newest entry, if any,
        has proposed and has not been issued on (O2).  The base is that proposal -- the permit
        then names it in ``after`` and is never ``certain`` (H2) -- or, with an empty pipeline,
        the newest root the owner knows committed (D50's permit)."""
        if not own.queue or not own.serving(self._clock()) or len(own.pipeline) > DEPTH:
            return []
        newest = own.pipeline[-1] if own.pipeline else None
        if newest is not None and (newest.proposal is None or newest.consumed):
            return []
        if self._idle_release is not None and time.monotonic() - own.last_local > self._idle_release:
            # Draining (ruling R80): no new permission; the ownership thread ends the term as
            # soon as the outstanding ones are done.  Without the drain, a member that asks
            # continuously kept a permission outstanding at nearly every check, and the term
            # never ended (measured with the GIL on).
            self._cond.notify_all()
            return []
        if own.lease.remaining() < self._belief / 4:
            # Too little lease left for a replication to finish under it: the ownership
            # thread renews first and dispatches once renewed (or ends the term).
            self._cond.notify_all()
            return []
        node, rid, inc, wait = own.queue.popleft()
        own.seq += 1
        # Valid for what remains of the owner's lease (never longer): a holder's commit may
        # include a recovery round, which can take the whole request timeout.
        valid = max(0.0, own.lease.remaining())
        now = self._clock()
        self._stats["permits_issued"] += 1
        if newest is not None:
            # Rule O2: a proposal is issued on once.
            newest.consumed = True
            root, label = newest.proposal
            known, certain, after = True, False, [newest.seq, newest.node]
            self._stats["pipelined_permits"] += 1
        else:
            known, root, label = own.base
            certain, after = known and own.committed, None
        issued = _Issued(node, own.seq, inc, rid, now + valid + self._request_timeout, None, (known, root, label))
        own.pipeline.append(issued)
        if wait is not None:            # one of this node's own threads
            wait.reply = Permission(term=own.term, seq=own.seq, owner=self._node, acquired_at=now,
                                    valid_until=now + valid, root=root, root_certain=certain,
                                    clock=self._clock, root_term=label, rid=rid, after=after)
            self._cond.notify_all()
            return []
        issued.permit = make_message("permit", self._node, term=own.term, rid=rid, seq=own.seq, inc=inc,
                                     root=encode_root(root, known=known, term=label), certain=certain,
                                     valid=valid, after=after)
        return [(node, issued.permit)]

    def _on_permit_req(self, sender: str, msg: dict) -> None:
        """A member asks this node, as owner of ``term``, for a permission (transport thread)."""
        with self._cond:
            if self._state != "running":
                return
            own = self._ownership
            if own is not None and own.term == msg["term"] and own.serving(self._clock()) \
                    and self._failure is None:
                out = next((e for e in own.pipeline if e.node == sender and e.inc == msg["inc"]
                            and e.rid == msg["rid"]), None)
                if out is not None:
                    # A retried request whose permit is outstanding (it, or the permit, was
                    # lost; review M6; rule O7 per entry): send the same permit again -- never
                    # a second one.
                    sends = [(sender, out.permit)] if out.permit is not None else []
                else:
                    if not any(q[0] == sender and q[1] == msg["rid"] for q in own.queue):
                        own.queue.append((sender, msg["rid"], msg["inc"], None))
                    sends = self._dispatch(own)
                own.last_demand = time.monotonic()
            else:
                reason = "not_owner" if own is None or own.term != msg["term"] else "ending"
                term = max(self._grants.highest_term, own.term if own is not None else 0)
                sends = [(sender, make_message("permit_no", self._node, term=term, rid=msg["rid"],
                                               inc=msg["inc"], reason=reason))]
        self._send_all(sends)

    def _on_permit_reply(self, sender: str, msg: dict) -> None:
        """An owner answered a permit request of this node (transport thread)."""
        sends = []
        with self._cond:
            if self._state != "running":
                return
            if msg["type"] == "permit":
                self._note_owner(msg["term"])        # a permit proves its term; a refusal does not
            wait = self._permit_waits.get(msg["rid"])
            if msg["type"] == "permit_no":
                if msg["inc"] != self._inc:
                    # A refusal sent to a previous incarnation of this node, whose request ids
                    # restarted: never this incarnation's answer (re-review N1).
                    self._stats["stale_replies"] += 1
                    return
                if wait is not None and wait.reply is None:
                    wait.reply = "refused"
                    self._stats["permits_refused"] += 1
                    if self._owner_hint[1] == sender and self._owner_hint[0] == wait.term:
                        # The member this node took for the owner answered this very request:
                        # it does not serve that term (not its owner, ending or abandoned).
                        # Forget it whatever term the refusal carries -- the sender's highest
                        # granted term, which a refused ballot of another member can raise
                        # above the hint's.  With the old ``term <= hint`` condition the hint
                        # survived, ``_become_owner`` kept deferring to it, and a member asked
                        # the former owner again until its permission timed out (measured: 27 s
                        # after an ``owner_idle_release`` end, ruling R80).  The term is kept,
                        # so the next owner's messages update the hint (timing only, D50).
                        # Only while the hint still names the term the request was sent
                        # under: a refusal of an older term's request says nothing about a
                        # newer term of the same member, learnt since from its renewals, and
                        # forgetting that one was never undone -- its renewals teach only a
                        # term above the hint's, so this node asked nobody while that member
                        # kept the term (Task 8.3n fix round 2: no commit for 30 s under a
                        # 0.25 s owner-to-member delay).
                        self._owner_hint = (self._owner_hint[0], None)
                    self._cond.notify_all()
                return
            if msg["inc"] != self._inc:
                # Issued to another incarnation of this node (a previous life: a delayed or
                # duplicated copy).  Dropped unanswered: only the incarnation that received
                # a permission may say what it did with it (8.3f review, C1); its owner learns
                # the outcome as unknown (no done of that incarnation) and abandons the term.
                self._stats["permits_foreign_dropped"] += 1
                return
            key = (msg["term"], msg["seq"])
            if key in self._permits_seen:
                # A duplicate within this incarnation: the first copy was used or handed back.
                # Bounded: this incarnation holds one permission at a time and the owner
                # matches every done against its single outstanding permission, so an entry
                # evicted after 64 newer permits can only lead to a "none" the owner ignores
                # (review M3); the incarnation check above is what safety needs.
                return
            self._permits_seen.append(key)
            if wait is None or wait.reply is not None:
                # Nobody waits for it any more: hand it back at once, so the owner moves on.
                sends = [(sender, make_message("done", self._node, term=msg["term"], seq=msg["seq"], inc=self._inc,
                                               outcome="none", root=None, copies=[], ranges=None))]
            else:
                known, root, label = decode_labeled_root(msg["root"])
                now = self._clock()
                # Measured from the request's send (the D45 argument): the owner issued the
                # permission later, so it never outlasts the owner's lease here.
                after = msg["after"]
                wait.reply = Permission(term=msg["term"], seq=msg["seq"], owner=sender, acquired_at=now,
                                        valid_until=wait.sent_at + msg["valid"], root=root if known else None,
                                        # H2: a proposal base is never certain, whatever the message says.
                                        root_certain=msg["certain"] and known and after is None, clock=self._clock,
                                        root_term=label, rid=msg["rid"],
                                        after=tuple(after) if after is not None else None)
                self._cond.notify_all()
        self._send_all(sends)

    def finish(self, permission: Permission) -> None:
        """End ``permission`` with its ``outcome`` (and ``committed`` root): tell its owner and
        every member (D50).  Never raises for the permission this node holds; twice is a no-op."""
        sends = []
        with self._cond:
            if permission is not self._permit:
                if permission.released_at is not None:
                    return
                raise ProtoUserError("This permission is not held by this coordinator")
            self._permit = None
            permission._mark_released()
            outcome = permission.outcome
            label, root = permission.committed if permission.committed is not None else (0, None)
            if permission.owner == self._node:
                own = self._ownership
                issued = own.entry(self._node, permission.seq) if own is not None and own.term == permission.term \
                    else None
                if issued is not None and issued.inc == self._inc:
                    sends = self._permit_outcome(own, issued, outcome, root, label)
            running = self._state == "running"
            placed = self._placements.get(root_key(label, root)) if outcome == "committed" else None
            copies, ranges = placed if placed is not None else ([], None)
            msg = make_message("done", self._node, term=permission.term, seq=permission.seq, inc=self._inc,
                               outcome=outcome, root=encode_root(root, known=outcome == "committed", term=label),
                               copies=copies, ranges=protocol.encode_ranges(ranges))
        if running:
            self._broadcast(msg)
            self._send_all(sends)

    def _permit_outcome(self, own: "_Ownership", issued: _Issued, outcome: str, root: Optional[RootRecord],
                        label: int) -> list:
        """The issued permission ``issued`` of ``own`` ended with ``outcome`` (lock held; the
        sender is the incarnation it was issued to); return the messages to send.

        O3 (``committed``): a permission that proposed must report its proposal, else the term
        is abandoned; it and every older entry are retired (a successor's commit implies its
        predecessor's: Lemma C, at the voters), and ``root`` becomes the committed base.  O5
        (``none``): the only entry, which did not propose, is removed and the next permit
        reuses the committed base (D50); any other ``none`` abandons the term.  O6: any other
        outcome abandons the term (D50)."""
        if self._idle_release is not None:
            self._cond.notify_all()     # a draining ownership thread ends the term now (R80)
        if outcome == "committed":
            if issued.proposal is not None and issued.proposal != (root, label):
                return self._abandon(own, f"permission {issued.seq} of {issued.node} committed another root "
                                          f"than it proposed")
            index = own.pipeline.index(issued)
            del own.pipeline[:index + 1]
            known, base, base_label = own.base
            if not known or compare_roots(label, root, base_label, base) > 0:
                own.base = (True, root, label)
            own.committed = True
        elif outcome == "none":
            if own.pipeline != [issued] or issued.proposal is not None:
                return self._abandon(own, f"permission {issued.seq} of {issued.node} ended without a root "
                                          f"with {'a proposal' if issued.proposal is not None else 'another'} "
                                          f"permission outstanding")
            own.pipeline.clear()
        else:
            return self._abandon(own, outcome)
        return self._dispatch(own)

    def _on_propose(self, sender: str, msg: dict) -> None:
        """A holder proposes the root it built under permission ``seq`` (dispatcher; rule O4,
        D55).  Ignored unless it comes from an issued ``(node, seq, inc)``; a duplicate is
        ignored; a changed proposal, a proposal from an entry that is not the newest and had
        none, or a proposal not built on the issued base abandons the term; otherwise the
        next permission may be issued on it (O1)."""
        known, root, label = decode_labeled_root(msg["root"])
        pred = decode_labeled_root(msg["pred"])
        sends = []
        with self._cond:
            if self._state != "running":
                return
            own = self._ownership
            issued = own.entry(sender, msg["seq"]) if own is not None and own.term == msg["term"] else None
            if issued is None or issued.inc != msg["inc"] or not known or label != own.term:
                self._stats["foreign_proposals"] += 1
                return
            if issued.proposal == (root, label):
                return                                              # O4.2: a duplicate
            if issued.proposal is not None:
                sends = self._abandon(own, f"permission {issued.seq} of {sender} proposed two roots")
            elif issued is not own.pipeline[-1]:
                sends = self._abandon(own, f"permission {issued.seq} of {sender} proposed after its successor "
                                           f"was issued")
            elif pred != issued.base:
                sends = self._abandon(own, f"permission {issued.seq} of {sender} proposed a root not built on "
                                           f"its base")
            else:
                issued.proposal = (root, label)
                self._stats["proposals_received"] += 1
                sends = self._dispatch(own)
        self._send_all(sends)

    def _abandon(self, own: "_Ownership", why: str) -> list:
        """Stop issuing permissions of ``own``'s term (lock held): an outcome it cannot know.
        The lease is released as uncertain by the ownership thread."""
        if own.abandoned:
            return []
        own.abandoned = True
        own.pipeline.clear()
        self._stats["terms_abandoned"] += 1
        self._suspect_term = max(self._suspect_term, own.term)
        logger.info("Node %s abandons lease term %d: %s", self._node, own.term, why)
        sends = self._refuse_queue(own, "abandoned")
        self._cond.notify_all()
        return sends

    def _refuse_queue(self, own: "_Ownership", reason: str) -> list:
        sends = []
        while own.queue:
            node, rid, inc, wait = own.queue.popleft()
            if wait is None:
                sends.append((node, make_message("permit_no", self._node, term=own.term, rid=rid, inc=inc,
                                                 reason=reason)))
        self._cond.notify_all()         # local waiters see the ownership end
        return sends

    def _on_done_owner(self, msg: dict) -> None:
        """A permission ended (dispatcher): when this node owns that term, sequence the next
        permission now.  The owner reads only the message and its own term's state, never this
        node's acceptor state, so it need not wait for its own copy of the replication (R81)."""
        sender = msg["from"]
        sends = []
        with self._cond:
            if self._state != "running":
                return
            self._note_owner(msg["term"])
            if msg["outcome"] != "committed":
                self._failed_permits.append((msg["term"], msg["seq"], sender))
            if msg["outcome"] == "committed" and msg["copies"]:
                # Where the committed root's records are (D54): reads fetch from there first.
                known, root, label = decode_labeled_root(msg["root"])
                self._learn_locator(label, root, msg["copies"], protocol.decode_ranges(msg["ranges"]))
            own = self._ownership
            issued = own.entry(sender, msg["seq"]) if own is not None and own.term == msg["term"] else None
            if issued is not None:
                # Only the incarnation the permission was issued to knows what was done
                # with it; any other incarnation's done (a restarted holder answering a
                # duplicated permit, before this fix) is an unknown outcome (C1).
                known, root, label = decode_labeled_root(msg["root"])
                outcome = msg["outcome"] if msg["inc"] == issued.inc else "unknown"
                sends = self._permit_outcome(own, issued, outcome, root, label)
            self._wakeups += 1
            self._progress += 1
            self._cond.notify_all()
        self._send_all(sends)

    def _on_done(self, msg: dict) -> None:
        """A permission ended (acceptor lane, after the sender's earlier replications): learn
        its committed root and log it, atomically with acceptor decisions (ruling R55)."""
        learnt, tier = None, None
        with self._log_lock:
            with self._cond:
                if self._state != "running":
                    return
                known, root, label = decode_labeled_root(msg["root"])
                if msg["outcome"] == "committed" and known and self._observe_committed(True, root, label,
                                                                                       msg["from"]):
                    learnt = (label, root)
                tier = self._tier
            if learnt is not None and tier is not None:
                try:
                    tier.log_root("committed", learnt[0], learnt[1])
                except (ProtoDBError, OSError) as e:
                    self._tier_failed(tier, e, "logging a committed root")

    def _own_loop(self, own: "_Ownership") -> None:
        """The ownership thread: renews the lease while there is demand, abandons a term whose
        outstanding permission timed out, releases the lease when idle (D50).  An unexpected
        failure fences the node; the ownership never outlives this thread (review, quality)."""
        try:
            self._own_loop_body(own)
        except Exception as e:
            with self._cond:
                if self._ownership is own:
                    own.ended = True
                    self._ownership = None
                    sends = self._abandon(own, f"its ownership thread failed: {e!r}") + \
                        self._refuse_queue(own, "ending")
                else:
                    sends = []
            self._send_all(sends)
            self.fence(e)
            raise

    def _own_loop_body(self, own: "_Ownership") -> None:
        """The loop of :meth:`_own_loop`."""
        idle = self._duration / 4
        while True:
            renew = False
            sends = []
            with self._cond:
                if self._state != "running" or self._ownership is not own:
                    return
                now, mono = self._clock(), time.monotonic()
                late = next((e for e in own.pipeline if now > e.deadline), None)
                if late is not None:
                    sends = self._abandon(own, f"permission {late.seq} of {late.node} timed out")
                end = None
                if own.abandoned:
                    end = "abandoned"
                elif not own.lease.is_valid():
                    end = "expired"
                    if own.pipeline:
                        # The lease ended with a permission whose outcome this owner does not
                        # know yet: abandon, so the release is uncertain (review I3).
                        sends = self._abandon(own, f"lease expired with permission {own.pipeline[0].seq} "
                                                   f"of {own.pipeline[0].node} outstanding")
                elif not own.pipeline and not own.queue and mono - own.last_demand > idle:
                    end = "idle"
                elif not own.pipeline and self._idle_release is not None \
                        and mono - own.last_local > self._idle_release:
                    # _dispatch issues nothing new past the threshold, so the outstanding
                    # permission drains and this check is reached.
                    # Serving only other members (ruling R80): one of them takes the term.
                    end = "idle_local"
                    self._stats["terms_released_idle_local"] += 1
                if end is not None:
                    own.ended = True
                    self._ownership = None
                    sends += self._refuse_queue(own, "ending")
                else:
                    demand = own.pipeline or own.queue or mono - own.last_demand <= self._duration / 2
                    # Renewed when half of the holder's belief is left (ruling R78b: with the
                    # default margin, every lease_duration / 6 -- a message, no fsync).
                    renew = demand and own.lease.remaining() < self._belief / 2
                    if not renew:
                        waits = [max(0.001, own.lease.remaining() - self._belief / 2) if demand else idle]
                        if own.pipeline:
                            waits.append(max(0.001, min(e.deadline for e in own.pipeline) - now))
                        elif not own.queue:
                            waits.append(max(0.001, idle - (mono - own.last_demand)))
                        if self._idle_release is not None and not own.pipeline:
                            # While a permission is outstanding the drain waits for its done,
                            # which wakes this thread (_permit_outcome): no 1 ms polling.
                            waits.append(max(0.001, self._idle_release - (mono - own.last_local)))
                        self._cond.wait(timeout=min(waits))
            self._send_all(sends)
            if end is not None:
                try:
                    self.release(own.lease)
                except (ProtoDBError, OSError) as e:
                    logger.error("Node %s: releasing its lease (%s) failed: %s", self._node, end, e)
                return
            if renew:
                self._renew(own)

    def _renew(self, own: "_Ownership") -> None:
        """Ask the granters to extend the lease of ``own`` (same term; D50, renewal)."""
        with self._log_lock, self._lock:
            if self._state != "running" or self._ownership is not own:
                return
            attempt = LeaseAttempt(term=own.term, rid=next(self._rids), sent_at=self._clock(),
                                   cluster_size=len(self._voters), lease_duration=self._duration,
                                   drift_margin=self._margin)
            self._attempt = attempt
            refusal = self._refusal()
            if refusal == "learner":
                pass                            # no vote of its own (D54)
            elif refusal is None:
                decision = self._grants.on_request(self._node, own.term, self._inc, renew=True)
                if decision.granted:
                    known, root, label = self._accepted
                    attempt.on_grant(self._node, known, root, True, label)
                else:
                    attempt.on_reject(self._node, decision.reason, decision.highest, decision.remaining)
            else:
                attempt.on_reject(self._node, refusal, self._grants.highest_term, 0.0)
            msg = make_message("renew", self._node, term=own.term, rid=attempt.rid, inc=self._inc,
                               vs=self._fingerprint)
        self._send_voters(msg)
        with self._cond:
            started = time.monotonic()
            self._cond.wait_for(lambda: attempt.state != "pending" or self._state != "running"
                                or attempt.replies >= attempt.needed, timeout=self._request_timeout)
            if attempt.state == "pending" and self._state == "running":
                straggle = max(_MIN_STRAGGLE, _STRAGGLE_FACTOR * (time.monotonic() - started))
                self._cond.wait_for(lambda: attempt.state != "pending" or self._state != "running",
                                    timeout=max(0.0, min(straggle, started + self._request_timeout - time.monotonic())))
            if self._attempt is attempt:
                self._attempt = None
            sends = []
            if attempt.state == "won" and self._clock() < attempt.valid_until and self._ownership is own:
                own.lease.valid_until = max(own.lease.valid_until, attempt.valid_until)
                self._stats["renewals"] += 1
                sends = self._dispatch(own)        # a request may have waited for the renewal
            else:
                self._stats["renewal_failures"] += 1
            self._cond.notify_all()
        self._send_all(sends)

    # -- the commit point --------------------------------------------------------------------

    def _new_waiter(self, kind: str, term: int = 0) -> tuple:
        rid = next(self._rids)
        waiter = _Waiter(kind, term)
        self._waiters[rid] = waiter
        return rid, waiter

    def replicate(self, lease: Lease, root: RootRecord, *, base: Optional[tuple] = None, tier=None,
                  propose: bool = False) -> float:
        """Commit ``root`` (built on ``lease.root``) by majority-acknowledged replication (D46).

        Makes the records the tier has not shipped yet durable locally (accepting the root
        here when this node is a voter), sends them to ``majority - 1`` data replicas and,
        once they hold them durably, sends ``(epoch, root pointer)`` labelled with the lease
        term, naming those copies, to the other voters (D54; on a storage shared by every node
        the root and the record hints go to every voter at once); returns once a majority of
        the voters has acknowledged, before the lease ends; then records the root as
        committed.  Returns the seconds spent.

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

        Pipelined permissions (Task 8.3k, D55).  ``propose``: the caller's round had no
        conflict; when this is the permission's first root, built on the base its permit gave
        (a certain base or a proposal), the root is proposed to the owner (rule H4: after the
        data-stage and ``warm`` sends, before the own append), which may then issue the next
        permission on it.  Under a permit whose base is a proposal (``after``), nothing a voter
        may accept leaves this node, and no own acceptance is logged, until this node knows the
        base committed (the gate, H5; a liveness and outcome rule only: voters verify by
        extension, V2').  When the gate is already open at the send, the replication runs as
        above (the data replica that is a voter stores and accepts in one append).  When it is
        closed, the data stage is ``store`` messages and a store-only own append (never
        acceptable, V1); after the gate opens, the holder logs its own acceptance of the root
        alone and sends the root alone to every other voter.  If the gate never opens (the
        permission ends, the node stops, a newer term), nothing acceptable was sent.

        Raises ``ProtoConflictError`` exactly when nothing a voter may accept left this node and
        no own acceptance was logged (rule H7: nothing was replicated, safe to retry), and
        ``ProtoOutcomeUnknownError`` on every other failure: no majority acknowledged in time,
        or anything failed once an acceptable message was sent or an own acceptance logged (the
        space closing, an I/O error of the local log): peers may have acknowledged it, and a
        later holder may still adopt it, so the commit may or may not take effect.
        """
        started = time.perf_counter()
        attached = self._tier
        if attached is None:
            raise ProtoValidationError("replicate needs an attached tier (ObjectSpace attaches it)")
        if tier is not None and tier is not attached:
            raise ProtoValidationError("The space this commit belongs to was closed: its tier is no longer "
                                       "attached to the coordinator; nothing was replicated")
        tier = attached
        if lease is not self._held and lease is not self._permit:
            raise ProtoUserError("This lease is not held by this coordinator")
        records = tier.outgoing()
        label = lease.term
        seq = getattr(lease, "seq", 0)
        # The root this one is built on (D50, chain rule): the last root sent under this lease
        # or permission, else its base.
        pred_label, pred_root = lease.sent if lease.sent is not None else (lease.root_term, lease.root)
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
        # In a shared storage our own records are durable there before any of them leaves this
        # node: no peer may acknowledge their root before they are durable where every node
        # reads them.  Phase B (Task 8.3f): a tier that accepts with its records makes them
        # durable together with this node's own acceptance, in one synced append.  With bounded
        # replication that append runs while the records travel to the data replicas (Task
        # 8.3j, ruling R90), so a replica may hold bytes of an own segment that this node has
        # not made durable yet; if it dies meanwhile, it restarts with a strict prefix of that
        # segment and seals it shorter.  The store judges a segment by length (ruling R54) and
        # the replica's copy fills the rest with a part (D50 amendment of Task 8.3j), so no
        # root built on those records becomes unreadable.
        with_records = callable(getattr(tier, "accept_replication", None))
        if not with_records:
            try:
                tier.flush_local()
            except (ProtoDBError, OSError) as e:
                self._tier_failed(tier, e, "making the commit's records durable")
                raise
        all_records = extra + records
        # Bounded replication (D54): on a tier whose records live on each node's disk, the
        # records go to the commit's data replicas first and the root alone to the other voters
        # once ``majority`` members hold the records durably.  Elsewhere (a storage shared by
        # every node) the records are already where every node reads them.
        local = bool(getattr(tier, "local_records", False))
        ranges = protocol.record_ranges(all_records) if local else None
        bounded = (with_records and local and bool(ranges) and self._majority > 1
                   and callable(getattr(tier, "accept_replication", None)))
        chunks = _split_records(all_records, protocol.CHUNK_BYTES)
        with self._lock:
            if self._tier is tier:
                self._note_stored(label, root)        # our own records, on the attached tier
            self._check_running()
            if not lease.is_valid():
                raise ProtoConflictError("The cluster write lease expired before the root was replicated; "
                                         "nothing was replicated")
            # Ruling R121: every path that sends something acceptable passes here first.  The
            # data stage and an ungated round send right after it; a gated round waits for its
            # gate and checks again below.
            self._check_reserve(lease, locked=True)
            if label < self._rejoin_ballot:
                # This node's rejoin ballot (R78a) was promised by V - majority + 1 voters: no
                # term below it can gain a majority again, and every one of them refuses this
                # root.  Nothing is sent: a retry runs under a live term instead of reporting
                # an outcome that the refusals would leave unknown.
                raise ProtoConflictError(f"Term {label} is below this node's rejoin ballot {self._rejoin_ballot}, "
                                         f"which a quorum promised; nothing was replicated")
            rid, waiter = self._new_waiter("replicate", label)
            parts = [make_message("records", self._node, term=label, rid=rid, inc=self._inc, index=i, records=chunk)
                     for i, chunk in enumerate(chunks[:-1])]
            pred = (True, pred_root, pred_label)
            previous_sent = lease.sent
            # The gate (D55, H5): a permit whose base is a proposal, for the first root sent
            # under it (built on that base).  Open when this node knows the base committed.
            after = getattr(lease, "after", None)
            gated = after is not None and lease.sent is None
            gate_open = not gated or self._gate_opens(pred)
            if gated and gate_open:
                self._stats["gate_open_at_send"] += 1
            propose = (propose and isinstance(lease, Permission) and lease.sent is None and base is None
                       and lease.issued_base)
            # A data stage that no voter can accept (V1): only while the gate is closed.
            use_store = bounded and not gate_open
            wire_root = encode_root(root, known=True, term=label)
            wire_pred = encode_root(pred_root, known=True, term=pred_label)
            wire_committed = self._wire(self._committed)
            wire_ranges = protocol.encode_ranges(ranges)

            def replication(carried: list, n_chunks: int, copies: list) -> dict:
                return make_message("replicate", self._node, term=label, rid=rid, inc=self._inc, root=wire_root,
                                    committed=wire_committed, records=carried, chunks=n_chunks, base=base_wire,
                                    base_ranges=base_ranges, seq=seq, pred=wire_pred,
                                    vs=self._fingerprint, copies=copies,
                                    ranges=wire_ranges)

            if use_store:
                msg = make_message("store", self._node, term=label, rid=rid, inc=self._inc, root=wire_root,
                                   records=chunks[-1], chunks=len(parts), ranges=wire_ranges, copies=[self._node],
                                   vs=self._fingerprint)
                self._stats["stores_sent"] += 1
            else:
                msg = replication(chunks[-1], len(parts), [self._node])
            preference = self._replica_preference() if bounded else []
            lease.sent = (label, root)        # the next root of this lease extends this one
            self._stats["replications"] += 1
        proposal = make_message("propose", self._node, term=label, seq=seq, inc=self._inc, root=wire_root,
                                pred=wire_pred) if propose else None
        acceptable = [False]                  # rule H7: something a voter may accept left, or was logged

        def sending_acceptable():
            acceptable[0] = True
            if isinstance(lease, Permission):
                lease.sent_acceptable = True

        failure = None
        acks = nacks = 0
        stopped = False
        copies = [self._node]
        try:
            if not bounded:
                if proposal is not None:
                    self._propose(lease, proposal)
                if not gate_open and not self._await_gate(lease, pred, tier):
                    raise ProtoConflictError("The base of this pipelined permission was not learnt committed "
                                             "while the permission lasted; nothing was replicated")
                if not gate_open:
                    self._check_reserve(lease)      # the gate's wait was charged to the permission
                sending_acceptable()
            if with_records and not bounded:
                self._self_accept_with_records(tier, waiter, label, root, seq, pred, pred_label,
                                               pred_root, all_records)
            if bounded:
                # The data phase: ``majority - 1`` other members store the records durably (a
                # voter among them may accept the root in the same append when that makes
                # ``majority`` copies, this node counted).  This node's own synced append runs
                # while they do (ruling R90: the two syncs overlap), and the data phase ends
                # only after it.  Then the root alone goes to every other voter, naming the
                # copies: no voter accepts the root without carrying a copy itself before the
                # ``majority`` copies exist (D54 amendment of Task 8.3j).
                # Ruling R89: every other member caches the records (memory only, best effort,
                # never awaited), so the next committer finds its base cached.
                replicas = set(preference[:self._majority - 1])

                def warm():
                    for peer in self._others:
                        if peer not in replicas:
                            for chunk in chunks:
                                self._transport.send(peer, make_message("warm", self._node, term=label,
                                                                        records=chunk))

                def own_append():
                    if proposal is not None:
                        # H4: the next holder builds on this root, so its records go first.
                        warm()
                        self._propose(lease, proposal)
                    if use_store:
                        self._store_own(tier, label, root, seq, pred_label, pred_root, all_records)
                    else:
                        self._self_accept_with_records(tier, waiter, label, root, seq, pred,
                                                       pred_label, pred_root, all_records)
                    if proposal is None:
                        warm()

                if self._data_stage_acceptable(use_store):
                    sending_acceptable()        # a data replica that is a voter may accept it
                copies = self._place_records(waiter, lease, parts, msg, preference, own_append)
                if copies is not None and use_store:
                    # The root stage of a gated round (H5): only once this node knows the base
                    # committed; then its own acceptance of the root alone, then the voters.
                    if not self._await_gate(lease, pred, tier):
                        raise ProtoConflictError("The base of this pipelined permission was not learnt committed "
                                                 "while the permission lasted; nothing was replicated")
                    self._check_reserve(lease)      # the data stage and the gate took their time
                    sending_acceptable()
                    self._self_accept_root(tier, waiter, label, root, seq, pred, pred_label, pred_root, ranges)
                if copies is not None:
                    with self._lock:
                        targets = [v for v in self._other_voters if v not in waiter.acks]
                        waiter.nacks.difference_update(targets)     # each answers the root alone again
                    root_msg = replication([], 0, copies)
                    for peer in targets:
                        self._transport.send(peer, root_msg)
            else:
                for part in parts:                         # same ordered link, before the replicate
                    self._send_voters(part)
                self._send_voters(msg)
            with self._log_lock if not with_records else contextlib.nullcontext():
                # Our own acceptance follows the acceptor rule (ruling R55): decided only on
                # the tier this commit's records went to, logged on it while it is attached,
                # then published and counted.
                with self._lock:
                    self_ok = not with_records and self._tier is tier and self._may_accept(
                        label, root, label, pred, seq)
                if self_ok:
                    # Where the records of this root are, before its outcome is known: a node
                    # that adopts it later (a rejoin, a holder re-proposing it) copies them
                    # from here.
                    try:
                        tier.remember_commit(all_records, root, label)
                        tier.log_root("accepted", label, root)
                    except (ProtoDBError, OSError) as e:
                        # Attached throughout (_log_lock): the tier failed (ruling R57); our own
                        # vote is not counted, and the outcome is unknown (below).
                        self._tier_failed(tier, e, "logging the holder's own acceptance")
                        raise
                    with self._lock:
                        self._observe_accepted(True, root, label)
                        self._note_seq(label, seq)
                        waiter.acks.add(self._node)
            with self._cond:
                total = len(self._voters)

                def decided():
                    return (copies is None or len(waiter.acks) >= self._majority
                            or len(waiter.nacks) > total - self._majority
                            or self._state != "running" or not lease.is_valid())

                while not decided():
                    self._cond.wait(timeout=max(0.001, min(0.05, lease.remaining())))
                acks, nacks = len(waiter.acks), len(waiter.nacks)
                ok = copies is not None and acks >= self._majority and lease.is_valid()
                stopped = self._state != "running"
                if ok:
                    self._observe_committed(True, root, label, self._node)
                    self._learn_locator(label, root, copies, ranges)
                    if self._suspect_term and self._suspect_term <= label:
                        self._suspect_term = 0
                elif acceptable[0]:
                    # As in the handler below (M1): a gated data stage that ended before
                    # anything acceptable left (``copies is None``) leaves no outcome this node
                    # cannot know.
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
            if acceptable[0]:
                # Only when something a voter may accept left this node (re-review 2 of 8.3n,
                # M1): a refusal before that (the reserve or the gate of a gated round) leaves
                # nothing whose outcome this node cannot know, so its next grant replies stay
                # certain.
                with self._lock:
                    self._stats["replication_failures"] += 1
                    self._suspect_term = max(self._suspect_term, label)
        finally:
            with self._lock:
                self._waiters.pop(rid, None)
                if not acceptable[0]:
                    lease.sent = previous_sent      # nothing of this root left: the next one does not extend it
        if failure is not None:
            if not acceptable[0]:
                # Rule H7: no own acceptance was logged and nothing a voter may accept left this
                # node (a store, records and warm messages are never accepted, V1), so the root
                # can never be committed (D55, Lemma N): a retryable conflict.
                if isinstance(failure, ProtoConflictError):
                    raise failure
                raise ProtoConflictError(f"Node {self._node}: the commit of root epoch {root.epoch} failed before "
                                         f"anything acceptable was sent ({failure}); nothing was replicated") \
                    from failure
            raise ProtoOutcomeUnknownError(
                f"Node {self._node}: the commit of root epoch {root.epoch} failed after the root was sent "
                f"({failure}); peers may have acknowledged it, so it may or may not take effect") from failure
        if not ok and not acceptable[0]:
            raise ProtoConflictError(f"Node {self._node}: fewer than {self._majority - 1} other members stored the "
                                     f"records of root epoch {root.epoch} before the permission ended; nothing a "
                                     f"voter may accept was sent")
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
            elif copies is None:
                why = (f"fewer than {self._majority - 1} other members stored the commit's records before "
                       f"the lease ended")
            elif nacks > len(self._voters) - self._majority:
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

    def _check_reserve(self, lease: Lease, *, locked: bool = False) -> None:
        """Rule R121 (``send_reserve``): raise ``ProtoConflictError`` when ``lease`` has less
        than the reserve left, before anything a voter may accept leaves this node.  The
        caller has sent nothing acceptable yet, so the conflict is truthful (H7).  ``locked``:
        the caller holds ``_lock``."""
        left = lease.remaining()
        if left < self._send_reserve:
            with contextlib.nullcontext() if locked else self._lock:
                self._stats["sends_refused_reserve"] += 1
            raise ProtoConflictError(f"Node {self._node}: the permission had {left * 1000:.0f} ms left, less than "
                                     f"the {self._send_reserve * 1000:.0f} ms a replication needs (send_reserve); "
                                     f"nothing was replicated")

    def _self_accept_with_records(self, tier, waiter: _Waiter, label: int, root: RootRecord, seq: int, pred: tuple,
                                  pred_label: int, pred_root, records: list) -> None:
        """The holder's own acceptance on a tier that accepts with its records (Task 8.3f phase
        B): decided by the acceptor rule (ruling R55) and logged as ONE synced kind-5 entry with
        every record of the replication.  When the holder may not accept its own root (a
        learner, D54, or a refusal), its records are made durable anyway, by one synced entry
        that does not accept the root.  With bounded replication it runs while the records
        travel to the data replicas (Task 8.3j, ruling R90), and the root goes alone to the
        other voters only after it returned; otherwise it runs before the replication is sent.
        Called inside :meth:`replicate`'s outcome handling: a failure here leaves the outcome
        unknown (the entry may be durable, and the records may be on the replicas)."""
        with self._log_lock:
            with self._lock:
                self_ok = self._tier is tier and self._may_accept(label, root, label, pred, seq)
            if self_ok:
                try:
                    tier.accept_replication(label, root, records, seq=seq, pred=(pred_label, pred_root))
                except (ProtoDBError, OSError) as e:
                    self._tier_failed(tier, e, "logging the holder's own acceptance")
                    raise
                with self._lock:
                    self._observe_accepted(True, root, label)
                    self._note_seq(label, seq)
                    waiter.acks.add(self._node)
                return
        try:
            # A learner, or a voter refusing its own root: the records are made durable by one
            # synced entry that does not accept the root (D54), as a data replica stores them;
            # the redo rebuilds the own segment from it, like from an acceptance.
            tier.accept_replication(label, root, records, seq=seq, pred=(pred_label, pred_root), accepted=False)
        except (ProtoDBError, OSError) as e:
            self._tier_failed(tier, e, "making the commit's records durable")
            raise

    @staticmethod
    def _data_stage_acceptable(use_store: bool) -> bool:
        """Rule H7 for the data stage: a ``replicate`` carrying records may be accepted by a data
        replica that is a voter (and the holder logs its own acceptance with them); a
        ``store`` never is (V1).  Planted test P4 breaks this seam."""
        return not use_store

    def _store_own(self, tier, label: int, root: RootRecord, seq: int, pred_label: int, pred_root,
                   records: list) -> None:
        """The holder's own append in a gated round (D55, H4): its records made durable by one
        synced entry that does not accept the root (never an acceptance, V1); the redo rebuilds
        the own segment from it, as from a data replica's entry."""
        try:
            tier.accept_replication(label, root, records, seq=seq, pred=(pred_label, pred_root), accepted=False)
        except (ProtoDBError, OSError) as e:
            self._tier_failed(tier, e, "making the commit's records durable")
            raise

    def _self_accept_root(self, tier, waiter: _Waiter, label: int, root: RootRecord, seq: int, pred: tuple,
                          pred_label: int, pred_root, ranges: list) -> None:
        """The holder's own acceptance of its root alone, once the gate opened (D55, H5): by the
        acceptor rule (ruling R55; V2' -- the base is accepted here since this node learnt it
        committed), as ONE synced entry naming the commit's ranges (its records are in the
        store-only entry :meth:`_store_own` wrote).  Refused like any acceptance when the rule
        says no: the vote is then missing (liveness only)."""
        with self._log_lock:
            with self._lock:
                ok = self._tier is tier and self._may_accept(label, root, label, pred, seq)
            if not ok:
                return
            try:
                tier.accept_replication(label, root, [], seq=seq, pred=(pred_label, pred_root), ranges=ranges)
            except (ProtoDBError, OSError) as e:
                self._tier_failed(tier, e, "logging the holder's own acceptance")
                raise
            with self._lock:
                self._observe_accepted(True, root, label)
                self._note_seq(label, seq)
                waiter.acks.add(self._node)

    def _propose(self, lease: Permission, proposal: dict) -> None:
        """Send ``proposal`` to the owner of ``lease`` (D55, H4), or apply it here when this node
        owns the term."""
        lease.proposed = True
        with self._lock:
            self._stats["proposals"] += 1
        if lease.owner == self._node:
            self._on_propose(self._node, proposal)
        else:
            self._transport.send(lease.owner, proposal)

    def _gate_opens(self, base: tuple) -> bool:
        """The gate's condition (D55, H5; lock held): this node knows ``base`` committed AND
        holds it as its accepted root.  A committed root read back from the persisted root
        (the persister, ``accept=False``, D46) is not accepted knowledge, so the holder's own
        acceptance of a root built on it would be refused (V2'); the gate then keeps waiting
        for cluster knowledge -- a ``done``, or the fallback query's answer, both learnt as
        accepted.  Task 8.3k ruling (measured: the holder's round expired without its own
        acceptance, tests/concurrency/test_cluster_permit_hot_key.py with the GIL on).
        Liveness only, like the gate itself."""
        return self._committed == base and self._accepted == base

    def await_proposal_base(self, permission: Permission) -> bool:
        """Wait until this node knows the base of ``permission`` -- a proposal (``after``) --
        committed, as the gate does (D55, H5); True then, False once the gate closes.  For a
        pipelined round that sends nothing: its conflicted commits retry from that base."""
        if getattr(permission, "after", None) is None:
            return True
        return self._await_gate(permission, (True, permission.root, permission.root_term), self._tier)

    def _await_gate(self, lease: Lease, base: tuple, tier) -> bool:
        """Wait, on the committing thread, until this node knows ``base`` -- the proposal its
        permit was issued on -- committed (the gate, D55 rule H5): True then; False when the
        gate closes first (the permission ended, the node stopped, a newer term was seen, or a
        committed root of this term above ``base`` is known: impossible under correct
        sequencing).  A closed gate never reopens.  Any committed knowledge opens it: a
        ``done``, a permit, a release, a recovery answer.  If none arrived within
        ``request_timeout``, a plain ``query`` goes to the proposal's holder and every voter
        (ruling R94), and again every ``request_timeout`` (a lost answer is asked again); an
        ``answer`` whose committed root is ``base`` is learnt (accepted knowledge, as from a
        ``done``) and logged.  The gate also closes as soon as the predecessor's ``done``
        says it did not commit (``unknown`` or ``none``; review 8.3k, M4).  Liveness and
        outcome quality only: voters accept the next root only by extension of what they hold
        (V2'), and a closed gate means nothing acceptable was sent."""
        label = base[2]
        after = getattr(lease, "after", None)
        holder = after[1] if after else None
        failed = (lease.term, after[0], after[1]) if after else None
        fallback_at = time.monotonic() + self._request_timeout
        rid = waiter = query = None
        waited = False
        try:
            while True:
                send = learn = None
                with self._cond:
                    while True:
                        if self._gate_opens(base):
                            return True
                        committed = self._committed
                        if self._state != "running" or not lease.is_valid() or self._owner_hint[0] > label \
                                or (committed[0] and committed[2] >= label
                                    and compare_roots(committed[2], committed[1], label, base[1]) > 0) \
                                or (failed is not None and failed in self._failed_permits):
                            self._stats["gate_closures"] += 1
                            if isinstance(lease, Permission):
                                lease.gate_closed = True    # ends it after this round (N2)
                            return False
                        if waiter is not None and any(a[1] == base for a in waiter.answers.values()):
                            learn = base
                            break
                        if not waited:
                            waited = True
                            self._stats["gate_waits"] += 1
                        now = time.monotonic()
                        if now >= fallback_at:
                            if waiter is None:
                                rid, waiter = self._new_waiter("query")
                                self._stats["gate_fallbacks"] += 1
                                targets = list(dict.fromkeys(m for m in [holder] + list(self._other_voters)
                                                             if m is not None and m != self._node))
                                query = make_message("query", self._node, rid=rid, inc=self._inc, ballot=0,
                                                     vs=self._fingerprint)
                            send = query        # the same rid: every answer lands in one waiter
                            fallback_at = now + self._request_timeout
                            break
                        wake = (fallback_at - now) if waiter is None else min(0.05, fallback_at - now)
                        self._cond.wait(timeout=max(0.001, min(wake, lease.remaining())))
                if send is not None:
                    for peer in targets:
                        self._transport.send(peer, send)
                if learn is not None:
                    with self._log_lock:
                        with self._cond:
                            learnt = self._observe_committed(True, base[1], label, holder)
                            attached = self._tier
                        if learnt and attached is not None:
                            try:
                                attached.log_root("committed", label, base[1])
                            except (ProtoDBError, OSError) as e:
                                self._tier_failed(attached, e, "logging a committed root")
        finally:
            if rid is not None:
                with self._lock:
                    self._waiters.pop(rid, None)

    # -- recovery and peer services ----------------------------------------------------------

    def _ask(self, kind: str, msg_fields: dict, done: Callable, timeout: float, targets=None) -> _Waiter:
        with self._lock:
            self._check_running()
            rid, waiter = self._new_waiter(kind)
            if kind == "fetch":
                waiter.key = (msg_fields["segment"], msg_fields["offset"])
        msg = make_message(kind, self._node, rid=rid, inc=self._inc, **msg_fields)
        try:
            for peer in (self._others if targets is None else targets):
                self._transport.send(peer, msg)
            with self._cond:
                self._cond.wait_for(lambda: done(waiter) or self._state != "running", timeout=timeout)
                return waiter
        finally:
            with self._lock:
                self._waiters.pop(rid, None)

    def recover_root(self, every: bool = False) -> tuple:
        """Recovery round: ask every peer for its roots and wait for a majority (this node included).

        Returns ``(known, root, label)``: the highest accepted root among the answers and
        this node's.  The query carries ballot 0 and promises nothing: an answering voter may
        accept a root of an older term after it answered, so the result orders at or above
        every root a majority accepted *before* the intersecting member answered, and may miss
        one accepted after (re-review 2 of 8.3n, C1).  A commit's base therefore never comes
        from this result alone: :func:`proto_db.transactions._lease_base` floors it at the
        lease's phase-1 root, whose granters promised the term before they replied.  With
        ``every``, the round waits for every other voter (bounded by ``request_timeout``)
        instead of ending at a majority: a best-effort read that wants the committed roots
        every live voter knows, such as a just-acknowledged commit whose release is still on
        its way (``ObjectSpace.open_database``, re-review 2, M6).
        Committed roots learnt are adopted and the suspicion is cleared; the accepted
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

        def settled(w) -> bool:
            # Evaluated with the lock held.  The round ends once a majority of witnesses
            # answered (D46): waiting for every voter made each round last request_timeout
            # while one was down, inside the commit's lease (Task 8.3n fix round 3).
            if len(w.answers) >= len(self._other_voters):
                return True
            if every:
                return False
            ok = sum(1 for p in w.answers if w.states.get(p) == "ok")
            return ok + int(self._witness_state() == "ok") >= self._majority

        waiter = self._ask("query", {"ballot": 0, "vs": self._fingerprint}, settled, self._request_timeout,
                           targets=self._other_voters)
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
        of a lease this node holds).  The members the locator names for the record's segment
        (the data replicas of the commits that wrote it, D54) are asked first; the others only
        when none of them has it."""
        if not self._others:
            return None
        tier = self._tier
        permitted = bool(tier is not None and getattr(tier, "in_critical", lambda: False)())
        with self._lock:
            if self._state != "running":
                return None
            self._stats["fetches_requested"] += 1
            self._stats["record_fetches"] += 1
            if permitted:
                self._stats["record_fetches_permitted"] += 1
            timeout = self._fetch_timeout
            if self._held is not None:
                timeout = min(timeout, self._held.remaining())
            located = [m for m in self._locator.get(ptr.segment, ()) if m != self._node]
            if located:
                self._stats["fetches_located"] += 1
        rounds = [located, [m for m in self._others if m not in located]] if located else [list(self._others)]
        for i, targets in enumerate(rounds):
            if not targets:
                continue
            if i:
                with self._lock:
                    self._stats["fetch_fallbacks"] += 1
            waiter = self._ask("fetch", {"segment": ptr.segment.bytes, "offset": ptr.offset},
                               lambda w, n=len(targets): w.data is not None or w.missing >= n, timeout,
                               targets=targets)
            if waiter.data is not None:
                return waiter.data
        return None


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
