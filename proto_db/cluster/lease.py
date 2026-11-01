"""The majority-granted write lease of the cluster (Tasks 8.3c and 8.3d, decisions D44, D45, D46).

Three pieces of pure state, with no I/O and no locking (the coordinator serializes
access under its own lock):

:class:`PeerGrants`
    What one peer has promised.  It grants the lease to at most one holder per term and,
    once it has granted, to no other holder until the lease expires on its own clock or
    the holder releases it.  It does no I/O itself: the coordinator makes every grant's
    term durable in the node's acceptor log before sending the grant, and restores the
    logged promise after a restart (:meth:`PeerGrants.restore`), so a term is never granted
    twice, even across a crash.  After a (re)start it also refuses every grant for
    ``startup_grace`` seconds, so a lease *timing* promise made before a crash is honoured.
:class:`LeaseAttempt`
    One request of a requester: grants and rejections gathered from the peers.  It is won
    when a majority (``floor(N/2) + 1``, the requester's own vote included) has granted,
    lost when a majority can no longer be reached.
:class:`Lease`
    What a winner holds: valid on the holder's clock until ``sent_at + lease_duration -
    drift_margin``, measured from the moment the request was SENT, which precedes every
    grant.

Safety argument (D45): let clocks run at rates within ``[1 - rho, 1 + rho]`` of real time.
A peer that grants at real time ``g`` (after the request was sent at real time ``s``)
refuses other holders until at least ``g + D / (1 + rho)``; the holder believes it holds
the lease until at most ``s + (D - m) / (1 - rho)``.  With ``m >= 2 * rho * D / (1 + rho)``
the belief ends before the promise.  Two majorities share a peer, so two holders' beliefs
never overlap in real time.

Since D46 the data does not depend on that timing argument: a peer refuses to accept a
replicated root of a term lower than one it has granted (:attr:`PeerGrants.granted_top`),
so a holder whose lease ended without its knowing cannot reach a majority once a newer
lease exists (the ballot rule of Paxos).  Because the promises are durable, each term has
at most one holder, so two different roots never share a label and an epoch
(:mod:`proto_db.cluster.rootlog`).  The lease timing only keeps holders from wasting each
other's work.
"""
from typing import Callable, NamedTuple, Optional

from ..storage.format import RootRecord
from .rootlog import compare_roots

__all__ = ["majority", "FencingToken", "GrantDecision", "PeerGrants", "LeaseAttempt", "Lease"]


def majority(cluster_size: int) -> int:
    """Votes needed out of ``cluster_size``: ``floor(N/2) + 1``."""
    return cluster_size // 2 + 1


class FencingToken(NamedTuple):
    """Identifies one lease holder's write: the lease term and the root epoch it builds on.

    Tokens order by term first.  The storage enforces the epoch part through the
    ``expected_epoch`` of its conditional root write: a holder whose base epoch is no
    longer current cannot publish.
    """

    term: int
    epoch: int


class GrantDecision(NamedTuple):
    """Outcome of :meth:`PeerGrants.on_request`.

    ``reason`` is ``"granted"``, ``"busy"`` (another holder's lease runs), ``"stale_term"``
    (lower than the highest term seen), ``"term_taken"`` (this term was already granted or
    closed here) or ``"starting"`` (startup grace).  ``remaining`` is the seconds left of the
    promise or grace that blocks a grant.  ``previous_expired`` is True when deciding ended
    a previous grant without a release from its holder -- by expiry, or because the request
    comes from a new incarnation of that holder -- i.e. this peer cannot know what that
    holder did with its lease; ``ended_term`` is that grant's term (``0`` when none).
    """

    granted: bool
    reason: str
    highest: int
    remaining: float
    previous_expired: bool
    ended_term: int = 0


class PeerGrants:
    """Grant rules of one peer (see the module docstring).  Not thread-safe by itself.

    Args:
        lease_duration: seconds, on this peer's clock, a grant blocks other holders.
        clock: monotonic clock of this peer (seconds).
        startup_grace: seconds after construction during which every request is refused.
    """

    def __init__(self, lease_duration: float, clock: Callable[[], float], *, startup_grace: float):
        self._duration = lease_duration
        self._clock = clock
        self._quiet_until = clock() + startup_grace
        self._highest = 0          # highest term seen in any request or release
        self._granted_max = 0      # highest term granted or closed here
        self._granted_top = 0      # highest term actually granted here
        self._holder = None
        self._inc = 0
        self._term = 0
        self._expiry = 0.0
        self._active = False

    @property
    def highest_term(self) -> int:
        """Highest term seen in a request or release."""
        return self._highest

    @property
    def granted_top(self) -> int:
        """Highest term this peer has granted (the ballot a replicated root must not be below)."""
        return self._granted_top

    def restore(self, term: int) -> None:
        """Honour a promise logged before a restart: ``term`` (and every lower term) counts as
        granted here, so it is never granted again."""
        self._granted_max = max(self._granted_max, term)
        self._granted_top = max(self._granted_top, term)
        self._highest = max(self._highest, term)

    @property
    def starting(self) -> bool:
        """True during the startup grace."""
        return self._clock() < self._quiet_until

    @property
    def holder(self) -> Optional[str]:
        """The node holding an unexpired, unreleased grant from this peer, or ``None``."""
        self._settle(self._clock())
        return self._holder if self._active else None

    def _settle(self, now: float) -> int:
        """End an expired grant; return its term if it ended now (by expiry), else ``0``."""
        if self._active and now >= self._expiry:
            self._active = False
            return self._term
        return 0

    def on_request(self, holder: str, term: int, inc: int = 0) -> GrantDecision:
        """Decide a request of ``holder`` (incarnation ``inc``) for ``term`` (see :class:`GrantDecision`).

        A request from another incarnation of the node holding an active grant proves that
        node restarted: the old grant is void (its process is gone) and ends here, reported
        as ``previous_expired`` because its holder may have used it before restarting.
        """
        now = self._clock()
        ended = self._settle(now)
        if self._active and self._holder == holder and self._inc != inc:
            self._active = False
            ended = self._term
        try:
            if now < self._quiet_until:
                return GrantDecision(False, "starting", self._highest, self._quiet_until - now, bool(ended), ended)
            if term < self._highest:
                return GrantDecision(False, "stale_term", self._highest, 0.0, bool(ended), ended)
            if self._active:
                if self._holder == holder and self._term == term:
                    # A duplicate: grant again, but never extend the promise.
                    return GrantDecision(True, "granted", self._highest, self._expiry - now, False, 0)
                if self._holder != holder:
                    return GrantDecision(False, "busy", self._highest, self._expiry - now, bool(ended), ended)
            if term <= self._granted_max:
                return GrantDecision(False, "term_taken", self._highest, 0.0, bool(ended), ended)
            self._granted_max = term
            self._granted_top = max(self._granted_top, term)
            self._holder = holder
            self._inc = inc
            self._term = term
            self._expiry = now + self._duration
            self._active = True
            return GrantDecision(True, "granted", term, self._duration, bool(ended), ended)
        finally:
            self._highest = max(self._highest, term)

    def on_release(self, holder: str, term: int, inc: int = 0) -> int:
        """Apply ``holder``'s release (or cancel) of ``term`` from incarnation ``inc``.

        Frees the grant if it is that incarnation's grant for ``term``, and closes ``term``
        here so a delayed duplicate of the request is refused.  Returns the term of an
        active grant that ended by expiry while settling (the caller may have missed what
        its holder did), else ``0``.
        """
        expired = self._settle(self._clock())
        if self._active and self._holder == holder and self._term == term and self._inc == inc:
            self._active = False
        self._granted_max = max(self._granted_max, term)
        self._highest = max(self._highest, term)
        return expired


class LeaseAttempt:
    """Replies gathered for one lease request (see the module docstring).  Not thread-safe."""

    def __init__(self, *, term: int, rid: int, sent_at: float, cluster_size: int,
                 lease_duration: float, drift_margin: float):
        self.term = term
        self.rid = rid
        self.sent_at = sent_at
        self._size = cluster_size
        self._needed = majority(cluster_size)
        self.valid_until = sent_at + lease_duration - drift_margin
        self.granted_by = {}        # peer -> (known, root, certain, label term)
        self.rejected_by = {}       # peer -> (reason, highest, remaining)
        self.highest_seen = term

    def on_grant(self, peer: str, known: bool, root: Optional[RootRecord], certain: bool,
                 label: int = 0) -> None:
        """Record ``peer``'s grant with the root it has accepted and that root's label term
        (counted once per peer)."""
        if peer not in self.rejected_by:
            self.granted_by.setdefault(peer, (known, root, certain, label))

    def on_reject(self, peer: str, reason: str, highest: int, remaining: float) -> None:
        """Record ``peer``'s refusal."""
        self.highest_seen = max(self.highest_seen, highest)
        if peer not in self.granted_by:
            self.rejected_by.setdefault(peer, (reason, highest, remaining))

    @property
    def replies(self) -> int:
        """Peers that answered (granted or refused), the requester's own vote included."""
        return len(self.granted_by) + len(self.rejected_by)

    @property
    def needed(self) -> int:
        """Grants needed to win (a majority)."""
        return self._needed

    @property
    def state(self) -> str:
        """``"won"``, ``"lost"`` or ``"pending"``."""
        if len(self.granted_by) >= self._needed:
            return "won"
        if len(self.rejected_by) > self._size - self._needed:
            return "lost"
        return "pending"

    @property
    def busy_remaining(self) -> float:
        """Longest remaining promise among the refusals (``0.0`` if none)."""
        return max((r[2] for r in self.rejected_by.values() if r[0] in ("busy", "starting")), default=0.0)

    def best_root(self, own: Optional[tuple] = None) -> tuple:
        """``(known, root, certain, label)`` over the granters (and ``own``, the requester's
        ``(known, root, label)``, counted even when its own vote was refused).

        ``root`` is the highest root in the order of
        :func:`~proto_db.cluster.rootlog.root_key`: by the intersection of the acknowledging
        majority of every committed root with this granting majority, it is the newest
        committed root or a root built on it (D46).  ``certain`` holds when every granter
        knows a root and none reported a grant whose outcome it could not know; otherwise
        the holder runs a recovery round.  Two different roots with the same label and
        epoch raise ``ProtoCorruptionError`` (:func:`~proto_db.cluster.rootlog.compare_roots`).
        """
        candidates = [(k, r, lab) for k, r, _, lab in self.granted_by.values()]
        if own is not None:
            candidates.append(own)
        best, best_label, any_known = None, 0, False
        for known, root, label in candidates:
            if not known:
                continue
            if not any_known or compare_roots(label, root, best_label, best) > 0:
                best, best_label = root, label
            any_known = True
        certain = any_known and all(k and c for k, _, c, _ in self.granted_by.values())
        return any_known, best, certain, best_label


class Lease:
    """A won write lease (see the module docstring); returned by the coordinator.

    ``root`` is the latest root propagated by the granting majority and
    ``root_certain`` tells whether it can be trusted without reading the storage.
    The holder binds the root it actually builds on with :meth:`bind_root`.
    """

    def __init__(self, *, term: int, acquired_at: float, valid_until: float,
                 root: Optional[RootRecord], root_certain: bool, clock: Callable[[], float],
                 root_term: int = 0):
        self.term = term
        self.acquired_at = acquired_at
        self.valid_until = valid_until
        self.root = root
        self.root_certain = root_certain
        self.root_term = root_term
        self.released_at = None
        self._clock = clock
        self._released = False

    def is_valid(self) -> bool:
        """True while unreleased and before ``valid_until`` on the holder's clock."""
        return not self._released and self._clock() < self.valid_until

    def remaining(self) -> float:
        """Seconds of validity left (``0.0`` once expired or released)."""
        return 0.0 if self._released else max(0.0, self.valid_until - self._clock())

    def bind_root(self, root: Optional[RootRecord], term: Optional[int] = None) -> None:
        """Set the root the holder validates against (and its label term, if given)."""
        self.root = root
        self.root_certain = True
        if term is not None:
            self.root_term = term

    def revoke(self) -> None:
        """End the holder's belief now (the coordinator stopped): ``is_valid()`` turns False."""
        self.valid_until = min(self.valid_until, self._clock())

    @property
    def expected_epoch(self) -> int:
        """Epoch the holder's conditional root write expects."""
        return self.root.epoch if self.root is not None else 0

    @property
    def fencing_token(self) -> FencingToken:
        """The token of this holder's write: ``(term, expected_epoch)``."""
        return FencingToken(self.term, self.expected_epoch)

    def _mark_released(self) -> None:
        self._released = True
        self.released_at = self._clock()

    def __repr__(self):
        return (f"Lease(term={self.term}, epoch={self.expected_epoch}, "
                f"remaining={self.remaining():.3f}s, released={self._released})")
