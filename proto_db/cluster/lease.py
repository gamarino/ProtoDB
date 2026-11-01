"""The majority-granted write lease of the cluster (Task 8.3c, decisions D44 and D45).

Three pieces of pure state, with no I/O and no locking (the coordinator serializes
access under its own lock):

:class:`PeerGrants`
    What one peer has promised.  It grants the lease to at most one holder per term and,
    once it has granted, to no other holder until the lease expires on its own clock or
    the holder releases it.  It persists nothing: after a (re)start it refuses every grant
    for ``startup_grace`` seconds, so a promise made before a crash is honoured.
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
never overlap in real time.  The storage's conditional root write remains the fencing net
for whatever this argument does not cover (a holder paused between its validity check and
its write).
"""
from typing import Callable, NamedTuple, Optional

from ..storage.format import RootRecord

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
    promise or grace that blocks a grant.  ``previous_expired`` is True when the grant ended
    the previous holder's lease by expiry rather than by release, i.e. this peer may have
    missed the root that holder published.
    """

    granted: bool
    reason: str
    highest: int
    remaining: float
    previous_expired: bool


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
        self._holder = None
        self._term = 0
        self._expiry = 0.0
        self._active = False

    @property
    def highest_term(self) -> int:
        """Highest term seen in a request or release."""
        return self._highest

    @property
    def holder(self) -> Optional[str]:
        """The node holding an unexpired, unreleased grant from this peer, or ``None``."""
        self._settle(self._clock())
        return self._holder if self._active else None

    def _settle(self, now: float) -> bool:
        """End an expired grant; True if it ended now (by expiry)."""
        if self._active and now >= self._expiry:
            self._active = False
            return True
        return False

    def on_request(self, holder: str, term: int) -> GrantDecision:
        """Decide a request of ``holder`` for ``term`` (see :class:`GrantDecision`)."""
        now = self._clock()
        expired = self._settle(now)
        try:
            if now < self._quiet_until:
                return GrantDecision(False, "starting", self._highest, self._quiet_until - now, expired)
            if term < self._highest:
                return GrantDecision(False, "stale_term", self._highest, 0.0, expired)
            if self._active:
                if self._holder == holder and self._term == term:
                    # A duplicate: grant again, but never extend the promise.
                    return GrantDecision(True, "granted", self._highest, self._expiry - now, False)
                if self._holder != holder:
                    return GrantDecision(False, "busy", self._highest, self._expiry - now, expired)
            if term <= self._granted_max:
                return GrantDecision(False, "term_taken", self._highest, 0.0, expired)
            self._granted_max = term
            self._holder = holder
            self._term = term
            self._expiry = now + self._duration
            self._active = True
            return GrantDecision(True, "granted", term, self._duration, expired)
        finally:
            self._highest = max(self._highest, term)

    def on_release(self, holder: str, term: int) -> bool:
        """Apply ``holder``'s release (or cancel) of ``term``.

        Frees the grant if it is ``holder``'s for ``term``, and closes ``term`` here so a
        delayed duplicate of the request is refused.  Returns True if an active grant
        ended by expiry while settling (the caller may have missed a root).
        """
        expired = self._settle(self._clock())
        if self._active and self._holder == holder and self._term == term:
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
        self.granted_by = {}        # peer -> (known, root, certain)
        self.rejected_by = {}       # peer -> (reason, highest, remaining)
        self.highest_seen = term

    def on_grant(self, peer: str, known: bool, root: Optional[RootRecord], certain: bool) -> None:
        """Record ``peer``'s grant with the root it knows (counted once per peer)."""
        if peer not in self.rejected_by:
            self.granted_by.setdefault(peer, (known, root, certain))

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

    def best_root(self) -> tuple:
        """``(known, root, certain)`` over the granters.

        ``root`` is the granted root of highest epoch.  ``certain`` holds only when every
        granter knows a root, none of them may have missed one, and they all agree on the
        epoch; otherwise the holder reads the storage (D45).
        """
        best = None
        best_epoch = -1
        epochs = set()
        certain = True
        any_known = False
        for known, root, peer_certain in self.granted_by.values():
            if not known:
                certain = False
                continue
            any_known = True
            epoch = root.epoch if root is not None else 0
            epochs.add(epoch)
            certain = certain and peer_certain
            if epoch > best_epoch:
                best, best_epoch = root, epoch
        certain = certain and any_known and len(epochs) == 1
        return any_known, best, certain


class Lease:
    """A won write lease (see the module docstring); returned by the coordinator.

    ``root`` is the latest root propagated by the granting majority and
    ``root_certain`` tells whether it can be trusted without reading the storage.
    The holder binds the root it actually builds on with :meth:`bind_root`.
    """

    def __init__(self, *, term: int, acquired_at: float, valid_until: float,
                 root: Optional[RootRecord], root_certain: bool, clock: Callable[[], float]):
        self.term = term
        self.acquired_at = acquired_at
        self.valid_until = valid_until
        self.root = root
        self.root_certain = root_certain
        self.released_at = None
        self._clock = clock
        self._released = False
        self.published = None

    def is_valid(self) -> bool:
        """True while unreleased and before ``valid_until`` on the holder's clock."""
        return not self._released and self._clock() < self.valid_until

    def remaining(self) -> float:
        """Seconds of validity left (``0.0`` once expired or released)."""
        return 0.0 if self._released else max(0.0, self.valid_until - self._clock())

    def bind_root(self, root: Optional[RootRecord]) -> None:
        """Set the root the holder validates and publishes against."""
        self.root = root
        self.root_certain = True

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
