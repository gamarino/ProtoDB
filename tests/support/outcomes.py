"""Per-commit tokens that resolve every commit outcome of a cluster test (rulings R114 N1, R113;
Task 8.3n: re-review 2 of 8.3m, P1, P3, P4).

A counter alone cannot tell an acknowledged commit that was lost from an unknown commit that
took effect: one of each leaves the count unchanged.  So every commit a test makes also writes
a token unique to it, and once the members converge the test checks, token by token:

- every acknowledged token is present (no acknowledged commit lost);
- every unknown token is present on all members or absent on all of them (the unknown outcome
  resolved: P5 / R86 allow either, never a split);
- no other token is present (nothing applied that no commit wrote);
- every token any member was ever seen to show is still present (history never loses a commit
  once a member has observed it, P1: an unknown commit that became visible and then vanished
  is a forked history, not a resolution);
- at least ``min_acked`` tokens are acknowledged (P3: an empty run proves nothing).

A reader returns a :class:`Snapshot`: the tokens and, from the SAME transaction, whatever else
the test compares with them (P4: a counter read in another transaction can see an unknown
commit that landed in between).  :func:`snapshot_of` builds one.  A workload of blind writes
records its tokens with :func:`add_blind_token` instead (a ``CountedSet`` merged blind, which
also catches a commit applied twice) and reads them with :func:`blind_snapshot_of`.

Observations (P1) come from three places: :meth:`CommitTokens.observe`, which a test calls
with any token set it read during the run; :func:`observe_members`, one read of every member;
and every read :func:`assert_outcomes_resolved` itself makes while it waits.

Typical use::

    tokens = CommitTokens()
    tok = tokens.new("n0")
    try:
        db.run(lambda tr: (incr(tr), write_token(tr, tok)))
        tokens.acked(tok)
    except ProtoOutcomeUnknownError:
        tokens.unknown(tok)
    observe_members(readers, tokens)          # e.g. after every round
    ...
    readers = {n: (lambda n=n: snapshot_of(tc.db(n), counter)) for n in ids}
    res = assert_outcomes_resolved(readers, tokens)
    assert all(v == len(res.present) for v in res.values.values())
"""
import itertools
import threading
import time
from typing import NamedTuple

#: The root holding the tokens (a ``Dictionary`` mapping each token to ``True``).
TOKENS_ROOT = "commit-tokens"


class Snapshot(NamedTuple):
    """What one transaction on one member saw: its tokens and the test's ``value``."""
    tokens: frozenset
    value: object = None


class Resolution(NamedTuple):
    """The converged outcome: the tokens present, and each member's ``value`` read in the same
    transaction as the tokens it showed."""
    present: frozenset
    values: dict


class CommitTokens:
    """The tokens of a test's commits, their outcomes and what members were seen to show
    (thread-safe)."""

    def __init__(self):
        self._lock = threading.Lock()
        self._counter = itertools.count()
        self.issued = set()
        self.acked_tokens = set()
        self.unknown_tokens = set()
        self.observed = set()

    def new(self, label: str = "c") -> str:
        """A token no other commit of this test carries."""
        with self._lock:
            token = f"{label}-{next(self._counter)}"
            self.issued.add(token)
            return token

    def acked(self, token: str) -> None:
        with self._lock:
            self.acked_tokens.add(token)

    def unknown(self, token: str) -> None:
        with self._lock:
            self.unknown_tokens.add(token)

    def observe(self, view) -> None:
        """Record that some member showed the tokens ``view`` (a set, or a :class:`Snapshot`)."""
        view = view.tokens if isinstance(view, Snapshot) else view
        with self._lock:
            self.observed |= view


def write_token(tr, token: str, root: str = TOKENS_ROOT) -> None:
    """Record ``token`` in the transaction ``tr`` (call it inside the commit's function)."""
    tokens = tr.get_root_object(root)
    if tokens is None:
        tokens = tr.new_dictionary()
    tr.set_root_object(root, tokens.set_at(token, True))


def read_tokens(tr, root: str = TOKENS_ROOT) -> frozenset:
    """The tokens visible to the transaction ``tr``."""
    tokens = tr.get_root_object(root)
    return frozenset() if tokens is None else frozenset(tokens.keys())


def snapshot_of(db, value=None, root: str = TOKENS_ROOT) -> Snapshot:
    """The tokens a new transaction on ``db`` sees and ``value(tr)`` from that same transaction
    (P4)."""
    tr = db.new_transaction()
    try:
        return Snapshot(read_tokens(tr, root), value(tr) if value is not None else None)
    finally:
        tr.abort()


def read_tokens_of(db, root: str = TOKENS_ROOT) -> frozenset:
    """The tokens a new transaction on ``db`` sees."""
    return snapshot_of(db, root=root).tokens


#: The root of :func:`add_blind_token`: a ``CountedSet`` written blind (merged, never read).
BLIND_ROOT = "commit-token-set"


def add_blind_token(tr, token, root: str = BLIND_ROOT) -> None:
    """Record ``token`` without reading anything: a blind add to a ``CountedSet`` root, merged
    with concurrent adds (D33 intent merge), so a workload of blind writes keeps its contention
    profile (a read of the token root would make every commit conflict with every other)."""
    tr.set_root_object(root, tr.new_counted_set().add(token))


def read_blind_tokens(tr, root: str = BLIND_ROOT) -> frozenset:
    """The tokens of :func:`add_blind_token` visible to ``tr``; a token added twice (a commit
    applied twice) fails here."""
    counted = tr.get_root_object(root)
    counts = {} if counted is None else dict(counted.items())
    doubled = sorted(t for t, c in counts.items() if c != 1)
    assert not doubled, f"commits applied more than once: {doubled[:10]}"
    return frozenset(counts)


def blind_snapshot_of(db, value=None, root: str = BLIND_ROOT) -> Snapshot:
    """:func:`snapshot_of` for the tokens of :func:`add_blind_token`."""
    tr = db.new_transaction()
    try:
        return Snapshot(read_blind_tokens(tr, root), value(tr) if value is not None else None)
    finally:
        tr.abort()


#: The root of :func:`commit_once`'s step tokens.
STEP_ROOT = "commit-once-tokens"
_STEPS = itertools.count()


def commit_once(db, fn, *, attempts: int = 10, root: str = STEP_ROOT, **run_kw) -> int:
    """Commit ``fn`` on ``db`` so that it takes effect exactly once, whatever outcomes the
    attempts report; return the number of unknown outcomes met (review I6, Task 8.3n fix
    round 1).  For sequential setup steps: a timing-bound unknown outcome (a lease that ended
    under load, R82) becomes another attempt instead of a test failure.

    Every attempt writes a fresh token with ``fn``'s effect, and first reads the tokens of the
    earlier attempts in its snapshot, a committed root.  If one is there, that attempt took
    effect: the step is done and this attempt writes nothing.  If none is, this attempt applies
    ``fn``; when it commits, its base -- a committed root without the earlier tokens -- shows
    that no earlier attempt can take effect any more (one linear history).  A snapshot older
    than an earlier attempt that did commit conflicts on the token root and is retried by
    ``Database.run`` from the newer root.  A timeout (no permission obtained, nothing applied)
    is retried the same way."""
    from proto_db.errors import ProtoOutcomeUnknownError, ProtoTimeoutError
    tried, unknown = [], 0
    for _ in range(attempts):
        token = f"step-{next(_STEPS)}"

        def step(tr, token=token, earlier=tuple(tried)):
            seen = read_tokens(tr, root)
            if seen & set(earlier):
                return                      # an earlier attempt took effect: write nothing
            fn(tr)
            write_token(tr, token, root)
        tried.append(token)
        try:
            db.run(step, **run_kw)
            return unknown
        except ProtoOutcomeUnknownError:
            unknown += 1
        except ProtoTimeoutError:
            pass
    raise AssertionError(f"no attempt of the step was acknowledged in {attempts} attempts ({unknown} unknown)")


def _snapshot(read) -> Snapshot:
    view = read()
    return view if isinstance(view, Snapshot) else Snapshot(frozenset(view))


def observe_members(readers: dict, tokens: CommitTokens) -> None:
    """Read every member once and record what each shows (P1)."""
    for read in readers.values():
        tokens.observe(_snapshot(read))


class observing:
    """Context manager: while the body runs, a thread reads every member every ``interval``
    seconds and records what it shows (P1 during the run, review M1).  Reads take snapshots and
    write nothing, so they add no conflicts.  An error raised by a read (a token applied twice,
    say) is raised again when the body exits."""

    def __init__(self, readers: dict, tokens: CommitTokens, interval: float = 0.005):
        self.readers, self.tokens, self.interval = readers, tokens, interval
        self.reads = 0
        self._stop = threading.Event()
        self._errors = []
        self._thread = threading.Thread(target=self._run, name="outcome-observer", daemon=True)

    def _run(self):
        while not self._stop.is_set():
            try:
                observe_members(self.readers, self.tokens)
                self.reads += 1
            except BaseException as e:      # raised again on exit
                self._errors.append(e)
                return
            self._stop.wait(self.interval)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join(30)
        if self._errors and exc[0] is None:
            raise self._errors[0]
        return False


def assert_outcomes_resolved(readers: dict, tokens: CommitTokens, timeout: float = 10.0, *,
                             min_acked: int = 1) -> Resolution:
    """Wait until every member (``readers``: member -> callable returning its :class:`Snapshot`
    or token set) sees the same tokens, including every acknowledged one, then assert the rules
    of the module docstring.  Return the tokens present and each member's value."""
    assert len(tokens.acked_tokens) >= min_acked, \
        f"only {len(tokens.acked_tokens)} acknowledged commits (at least {min_acked} needed): the run proves nothing"
    assert readers, "no member to read"
    deadline = time.monotonic() + timeout
    while True:
        snaps = {member: _snapshot(read) for member, read in readers.items()}
        seen = {member: s.tokens for member, s in snaps.items()}
        for view in seen.values():
            tokens.observe(view)
        views = set(seen.values())
        converged = len(views) == 1 and tokens.acked_tokens <= next(iter(views))
        if converged or time.monotonic() > deadline:
            break
        time.sleep(0.02)
    lost = {m: sorted(tokens.acked_tokens - s) for m, s in seen.items() if tokens.acked_tokens - s}
    assert not lost, f"acknowledged commits lost: {lost}"
    split = {t: sorted(m for m, s in seen.items() if t in s)
             for t in tokens.unknown_tokens if 0 < sum(t in s for s in seen.values()) < len(seen)}
    assert not split, f"unknown outcomes not resolved alike on every member: {split}"
    present = next(iter(seen.values()))
    assert len(views) == 1, f"members did not converge: {seen}"
    phantom = sorted(present - tokens.acked_tokens - tokens.unknown_tokens)
    assert not phantom, f"tokens no acknowledged or unknown commit wrote: {phantom}"
    vanished = sorted(tokens.observed - present)
    assert not vanished, f"commits a member showed and history later lost: {vanished}"
    return Resolution(present, {member: s.value for member, s in snaps.items()})
