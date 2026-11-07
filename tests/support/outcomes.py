"""Per-commit tokens that resolve every commit outcome of a cluster test (ruling R114, N1).

A counter alone cannot tell an acknowledged commit that was lost from an unknown commit that
took effect: one of each leaves the count unchanged.  So every commit a test makes also writes
a token unique to it, and once the members converge the test checks, token by token:

- every acknowledged token is present (no acknowledged commit lost);
- every unknown token is present on all members or absent on all of them (the unknown outcome
  resolved: P5 / R86 allow either, never a split);
- no other token is present (nothing applied that no commit wrote).

Typical use::

    tokens = CommitTokens()
    tok = tokens.new("n0")
    try:
        db.run(lambda tr: (incr(tr), write_token(tr, tok)))
        tokens.acked(tok)
    except ProtoOutcomeUnknownError:
        tokens.unknown(tok)
    ...
    present = assert_outcomes_resolved({n: lambda n=n: read_tokens_of(tc.db(n)) for n in ids}, tokens)
"""
import itertools
import threading
import time

#: The root holding the tokens (a ``Dictionary`` mapping each token to ``True``).
TOKENS_ROOT = "commit-tokens"


class CommitTokens:
    """The tokens of a test's commits and their outcomes (thread-safe)."""

    def __init__(self):
        self._lock = threading.Lock()
        self._counter = itertools.count()
        self.issued = set()
        self.acked_tokens = set()
        self.unknown_tokens = set()

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


def read_tokens_of(db, root: str = TOKENS_ROOT) -> frozenset:
    """The tokens a new transaction on ``db`` sees."""
    tr = db.new_transaction()
    try:
        return read_tokens(tr, root)
    finally:
        tr.abort()


def assert_outcomes_resolved(readers: dict, tokens: CommitTokens, timeout: float = 10.0) -> frozenset:
    """Wait until every member (``readers``: member -> callable returning its token set) sees the
    same tokens, including every acknowledged one, then assert the three rules of the module
    docstring.  Return the tokens present."""
    deadline = time.monotonic() + timeout
    while True:
        seen = {member: read() for member, read in readers.items()}
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
    return present
