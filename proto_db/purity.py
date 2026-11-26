"""Extractor purity (spec R27, N4): a key extractor is a pure function of the value it is given.

While an extractor runs (:func:`extracting`), every public read of a transaction -- roots, mutables,
atom loads, the :class:`~proto_db.values.MutableObject` handle -- raises ``ProtoUserError``
(:func:`check_not_extracting`).  This is independent of ``engine_reads`` (which only suppresses
recording): the context variable here is set by ``KeySpec.extract`` around each extractor call.
The check is a single ``ContextVar.get`` because it sits on hot read paths.

No user code with the space root held (P-C): the commit marks the threads that hold the space root
(:func:`holding_root`, set by :mod:`proto_db.transactions` around the lock region of a round and
around every build), and every entry point of user code -- an index extractor
(``KeySpec.extract``), a Python computed attribute (``Computed.value``), an intent -- calls
:func:`check_not_holding_root` first, so a regression that reaches one under the root fails loudly
(``ProtoUnexpectedError``) instead of running it.

A leaf module (it imports only :mod:`proto_db.errors`), so :mod:`proto_db.values` and
:mod:`proto_db.transactions` import it at top level; :mod:`proto_db.indexing.purity`, its
former home, re-exports it.
"""
from __future__ import annotations

import threading
from contextvars import ContextVar

from .errors import ProtoUnexpectedError, ProtoUserError

__all__ = ["extracting", "check_not_extracting", "holding_root", "check_not_holding_root", "HOLDING_ROOT"]

_EXTRACTING: ContextVar[bool] = ContextVar("proto_db_extracting", default=False)


class _Extracting:
    """Re-entrant context manager marking the current context as running an extractor."""

    __slots__ = ("_tokens",)

    def __init__(self):
        self._tokens = []

    def __enter__(self):
        self._tokens.append(_EXTRACTING.set(True))
        return self

    def __exit__(self, *exc):
        _EXTRACTING.reset(self._tokens.pop())
        return False


def extracting() -> _Extracting:
    """A context manager under which :func:`check_not_extracting` raises (a new instance per use,
    so concurrent threads never share its token stack)."""
    return _Extracting()


def check_not_extracting(what: str) -> None:
    """Raise ``ProtoUserError`` naming ``what`` if an extractor is running in this context."""
    if _EXTRACTING.get():
        raise ProtoUserError(
            f"{what} was called from inside a key extractor: extractors must be pure functions of "
            f"the value they are given and may not read through a transaction (R27)")


# -- the space root (P-C) ------------------------------------------------------------------------

#: Per thread: how many times this thread holds the space root (the root lock, or a cluster lease
#: or permit) -- set around the lock region of a standalone round and around every build, which
#: runs only while the space root is held.
_root_held = threading.local()


class _HoldingRoot:
    """Context manager marking this thread as holding the space root (re-entrant)."""

    __slots__ = ()

    def __enter__(self):
        _root_held.depth = getattr(_root_held, "depth", 0) + 1
        return self

    def __exit__(self, *exc):
        _root_held.depth -= 1
        return False


#: The one instance of :class:`_HoldingRoot` (the depth is per thread).
HOLDING_ROOT = _HoldingRoot()


def holding_root() -> bool:
    """Whether the current thread holds the space root."""
    return getattr(_root_held, "depth", 0) > 0


def check_not_holding_root(what: str) -> None:
    """Raise ``ProtoUnexpectedError`` naming ``what`` (an entry point of user code) when the current
    thread holds the space root: an engine defect, since nothing but engine code runs there (P-C)."""
    if getattr(_root_held, "depth", 0) > 0:
        raise ProtoUnexpectedError(
            f"{what} was reached with the space root held: no user code runs there (P-C); this is a "
            f"defect of proto_db, please report it")
