"""Extractor purity (spec R27, N4): a key extractor is a pure function of the value it is given.

While an extractor runs (:func:`extracting`), every public read of a transaction -- roots, mutables,
atom loads, the :class:`~proto_db.values.MutableObject` handle -- raises ``ProtoUserError``
(:func:`check_not_extracting`).  This is independent of ``engine_reads`` (which only suppresses
recording): the context variable here is set by ``KeySpec.extract`` around each extractor call.
The check is a single ``ContextVar.get`` because it sits on hot read paths.

A leaf module (it imports only :mod:`proto_db.errors`), so :mod:`proto_db.values` and
:mod:`proto_db.transactions` import it at top level; :mod:`proto_db.indexing.purity`, its
former home, re-exports it.
"""
from __future__ import annotations

from contextvars import ContextVar

from .errors import ProtoUserError

__all__ = ["extracting", "check_not_extracting"]

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
