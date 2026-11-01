"""Catches: wrong default error codes, explicit codes being overridden (notably 0),
alias drift from the canonical names, and lost exception chaining."""
import pytest

from proto_db import errors as e

DEFAULTS = [
    (e.ProtoValidationError, 10_000),
    (e.ProtoUserError, 20_000),
    (e.ProtoCorruptionError, 30_000),
    (e.ProtoNotSupportedError, 40_000),
    (e.ProtoNotAuthorizedError, 50_000),
    (e.ProtoUnexpectedError, 60_000),
    (e.ProtoOutcomeUnknownError, 60_100),
    (e.ProtoLockingError, 70_000),
    (e.ProtoConflictError, 70_100),
    (e.ProtoTimeoutError, 70_200),
]


@pytest.mark.parametrize("cls,code", DEFAULTS)
def test_default_codes(cls, code):
    err = cls("boom")
    assert err.code == code
    assert issubclass(cls, e.ProtoDBError)
    assert "boom" in str(err)


@pytest.mark.parametrize("cls,_", DEFAULTS)
def test_explicit_code_wins_including_zero(cls, _):
    assert cls("x", code=0).code == 0
    assert cls("x", code=7).code == 7


def test_outcome_unknown_is_unexpected_and_never_a_conflict():
    assert issubclass(e.ProtoOutcomeUnknownError, e.ProtoUnexpectedError)
    assert not issubclass(e.ProtoOutcomeUnknownError, e.ProtoLockingError)


def test_locking_hierarchy():
    assert issubclass(e.ProtoConflictError, e.ProtoLockingError)
    assert issubclass(e.ProtoTimeoutError, e.ProtoLockingError)
    assert issubclass(e.ProtoDBError, Exception)


def test_aliases_are_same_classes():
    assert e.ProtoBaseException is e.ProtoDBError
    assert e.ProtoLockingException is e.ProtoLockingError
    assert e.ProtoValidationException is e.ProtoValidationError
    assert e.ProtoUserException is e.ProtoUserError
    assert e.ProtoCorruptionException is e.ProtoCorruptionError
    assert e.ProtoNotSupportedException is e.ProtoNotSupportedError
    assert e.ProtoNotAuthorizedException is e.ProtoNotAuthorizedError
    assert e.ProtoUnexpectedException is e.ProtoUnexpectedError


def test_cause_is_preserved():
    original = ValueError("low level")
    try:
        try:
            raise original
        except ValueError as exc:
            raise e.ProtoCorruptionError("bad") from exc
    except e.ProtoDBError as caught:
        assert caught.__cause__ is original


def test_no_args_constructible():
    assert e.ProtoDBError().code == e.ProtoDBError.default_code
