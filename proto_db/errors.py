"""ProtoDB 2.0 exception hierarchy (decision D12).

All ProtoDB errors derive from :class:`ProtoDBError`. Every class carries a numeric
``code``; an explicitly given code always wins, including ``0``.
"""

VALIDATION_ERROR = 10_000
USER_ERROR = 20_000
CORRUPTION_ERROR = 30_000
NOT_SUPPORTED_ERROR = 40_000
NOT_AUTHORIZED_ERROR = 50_000
UNEXPECTED_ERROR = 60_000
LOCKING_ERROR = 70_000
CONFLICT_ERROR = 70_100
TIMEOUT_ERROR = 70_200
BASE_ERROR = 1


class ProtoDBError(Exception):
    """Base class of every error raised by ProtoDB."""

    default_code = BASE_ERROR

    def __init__(self, message: str = "", *, code: int | None = None):
        super().__init__(message)
        self.message = message
        self.code = self.default_code if code is None else code


class ProtoValidationError(ProtoDBError):
    """Invalid argument, schema or data shape."""
    default_code = VALIDATION_ERROR


class ProtoUserError(ProtoDBError):
    """Error caused by incorrect use of the API."""
    default_code = USER_ERROR


class ProtoCorruptionError(ProtoDBError):
    """Stored data failed an integrity check."""
    default_code = CORRUPTION_ERROR


class ProtoNotSupportedError(ProtoDBError):
    """The requested operation is not supported."""
    default_code = NOT_SUPPORTED_ERROR


class ProtoNotAuthorizedError(ProtoDBError):
    """The caller is not authorized to perform the operation."""
    default_code = NOT_AUTHORIZED_ERROR


class ProtoUnexpectedError(ProtoDBError):
    """Internal error that should not happen."""
    default_code = UNEXPECTED_ERROR


class ProtoLockingError(ProtoDBError):
    """Failure related to locking or concurrent access."""
    default_code = LOCKING_ERROR


class ProtoConflictError(ProtoLockingError):
    """A transaction conflicted with a concurrent commit; retrying is appropriate."""
    default_code = CONFLICT_ERROR


class ProtoTimeoutError(ProtoLockingError):
    """A lock or wait timed out."""
    default_code = TIMEOUT_ERROR


# Backward-compatible names (kept for one release, D12).
ProtoBaseException = ProtoDBError
ProtoValidationException = ProtoValidationError
ProtoUserException = ProtoUserError
ProtoCorruptionException = ProtoCorruptionError
ProtoNotSupportedException = ProtoNotSupportedError
ProtoNotAuthorizedException = ProtoNotAuthorizedError
ProtoUnexpectedException = ProtoUnexpectedError
ProtoLockingException = ProtoLockingError
