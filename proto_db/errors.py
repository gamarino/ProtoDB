"""The proto_db 2.0 exception hierarchy (decision D12).

All proto_db errors derive from :class:`ProtoDBError` (the class keeps its name: it is API). Every class carries a numeric
``code``; an explicitly given code always wins, including ``0``.

:func:`secondary_errors` returns the exceptions that were raised concurrently with
an exception re-raised by the parallel scan or the FSM (decisions D40, D41).
"""

VALIDATION_ERROR = 10_000
CONSTRAINT_ERROR = 10_100
USER_ERROR = 20_000
CORRUPTION_ERROR = 30_000
MISFILED_ERROR = 30_100
NOT_SUPPORTED_ERROR = 40_000
NOT_AUTHORIZED_ERROR = 50_000
UNEXPECTED_ERROR = 60_000
OUTCOME_UNKNOWN_ERROR = 60_100
BACKLOG_FULL_ERROR = 60_200
WRITE_REFUSED_ERROR = 60_300
LOCKING_ERROR = 70_000
CONFLICT_ERROR = 70_100
TIMEOUT_ERROR = 70_200
BASE_ERROR = 1


class ProtoDBError(Exception):
    """Base class of every error raised by proto_db."""

    default_code = BASE_ERROR

    def __init__(self, message: str = "", *, code: int | None = None):
        super().__init__(message)
        self.message = message
        self.code = self.default_code if code is None else code


class ProtoValidationError(ProtoDBError):
    """Invalid argument, schema or data shape."""
    default_code = VALIDATION_ERROR


class ProtoConstraintError(ProtoValidationError):
    """A declared rule of the semantic catalog does not hold (spec section 10, R273): an enforced invariant, a
    role's cardinality or an intent's ``requires``.  Being a ``ProtoValidationError``, it is never retried by
    ``Database.run``; nothing of the commit or the operation it refuses is published.

    ``constraint`` is the rule's name (an invariant's catalogued name, R279; ``<type>.<role>.cardinality``;
    ``<type>.<intent>.requires``), ``meaning`` its catalogued meaning, ``entity`` the cell name of the entity it
    was checked on, and ``values`` the operands it read, by attribute path (``MISSING`` for an absent one).
    The message is the rule's ``message`` rendered with them.  :meth:`describe` gives all of it as neutral
    data."""

    default_code = CONSTRAINT_ERROR

    def __init__(self, message: str = "", *, constraint: str | None = None, meaning: str | None = None,
                 entity: str | None = None, values=None, code: int | None = None):
        super().__init__(message, code=code)
        self.constraint = constraint
        self.meaning = meaning
        self.entity = entity
        self.values = dict(values or {})

    def describe(self) -> dict:
        """The error as neutral data: ``{"error", "code", "constraint", "meaning", "message", "entity",
        "values", "missing"}``.  An absent operand is ``None`` in ``values`` and its name is in ``missing``."""
        from .values import MISSING
        values = {name: None if value is MISSING else value for name, value in self.values.items()}
        missing = [name for name, value in self.values.items() if value is MISSING]
        return {"error": type(self).__name__, "code": self.code, "constraint": self.constraint,
                "meaning": self.meaning, "message": self.message, "entity": self.entity, "values": values,
                "missing": missing}


class ProtoAttributeNameError(ProtoValidationError, KeyError):
    """``o[name]`` on a :class:`~proto_db.core.dbobject.DBObject` with a name that is not an
    attribute name: an API name (``oid``, ``context``, ``parents``, ``with_``, ...) or one starting
    with ``_`` (runtime registry, ruling R238).  It is also a ``KeyError``, so code that tries the
    mapping protocol first and falls back to attribute access on ``KeyError`` (a Django template's
    ``{{ obj.oid }}``) reads the attribute.  Its message is the plain message, not
    ``KeyError``'s quoted form."""

    __str__ = Exception.__str__


class ProtoReferenceError(ProtoValidationError):
    """A snapshot reference or pager token that is malformed or names another database (D56, R99).

    A misuse check, not a security boundary: references and tokens carry no MAC."""


class ProtoStalePagerError(ProtoReferenceError):
    """A pager token whose query no longer plans the same way (another planner or library
    version, another extractor function): resuming it could page another order (D56, R99)."""


class ProtoClosedError(ProtoValidationError):
    """An operation on a closed ``ObjectSpace`` or storage: in particular reading an object of a
    closed space whose state was not loaded.  Objects outlive their space only as values already
    materialized; copy data out before closing to keep it (Task 8.3m, ruling R104)."""


class ProtoUserError(ProtoDBError):
    """Error caused by incorrect use of the API."""
    default_code = USER_ERROR


class ProtoCorruptionError(ProtoDBError):
    """Stored data failed an integrity check."""
    default_code = CORRUPTION_ERROR


class ProtoMisfiledError(ProtoCorruptionError):
    """A collection or index holds an entry where its own key does not place it.

    The known cause is format-3 data written through a built-in subclass's own conversion (a
    ``(str, Enum)`` member written as ``"Color.RED"`` but filed as ``"red"``; decision D58): the
    entry, and in a sorted dictionary its neighbours, cannot be found.  ``collection.rebuilt()``
    repairs it.
    """
    default_code = MISFILED_ERROR


class ProtoNotSupportedError(ProtoDBError):
    """The requested operation is not supported."""
    default_code = NOT_SUPPORTED_ERROR


class ProtoNotAuthorizedError(ProtoDBError):
    """The caller is not authorized to perform the operation."""
    default_code = NOT_AUTHORIZED_ERROR


class ProtoUnexpectedError(ProtoDBError):
    """Internal error that should not happen."""
    default_code = UNEXPECTED_ERROR


class ProtoOutcomeUnknownError(ProtoUnexpectedError):
    """The outcome of a write (typically a root publish) is unknown: it may or may not have been applied.

    Never a :class:`ProtoConflictError`: retrying the operation could apply it twice, so
    ``Database.run`` does not retry it.  Reading the current state tells what happened.
    """
    default_code = OUTCOME_UNKNOWN_ERROR


class ProtoBacklogFullError(ProtoUnexpectedError):
    """A cluster node's local backlog of data not yet in the object store reached its limit (D46).

    Raised by a commit before it asks for the cluster lease, so nothing is written; commits
    succeed again once the object store is reachable and the replicator has caught up.
    """
    default_code = BACKLOG_FULL_ERROR


class ProtoWriteRefusedError(ProtoUnexpectedError):
    """A storage tier refused one write without failing: nothing of it was written or logged.

    The tier stays healthy and a later attempt may succeed (for instance once the object store
    is reachable again).  A cluster node refuses the acceptance or the acknowledgement that
    write was for, and goes on serving; it does not stop acting as an acceptor.
    """
    default_code = WRITE_REFUSED_ERROR


class ProtoLockingError(ProtoDBError):
    """Failure related to locking or concurrent access."""
    default_code = LOCKING_ERROR


class ProtoConflictError(ProtoLockingError):
    """A transaction conflicted with a concurrent commit; retrying is appropriate.

    ``unit`` is the :class:`~proto_db.preconditions.Unit` whose precondition failed (``None``
    when the conflict is not about one unit) and ``database`` the name of the database it
    belongs to (R36).  Both are instance attributes, so a copy of the error keeps them.
    """
    default_code = CONFLICT_ERROR

    def __init__(self, message: str = "", *, code: int | None = None, unit=None, database: str | None = None):
        super().__init__(message, code=code)
        self.unit = unit
        self.database = database


class ProtoTimeoutError(ProtoLockingError):
    """A lock or wait timed out."""
    default_code = TIMEOUT_ERROR


SECONDARY_ERRORS_ATTRIBUTE = "proto_secondary_errors"


def attach_secondary_errors(primary: BaseException, others) -> None:
    """Record ``others`` (exceptions raised alongside ``primary``) on ``primary`` (decisions D40, D41).

    Used when several concurrent failures are reported by re-raising the first one:
    the others stay available as objects through :func:`secondary_errors`.
    """
    previous = tuple(getattr(primary, SECONDARY_ERRORS_ATTRIBUTE, ()))
    setattr(primary, SECONDARY_ERRORS_ATTRIBUTE, previous + tuple(others))


def secondary_errors(exc: BaseException) -> tuple:
    """The exceptions raised concurrently with ``exc`` and reported through it (empty tuple if none)."""
    return tuple(getattr(exc, SECONDARY_ERRORS_ATTRIBUTE, ()))


# Backward-compatible 0.1 names (D12): deprecated in 2.0 and removed in 2.1.
ProtoBaseException = ProtoDBError
ProtoValidationException = ProtoValidationError
ProtoUserException = ProtoUserError
ProtoCorruptionException = ProtoCorruptionError
ProtoNotSupportedException = ProtoNotSupportedError
ProtoNotAuthorizedException = ProtoNotAuthorizedError
ProtoUnexpectedException = ProtoUnexpectedError
ProtoLockingException = ProtoLockingError
