"""Stable pagination over immutable snapshots (Task 8.4b, D56; rulings R77, R77a).

A :class:`Pager` holds one plan built on one snapshot.  Every page comes from the same
immutable versions, so a pager never shows a row twice and never skips one, whatever is
committed meanwhile (P1), and it needs no pinning: every snapshot stays readable (R77a).

Page k costs O(log n + page size) when the plan offers rank access (an index lookup,
range or ordered scan, a List or Dictionary scan, D56).  Any other plan is materialized
once -- through the space's query result cache when the pager has one -- and then served
by slicing.  The pager keeps its own reference to that result, so an eviction from the
cache never makes it compute again.

A *portable* pager (made by ``Transaction.pager``, ``Snapshot.pager`` or
``Database.resume_pager``) also has a token: msgpack naming the database, the
``DatabaseRoot`` of the snapshot, the root holding the collection, the predicate (in the
0.1 list syntax, operands in the codec's wire form), the order, the page size and
:data:`~proto_db.query.cache.QUERY_SEMANTICS_VERSION`.  Any process may resume it with
``Database.resume_pager(token)``: it plans the query again on that snapshot (planning is
deterministic) and serves the same pages.  A token written by another semantics version
is refused rather than risk another tie order.
"""
from __future__ import annotations

import threading
import uuid

import msgpack

from ..core import codec
from ..core.atom import Atom, AtomPointer
from ..errors import ProtoUserError, ProtoValidationError
from .cache import QUERY_SEMANTICS_VERSION, QueryCache, QueryResult, as_plan
from .expressions import Expression

__all__ = ["Pager", "encode_ref", "decode_ref", "REF_VERSION"]

#: Version of the reference/token wire form.
REF_VERSION = 1


# -- references and tokens --------------------------------------------------------------------

def _saved_only(atom: Atom) -> AtomPointer:
    ptr = atom.atom_pointer
    if ptr is None:
        raise ProtoUserError(f"A pager token cannot name {atom!r}: it is not saved")
    return ptr


def _no_load(ptr, type_name):  # pragma: no cover - replaced by the caller's loader
    raise ProtoValidationError("This reference holds no atoms to load")


def encode_ref(kind: str, database: str, root_ptr: AtomPointer, **extra) -> bytes:
    """A reference to the ``DatabaseRoot`` at ``root_ptr`` of ``database`` (``kind`` "snapshot" or
    "pager"; ``extra`` fields are encoded with the codec's wire form)."""
    body = {"v": REF_VERSION, "kind": kind, "db": database, "root": [root_ptr.segment.bytes, root_ptr.offset]}
    for name, value in extra.items():
        body[name] = codec.encode_value(value, _saved_only)
    return msgpack.packb(body, use_bin_type=True)


def decode_ref(data: bytes, load=_no_load) -> dict:
    """The fields of a reference or token: ``kind``, ``db``, ``root`` (an AtomPointer) and the
    extra fields decoded (atoms through ``load``).  ``ProtoValidationError`` if malformed."""
    if not isinstance(data, (bytes, bytearray)):
        raise ProtoValidationError(f"A snapshot reference is bytes, not {type(data).__name__}")
    try:
        body = msgpack.unpackb(bytes(data), raw=False)
    except (ValueError, msgpack.ExtraData, msgpack.FormatError, msgpack.StackError) as e:
        raise ProtoValidationError(f"Malformed snapshot reference: {e}") from None
    if not isinstance(body, dict) or body.get("v") != REF_VERSION or body.get("kind") not in ("snapshot", "pager"):
        raise ProtoValidationError("Malformed snapshot reference (unknown version or kind)")
    root = body.get("root")
    if (not isinstance(body.get("db"), str) or not isinstance(root, list) or len(root) != 2
            or not isinstance(root[0], bytes) or len(root[0]) != 16 or type(root[1]) is not int or root[1] < 0):
        raise ProtoValidationError("Malformed snapshot reference (database or root)")
    out = {"kind": body["kind"], "db": body["db"], "root": AtomPointer(uuid.UUID(bytes=root[0]), root[1])}
    for name, value in body.items():
        if name not in ("v", "kind", "db", "root"):
            out[name] = codec.decode_value(value, load)
    return out


def _normalize_order(order_by) -> list | None:
    """``order_by`` as a list of ``[path, descending]`` (paths as str or None), the planner's forms."""
    if order_by is None:
        return None
    items = order_by if isinstance(order_by, list) else [order_by]
    out = []
    for item in items:
        path, descending = (item if isinstance(item, tuple) else (item, False))
        if not (path is None or isinstance(path, str)) or not isinstance(descending, bool):
            raise ProtoValidationError(f"A portable pager orders by paths: path or (path, descending), "
                                       f"not {item!r}")
        out.append([path, descending])
    return out


def _order_arg(order: list | None):
    return None if order is None else [(path, descending) for path, descending in order]


# -- the pager --------------------------------------------------------------------------------

class Pager:
    """Pages of ``page_size`` rows of one plan on one snapshot (module docstring).

    ``source`` is a QueryPlan, a Queryable or a collection.  ``cache`` (a space's
    :class:`QueryCache`) serves plans without rank access; without it such a plan is
    materialized privately.  Thread-safe: pages may be read from any thread.
    """

    def __init__(self, source, page_size: int, *, cache: QueryCache | None = None):
        if type(page_size) is not int or page_size < 1:
            raise ProtoValidationError(f"page_size must be an int >= 1, not {page_size!r}")
        if cache is not None and not isinstance(cache, QueryCache):
            raise ProtoValidationError(f"cache must be a QueryCache, not {type(cache).__name__}")
        self._plan = as_plan(source)
        self._page_size = page_size
        self._cache = cache
        self._rows = self._plan.exact_rows()
        self._ranked = self._rows is not None and self._plan.window(0, 0) is not None
        self._result: QueryResult | None = None
        self._lock = threading.Lock()
        self._token: bytes | None = None

    @property
    def page_size(self) -> int:
        """Rows per page."""
        return self._page_size

    @property
    def by_rank(self) -> bool:
        """True when pages are served by rank (O(log n + page size)), False when from a result."""
        return self._ranked

    @property
    def plan(self):
        """The plan whose rows are paged."""
        return self._plan

    def _materialized(self) -> QueryResult:
        with self._lock:
            if self._result is None:
                if self._cache is not None:
                    self._result = self._cache.result(self._plan)
                else:
                    self._result = QueryResult(None, "no cache", tuple(self._plan.execute()), None, 0)
            return self._result

    @property
    def rows(self) -> int:
        """The exact number of rows."""
        return self._rows if self._ranked else len(self._materialized())

    @property
    def pages(self) -> int:
        """The number of pages (0 for an empty result)."""
        return -(-self.rows // self._page_size)

    def page(self, k: int) -> tuple:
        """Page ``k`` (0-based) as a tuple of rows; an empty tuple past the last page."""
        if type(k) is not int or k < 0:
            raise ProtoValidationError(f"A page number must be an int >= 0, not {k!r}")
        start = k * self._page_size
        stop = start + self._page_size
        if self._ranked:
            return tuple(self._plan.window(start, stop))
        return self._materialized()[start:stop]

    def token(self) -> bytes:
        """The portable token of this pager (``ProtoUserError`` for a pager built from a plan or
        Queryable, which may hold Python callables and names no snapshot)."""
        if self._token is None:
            raise ProtoUserError("This pager has no token: make it with Transaction.pager, Snapshot.pager "
                                 "or Database.resume_pager to page across processes")
        return self._token

    def explain(self) -> dict:
        """``{"pager": {"page_size", "by_rank", "portable"}, "plan": plan.explain()}``."""
        return {"pager": {"page_size": self._page_size, "by_rank": self._ranked,
                          "portable": self._token is not None}, "plan": self._plan.explain()}

    def __repr__(self):
        return f"<Pager page_size={self._page_size} {'by rank' if self._ranked else 'materialized'}>"


def portable_pager(collection, *, database: str, root_ptr: AtomPointer, root_name: str, where, order_by,
                   page_size: int, cache: QueryCache | None) -> Pager:
    """A pager over ``collection`` (the value of ``root_name`` in the snapshot ``root_ptr``) whose
    token lets any process resume it (used by ``Transaction.pager`` and ``Snapshot.pager``)."""
    from ..collections.base import IndexedCollection
    from .planner import plan_query
    if not isinstance(collection, IndexedCollection):
        raise ProtoValidationError(f"Root {root_name!r} holds {type(collection).__name__}, not a collection")
    if collection.atom_pointer is None:
        raise ProtoUserError(f"Root {root_name!r} is not saved; a pager pages a committed snapshot")
    expr = None if where is None else Expression.compile(where)
    order = _normalize_order(order_by)
    pager = Pager(plan_query(collection, where=expr, order_by=_order_arg(order)), page_size, cache=cache)
    where_list = None if expr is None else expr.to_list()
    pager._token = encode_ref("pager", database, root_ptr, name=root_name, where=where_list, order=order,
                              page=page_size, qv=QUERY_SEMANTICS_VERSION)
    return pager


def pager_arguments(fields: dict) -> dict:
    """The ``Snapshot.pager`` arguments recorded in a decoded pager token; ``ProtoValidationError``
    when the token is not a pager token or was written by another query semantics version."""
    if fields.get("kind") != "pager":
        raise ProtoValidationError("Not a pager token (a snapshot reference names no query)")
    if fields.get("qv") != QUERY_SEMANTICS_VERSION:
        raise ProtoValidationError(f"The pager token was written by query semantics version {fields.get('qv')!r}, "
                                   f"this library has {QUERY_SEMANTICS_VERSION}: its pages could differ")
    order = fields.get("order")
    return {"root_name": fields.get("name"), "where": fields.get("where"),
            "order_by": None if order is None else _order_arg([list(o) for o in order]),
            "page_size": fields.get("page")}
