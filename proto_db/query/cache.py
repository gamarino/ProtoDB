"""Query results cached by pointer (Task 8.4b, D56; rulings R76, R77; principle P1).

A query's result depends only on the content of the collections and indexes it reads,
and a pointer names one content forever (P1).  So a result can be kept under a key made
of the pointers of everything the plan reads plus the plan's exact shape, and it is
valid for as long as anyone asks for that key: no invalidation exists or is needed.  A
commit produces new pointers, hence new keys; old entries simply stop being asked for
and the LRU evicts them.

The key (:func:`cache_key`) is ``(QUERY_SEMANTICS_VERSION, plan key)``, where the plan
key is built from the physical plan, node by node, without executing anything:

* a collection scan by the collection's pointer; an index plan by the index's pointer,
  its kind, its term and its details (an index belongs to one collection version);
* terms by path, operator and exact operands (:func:`proto_db.values.exact_token`, so
  ``1``, ``1.0`` and ``Decimal("1.00")`` are three keys; atoms by pointer);
* registered extractors (``@name`` paths, ``KeySpec.extractor``) by name and
  registration generation, so a name registered again with another function gets
  another key;
* every other node by its parameters and its children.

Equal keys give equal results: equal leaf pointers mean equal leaf contents, an equal
key means an equal plan tree, and every plan node is a deterministic function of its
children's sequences.  ``QUERY_SEMANTICS_VERSION`` is raised whenever an executor's
output order or content changes, so two library versions never share a key.

Not cacheable (the reason is reported, never silently skipped): Python callables
(``PythonFilter``, ``PythonPredicate``, callable keys, projections and aggregations),
``ValuesScan`` (values held only in memory), vector searches on a backend that is not
deterministic (``hnsw``), indexes that do not opt in (``Index._plan_cacheable``), any
leaf without a pointer (a collection changed in the running transaction), and any plan
node this module does not know.

A cached :class:`QueryResult` is immutable.  It holds atoms as ``(pointer, class)``
references, resolved through the space's decoded-atom cache when read, so it never pins
loaded atoms (the 8.3l rule); other values are held frozen (D25).  :class:`QueryCache`
is bounded by an estimated byte size; one result may take at most an eighth of it.
"""
from __future__ import annotations

import hashlib
import sys
import threading
import uuid
from collections.abc import Iterator, Sequence
from datetime import date, datetime, time, timedelta
from decimal import Decimal

from ..core.atom import Atom
from ..errors import ProtoValidationError
from ..storage.cache import BoundedCache
from ..values import MISSING, FrozenDict, FrozenList, MutableObject, exact_token
from .expressions import And, Not, Or, PythonPredicate
from .plans import (CollectionScanPlan, Count, Distinct, Filter, GroupBy, IndexPlan, Intersect, Join, Limit,
                    Project, PythonFilter, QueryPlan, Sort, Union, Unnest, ValuesScan, VectorSearch)
from .terms import NearQuery, Term

__all__ = ["QUERY_SEMANTICS_VERSION", "QUERY_CACHE_BYTES", "QueryCache", "QueryResult", "cache_key",
           "as_plan"]

#: Version of the executors' observable behaviour (row order, ties, grouping).  Part of every
#: key and of every pager token: raise it whenever a plan node's output changes for some input.
QUERY_SEMANTICS_VERSION = 1

#: Default capacity of a space's result cache, in estimated bytes.
QUERY_CACHE_BYTES = 64 * 1024 * 1024

#: A result may take at most this fraction of the capacity, so one query cannot flush the cache.
_MAX_ENTRY_FRACTION = 8


class _Uncacheable(Exception):
    """Internal signal: the plan has no key; ``args[0]`` says why."""


# -- the key ---------------------------------------------------------------------------------

def as_plan(source) -> QueryPlan:
    """The plan of ``source``: a QueryPlan, a Queryable (its ``plan()``) or a collection (a scan)."""
    if isinstance(source, QueryPlan):
        return source
    plan = getattr(source, "plan", None)
    if callable(plan) and not isinstance(source, Atom):
        built = plan()
        if isinstance(built, QueryPlan):
            return built
    as_query_plan = getattr(source, "as_query_plan", None)
    if callable(as_query_plan):
        return as_query_plan()
    raise ProtoValidationError(f"Expected a QueryPlan, a Queryable or a collection, not {type(source).__name__}")


def _ptr(atom, what: str) -> tuple:
    ptr = atom.atom_pointer
    if ptr is None:
        raise _Uncacheable(f"{what} is not saved (it was changed or created in a running transaction)")
    return (ptr.segment.bytes, ptr.offset)


def _atom_token(atom) -> tuple:
    return ("@", _ptr(atom, f"the atom {type(atom).__name__}"))


def _value_key(value):
    if isinstance(value, Atom):
        return _atom_token(value)
    if isinstance(value, NearQuery):
        return ("near", tuple(exact_token(x) for x in value.vector), value.k, exact_token(value.threshold),
                value.metric)
    return exact_token(value, _atom_token)


def _path_key(path):
    if isinstance(path, str) and path.startswith("@"):
        from ..indexing.keyspec import extractor_generation
        return ("@", path[1:], extractor_generation(path[1:]))
    return path


def _spec_key(spec) -> tuple:
    from ..indexing.keyspec import EXTRACTOR, KeySpec, extractor_generation
    if not isinstance(spec, KeySpec):
        raise _Uncacheable(f"a key given as a Python callable ({getattr(spec, '__qualname__', spec)!r})")
    kind, arg = spec.to_data()
    return (kind, arg, extractor_generation(arg)) if kind == EXTRACTOR else (kind, arg)


def _extract_key(extract) -> tuple:
    """Key of an extraction function: a KeySpec's bound ``extract``, else not cacheable."""
    return _spec_key(getattr(extract, "__self__", extract))


def _expr_key(expr) -> tuple:
    if isinstance(expr, Term):
        return ("term", _path_key(expr.path), expr.op, _value_key(expr.value))
    if isinstance(expr, And):
        return ("and", tuple(_expr_key(o) for o in expr.operands))
    if isinstance(expr, Or):
        return ("or", tuple(_expr_key(o) for o in expr.operands))
    if isinstance(expr, Not):
        return ("not", _expr_key(expr.operand))
    if isinstance(expr, PythonPredicate):
        raise _Uncacheable(f"a Python predicate ({expr.description})")
    raise _Uncacheable(f"an unknown expression node {type(expr).__name__}")


def _index_leaf(index, what: str) -> tuple:
    deterministic = getattr(index, "_plans_deterministic", None)
    if deterministic is None or not deterministic():
        raise _Uncacheable(f"the index {getattr(index, 'name', index)!r} ({type(index).__name__}) does not "
                           f"declare its plans deterministic")
    return (type(index).__type_name__, _ptr(index, what))


def _plan_key(plan) -> tuple:
    t = type(plan)
    if t is CollectionScanPlan:
        c = plan.collection
        return ("scan", type(c).__type_name__, _ptr(c, f"the collection {type(c).__name__}"))
    if t is IndexPlan:
        details = tuple(sorted((k, _value_key(v)) for k, v in plan._details.items()))
        term = None if plan.term is None else _expr_key(plan.term)
        return ("index", _index_leaf(plan.index, "the index"), plan.kind, term, details)
    if t is VectorSearch:
        index = plan.index
        where = None if plan.where is None else _expr_key(plan.where)
        return ("vector", _index_leaf(index, "the vector index"), _expr_key(plan.term), where)
    if t is Filter:
        return ("filter", _expr_key(plan.expr), _plan_key(plan.source))
    if t in (Union, Intersect):
        return (t.__name__, tuple(_plan_key(s) for s in plan.sources))
    if t is Project:
        if plan.fn is not None:
            raise _Uncacheable(f"a Python projection ({plan.function})")
        fields = tuple((name, _extract_key(extract)) for name, extract in plan._extractors)
        return ("project", fields, _plan_key(plan.source))
    if t is Sort:
        keys = tuple((_extract_key(extract), descending) for extract, _, descending in plan._keys)
        return ("sort", keys, _plan_key(plan.source))
    if t is Limit:
        return ("limit", plan.n, plan.offset, _plan_key(plan.source))
    if t is Count:
        return ("count", _plan_key(plan.source))
    if t is Distinct:
        return ("distinct", _plan_key(plan.source))
    if t is GroupBy:
        aggs = []
        for name, op, count_all, extract in plan._aggregations:
            if callable(op):
                raise _Uncacheable(f"a Python aggregation ({plan.aggregations[name]['op']})")
            aggs.append((name, op, count_all, _extract_key(extract)))
        return ("group", _extract_key(plan._extract), tuple(aggs), _plan_key(plan.source))
    if t is Join:
        return ("join", _extract_key(plan._left_key), _extract_key(plan._right_key), plan.kind,
                _plan_key(plan.left), _plan_key(plan.right))
    if t is Unnest:
        return ("unnest", _extract_key(plan._extract), plan.with_parent, _plan_key(plan.source))
    if t is PythonFilter:
        raise _Uncacheable(f"a Python filter ({plan.description})")
    if t is ValuesScan:
        raise _Uncacheable("a ValuesScan (its values exist only in memory)")
    raise _Uncacheable(f"an unknown plan node {t.__name__}")


def cache_key(source) -> tuple:
    """``(key, None)`` for a cacheable plan, ``(None, reason)`` otherwise; nothing is executed.

    ``source`` is a QueryPlan, a Queryable or a collection (:func:`as_plan`)."""
    plan = as_plan(source)
    try:
        return (QUERY_SEMANTICS_VERSION, _plan_key(plan)), None
    except _Uncacheable as e:
        return None, str(e.args[0])


def key_digest(key: tuple) -> str:
    """A short, stable hex digest of a key, for ``explain()`` and logs (BLAKE2b-128 of its repr)."""
    return hashlib.blake2b(repr(key).encode(), digest_size=16).hexdigest()


# -- results ---------------------------------------------------------------------------------

class _AtomRef:
    """An atom held by a cached result: its pointer and type name, loaded again on access."""

    __slots__ = ("ptr", "type_name")

    def __init__(self, ptr, type_name: str):
        self.ptr = ptr
        self.type_name = type_name


_SCALARS = (bool, int, float, Decimal, str, bytes, datetime, date, time, timedelta, uuid.UUID, MutableObject)
_REF_SIZE = 72


class _Thaw:
    """A stored value holding atom references somewhere inside: rebuilt on access."""

    __slots__ = ("value",)

    def __init__(self, value):
        self.value = value


def _freeze(value, size: list, refs: list):
    """The stored form of one result value; ``size[0]`` accumulates its estimated bytes and
    ``refs[0]`` counts the atoms replaced by references."""
    if value is None or value is MISSING:
        return value
    if isinstance(value, Atom):
        ptr = value.atom_pointer
        if ptr is None:
            raise _Uncacheable(f"the result holds an unsaved atom {type(value).__name__}")
        size[0] += _REF_SIZE
        refs[0] += 1
        return _AtomRef(ptr, type(value).__type_name__)
    if isinstance(value, dict):
        size[0] += sys.getsizeof(value)
        return FrozenDict((_freeze(k, size, refs), _freeze(v, size, refs)) for k, v in value.items())
    if isinstance(value, list):
        size[0] += sys.getsizeof(value)
        return FrozenList(_freeze(v, size, refs) for v in value)
    if isinstance(value, tuple):
        size[0] += sys.getsizeof(value)
        return tuple(_freeze(v, size, refs) for v in value)
    if isinstance(value, _SCALARS):
        size[0] += sys.getsizeof(value)
        return value
    raise _Uncacheable(f"the result holds a value of type {type(value).__name__}, which is not a stored value")


def _entry(value, size: list):
    """One result row in stored form: an atom reference, a :class:`_Thaw` wrapper when atoms are
    nested inside, else the frozen value itself (returned as is on every access)."""
    refs = [0]
    frozen = _freeze(value, size, refs)
    if type(frozen) is _AtomRef or not refs[0]:
        return frozen
    return _Thaw(frozen)


def _thaw(value, load):
    if type(value) is _AtomRef:
        return load(value.ptr, value.type_name)
    if isinstance(value, FrozenDict):
        return FrozenDict((_thaw(k, load), _thaw(v, load)) for k, v in value.items())
    if isinstance(value, FrozenList):
        return FrozenList(_thaw(v, load) for v in value)
    if type(value) is tuple:
        return tuple(_thaw(v, load) for v in value)
    return value


class QueryResult(Sequence):
    """The immutable result of one query: ``len``, ``[i]``, ``[a:b]`` (a tuple), iteration.

    A cacheable result (``key`` set) holds its values in stored form: atoms as
    ``(pointer, class)``, resolved through the space's atom cache on access, and dicts
    and lists frozen (D25).  So a cache hit and a miss return equal values, and the
    result never keeps loaded atoms alive.  An uncacheable result (``key`` None, ``reason``
    set) holds the values the plan produced, as they are.
    """

    __slots__ = ("key", "reason", "_entries", "_load", "_size")

    def __init__(self, key, reason, entries: tuple, load, size: int):
        self.key = key
        self.reason = reason
        self._entries = entries
        self._load = load
        self._size = size

    def __len__(self) -> int:
        return len(self._entries)

    def __getitem__(self, item):
        if isinstance(item, slice):
            return tuple(self._value(v) for v in self._entries[item])
        return self._value(self._entries[item])

    def __iter__(self) -> Iterator:
        for v in self._entries:
            yield self._value(v)

    def _value(self, v):
        if self._load is None:
            return v
        t = type(v)
        if t is _AtomRef:
            return self._load(v.ptr, v.type_name)
        if t is _Thaw:
            return _thaw(v.value, self._load)
        return v

    def __repr__(self):
        return f"<QueryResult rows={len(self._entries)} {'cached' if self.key else 'uncacheable'}>"


# -- the cache -------------------------------------------------------------------------------

def _entry_size(result: QueryResult) -> int:
    return result._size


class QueryCache:
    """A space's query results by key (D56): bounded LRU by estimated bytes, shared by every
    transaction and thread of the space.  Thread-safe.

    ``load(ptr, type_name)`` resolves the atoms of cached results (the space's atom cache).
    ``capacity`` 0 disables storing; results are still computed and returned.
    """

    def __init__(self, capacity: int, load):
        if type(capacity) is not int or capacity < 0:
            raise ProtoValidationError("query_cache_bytes must be a non-negative int")
        self._cache = BoundedCache(capacity, sizeof=_entry_size)
        self._load = load
        self._lock = threading.Lock()
        self._uncacheable = 0
        self._refused = 0

    @property
    def capacity(self) -> int:
        """The configured capacity in estimated bytes."""
        return self._cache.max_bytes

    def result(self, source) -> QueryResult:
        """The result of ``source`` (a QueryPlan, a Queryable or a collection): from the cache when
        its key is cached, else executed now (and stored when cacheable and small enough)."""
        plan = as_plan(source)
        key, reason = cache_key(plan)
        if key is None:
            with self._lock:
                self._uncacheable += 1
            return QueryResult(None, reason, tuple(plan.execute()), None, 0)
        hit = self._cache.get(key)
        if hit is not None:
            return hit
        rows = tuple(plan.execute())
        size = [64]
        try:
            entries = tuple(_entry(v, size) for v in rows)
        except _Uncacheable as e:
            with self._lock:
                self._uncacheable += 1
            return QueryResult(None, str(e.args[0]), rows, None, 0)
        size[0] += 8 * len(entries)
        result = QueryResult(key, None, entries, self._load, size[0])
        if size[0] * _MAX_ENTRY_FRACTION > self._cache.max_bytes:
            with self._lock:
                self._refused += 1
            return result
        self._cache.put(key, result)
        return result

    def explain(self, source) -> dict:
        """``plan.explain()`` plus ``"cache"``: ``{"key": digest, "cached": bool}`` or ``{"uncacheable": reason}``."""
        plan = as_plan(source)
        info = plan.explain()
        key, reason = cache_key(plan)
        if key is None:
            info["cache"] = {"uncacheable": reason}
        else:
            info["cache"] = {"key": key_digest(key), "cached": self.contains(key)}
        return info

    def contains(self, key) -> bool:
        """True when ``key`` is cached now (does not count as a hit or a miss)."""
        return self._cache.peek(key) is not None

    def stats(self) -> dict:
        """``hits``, ``misses``, ``entries``, ``bytes``, ``capacity``, ``uncacheable`` (plans or
        results that could not be keyed) and ``refused`` (results too large to store)."""
        stats = self._cache.stats()
        with self._lock:
            stats.update(uncacheable=self._uncacheable, refused=self._refused)
        return stats

    def clear(self) -> None:
        """Drop every entry (never needed for correctness: entries cannot go stale)."""
        self._cache.clear()
