"""ProtoDB: an embedded, transactional, object-oriented database for Python.

This module is the public API of ProtoDB 2.0 (spec section 3, decision D15; the
exported surface is decision D47).  Every name listed in ``__all__`` is re-exported
from the subpackage that defines it:

- transactions: :class:`ObjectSpace`, :class:`Database`, :class:`Transaction`
- atoms and records: :class:`Atom`, :class:`DBObject`, :class:`MutableObject`
- collections: :class:`List`, :class:`Dictionary`, :class:`RepeatedKeysDictionary`,
  :class:`HashDictionary`, :class:`Set`, :class:`CountedSet`
- indexes: :class:`EqualityIndex`, :class:`RangeIndex`, :class:`VectorIndex`,
  :class:`KeySpec` and the :class:`Index` base for custom indexes
- queries: :func:`from_collection` and ``F`` (LINQ front end), :class:`Term`,
  :class:`And`, :class:`Or`, :class:`Not`, :func:`plan_query`
- storages: :class:`MemoryStorage`, :class:`FileStorage`, :class:`CloudStorage`
  (with :class:`S3ObjectStore` / :class:`GCSObjectStore`), :class:`TieredStorage`
- clusters: :class:`ClusterCoordinator`, :class:`Peer`, :class:`TcpTransport`
- runtime services: :func:`parallel_scan`, :func:`parallel_filter`, :class:`FSM`
- Arrow/Parquet bridge: :func:`to_arrow`, :func:`to_parquet`, :func:`scan_parquet`
- the value model (:func:`canonical`, :func:`values_equal`, ``MISSING``) and the
  exception hierarchy rooted at :class:`ProtoDBError` (D12)

Every name is resolved lazily, on first access (PEP 562): ``import proto_db``
imports no submodule, no optional third-party package (boto3, google-cloud-storage,
numpy, pyarrow, hnswlib) and starts no thread.  Importing a name imports only the
subpackage that defines it; optional dependencies are imported later still, when a
feature that needs them is first used.
"""
from importlib import import_module
from typing import TYPE_CHECKING

__version__ = "2.0.0.dev0"

#: Public name -> module (relative to this package) that defines it.
_EXPORTS = {
    # transactions
    "ObjectSpace": "transactions", "Database": "transactions", "Transaction": "transactions",
    "HistoryEntry": "transactions",
    # atoms and records
    "Atom": "core.atom", "AtomPointer": "core.atom", "PersistenceContext": "core.atom",
    "register_atom_class": "core.atom", "DBObject": "core.dbobject", "MutableObject": "core.dbobject",
    # collections
    "IndexedCollection": "collections.base", "ConcurrentOptimized": "collections.base",
    "List": "collections.lists", "Dictionary": "collections.dictionaries",
    "RepeatedKeysDictionary": "collections.dictionaries", "HashDictionary": "collections.hash_dictionaries",
    "Set": "collections.sets", "CountedSet": "collections.sets",
    # indexes
    "Index": "indexing.base", "KeySpec": "indexing.keyspec", "register_extractor": "indexing.keyspec",
    "EqualityIndex": "indexing.equality", "RangeIndex": "indexing.range", "VectorIndex": "indexing.vector",
    # queries
    "Expression": "query.expressions", "Term": "query.terms", "NearQuery": "query.terms",
    "And": "query.expressions", "Or": "query.expressions", "Not": "query.expressions",
    "evaluate": "query.expressions", "QueryPlan": "query.plans", "plan_query": "query.planner",
    "from_collection": "query.linq", "Queryable": "query.linq", "F": "query.linq", "Agg": "query.linq",
    # storages
    "SharedStorage": "storage.base", "MemoryStorage": "storage.memory", "FileStorage": "storage.file",
    "CloudStorage": "storage.cloud", "ObjectStore": "storage.cloud", "S3ObjectStore": "storage.cloud",
    "GCSObjectStore": "storage.cloud", "TieredStorage": "storage.tiered",
    # clusters
    "ClusterCoordinator": "cluster.coordinator", "Peer": "cluster.coordinator", "TcpTransport": "cluster.transport",
    # runtime services
    "parallel_scan": "runtime.parallel", "parallel_filter": "runtime.parallel", "ParallelConfig": "runtime.parallel",
    "ParallelPool": "runtime.parallel", "FSM": "runtime.fsm", "Timer": "runtime.fsm",
    # Arrow / Parquet bridge
    "to_arrow": "interop.arrow", "to_parquet": "interop.arrow", "scan_parquet": "interop.arrow",
    "from_parquet_to_rows": "interop.arrow",
    # value model
    "MISSING": "values", "canonical": "values", "values_equal": "values", "order_key": "values",
    "hash_key": "values", "FrozenList": "values", "FrozenDict": "values",
    # errors (D12); the *Exception names are the 0.1 spellings, kept as aliases for one release
    "ProtoDBError": "errors", "ProtoValidationError": "errors", "ProtoUserError": "errors",
    "ProtoCorruptionError": "errors", "ProtoNotSupportedError": "errors", "ProtoNotAuthorizedError": "errors",
    "ProtoUnexpectedError": "errors", "ProtoOutcomeUnknownError": "errors", "ProtoBacklogFullError": "errors",
    "ProtoLockingError": "errors", "ProtoConflictError": "errors", "ProtoTimeoutError": "errors",
    "ProtoBaseException": "errors", "ProtoValidationException": "errors", "ProtoUserException": "errors",
    "ProtoCorruptionException": "errors", "ProtoNotSupportedException": "errors",
    "ProtoNotAuthorizedException": "errors", "ProtoUnexpectedException": "errors",
    "ProtoLockingException": "errors",
}

__all__ = list(_EXPORTS)


def __getattr__(name):
    """Resolve a public name on first access (PEP 562) and cache it in the module namespace."""
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(f".{module}", __name__), name)
    globals()[name] = value
    return value


def __dir__():
    """The module's attributes, including the public names not resolved yet."""
    return sorted(set(globals()) | set(_EXPORTS))


if TYPE_CHECKING:  # pragma: no cover - static analysers only; never executed
    from .cluster.coordinator import ClusterCoordinator, Peer
    from .cluster.transport import TcpTransport
    from .collections.base import ConcurrentOptimized, IndexedCollection
    from .collections.dictionaries import Dictionary, RepeatedKeysDictionary
    from .collections.hash_dictionaries import HashDictionary
    from .collections.lists import List
    from .collections.sets import CountedSet, Set
    from .core.atom import Atom, AtomPointer, PersistenceContext, register_atom_class
    from .core.dbobject import DBObject, MutableObject
    from .errors import (ProtoBacklogFullError, ProtoBaseException, ProtoConflictError, ProtoCorruptionError,
                         ProtoCorruptionException, ProtoDBError, ProtoLockingError, ProtoLockingException,
                         ProtoNotAuthorizedError, ProtoNotAuthorizedException, ProtoNotSupportedError,
                         ProtoNotSupportedException, ProtoOutcomeUnknownError, ProtoTimeoutError,
                         ProtoUnexpectedError, ProtoUnexpectedException, ProtoUserError, ProtoUserException,
                         ProtoValidationError, ProtoValidationException)
    from .indexing.base import Index
    from .indexing.equality import EqualityIndex
    from .indexing.keyspec import KeySpec, register_extractor
    from .indexing.range import RangeIndex
    from .indexing.vector import VectorIndex
    from .interop.arrow import from_parquet_to_rows, scan_parquet, to_arrow, to_parquet
    from .query.expressions import And, Expression, Not, Or, evaluate
    from .query.linq import Agg, F, Queryable, from_collection
    from .query.planner import plan_query
    from .query.plans import QueryPlan
    from .query.terms import NearQuery, Term
    from .runtime.fsm import FSM, Timer
    from .runtime.parallel import ParallelConfig, ParallelPool, parallel_filter, parallel_scan
    from .storage.base import SharedStorage
    from .storage.cloud import CloudStorage, GCSObjectStore, ObjectStore, S3ObjectStore
    from .storage.file import FileStorage
    from .storage.memory import MemoryStorage
    from .storage.tiered import TieredStorage
    from .transactions import Database, HistoryEntry, ObjectSpace, Transaction
    from .values import MISSING, FrozenDict, FrozenList, canonical, hash_key, order_key, values_equal
