"""Search backends of :class:`proto_db.indexing.vector.VectorIndex`: exact, IVF-Flat and HNSW (D39).

Vocabulary
----------
A *member* is one distinct stored element (the exact member key of
:mod:`.keyed`: an element held twice in a List is one member with count 2).  An
*entry* is ``(member key, element, count, vector)``.  Every backend returns
members ranked by :func:`exact_score` (higher is nearer), ties broken by the
order key of the member key, each with its count; ``k`` counts members, so the
occurrences of one element are never split between "in" and "out" (D37).

Scores (:func:`exact_score`), computed with ``math.fsum`` (correctly rounded,
so independent of summation order and identical in the oracle):

* ``cosine`` -- ``dot(q, v) / (|q| |v|)``; 0.0 when either vector is zero.
  ``threshold`` keeps similarity >= threshold.
* ``l2`` -- minus the Euclidean distance; ``threshold`` keeps distance <= threshold.
* ``ip`` -- the inner product; ``threshold`` keeps score >= threshold.

Backends (stateless strategy objects; all state lives in the immutable index):

* ``exact`` -- brute force over a per-version flat snapshot (entries and numpy
  matrix, cached by index oid: the first query of a new version pays O(n)).  With numpy the
  scores are screened in bulk, then every candidate that could reach the k-th
  place or the threshold within a generous floating-point margin is re-scored
  exactly, so the answer equals brute force bit for bit.  ``numpy=False``
  forces the pure-Python path.
* ``ivf_flat`` -- persisted ``centroids`` (in the index atom) and inverted
  lists (the index store is keyed by ``(list number, member key)``).  Training
  (deterministic k-means: training depends on the training vectors and ``seed`` only, never on
  the members' oids; a bulk ``build`` trains on all its vectors, so the same vectors give the same
  centroids in any order, while an index grown by inserts trains on the members present when a
  threshold is reached, which depends on the insertion order) happens in ``build`` and when the store
  grows: an untrained index (fewer than ``nlist`` vectors) keeps everything in
  list -1, scanned exhaustively; the index retrains itself when it first holds
  ``nlist`` vectors and whenever it reaches ``retrain_growth`` times the size it
  was trained at (amortised O(1) per insert).  A vector goes to the list of its
  best-scoring centroid (exact scores, lowest list number on ties), so the
  assignment is reproducible for removal and checkable by ``validate``.  A
  search probes the ``nprobe`` best lists and widens (doubling) while fewer
  than ``k`` matching members were found; ``nprobe == nlist`` is exact.  The
  probed lists are read from a per-version snapshot (each list's entries and
  numpy matrix), cached like the exact backend's.
* ``hnsw`` -- an hnswlib graph built lazily from the store and cached per
  process by index version (oid) in a thread-safe LRU; the search widens its
  candidate count until ``k`` matching members are found, the threshold is
  passed, or the whole store is searched (then exactly).  hnswlib is imported
  only at first use: on CPython 3.14t importing it re-enables the GIL for the
  whole process.  A missing hnswlib raises ``ProtoNotSupportedError``.

A residual predicate (``eligible``) restricts every backend to the members
that satisfy it before ranking (ruling R27).
"""
from __future__ import annotations

import math
import random
import struct
import threading
from collections import OrderedDict
from collections.abc import Callable, Iterable, Iterator
from decimal import Decimal

from ..collections import avl
from ..errors import ProtoNotSupportedError, ProtoValidationError
from ..query.plans import FILTER_SELECTIVITY, Estimate
from ..values import MISSING, order_key

__all__ = ["METRICS", "BACKENDS", "canonical_vector", "exact_score", "score_floor", "rank_entries",
           "backend_for", "normalize_params", "DerivedCache", "DERIVED", "load_hnswlib", "UNTRAINED"]

#: Metrics of a vector index (and of a ``near[]`` query).
METRICS = ("cosine", "l2", "ip")
#: Backend names.
BACKENDS = ("exact", "ivf_flat", "hnsw")
#: List number of the vectors of an untrained IVF index (and of every vector of the other backends: 0).
UNTRAINED = -1

_R_TUPLE = order_key(())[0]
_ABOVE_EVERY_RANK = order_key(MISSING)[0] + 1000
#: Below this many candidates the exact backend scores in pure Python (numpy set-up costs more).
_NUMPY_MIN = 64
#: Relative margin of the numpy screening; float64 dot products err by ~1e-16 * dim relative.
_SCREEN_MARGIN = 1e-9
#: Norm range in which a numpy cosine is accurate: products of norms stay normal floats (>= 1e-300) and finite.
_NORM_LOW = 1e-150
_NORM_HIGH = 1e150


# -- vectors and scores -------------------------------------------------------------------------

def _sum_nonnegative(values: Iterable[float]) -> float:
    """``fsum`` of non-negative terms; an overflowing sum is ``inf`` (its true value)."""
    try:
        return math.fsum(values)
    except OverflowError:
        return math.inf


def canonical_vector(key, dim: int):
    """``key`` as a tuple of ``dim`` floats, or None when it is not a valid vector.

    Valid: a tuple or list (FrozenList) of exactly ``dim`` numbers (int, float,
    Decimal; not bool), every one finite as a float, with a finite squared norm.
    MISSING, None and everything else are not vectors.
    """
    if not isinstance(key, (tuple, list)) or len(key) != dim:
        return None
    out = []
    for x in key:
        if isinstance(x, bool) or not isinstance(x, (int, float, Decimal)):
            return None
        try:
            f = float(x)
        except OverflowError:
            return None
        if not math.isfinite(f):
            return None
        out.append(f)
    if not math.isfinite(_sum_nonnegative(x * x for x in out)):
        return None
    return tuple(out)


def exact_score(metric: str, q: tuple, v: tuple) -> float:
    """The score of ``v`` for the query ``q`` (higher is nearer; see the module docstring)."""
    if metric == "ip":
        return math.fsum(a * b for a, b in zip(q, v))
    if metric == "l2":
        return -math.sqrt(_sum_nonnegative((a - b) * (a - b) for a, b in zip(q, v)))
    if metric == "cosine":
        nq = math.sqrt(math.fsum(a * a for a in q))
        nv = math.sqrt(math.fsum(b * b for b in v))
        if nq == 0.0 or nv == 0.0:
            return 0.0
        return (math.fsum(a * b for a, b in zip(q, v)) / nq) / nv
    raise ProtoValidationError(f"Unknown metric {metric!r}; expected one of {list(METRICS)}")


def score_floor(metric: str, threshold: float | None):
    """The least score a ``threshold`` admits (``l2``: minus the distance), or None without threshold."""
    if threshold is None:
        return None
    return -threshold if metric == "l2" else threshold


def rank_entries(entries: Iterable[tuple], q: tuple, metric: str, k: int | None,
                 threshold: float | None) -> list:
    """``[(element, count)]`` of the best ``k`` entries passing the threshold, nearest first.

    Exact scores; ties are broken by the order key of the member key.
    """
    floor = score_floor(metric, threshold)
    scored = []
    for mk, element, count, vector in entries:
        s = exact_score(metric, q, vector)
        if floor is not None and not s >= floor:
            continue
        scored.append((-s, order_key(mk), element, count))
    scored.sort(key=_rank_key)
    if k is not None:
        del scored[k:]
    return [(element, count) for _, _, element, count in scored]


def _rank_key(item):
    return item[0], item[1]


def _filtered(entries: Iterable[tuple], eligible: Callable | None) -> Iterable[tuple]:
    if eligible is None:
        return entries
    return (e for e in entries if eligible(e[1]))


# -- the store ------------------------------------------------------------------------------------

def store_key_okey(list_no: int, mk_okey) -> tuple:
    """The order key of the store key ``(list_no, member key)`` from the member key's order key."""
    return (_R_TUPLE, (order_key(list_no), mk_okey))


def list_bounds(list_no: int) -> tuple:
    """Order-key bounds enclosing every store key of list ``list_no``."""
    ok = order_key(list_no)
    return (_R_TUPLE, (ok,)), (_R_TUPLE, (ok, (_ABOVE_EVERY_RANK,)))


def iter_entries(store, list_no: int | None = None) -> Iterator[tuple]:
    """Entries ``(member key, element, count, vector)`` of one list, or of the whole store."""
    if list_no is None:
        items = avl.key_iter(store)
    else:
        low, high = list_bounds(list_no)
        items = avl.key_iter_okey_range(store, low, high)
    for key, (element, count, vector) in items:
        yield key[1], element, count, vector


# -- derived per-version structures ---------------------------------------------------------------

class _Slot:
    __slots__ = ("lock", "value", "ready", "owner")

    def __init__(self, owner):
        self.lock = threading.Lock()
        self.value = None
        self.ready = False
        self.owner = owner


class DerivedCache:
    """A thread-safe LRU of structures derived from immutable index versions (D39).

    ``get(key, build)`` returns the structure cached under ``key``, building it
    once with ``build()``: concurrent callers for one key wait for a single
    build; a failed build leaves nothing cached.  Keys are ``(index oid, kind)``,
    so a version and every stub loaded from it share one structure, and a new
    version never sees a stale one.  Each entry records its *owner*, the storage of
    the index it was built from: :meth:`release` drops an owner's entries when its
    space closes, so a structure (which holds the stored elements) never keeps a
    closed space's atoms reachable.
    """

    def __init__(self, capacity: int = 16):
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 1:
            raise ProtoValidationError(f"DerivedCache capacity must be an int >= 1, not {capacity!r}")
        self._capacity = capacity
        self._lock = threading.Lock()
        self._slots: OrderedDict = OrderedDict()

    def get(self, key, build: Callable, owner=None):
        """The structure for ``key``, built by ``build()`` on a miss; ``owner`` (the storage of
        the index, or None) is recorded for :meth:`release`."""
        with self._lock:
            slot = self._slots.get(key)
            if slot is None:
                slot = self._slots[key] = _Slot(owner)
                while len(self._slots) > self._capacity:
                    self._slots.popitem(last=False)
            else:
                self._slots.move_to_end(key)
        with slot.lock:
            if not slot.ready:
                slot.value = build()
                slot.ready = True
            return slot.value

    def release(self, owner) -> None:
        """Drop every entry whose owner is ``owner`` (its space is closing).  A build in progress
        for such an entry completes for its caller and is not cached."""
        with self._lock:
            for key in [k for k, slot in self._slots.items() if slot.owner is owner]:
                del self._slots[key]

    def owners(self) -> list:
        """The owners of the cached entries (diagnostics, tests)."""
        with self._lock:
            return [slot.owner for slot in self._slots.values()]

    def __len__(self):
        with self._lock:
            return len(self._slots)


def _owner(index):
    """The storage an index version belongs to (its context's), or None for a context-free one."""
    return getattr(getattr(index, "context", None), "storage", None)


#: The process-wide cache of flat snapshots and HNSW graphs.
DERIVED = DerivedCache()

_numpy_lock = threading.Lock()
_numpy_state: list = []


def numpy_module():
    """numpy, imported once, or None when it is not installed."""
    with _numpy_lock:
        if not _numpy_state:
            try:
                import numpy
            except ImportError:
                numpy = None
            _numpy_state.append(numpy)
        return _numpy_state[0]


def load_hnswlib():
    """Import hnswlib (at first use only: on CPython 3.14t the import re-enables the GIL).

    Raises ``ProtoNotSupportedError`` (from the ImportError) when it is missing.
    """
    try:
        import hnswlib
    except ImportError as e:
        raise ProtoNotSupportedError(
            "The 'hnsw' vector backend needs the optional package hnswlib (pip install hnswlib); use the "
            "'exact' or 'ivf_flat' backend without it. Note: on free-threaded CPython importing hnswlib "
            "re-enables the GIL for the whole process") from e
    return hnswlib


# -- parameters -----------------------------------------------------------------------------------

def _positive_int(name: str, value) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ProtoValidationError(f"Vector index parameter {name!r} must be an int >= 1, not {value!r}")
    return value


def _bool(name: str, value) -> bool:
    if not isinstance(value, bool):
        raise ProtoValidationError(f"Vector index parameter {name!r} must be a bool, not {value!r}")
    return value


def _growth(name: str, value) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 1:
        raise ProtoValidationError(f"Vector index parameter {name!r} must be a finite number > 1, not {value!r}")
    return float(value)


def normalize_params(backend: str, params: dict) -> tuple:
    """The validated, complete parameters of ``backend`` as a tuple of ``(name, value)`` sorted by name."""
    impl = backend_for(backend)
    unknown = sorted(set(params) - set(impl.DEFAULTS))
    if unknown:
        raise ProtoValidationError(f"Unknown parameters {unknown} for the {backend!r} vector backend; "
                                   f"expected {sorted(impl.DEFAULTS)}")
    values = impl.check(dict(params))
    return tuple(sorted(values.items()))


# -- backends --------------------------------------------------------------------------------------

class _Backend:
    """Common interface of the backends (see the module docstring)."""

    name = ""
    #: Whether a search is a deterministic function of the index's stored content (D56): the
    #: exact and ivf_flat backends are; hnsw builds its graph in each process with hnswlib.
    deterministic = True
    kind = ""
    DEFAULTS: dict = {}

    @classmethod
    def check(cls, params: dict) -> dict:
        """Validated complete parameters (defaults filled in)."""
        raise NotImplementedError

    def list_for(self, index, vector: tuple) -> int:
        """The store list of ``vector`` (0 for backends without lists)."""
        return 0

    def needs_training(self, index) -> bool:
        """True when ``index`` (just grown by one vector) must be retrained."""
        return False

    def train(self, index, vectors: list) -> tuple:
        """Centroids for ``vectors`` (``()`` for backends without lists)."""
        return ()

    def search(self, index, q: tuple, k, threshold, eligible) -> list:
        """``[(element, count)]`` answering the query (module docstring)."""
        raise NotImplementedError

    def estimate(self, index, k, threshold, filtered: bool) -> Estimate:
        """Planner estimate in D30 units."""
        raise NotImplementedError

    def explain(self, index) -> dict:
        """Backend-specific ``explain()`` entries."""
        return {}

    @staticmethod
    def _rows(index, k, threshold, filtered: bool) -> float:
        members = 0 if index.store is None else index.store.count
        rows = members * FILTER_SELECTIVITY if k is None else min(k, members)
        if threshold is not None and k is not None:
            rows *= FILTER_SELECTIVITY
        return rows


class _FlatSnapshot:
    """Every entry of one index version, with its numpy matrix and norms when numpy is used."""

    __slots__ = ("entries", "matrix", "norms")

    def __init__(self, entries: tuple, matrix, norms):
        self.entries = entries
        self.matrix = matrix
        self.norms = norms


def _numpy_scores(np, metric: str, q, qnorm: float, matrix, norms):
    if metric == "ip":
        return matrix @ q
    if metric == "l2":
        diff = matrix - q
        return -np.sqrt(np.einsum("ij,ij->i", diff, diff))
    dots = matrix @ q
    denom = norms * qnorm
    out = np.zeros_like(dots)
    nz = denom != 0.0
    out[nz] = dots[nz] / denom[nz]
    return out


def _screen_margin(np, metric: str, approx, norms, qnorm: float):
    """A per-entry bound, far above float64 rounding, on |numpy score - exact score|."""
    if metric == "cosine":
        return _SCREEN_MARGIN * (1.0 + np.abs(approx))
    if metric == "ip":
        return _SCREEN_MARGIN * (1.0 + np.abs(approx) + norms * qnorm)
    return _SCREEN_MARGIN * (1.0 + np.abs(approx) + norms + qnorm)


def _unreliable(metric: str, norms, qnorm: float):
    """Entries whose numpy cosine may be far off: a norm outside [1e-150, 1e150] (subnormal or overflowing
    products, :data:`_NORM_LOW`); None for the other metrics, whose margins are absolute or scaled."""
    if metric != "cosine" or qnorm == 0.0:
        return None  # a zero query scores every entry exactly 0.0 in both computations
    if not _NORM_LOW <= qnorm <= _NORM_HIGH:
        return norms > 0.0
    return (norms > 0.0) & ((norms < _NORM_LOW) | (norms > _NORM_HIGH))


def screened_rank(np, metric: str, entries, matrix, norms, q: tuple, k, threshold) -> list:
    """:func:`rank_entries` of ``entries`` (rows of ``matrix``, with their ``norms``), screened by numpy.

    numpy scores every entry; only the entries whose score lies within a safe
    margin (:func:`_screen_margin`) of the threshold and of the k-th best score
    are re-scored exactly, so the result equals :func:`rank_entries` over all of
    them.  Entries whose numpy score is unreliable (:func:`_unreliable`: cosine
    with tiny or huge norms) are always re-scored.  Small inputs, and inputs of
    extreme magnitude (a non-finite numpy score), are scored exactly without
    screening.
    """
    if len(entries) < _NUMPY_MIN:
        return rank_entries(entries, q, metric, k, threshold)
    qv = np.asarray(q, dtype=np.float64)
    qnorm = float(np.sqrt(qv @ qv))
    forced = _unreliable(metric, norms, qnorm)
    if forced is not None and not forced.any():
        forced = None
    if forced is None:
        reliable = np.arange(len(entries))
    else:
        reliable = np.nonzero(~forced)[0]
        if len(reliable) == 0:
            return rank_entries(entries, q, metric, k, threshold)
        matrix, norms = matrix[reliable], norms[reliable]
    with np.errstate(all="ignore"):
        approx = _numpy_scores(np, metric, qv, qnorm, matrix, norms)
        margin = _screen_margin(np, metric, approx, norms, qnorm)
    if not (np.all(np.isfinite(approx)) and np.all(np.isfinite(margin))):
        return rank_entries(entries, q, metric, k, threshold)
    keep = np.arange(len(approx))
    floor = score_floor(metric, threshold)
    if floor is not None:
        keep = keep[approx[keep] + margin[keep] >= floor]
    if k is not None and len(keep) > k:
        part = approx[keep]
        kth = -np.partition(-part, k - 1)[k - 1]
        keep = keep[part + 2 * float(margin.max()) >= kth]
    chosen = [int(i) for i in reliable[keep]]
    if forced is not None:
        chosen += [int(i) for i in np.nonzero(forced)[0]]
    return rank_entries([entries[i] for i in chosen], q, metric, k, threshold)


def _matrix(np, entries, dim: int) -> tuple:
    """``(matrix, norms)`` of the vectors of ``entries``."""
    matrix = np.array([e[3] for e in entries], dtype=np.float64).reshape(len(entries), dim)
    return matrix, np.sqrt(np.einsum("ij,ij->i", matrix, matrix))


class ExactBackend(_Backend):
    """Brute force: exact answers (numpy screening plus exact re-scoring)."""

    name = "exact"
    kind = "VectorExact"
    DEFAULTS = {"numpy": True}

    @classmethod
    def check(cls, params: dict) -> dict:
        """``numpy`` (bool, default True): use numpy for screening when it is installed."""
        return {"numpy": _bool("numpy", params.get("numpy", True))}

    @staticmethod
    def _numpy(index):
        return numpy_module() if dict(index.params).get("numpy", True) else None

    def _snapshot(self, index) -> _FlatSnapshot:
        def build():
            entries = tuple(iter_entries(index.store))
            np = self._numpy(index)
            if np is None or not entries:
                return _FlatSnapshot(entries, None, None)
            return _FlatSnapshot(entries, *_matrix(np, entries, index.dim))
        return DERIVED.get((index.oid, "flat"), build, _owner(index))

    def search(self, index, q: tuple, k, threshold, eligible) -> list:
        """Exact k nearest / threshold search over the members satisfying ``eligible``."""
        snap = self._snapshot(index)
        entries, matrix, norms = snap.entries, snap.matrix, snap.norms
        np = self._numpy(index)
        if eligible is not None:
            positions = [i for i, e in enumerate(entries) if eligible(e[1])]
            entries = [entries[i] for i in positions]
            if matrix is not None and np is not None:
                sel = np.asarray(positions, dtype=np.intp)
                matrix, norms = matrix[sel], norms[sel]
        if matrix is None or np is None:
            return rank_entries(entries, q, index.metric, k, threshold)
        return screened_rank(np, index.metric, entries, matrix, norms, q, k, threshold)

    def estimate(self, index, k, threshold, filtered: bool) -> Estimate:
        """Touches every member (twice when a predicate is evaluated)."""
        members = 0 if index.store is None else index.store.count
        rows = self._rows(index, k, threshold, filtered)
        return Estimate(rows=rows, cost=members * (2 if filtered else 1) + rows + 1)


def _kmeans(vectors: list, nlist: int, iters: int, seed: int) -> list:
    """Deterministic Lloyd iterations from ``nlist`` seeded samples (numpy when available)."""
    rng = random.Random(seed)
    centroids = [list(vectors[i]) for i in sorted(rng.sample(range(len(vectors)), nlist))]
    np = numpy_module()
    if np is not None:
        data = np.asarray(vectors, dtype=np.float64)
        cent = np.asarray(centroids, dtype=np.float64)
        for _ in range(iters):
            d = (np.einsum("ij,ij->i", data, data)[:, None] - 2 * data @ cent.T
                 + np.einsum("ij,ij->i", cent, cent)[None, :])
            labels = np.argmin(d, axis=1)
            for j in range(nlist):
                members = data[labels == j]
                if len(members):
                    cent[j] = members.mean(axis=0)
        return [list(map(float, row)) for row in cent]
    for _ in range(iters):
        sums = [[0.0] * len(vectors[0]) for _ in range(nlist)]
        counts = [0] * nlist
        for v in vectors:
            j = min(range(nlist), key=lambda c: (_sum_nonnegative((a - b) ** 2 for a, b in zip(v, centroids[c])), c))
            counts[j] += 1
            sums[j] = [s + x for s, x in zip(sums[j], v)]
        centroids = [[s / counts[j] for s in sums[j]] if counts[j] else centroids[j] for j in range(nlist)]
    return centroids


def _training_order(v: tuple) -> bytes:
    """A total order of vectors that depends on their values only: their IEEE 754 binary64
    big-endian bytes (``-0.0`` and ``0.0`` apart, so equal-comparing vectors keep no input order)."""
    return struct.pack(f">{len(v)}d", *v)


def _normalized(v) -> tuple:
    n = math.sqrt(math.fsum(x * x for x in v))
    return tuple(v) if n == 0.0 else tuple(x / n for x in v)


class IvfFlatBackend(_Backend):
    """Inverted lists around persisted centroids (module docstring)."""

    name = "ivf_flat"
    kind = "VectorIvfFlat"
    DEFAULTS = {"nlist": 16, "nprobe": 4, "train_iters": 10, "seed": 0, "retrain_growth": 4.0}

    @classmethod
    def check(cls, params: dict) -> dict:
        """``nlist`` (lists, default 16), ``nprobe`` (1..nlist, default min(4, nlist)), ``train_iters``
        (default 10), ``seed`` (int >= 0, default 0), ``retrain_growth`` (> 1, default 4.0)."""
        nlist = _positive_int("nlist", params.get("nlist", 16))
        nprobe = _positive_int("nprobe", params.get("nprobe", min(4, nlist)))
        if nprobe > nlist:
            raise ProtoValidationError(f"nprobe ({nprobe}) cannot exceed nlist ({nlist})")
        seed = params.get("seed", 0)
        if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
            raise ProtoValidationError(f"Vector index parameter 'seed' must be an int >= 0, not {seed!r}")
        return {"nlist": nlist, "nprobe": nprobe, "train_iters": _positive_int("train_iters",
                                                                              params.get("train_iters", 10)),
                "seed": seed, "retrain_growth": _growth("retrain_growth", params.get("retrain_growth", 4.0))}

    @staticmethod
    def _assign_metric(metric: str) -> str:
        return "cosine" if metric == "cosine" else "l2"

    def __init__(self):
        self._lock = threading.Lock()
        self._centroid_matrix = None  # (centroids tuple, matrix, norms) of the last centroids used

    def _matrix_of(self, np, centroids: tuple) -> tuple:
        with self._lock:
            cached = self._centroid_matrix
        if cached is not None and cached[0] is centroids:
            return cached[1], cached[2]
        matrix, norms = _matrix(np, [(None, None, None, c) for c in centroids], len(centroids[0]))
        with self._lock:
            self._centroid_matrix = (centroids, matrix, norms)
        return matrix, norms

    def list_for(self, index, vector: tuple) -> int:
        """The best-scoring centroid (exact scores, lowest number on ties); -1 when untrained.

        With numpy, centroids scoring clearly below the best are screened out
        first (as in :func:`screened_rank`); the result is the same.
        """
        centroids = index.centroids
        if not centroids:
            return UNTRAINED
        metric = self._assign_metric(index.metric)
        candidates = range(len(centroids))
        np = numpy_module()
        if np is not None and len(centroids) >= 8:
            matrix, norms = self._matrix_of(np, centroids)
            qv = np.asarray(vector, dtype=np.float64)
            qnorm = float(np.sqrt(qv @ qv))
            with np.errstate(all="ignore"):
                approx = _numpy_scores(np, metric, qv, qnorm, matrix, norms)
                margin = _screen_margin(np, metric, approx, norms, qnorm)
            forced = _unreliable(metric, norms, qnorm)  # tiny or huge magnitudes: score every centroid
            if (forced is None or not forced.any()) and np.all(np.isfinite(approx)) and np.all(np.isfinite(margin)):
                cut = float(approx.max()) - 2 * float(margin.max())
                candidates = [int(j) for j in np.nonzero(approx >= cut)[0]]
        best, best_score = 0, None
        for j in candidates:
            s = exact_score(metric, vector, centroids[j])
            if best_score is None or s > best_score:
                best, best_score = j, s
        return best

    def needs_training(self, index) -> bool:
        """Untrained with at least ``nlist`` members, or grown ``retrain_growth`` times since training."""
        params = dict(index.params)
        members = 0 if index.store is None else index.store.count
        if not index.centroids:
            return members >= params["nlist"]
        return members >= params["retrain_growth"] * index.trained_size

    def train(self, index, vectors: list) -> tuple:
        """Deterministic k-means centroids (spherical for cosine); ``()`` below ``nlist`` vectors."""
        params = dict(index.params)
        nlist = params["nlist"]
        if len(vectors) < nlist:
            return ()
        # The training order is the vectors' own (R128): members arrive in member-key order,
        # which for atoms follows their oids (fresh on every run), so sampling from it made the
        # centroids, and the recall at nprobe < nlist, differ between runs on the same data.
        # Which vectors are trained on is the caller's: all of them for a bulk build, the members
        # present at a retrain threshold for an index grown by inserts.
        data = sorted(vectors, key=_training_order)
        if index.metric == "cosine":
            data = [_normalized(v) for v in data]
        centroids = _kmeans(data, nlist, params["train_iters"], params["seed"])
        if index.metric == "cosine":
            centroids = [_normalized(c) for c in centroids]
        return tuple(tuple(float(x) for x in c) for c in centroids)

    def _probe_order(self, index, q: tuple) -> list:
        lists = [UNTRAINED]
        if index.centroids:
            metric = self._assign_metric(index.metric)
            scored = sorted((-exact_score(metric, q, c), j) for j, c in enumerate(index.centroids))
            lists.extend(j for _, j in scored)
        return lists

    def _lists(self, index) -> dict:
        """``{list number: (entries, matrix, norms)}`` of one version (matrix None without numpy), cached."""
        def build():
            grouped: dict = {}
            for key, (element, count, vector) in avl.key_iter(index.store):
                grouped.setdefault(key[0], []).append((key[1], element, count, vector))
            np = numpy_module()
            out = {}
            for j, entries in grouped.items():
                matrix, norms = _matrix(np, entries, index.dim) if np is not None else (None, None)
                out[j] = (tuple(entries), matrix, norms)
            return out
        return DERIVED.get((index.oid, "ivf"), build, _owner(index))

    def search(self, index, q: tuple, k, threshold, eligible) -> list:
        """Probe the best ``nprobe`` lists (plus list -1); widen while fewer than ``k`` members match."""
        lists = self._lists(index)
        order = [j for j in self._probe_order(index, q) if j in lists]
        # list -1 (untrained vectors) is always probed, in addition to nprobe trained lists
        probe = dict(index.params)["nprobe"] + (1 if order and order[0] == UNTRAINED else 0)
        floor = score_floor(index.metric, threshold)
        done = 0
        probed: list = []
        matching = 0
        while True:
            for j in order[done:probe]:
                probed.append(j)
                entries = lists[j][0]
                if k is not None and eligible is None and floor is None:
                    matching += len(entries)
                elif k is not None:
                    for entry in entries:
                        if (eligible is None or eligible(entry[1])) and \
                                (floor is None or exact_score(index.metric, q, entry[3]) >= floor):
                            matching += 1
            done = min(probe, len(order))
            if done >= len(order) or k is None or matching >= k:
                break
            probe = min(len(order), 2 * probe)
        np = numpy_module()
        entries = [e for j in probed for e in lists[j][0]]
        if np is None or not probed:
            return rank_entries(_filtered(entries, eligible), q, index.metric, k, threshold)
        matrix = np.concatenate([lists[j][1] for j in probed])
        norms = np.concatenate([lists[j][2] for j in probed])
        if eligible is not None:
            keep = [i for i, e in enumerate(entries) if eligible(e[1])]
            entries = [entries[i] for i in keep]
            sel = np.asarray(keep, dtype=np.intp)
            matrix, norms = matrix[sel], norms[sel]
        return screened_rank(np, index.metric, entries, matrix, norms, q, k, threshold)

    def estimate(self, index, k, threshold, filtered: bool) -> Estimate:
        """Touches the centroids and the probed fraction of the members."""
        params = dict(index.params)
        members = 0 if index.store is None else index.store.count
        fraction = min(1.0, params["nprobe"] / params["nlist"]) if index.centroids else 1.0
        rows = self._rows(index, k, threshold, filtered)
        touched = members * fraction * (2 if filtered else 1)
        return Estimate(rows=rows, cost=len(index.centroids) + touched + rows + 1)

    def explain(self, index) -> dict:
        """``lists`` (trained centroids, 0 when untrained) and ``trained_size``."""
        return {"lists": len(index.centroids), "trained_size": index.trained_size}


_HNSW_SPACES = {"cosine": "cosine", "l2": "l2", "ip": "ip"}


class _HnswGraph:
    """An hnswlib index over the entries of one version (label i is ``entries[i]``)."""

    __slots__ = ("graph", "entries", "np")

    def __init__(self, graph, entries: tuple, np):
        self.graph = graph
        self.entries = entries
        self.np = np


class HnswBackend(_Backend):
    """Approximate search on an hnswlib graph derived from the store (module docstring)."""

    name = "hnsw"
    deterministic = False
    kind = "VectorHnsw"
    DEFAULTS = {"M": 16, "ef_construction": 200, "ef_search": 64, "seed": 0}

    @classmethod
    def check(cls, params: dict) -> dict:
        """``M`` (>= 2, default 16), ``ef_construction`` (default 200), ``ef_search`` (default 64),
        ``seed`` (int >= 0, default 0)."""
        m = _positive_int("M", params.get("M", 16))
        if m < 2:
            raise ProtoValidationError(f"Vector index parameter 'M' must be >= 2, not {m!r}")
        seed = params.get("seed", 0)
        if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
            raise ProtoValidationError(f"Vector index parameter 'seed' must be an int >= 0, not {seed!r}")
        return {"M": m, "ef_construction": _positive_int("ef_construction", params.get("ef_construction", 200)),
                "ef_search": _positive_int("ef_search", params.get("ef_search", 64)), "seed": seed}

    def _graph(self, index) -> _HnswGraph:
        def build():
            hnswlib = load_hnswlib()
            np = numpy_module()
            if np is None:
                raise ProtoNotSupportedError("The 'hnsw' vector backend needs numpy (hnswlib depends on it)")
            entries = tuple(iter_entries(index.store))
            params = dict(index.params)
            graph = hnswlib.Index(space=_HNSW_SPACES[index.metric], dim=index.dim)
            graph.init_index(max_elements=max(1, len(entries)), ef_construction=params["ef_construction"],
                             M=params["M"], random_seed=params["seed"])
            if entries:
                data = np.asarray([e[3] for e in entries], dtype=np.float32).reshape(len(entries), index.dim)
                graph.add_items(data, np.arange(len(entries)), num_threads=1)
            graph.set_ef(params["ef_search"])
            return _HnswGraph(graph, entries, np)
        return DERIVED.get((index.oid, "hnsw"), build, _owner(index))

    def search(self, index, q: tuple, k, threshold, eligible) -> list:
        """Widen the hnsw candidate count until the answer is settled (module docstring)."""
        g = self._graph(index)
        entries, total = g.entries, len(g.entries)
        if total == 0:
            return []
        floor = score_floor(index.metric, threshold)
        query = g.np.asarray([q], dtype=g.np.float32)
        want = k if k is not None else min(total, dict(index.params)["ef_search"])
        while want < total:
            labels, _ = g.graph.knn_query(query, k=want)
            batch = [entries[int(i)] for i in labels[0]]
            scores = [exact_score(index.metric, q, entry[3]) for entry in batch]
            worst = min(scores)
            matching = sorted((s for s, entry in zip(scores, batch)
                               if (floor is None or s >= floor) and (eligible is None or eligible(entry[1]))),
                              reverse=True)
            # Settled when the batch reaches past the k-th match (so every member tied with it is in the
            # batch and the member-key tie-break decides, as in the exact backends) or past the threshold.
            if (k is not None and len(matching) >= k and worst < matching[k - 1]) or \
                    (floor is not None and worst < floor):
                return rank_entries(_filtered(batch, eligible), q, index.metric, k, threshold)
            want = min(total, 2 * want)
        return rank_entries(_filtered(entries, eligible), q, index.metric, k, threshold)

    def estimate(self, index, k, threshold, filtered: bool) -> Estimate:
        """About ``ef`` candidates per graph level visited, plus the predicate on those candidates."""
        members = 0 if index.store is None else index.store.count
        ef = max(dict(index.params)["ef_search"], k or 0)
        rows = self._rows(index, k, threshold, filtered)
        visited = min(members, ef * math.log2(members + 2))
        return Estimate(rows=rows, cost=visited * (2 if filtered else 1) + rows + 1)


_BACKENDS = {"exact": ExactBackend(), "ivf_flat": IvfFlatBackend(), "hnsw": HnswBackend()}


def backend_for(name: str) -> _Backend:
    """The backend strategy named ``name``; ``ProtoValidationError`` for an unknown name."""
    impl = _BACKENDS.get(name) if isinstance(name, str) else None
    if impl is None:
        raise ProtoValidationError(f"Unknown vector backend {name!r}; expected one of {list(BACKENDS)}")
    return impl
