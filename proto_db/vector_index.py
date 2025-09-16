from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Iterable, List, Tuple, Any, Optional

from .vectors import Vector, cosine_similarity, l2_distance
from .queries import QueryableIndex, QueryContext, Term, Near, VectorSearchPlan


class VectorIndex(QueryableIndex, ABC):
    """
    Abstract vector index interface and QueryableIndex participant.
    Concrete implementations may be ANN structures (e.g., HNSW) or exact.
    """

    # Satisfy DBCollections API minimally; indices are not iterated as collections.
    def as_query_plan(self):
        from .queries import ListPlan as _ListPlan
        return _ListPlan(base_list=[], transaction=getattr(self, 'transaction', None))

    # Cooperative planner hook: default to not handling any term
    def build_query_plan(self, term: Term, context: QueryContext) -> Optional[VectorSearchPlan]:
        return None

    @abstractmethod
    def build(self, vectors: Iterable[Vector] | Iterable[Iterable[float]], ids: Iterable[Any],
              metric: str = 'cosine', params: dict | None = None) -> None:
        ...

    @abstractmethod
    def add(self, id: Any, vector: Vector | Iterable[float]) -> None:
        ...

    @abstractmethod
    def remove(self, id: Any) -> None:
        ...

    @abstractmethod
    def search(self, query: Vector | Iterable[float], k: int, metric: str | None = None,
               params: dict | None = None) -> List[Tuple[Any, float]]:
        """Return list of (id, score). For cosine: score is similarity; for l2: negative distance."""
        ...

    @abstractmethod
    def range_search(self, query: Vector | Iterable[float], threshold: float, metric: str | None = None,
                     params: dict | None = None) -> List[Tuple[Any, float]]:
        ...

    @abstractmethod
    def save(self, path_or_bytes) -> None:
        ...

    @abstractmethod
    def load(self, path_or_bytes) -> None:
        ...

    @abstractmethod
    def stats(self) -> dict:
        ...


class ExactVectorIndex(VectorIndex):
    """
    Simple exact index using in-memory dictionary.
    Suitable as a correctness fallback when ANN libs are unavailable.

    Optimized for batch queries when NumPy is available.
    """
    def __init__(self, metric: str = 'cosine'):
        self._metric = metric
        self._dim: int | None = None
        self._vectors: Dict[Any, Vector] = {}
        self._tombstones: set[Any] = set()

    def _to_vector(self, v: Vector | Iterable[float]) -> Vector:
        if isinstance(v, Vector):
            return v
        return Vector.from_list(list(v), normalize=(self._metric == 'cosine'))

    def build(self, vectors: Iterable[Vector] | Iterable[Iterable[float]], ids: Iterable[Any],
              metric: str = 'cosine', params: dict | None = None) -> None:
        self._metric = metric or self._metric
        self._vectors.clear()
        self._tombstones.clear()
        for vid, vec in zip(ids, vectors):
            v = self._to_vector(vec)
            if self._dim is None:
                self._dim = v.dim
            elif v.dim != self._dim:
                raise ValueError("Inconsistent vector dimensions in build()")
            self._vectors[vid] = v

    def add(self, id: Any, vector: Vector | Iterable[float]) -> None:
        v = self._to_vector(vector)
        if self._dim is None:
            self._dim = v.dim
        elif v.dim != self._dim:
            raise ValueError("Vector dim mismatch")
        self._vectors[id] = v
        self._tombstones.discard(id)

    def remove(self, id: Any) -> None:
        # Soft delete (tombstone). Keep vector to allow simple rebuild decisions.
        if id in self._vectors:
            self._tombstones.add(id)

    def _sim(self, a: Vector, b: Vector) -> float:
        if self._metric == 'cosine':
            return cosine_similarity(a.data, b.data)
        elif self._metric == 'l2':
            # Return negative distance so higher is better
            return -l2_distance(a.data, b.data)
        else:
            raise ValueError(f"Unsupported metric {self._metric}")

    def search(self, query: Vector | Iterable[float], k: int, metric: str | None = None,
               params: dict | None = None) -> List[Tuple[Any, float]]:
        # Single-query path
        q = self._to_vector(query)
        metric = metric or self._metric
        # Compute all scores
        pairs: List[Tuple[Any, float]] = []
        for vid, v in self._vectors.items():
            if vid in self._tombstones:
                continue
            if metric == 'cosine':
                s = cosine_similarity(q.data, v.data)
            else:
                s = -l2_distance(q.data, v.data)
            pairs.append((vid, s))
        pairs.sort(key=lambda t: t[1], reverse=True)
        return pairs[: max(0, k)]

    # Optional optimized batch API
    def search_batch(self, queries: Iterable[Vector | Iterable[float]], k: int, metric: str | None = None) -> List[List[Tuple[Any, float]]]:
        metric = metric or self._metric
        try:
            import numpy as _np  # local import to keep dependency optional
        except Exception:
            # Fallback: iterate one by one
            return [self.search(q, k, metric=metric) for q in queries]
        # Build matrix of dataset once
        ids = [vid for vid in self._vectors.keys() if vid not in self._tombstones]
        if not ids:
            return [[] for _ in queries]
        A = _np.array([list(self._vectors[vid].data) for vid in ids], dtype=_np.float32)  # (n, d)
        Q = _np.array([list(self._to_vector(q).data) for q in queries], dtype=_np.float32)  # (m, d)
        results: List[List[Tuple[Any, float]]] = []
        if metric == 'cosine':
            sims = Q @ A.T  # (m, n)
            # top-k for each row
            for i in range(sims.shape[0]):
                row = sims[i]
                if k < len(row):
                    idx = _np.argpartition(-row, k-1)[:k]
                    idx = idx[_np.argsort(-row[idx])]
                else:
                    idx = _np.argsort(-row)
                results.append([(ids[j], float(row[j])) for j in idx])
        else:
            # l2: use ||a-b||^2 = ||a||^2 + ||b||^2 - 2 a·b
            a2 = _np.sum(A*A, axis=1)  # (n,)
            q2 = _np.sum(Q*Q, axis=1)  # (m,)
            dots = Q @ A.T
            d2 = (q2[:, None] + a2[None, :] - 2.0 * dots)
            for i in range(d2.shape[0]):
                row = d2[i]
                if k < len(row):
                    idx = _np.argpartition(row, k-1)[:k]
                    idx = idx[_np.argsort(row[idx])]
                else:
                    idx = _np.argsort(row)
                # convert to score (negative distance)
                results.append([(ids[j], -float(row[j])**0.5) for j in idx])
        return results

    def range_search(self, query: Vector | Iterable[float], threshold: float, metric: str | None = None,
                     params: dict | None = None) -> List[Tuple[Any, float]]:
        q = self._to_vector(query)
        metric = metric or self._metric
        out: List[Tuple[Any, float]] = []
        for vid, v in self._vectors.items():
            if vid in self._tombstones:
                continue
            if metric == 'cosine':
                s = cosine_similarity(q.data, v.data)
                if s >= threshold:
                    out.append((vid, s))
            else:
                # l2 threshold interpreted as max distance
                d = l2_distance(q.data, v.data)
                if d <= threshold:
                    out.append((vid, -d))
        out.sort(key=lambda t: t[1], reverse=True)
        return out

    def save(self, path_or_bytes) -> None:
        # Minimal: do nothing (in-memory only). Could be extended to write JSON/binary.
        return None

    def load(self, path_or_bytes) -> None:
        # Minimal: do nothing.
        return None

    def stats(self) -> dict:
        return {
            'backend': 'exact',
            'n_vecs': len(self._vectors) - len(self._tombstones),
            'dim': self._dim,
            'metric': self._metric,
            'tombstones': len(self._tombstones),
        }


# Optional HNSW-based index (ANN). Falls back to ExactVectorIndex if hnswlib is unavailable.
try:
    import hnswlib  # type: ignore
    import numpy as _np  # hnswlib expects numpy arrays
except Exception:  # pragma: no cover - optional dependency
    hnswlib = None  # type: ignore
    _np = None  # type: ignore


class HNSWVectorIndex(VectorIndex):
    """
    HNSW-based ANN index using hnswlib (optional dependency).
    - Supports cosine and l2 metrics.
    - Maintains soft-deletes via mark_deleted; exposes tombstone count; provides rebuild().
    - Persists index and id mapping via save(path_prefix) / load(path_prefix).
    """
    def __init__(self, metric: str = 'cosine', M: int = 16, efConstruction: int = 200, efSearch: int = 64):
        self._metric = metric
        self._dim: int | None = None
        self._index = None
        self._vectors: Dict[Any, Vector] = {}
        self._id_to_label: Dict[Any, int] = {}
        self._label_to_id: Dict[int, Any] = {}
        self._next_label: int = 0
        self._tombstones: set[Any] = set()
        self._params = {'M': M, 'efConstruction': efConstruction, 'efSearch': efSearch}
        # Fallback exact index if hnswlib (or numpy) is not present
        self._fallback = None if (hnswlib and _np) else ExactVectorIndex(metric=metric)

    # QueryableIndex implementation: build a VectorSearchPlan for Near terms
    def build_query_plan(self, term: Term, context: QueryContext) -> Optional[VectorSearchPlan]:
        try:
            if not isinstance(term, Term) or not isinstance(term.operation, Near):
                return None
            # Extract query vector and either threshold or k from term.value
            qv = None
            k = None
            threshold = None
            if isinstance(term.value, tuple) or isinstance(term.value, list):
                # Common shapes: (query_vec, threshold) or (query_vec, threshold, k)
                if len(term.value) >= 1:
                    qv = term.value[0]
                if len(term.value) >= 2 and term.value[1] is not None:
                    threshold = float(term.value[1])
                if len(term.value) >= 3 and term.value[2] is not None:
                    try:
                        k = int(term.value[2])
                    except Exception:
                        k = None
            elif isinstance(term.value, dict):
                qv = term.value.get('query') or term.value.get('vector')
                k = term.value.get('k')
                threshold = term.value.get('threshold')
            if qv is None:
                return None
            return VectorSearchPlan(index=self, query_vector=qv, k=k, threshold=threshold,
                                    metric=getattr(term.operation, 'metric', None),
                                    based_on=context.transaction and None,
                                    transaction=context.transaction)
        except Exception:
            return None

    def _to_vector(self, v: Vector | Iterable[float]) -> Vector:
        if isinstance(v, Vector):
            return v
        return Vector.from_list(list(v), normalize=(self._metric == 'cosine'))

    def _ensure_index(self):
        if self._fallback is not None:
            return
        if self._index is None:
            raise ValueError("Index not built. Call build() first or add() with initial vector to set dim.")

    def _space(self) -> str:
        # For cosine, use inner product space with normalized vectors for best performance
        if self._metric == 'cosine':
            return 'ip'
        elif self._metric == 'l2':
            return 'l2'
        else:
            raise ValueError(f"Unsupported metric: {self._metric}")

    def build(self, vectors: Iterable[Vector] | Iterable[Iterable[float]], ids: Iterable[Any],
              metric: str = 'cosine', params: dict | None = None) -> None:
        self._metric = metric or self._metric
        if params:
            self._params.update(params)
        self._vectors.clear()
        self._id_to_label.clear()
        self._label_to_id.clear()
        self._tombstones.clear()
        self._next_label = 0
        # Fallback path
        if self._fallback is not None:
            self._fallback.build(vectors, ids, metric=self._metric, params=params)
            return
        # hnsw path
        vec_list: List[Vector] = []
        id_list: List[Any] = []
        for vid, vec in zip(ids, vectors):
            v = self._to_vector(vec)
            vec_list.append(v)
            id_list.append(vid)
        if not vec_list:
            return
        self._dim = vec_list[0].dim
        for v in vec_list:
            if v.dim != self._dim:
                raise ValueError("Inconsistent vector dimensions in build()")
        np = _np  # use optional numpy bound at module import time
        data = np.array([list(v.data) for v in vec_list], dtype=np.float32)
        labels = np.arange(len(id_list), dtype=np.int64)
        # map ids
        for i, vid in enumerate(id_list):
            self._id_to_label[vid] = int(labels[i])
            self._label_to_id[int(labels[i])] = vid
            self._vectors[vid] = vec_list[i]
        self._next_label = len(id_list)
        self._index = hnswlib.Index(space=self._space(), dim=self._dim)  # type: ignore
        self._index.init_index(max_elements=len(id_list) or 1, M=self._params['M'], ef_construction=self._params['efConstruction'])
        self._index.add_items(data, labels)
        self._index.set_ef(self._params['efSearch'])

    def add(self, id: Any, vector: Vector | Iterable[float]) -> None:
        v = self._to_vector(vector)
        if self._fallback is not None:
            self._fallback.add(id, v)
            return
        if self._dim is None:
            # Initialize a new index on first add
            self._dim = v.dim
            self._index = hnswlib.Index(space=self._space(), dim=self._dim)  # type: ignore
            self._index.init_index(max_elements=1, M=self._params['M'], ef_construction=self._params['efConstruction'])
            self._index.set_ef(self._params['efSearch'])
        elif v.dim != self._dim:
            raise ValueError("Vector dim mismatch")
        # Resize capacity if needed
        current_max = int(self._index.get_max_elements()) if self._index is not None else 0
        current_count = int(self._index.get_current_count()) if self._index is not None else 0
        if self._index is not None and current_count + 1 > current_max:
            self._index.resize_index(current_count + 1)
        label = self._id_to_label.get(id)
        if label is None:
            label = self._next_label
            self._next_label += 1
        np = _np
        vec = np.array([list(v.data)], dtype=np.float32)
        self._index.add_items(vec, [label])
        self._id_to_label[id] = label
        self._label_to_id[label] = id
        self._vectors[id] = v
        self._tombstones.discard(id)

    def remove(self, id: Any) -> None:
        if self._fallback is not None:
            self._fallback.remove(id)
            return
        label = self._id_to_label.get(id)
        if label is None:
            return
        try:
            # hnswlib supports soft deletion
            self._index.mark_deleted(label)
            self._tombstones.add(id)
        except Exception:
            # If deletion not supported in this build, just record tombstone
            self._tombstones.add(id)

    def _scores_from_dist(self, dists) -> List[float]:
        # Convert library distances to scores (higher is better)
        # With space='ip' for cosine, hnswlib returns inner product directly.
        if self._metric == 'cosine':
            return [float(d) for d in dists]
        else:  # l2: smaller distances are better -> score is negative distance
            return [-float(d) for d in dists]

    def search(self, query: Vector | Iterable[float], k: int, metric: str | None = None,
               params: dict | None = None) -> List[Tuple[Any, float]]:
        q = self._to_vector(query)
        metric = metric or self._metric
        if metric != self._metric:
            # For now, require same metric as built
            raise ValueError("Metric mismatch between index and query")
        if self._fallback is not None:
            return self._fallback.search(q, k, metric=metric, params=params)
        self._ensure_index()
        self._index.set_ef((params or {}).get('efSearch', self._params['efSearch']))
        np = _np
        vec = np.array([list(q.data)], dtype=np.float32)
        labels, dists = self._index.knn_query(vec, k=k)
        out: List[Tuple[Any, float]] = []
        if getattr(labels, 'size', 0) > 0:
            scores = self._scores_from_dist(dists[0])
            for lab, score in zip(labels[0], scores):
                vid = self._label_to_id.get(int(lab))
                if vid is None or vid in self._tombstones:
                    continue
                out.append((vid, score))
        return out

    def range_search(self, query: Vector | Iterable[float], threshold: float, metric: str | None = None,
                     params: dict | None = None) -> List[Tuple[Any, float]]:
        q = self._to_vector(query)
        metric = metric or self._metric
        if metric != self._metric:
            raise ValueError("Metric mismatch between index and query")
        if self._fallback is not None:
            return self._fallback.range_search(q, threshold, metric=metric, params=params)
        self._ensure_index()
        # hnswlib lacks direct threshold search; approximate using large-k and filter
        max_candidates = (params or {}).get('max_candidates')
        n = int(self._index.get_current_count())
        k = min(n, int(max_candidates) if max_candidates else n)
        if k <= 0:
            return []
        np = _np
        vec = np.array([list(q.data)], dtype=np.float32)
        labels, dists = self._index.knn_query(vec, k=k)
        out: List[Tuple[Any, float]] = []
        if getattr(labels, 'size', 0) > 0:
            for lab, dist in zip(labels[0], dists[0]):
                vid = self._label_to_id.get(int(lab))
                if vid is None or vid in self._tombstones:
                    continue
                if self._metric == 'cosine':
                    score = float(dist)
                    if score >= float(threshold):
                        out.append((vid, score))
                else:
                    # l2: interpret threshold as max distance
                    if float(dist) <= float(threshold):
                        out.append((vid, -float(dist)))
        # Already roughly sorted by distance; convert to score sorting desc
        out.sort(key=lambda t: t[1], reverse=True)
        return out

    def rebuild(self) -> None:
        """Rebuild the index to drop tombstoned items and compact structure."""
        if self._fallback is not None:
            # Rebuild fallback simply by re-building
            items = [(vid, v) for vid, v in self._vectors.items() if vid not in self._tombstones]
            self._fallback.build((v for _, v in items), (vid for vid, _ in items), metric=self._metric)
            return
        # Collect alive vectors
        alive = [(vid, v) for vid, v in self._vectors.items() if vid not in self._tombstones]
        if not alive:
            # reset
            self._index = None
            self._id_to_label.clear()
            self._label_to_id.clear()
            self._next_label = 0
            return
        # Rebuild fresh
        self._id_to_label.clear()
        self._label_to_id.clear()
        self._next_label = 0
        self.build((v for _, v in alive), (vid for vid, _ in alive), metric=self._metric, params=self._params)

    def save(self, path_or_bytes) -> None:
        if self._fallback is not None:
            # No-op, or could pickle self._vectors
            return
        if not isinstance(path_or_bytes, str):
            raise ValueError("HNSWVectorIndex.save expects a file path prefix (str)")
        prefix = path_or_bytes
        index_path = f"{prefix}.hnsw"
        meta_path = f"{prefix}.meta.json"
        # Save index structure
        self._index.save_index(index_path)
        # Save metadata: mapping and params
        import json
        meta = {
            'metric': self._metric,
            'dim': self._dim,
            'params': self._params,
            'id_to_label': {str(k): int(v) for k, v in self._id_to_label.items()},
            # store vectors to allow exact rebuild if needed
            'vectors': {str(k): list(v.data) for k, v in self._vectors.items()},
            'tombstones': [str(k) for k in self._tombstones],
        }
        with open(meta_path, 'w', encoding='utf-8') as f:
            json.dump(meta, f)

    def load(self, path_or_bytes) -> None:
        if self._fallback is not None:
            # No-op
            return
        if not isinstance(path_or_bytes, str):
            raise ValueError("HNSWVectorIndex.load expects a file path prefix (str)")
        prefix = path_or_bytes
        index_path = f"{prefix}.hnsw"
        meta_path = f"{prefix}.meta.json"
        import json
        with open(meta_path, 'r', encoding='utf-8') as f:
            meta = json.load(f)
        self._metric = meta.get('metric', 'cosine')
        self._dim = int(meta.get('dim')) if meta.get('dim') is not None else None
        self._params.update(meta.get('params', {}))
        # Restore vectors and mappings
        self._vectors.clear()
        self._id_to_label.clear()
        self._label_to_id.clear()
        for k, arr in meta.get('vectors', {}).items():
            vid = k
            v = Vector.from_list(list(arr), normalize=False)
            self._vectors[vid] = v
        for k, v in meta.get('id_to_label', {}).items():
            self._id_to_label[k] = int(v)
            self._label_to_id[int(v)] = k
        self._tombstones = set(meta.get('tombstones', []))
        self._next_label = (max(self._label_to_id.keys()) + 1) if self._label_to_id else 0
        # Load index structure
        if self._dim is None:
            # Try to infer from any vector
            if self._vectors:
                any_v = next(iter(self._vectors.values()))
                self._dim = any_v.dim
            else:
                raise ValueError("Cannot infer dimension on load: missing vectors")
        self._index = hnswlib.Index(space=self._space(), dim=self._dim)  # type: ignore
        self._index.load_index(index_path)
        self._index.set_ef(self._params['efSearch'])

    def stats(self) -> dict:
        if self._fallback is not None:
            base = self._fallback.stats()
            base['backend'] = 'exact-fallback'
            return base
        return {
            'backend': 'hnsw',
            'n_vecs': len(self._vectors) - len(self._tombstones),
            'dim': self._dim,
            'metric': self._metric,
            'params': self._params.copy(),
            'tombstones': len(self._tombstones),
        }


class IVFFlatIndex(VectorIndex):
    """
    IVF-Flat index with immutable pages (copy-on-write), no persistent tombstones.
    - Coarse quantizer via simple k-means (if numpy available) or single-pass assignment fallback.
    - metric: 'cosine' (normalize and use inner product) or 'l2'.
    - Multiprobe search over top-nprobe centroids with per-page local top-k and global merge.
    - Persistence: save(path_prefix) -> prefix.ivf.meta.json + page blobs per centroid.
    """
    def __init__(self, metric: str = 'cosine', nlist: int = 256, nprobe: int = 8,
                 page_size: int = 1024, min_fill: float = 0.5):
        self._metric = metric
        self._dim: int | None = None
        self._nlist = int(max(1, nlist))
        self._nprobe = int(max(1, nprobe))
        self._page_size = int(max(1, page_size))
        self._min_fill = float(min_fill)
        # Data
        self._centroids = None  # numpy array (K,d) or list of tuples
        self._pages: Dict[int, list] = {}  # centroid_id -> list[{'ids': list, 'vecs': ndarray/list}]
        self._id_map: Dict[Any, tuple[int, int, int]] = {}  # id -> (cid,page_idx,offset)
        # Cache of all ids count
        self._count = 0

    # ---------------- Internal helpers -----------------
    def _to_vector(self, v: Vector | Iterable[float]) -> Vector:
        if isinstance(v, Vector):
            return v
        return Vector.from_list(list(v), normalize=(self._metric == 'cosine'))

    def _score_centroids(self, q) -> List[float]:
        try:
            import numpy as np
            C = self._centroids
            if C is None:
                return []
            qv = np.asarray(q, dtype=np.float32)
            if self._metric == 'cosine':
                return (C @ qv).tolist()
            else:
                # negative l2 distance for scores
                dif = C - qv[None, :]
                return (-np.sqrt((dif * dif).sum(axis=1))).tolist()
        except Exception:
            # Python fallback
            def dot(a, b):
                return sum(x*y for x, y in zip(a, b))
            scores = []
            for c in (self._centroids or []):
                if self._metric == 'cosine':
                    scores.append(dot(c, q))
                else:
                    from .vectors import l2_distance
                    scores.append(-l2_distance(c, q))
            return scores

    def _best_centroids(self, q, nprobe: int) -> List[int]:
        scores = self._score_centroids(q)
        if not scores:
            return []
        idx = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:max(1, nprobe)]
        return idx

    def _kmeans(self, data: List[Vector]):
        # minimal k-means with numpy if available; else pick first K as centroids
        K = min(self._nlist, max(1, len(data)))
        try:
            import numpy as np
            X = np.array([list(v.data) for v in data], dtype=np.float32)
            n, d = X.shape
            self._dim = d
            # init centroids using random selection
            rng = np.random.default_rng(42)
            if n < K:
                K = n
            idx = rng.choice(n, size=K, replace=False)
            C = X[idx].copy()
            iters = 5
            for _ in range(iters):
                # assign
                # cosine: argmax inner product (vectors already normalized)
                if self._metric == 'cosine':
                    dots = X @ C.T  # (n,K)
                    assign = np.argmax(dots, axis=1)
                else:
                    # l2: choose nearest
                    # compute squared distances efficiently
                    x2 = (X*X).sum(axis=1)[:, None]
                    c2 = (C*C).sum(axis=1)[None, :]
                    d2 = x2 + c2 - 2.0 * (X @ C.T)
                    assign = np.argmin(d2, axis=1)
                # update
                for k in range(K):
                    mask = assign == k
                    if not np.any(mask):
                        # reinitialize empty cluster
                        C[k] = X[rng.integers(0, n)]
                    else:
                        C[k] = X[mask].mean(axis=0)
                        if self._metric == 'cosine':
                            # renormalize for cosine
                            norm = np.linalg.norm(C[k])
                            if norm > 0:
                                C[k] /= norm
            self._centroids = C
            # build pages per centroid
            self._pages = {k: [] for k in range(K)}
            self._id_map.clear()
            self._count = 0
            # final assignment
            if self._metric == 'cosine':
                assign = np.argmax(X @ C.T, axis=1)
            else:
                x2 = (X*X).sum(axis=1)[:, None]
                c2 = (C*C).sum(axis=1)[None, :]
                d2 = x2 + c2 - 2.0 * (X @ C.T)
                assign = np.argmin(d2, axis=1)
            for i, vec in enumerate(data):
                cid = int(assign[i])
                self._append_to_centroid(cid, vec, data_id=data[i])
            # Note: _append_to_centroid with data_id Vector is wrong for id, will override below during build
        except Exception:
            # Fallback: pick first K as centroids
            self._dim = data[0].dim
            C = [list(v.data) for v in data[:K]]
            self._centroids = C
            self._pages = {k: [] for k in range(K)}
            self._id_map.clear()
            self._count = 0

    def _append_to_centroid(self, cid: int, v: Vector, data_id: Any):
        try:
            import numpy as np
            pages = self._pages.setdefault(cid, [])
            if pages and len(pages[-1]['ids']) < self._page_size:
                # COW: rewrite last page
                last = pages[-1]
                new_ids = list(last['ids'])
                new_ids.append(data_id)
                vecs = last['vecs']
                new_vecs = np.vstack([vecs, np.asarray(v.data, dtype=np.float32)])
                pages[-1] = {'ids': new_ids, 'vecs': new_vecs}
                offset = len(new_ids) - 1
                self._id_map[data_id] = (cid, len(pages)-1, offset)
            else:
                new_ids = [data_id]
                new_vecs = np.asarray([list(v.data)], dtype=np.float32)
                pages.append({'ids': new_ids, 'vecs': new_vecs})
                self._id_map[data_id] = (cid, len(pages)-1, 0)
            self._count += 1
        except Exception:
            # Python fallback with lists
            pages = self._pages.setdefault(cid, [])
            if pages and len(pages[-1]['ids']) < self._page_size:
                last = pages[-1]
                new_ids = list(last['ids'])
                new_ids.append(data_id)
                new_vecs = list(last['vecs'])
                new_vecs.append(list(v.data))
                pages[-1] = {'ids': new_ids, 'vecs': new_vecs}
                offset = len(new_ids) - 1
                self._id_map[data_id] = (cid, len(pages)-1, offset)
            else:
                pages.append({'ids': [data_id], 'vecs': [list(v.data)]})
                self._id_map[data_id] = (cid, len(pages)-1, 0)
            self._count += 1

    # ---------------- VectorIndex API -----------------
    def build(self, vectors: Iterable[Vector] | Iterable[Iterable[float]], ids: Iterable[Any],
              metric: str = 'cosine', params: dict | None = None) -> None:
        self._metric = metric or self._metric
        if params:
            self._nlist = int(params.get('nlist', self._nlist))
            self._nprobe = int(params.get('nprobe', self._nprobe))
            self._page_size = int(params.get('page_size', self._page_size))
            self._min_fill = float(params.get('min_fill', self._min_fill))
        # Normalize vectors for cosine
        data: List[Vector] = []
        id_list: List[Any] = []
        for vid, vec in zip(ids, vectors):
            v = self._to_vector(vec)
            data.append(v)
            id_list.append(vid)
        if not data:
            # reset
            self._centroids = None
            self._pages = {}
            self._id_map.clear()
            self._count = 0
            self._dim = None
            return
        # Train centroids and assign
        self._kmeans(data)
        # After kmeans used _append_to_centroid without ids; rebuild pages properly using id_list
        # Reset and reassign with true ids
        try:
            import numpy as np
            X = np.array([list(v.data) for v in data], dtype=np.float32)
            C = self._centroids
            K = (C.shape[0] if hasattr(C, 'shape') else len(C)) if C is not None else 0
            self._pages = {k: [] for k in range(K)}
            self._id_map.clear()
            self._count = 0
            if self._metric == 'cosine':
                assign = (X @ C.T).argmax(axis=1)
            else:
                x2 = (X*X).sum(axis=1)[:, None]
                c2 = (C*C).sum(axis=1)[None, :]
                d2 = x2 + c2 - 2.0 * (X @ C.T)
                assign = d2.argmin(axis=1)
            for i, v in enumerate(data):
                cid = int(assign[i])
                self._append_to_centroid(cid, v, id_list[i])
        except Exception:
            # Fallback: simple assignment by greedy best centroid
            self._pages = {k: [] for k in range(self._nlist)}
            self._id_map.clear()
            self._count = 0
            for vid, v in zip(id_list, data):
                # choose centroid with max score
                scores = self._score_centroids(v.data)
                cid = max(range(len(scores)), key=lambda i: scores[i]) if scores else 0
                self._append_to_centroid(cid, v, vid)

    def add(self, id: Any, vector: Vector | Iterable[float]) -> None:
        v = self._to_vector(vector)
        if self._dim is None:
            self._dim = v.dim
        elif v.dim != self._dim:
            raise ValueError("Vector dim mismatch")
        # choose centroid
        scores = self._score_centroids(v.data)
        cid = max(range(len(scores)), key=lambda i: scores[i]) if scores else 0
        self._append_to_centroid(cid, v, id)

    def remove(self, id: Any) -> None:
        loc = self._id_map.get(id)
        if not loc:
            return
        cid, pidx, off = loc
        pages = self._pages.get(cid, [])
        if pidx >= len(pages):
            return
        page = pages[pidx]
        # COW rewrite page without the item
        try:
            import numpy as np
            ids = list(page['ids'])
            vecs = page['vecs']
            # Build mask excluding off
            keep_idx = [i for i in range(len(ids)) if i != off]
            new_ids = [ids[i] for i in keep_idx]
            new_vecs = vecs[keep_idx] if hasattr(vecs, 'shape') else [page['vecs'][i] for i in keep_idx]
            pages[pidx] = {'ids': new_ids, 'vecs': new_vecs}
        except Exception:
            ids = list(page['ids'])
            vecs = list(page['vecs'])
            del ids[off]
            del vecs[off]
            pages[pidx] = {'ids': ids, 'vecs': vecs}
        # rebuild id_map entries for that page
        for i, _id in enumerate(pages[pidx]['ids']):
            self._id_map[_id] = (cid, pidx, i)
        # remove old id
        self._id_map.pop(id, None)
        self._count -= 1
        # Optional merge: if underfilled and neighbor also underfilled, merge
        if pidx+1 < len(pages):
            if len(pages[pidx]['ids']) < int(self._min_fill * self._page_size) and len(pages[pidx+1]['ids']) < int(self._min_fill * self._page_size):
                a = pages[pidx]
                b = pages[pidx+1]
                merged_ids = a['ids'] + b['ids']
                try:
                    import numpy as np
                    merged_vecs = None
                    if hasattr(a['vecs'], 'shape') and hasattr(b['vecs'], 'shape'):
                        merged_vecs = np.vstack([a['vecs'], b['vecs']])
                    else:
                        merged_vecs = list(a['vecs']) + list(b['vecs'])
                    pages[pidx] = {'ids': merged_ids, 'vecs': merged_vecs}
                    del pages[pidx+1]
                    for i, _id in enumerate(merged_ids):
                        self._id_map[_id] = (cid, pidx, i)
                except Exception:
                    merged_vecs = list(a['vecs']) + list(b['vecs'])
                    pages[pidx] = {'ids': merged_ids, 'vecs': merged_vecs}
                    del pages[pidx+1]
                    for i, _id in enumerate(merged_ids):
                        self._id_map[_id] = (cid, pidx, i)

    def search(self, query: Vector | Iterable[float], k: int, metric: str | None = None,
               params: dict | None = None) -> List[Tuple[Any, float]]:
        q = self._to_vector(query)
        metric = metric or self._metric
        if metric != self._metric:
            raise ValueError("Metric mismatch between index and query")
        nprobe = int((params or {}).get('nprobe', self._nprobe))
        alpha = float((params or {}).get('alpha', 3.0))
        best_c = self._best_centroids(q.data, nprobe)
        candidates: List[Tuple[Any, float]] = []
        import heapq
        # visited pages within chosen centroids
        for cid in best_c:
            for page in self._pages.get(cid, []):
                ids = page['ids']
                vecs = page['vecs']
                # compute scores for this page
                try:
                    import numpy as np
                    V = vecs if hasattr(vecs, 'shape') else np.asarray(vecs, dtype=np.float32)
                    qv = np.asarray(q.data, dtype=np.float32)
                    if self._metric == 'cosine':
                        scores = (V @ qv).tolist()
                    else:
                        dif = V - qv[None, :]
                        scores = (-np.sqrt((dif * dif).sum(axis=1))).tolist()
                except Exception:
                    scores = []
                    if self._metric == 'cosine':
                        for v in vecs:
                            from .vectors import cosine_similarity
                            scores.append(cosine_similarity(v, q.data))
                    else:
                        for v in vecs:
                            from .vectors import l2_distance
                            scores.append(-l2_distance(v, q.data))
                # local top-t
                t = min(len(scores), max(1, int(alpha * k)))
                idx = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:t]
                for i in idx:
                    candidates.append((ids[i], scores[i]))
        # global top-k
        if len(candidates) <= k:
            return sorted(candidates, key=lambda x: x[1], reverse=True)
        # use heap nlargest
        return heapq.nlargest(k, candidates, key=lambda x: x[1])

    def range_search(self, query: Vector | Iterable[float], threshold: float, metric: str | None = None,
                     params: dict | None = None) -> List[Tuple[Any, float]]:
        q = self._to_vector(query)
        metric = metric or self._metric
        if metric != self._metric:
            raise ValueError("Metric mismatch between index and query")
        nprobe = int((params or {}).get('nprobe', self._nprobe))
        best_c = self._best_centroids(q.data, nprobe)
        out: List[Tuple[Any, float]] = []
        for cid in best_c:
            for page in self._pages.get(cid, []):
                ids = page['ids']
                vecs = page['vecs']
                try:
                    import numpy as np
                    V = vecs if hasattr(vecs, 'shape') else np.asarray(vecs, dtype=np.float32)
                    qv = np.asarray(q.data, dtype=np.float32)
                    if self._metric == 'cosine':
                        scores = (V @ qv).tolist()
                        for i, s in enumerate(scores):
                            if s >= threshold:
                                out.append((ids[i], float(s)))
                    else:
                        dif = V - qv[None, :]
                        scores = (-np.sqrt((dif * dif).sum(axis=1))).tolist()
                        for i, s in enumerate(scores):
                            if -s <= threshold:  # s is -dist
                                out.append((ids[i], float(s)))
                except Exception:
                    from .vectors import cosine_similarity, l2_distance
                    for i, v in enumerate(vecs):
                        if self._metric == 'cosine':
                            s = cosine_similarity(v, q.data)
                            if s >= threshold:
                                out.append((ids[i], s))
                        else:
                            d = l2_distance(v, q.data)
                            if d <= threshold:
                                out.append((ids[i], -d))
        out.sort(key=lambda t: t[1], reverse=True)
        return out

    def save(self, path_or_bytes) -> None:
        if not isinstance(path_or_bytes, str):
            raise ValueError("IVFFlatIndex.save expects a file path prefix (str)")
        prefix = path_or_bytes
        meta_path = f"{prefix}.ivf.meta.json"
        # write pages to blobs and collect descriptors
        import json, os, tempfile
        page_files = {}
        for cid, pages in self._pages.items():
            pf_list = []
            for pidx, page in enumerate(pages):
                fname = f"{prefix}.ivf.c{cid}.p{pidx}.bin"
                # simple JSON-encoded blob for portability (acts as immutable page)
                blob = {
                    'ids': page['ids'],
                    'vecs': page['vecs'].tolist() if hasattr(page['vecs'], 'shape') else page['vecs']
                }
                with open(fname, 'w', encoding='utf-8') as f:
                    json.dump(blob, f)
                pf_list.append({'file': fname, 'len': len(page['ids'])})
            page_files[cid] = pf_list
        # Centroids to list
        C = None
        try:
            import numpy as np
            if self._centroids is not None:
                C = self._centroids.tolist() if hasattr(self._centroids, 'shape') else self._centroids
        except Exception:
            C = self._centroids
        meta = {
            'backend': 'ivf-flat',
            'metric': self._metric,
            'dim': self._dim,
            'params': {
                'nlist': self._nlist, 'nprobe': self._nprobe,
                'page_size': self._page_size, 'min_fill': self._min_fill
            },
            'centroids': C,
            'pages': page_files,
        }
        # atomic write of meta
        tmp = meta_path + ".tmp"
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(meta, f)
        import os
        if os.path.exists(meta_path):
            os.remove(meta_path)
        os.replace(tmp, meta_path)

    def load(self, path_or_bytes) -> None:
        if not isinstance(path_or_bytes, str):
            raise ValueError("IVFFlatIndex.load expects a file path prefix (str)")
        prefix = path_or_bytes
        meta_path = f"{prefix}.ivf.meta.json"
        import json
        with open(meta_path, 'r', encoding='utf-8') as f:
            meta = json.load(f)
        self._metric = meta.get('metric', 'cosine')
        self._dim = meta.get('dim')
        p = meta.get('params', {})
        self._nlist = int(p.get('nlist', self._nlist))
        self._nprobe = int(p.get('nprobe', self._nprobe))
        self._page_size = int(p.get('page_size', self._page_size))
        self._min_fill = float(p.get('min_fill', self._min_fill))
        self._centroids = meta.get('centroids')
        # load pages
        self._pages = {}
        self._id_map.clear()
        self._count = 0
        for cid_str, plist in meta.get('pages', {}).items():
            cid = int(cid_str)
            self._pages[cid] = []
            for pdesc in plist:
                fname = pdesc['file']
                with open(fname, 'r', encoding='utf-8') as f:
                    blob = json.load(f)
                ids = list(blob['ids'])
                vecs = blob['vecs']
                try:
                    import numpy as np
                    vecs_arr = np.asarray(vecs, dtype=np.float32)
                    page = {'ids': ids, 'vecs': vecs_arr}
                except Exception:
                    page = {'ids': ids, 'vecs': vecs}
                self._pages[cid].append(page)
                for i, _id in enumerate(ids):
                    self._id_map[_id] = (cid, len(self._pages[cid])-1, i)
                    self._count += 1

    def stats(self) -> dict:
        pages_total = sum(len(pgs) for pgs in self._pages.values()) if self._pages else 0
        pages_by_centroid = {int(cid): len(pgs) for cid, pgs in self._pages.items()}
        avg_fill = 0.0
        filled = 0
        for pgs in self._pages.values():
            for p in pgs:
                avg_fill += len(p['ids']) / float(self._page_size)
                filled += 1
        avg_fill = (avg_fill / filled) if filled else 0.0
        return {
            'backend': 'ivf-flat',
            'n_vecs': self._count,
            'dim': self._dim,
            'metric': self._metric,
            'nlist': self._nlist,
            'nprobe': self._nprobe,
            'page_size': self._page_size,
            'avg_fill': avg_fill,
            'pages_total': pages_total,
            'pages_by_centroid': pages_by_centroid,
        }
