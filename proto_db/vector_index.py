from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Iterable, List, Tuple, Any

from .vectors import Vector, cosine_similarity, l2_distance


class VectorIndex(ABC):
    """
    Abstract vector index interface.
    Concrete implementations may be ANN structures (e.g., HNSW) or exact.
    """

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
        if self._metric == 'cosine':
            return 'cosine'
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
        # Convert distances to scores (higher is better)
        if self._metric == 'cosine':
            # cosine distance in hnswlib is (1 - cosine_sim)
            return [1.0 - float(d) for d in dists]
        else:  # l2
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
        try:
            size = labels.size
        except Exception:
            size = 0
        if size > 0:
            scores = self._scores_from_dist(dists[0])
            for lab, score in zip(labels[0], scores):
                vid = self._label_to_id.get(int(lab))
                if vid is None:
                    continue
                if vid in self._tombstones:
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
        if labels.size > 0:
            for lab, dist in zip(labels[0], dists[0]):
                vid = self._label_to_id.get(int(lab))
                if vid is None or vid in self._tombstones:
                    continue
                if self._metric == 'cosine':
                    score = 1.0 - float(dist)
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
