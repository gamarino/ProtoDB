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
