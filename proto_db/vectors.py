from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional, Union
import math


def _validate_floats(values: Iterable[float]) -> None:
    for v in values:
        if v is None or math.isnan(v) or math.isinf(v):
            raise ValueError("Vector contains invalid value (NaN/Inf/None)")


def _norm2(values: Iterable[float]) -> float:
    return math.sqrt(sum((x * x) for x in values))


def cosine_similarity(a: Iterable[float], b: Iterable[float]) -> float:
    """Compute cosine similarity in pure Python (no numpy dependency)."""
    # Convert to lists for multiple passes
    a_list = list(a)
    b_list = list(b)
    if len(a_list) != len(b_list):
        raise ValueError("Vectors have different dimensions")
    _validate_floats(a_list)
    _validate_floats(b_list)
    dot = sum(x * y for x, y in zip(a_list, b_list))
    na = _norm2(a_list)
    nb = _norm2(b_list)
    if na == 0.0 or nb == 0.0:
        return 0.0
    return dot / (na * nb)


def l2_distance(a: Iterable[float], b: Iterable[float]) -> float:
    a_list = list(a)
    b_list = list(b)
    if len(a_list) != len(b_list):
        raise ValueError("Vectors have different dimensions")
    _validate_floats(a_list)
    _validate_floats(b_list)
    return math.sqrt(sum((x - y) * (x - y) for x, y in zip(a_list, b_list)))


@dataclass(frozen=True)
class Vector:
    """
    Simple immutable vector wrapper.
    - data: list of float32-like values (stored as Python floats).
    - dim: dimension (validated against data length).
    - normalized: whether data was L2-normalized (for cosine).

    Zero-copy buffers
    -----------------
    - from_buffer(buf, dtype='float32', copy='auto'|'true'|'false')
    - as_buffer() -> memoryview
    - as_numpy(copy: bool | None = None)
    """
    data: tuple[float, ...]
    dim: int
    normalized: bool = False

    @staticmethod
    def from_list(values: List[float], normalize: bool = False) -> "Vector":
        if values is None:
            raise ValueError("Vector cannot be None")
        _validate_floats(values)
        dim = len(values)
        if dim <= 0:
            raise ValueError("Vector must have positive dimension")
        if normalize:
            n = _norm2(values)
            if n == 0.0:
                raise ValueError("Cannot normalize zero vector")
            values = [v / n for v in values]
        return Vector(tuple(float(v) for v in values), dim, normalize)

    def to_list(self) -> List[float]:
        return list(self.data)

    def as_iterable(self) -> Iterable[float]:
        return self.data

    def similarity(self, other: "Union[Vector, Iterable[float]]", metric: str = "cosine") -> float:
        other_data = other.data if isinstance(other, Vector) else list(other)
        if metric == "cosine":
            return cosine_similarity(self.data, other_data)
        elif metric == "l2":
            # convert to a similarity-like score (negative distance)
            return -l2_distance(self.data, other_data)
        else:
            raise ValueError(f"Unsupported metric: {metric}")

    def to_bytes(self) -> bytes:
        # Compact binary: 4 bytes dim + 1 byte normalized + 8 bytes per float (double)
        # Avoid external deps; use struct
        import struct
        header = struct.pack("<Ib", self.dim, 1 if self.normalized else 0)
        body = b"".join(struct.pack("<d", float(x)) for x in self.data)
        return header + body

    @staticmethod
    def from_buffer(buf, dtype: str = 'float32', copy: str = 'auto') -> "Vector":
        """
        Create a Vector from a buffer (bytes, bytearray, memoryview, or numpy array if available).
        copy semantics:
          - 'false': never copy; requires C-contiguous, little-endian float32; otherwise raises.
          - 'auto': no copy if compatible; else copy once.
          - 'true': always copy into Python floats.
        """
        # numpy path for zero-copy
        try:
            import numpy as _np  # local optional
        except Exception:
            _np = None
        if _np is not None and isinstance(buf, _np.ndarray):
            arr = buf
            if dtype != 'float32':
                raise ValueError("Only float32 supported by default")
            if arr.dtype != _np.float32:
                if copy in ('true', 'auto'):
                    arr = arr.astype(_np.float32, copy=True)
                else:
                    raise ValueError("copy='false' requires float32 array")
            if not arr.flags['C_CONTIGUOUS']:
                if copy in ('true', 'auto'):
                    arr = _np.ascontiguousarray(arr)
                else:
                    raise ValueError("copy='false' requires C-contiguous array")
            values = arr.tolist() if copy == 'true' else [float(v) for v in arr]
            return Vector.from_list(values, normalize=False)
        # generic buffer
        mv = memoryview(buf)
        if dtype != 'float32':
            raise ValueError("Only float32 supported by default")
        if mv.format not in ('f', '<f'):  # float32
            if copy == 'false':
                raise ValueError("copy='false' requires float32 buffer")
            # attempt single copy: interpret as little-endian 4-byte chunks
            import struct
            floats = [struct.unpack('<f', mv[i:i+4])[0] for i in range(0, len(mv), 4)]
            return Vector.from_list(floats, normalize=False)
        # zero-copy read via memoryview of float32
        try:
            if mv.format in ('f', '<f'):
                values = list(mv)
            else:
                fa = mv.cast('f')
                values = list(fa)
            return Vector.from_list(values, normalize=False)
        finally:
            mv.release()

    def as_buffer(self) -> memoryview:
        """Return a read-only memoryview of float32 little-endian values (copy once)."""
        import array
        arr = array.array('f', [float(x) for x in self.data])
        mv = memoryview(arr)
        return mv

    def as_numpy(self, copy: bool | None = None):
        """
        Return a numpy array view if numpy is available.
        - copy=False: try zero-copy by building from buffer; may still copy to match dtype/contiguity.
        - copy=True: always copy into a new ndarray.
        - copy=None: default to zero-copy attempt.
        """
        try:
            import numpy as _np
        except Exception:
            raise RuntimeError("NumPy is required for as_numpy(); install numpy")
        mv = self.as_buffer()
        arr = _np.frombuffer(mv, dtype=_np.float32, count=self.dim)
        if copy:
            return arr.copy()
        return arr

    @staticmethod
    def from_bytes(b: bytes) -> "Vector":
        import struct
        if len(b) < 5:
            raise ValueError("Invalid vector bytes")
        dim, norm_flag = struct.unpack("<Ib", b[:5])
        expected = 5 + 8 * dim
        if len(b) != expected:
            raise ValueError("Invalid vector bytes length")
        vals = []
        off = 5
        for _ in range(dim):
            (v,) = struct.unpack("<d", b[off:off+8])
            off += 8
            vals.append(v)
        return Vector.from_list(vals, normalize=False)._replace_normalized(bool(norm_flag))

    # helper to adjust normalized flag when reconstructing
    def _replace_normalized(self, normalized: bool) -> "Vector":
        return Vector(self.data, self.dim, normalized)
