from __future__ import annotations

from typing import Iterable, Optional, Sequence, Any

# Optional imports
try:
    import pyarrow as pa  # type: ignore
    import pyarrow.parquet as pq  # type: ignore
    import pyarrow.dataset as ds  # type: ignore
except Exception:  # pragma: no cover
    pa = None
    pq = None
    ds = None

try:
    import numpy as np  # type: ignore
except Exception:  # pragma: no cover
    np = None


class ArrowNotAvailable(RuntimeError):
    pass


def _require_arrow():
    if pa is None or pq is None:
        raise ArrowNotAvailable(
            "pyarrow is required for Arrow/Parquet integration. Install 'pyarrow' to enable this feature.")


def vectors_fixed_size_list(array: Any, dim: int) -> Any:
    """
    Given a 2D-like vectors buffer (N x dim) return a pyarrow Array of FixedSizeList<float32>[dim].
    Accepts:
      - numpy array of shape (N, dim) dtype float32
      - Python list of lists (will copy)
    """
    _require_arrow()
    if np is not None and isinstance(array, np.ndarray):
        if array.dtype != np.float32:
            raise ValueError("Expected float32 for vectors")
        if array.ndim != 2 or array.shape[1] != dim:
            raise ValueError("Expected shape (N, dim)")
        # Zero-copy to Arrow if possible
        buf = pa.py_buffer(array)  # may be zero-copy
        flat = pa.Array.from_buffers(pa.float32(), array.size, [None, buf])
        return pa.FixedSizeListArray.from_arrays(flat, dim)
    # fallback: list of lists
    flat_list = [float(x) for row in array for x in row]
    flat = pa.array(flat_list, type=pa.float32())
    return pa.FixedSizeListArray.from_arrays(flat, dim)


def to_arrow(records: Iterable[dict], columns: Optional[Sequence[str]] = None,
             batch_size: int = 65536, zero_copy: bool = True) -> Any:
    """
    Build a pyarrow.Table from an iterable of dict rows. Minimal implementation: collect into columns.
    If zero_copy and numpy arrays are provided in values, attempt zero-copy conversion.
    """
    _require_arrow()
    # Collect rows into column-wise lists; small memory for examples.
    cols: dict[str, list] = {}
    for row in records:
        if columns is None:
            keys = row.keys()
        else:
            keys = columns
        for k in keys:
            cols.setdefault(k, []).append(row.get(k))
    pa_arrays = {}
    for k, col in cols.items():
        # Recognize numpy vector 2D and convert via vectors_fixed_size_list if tagged
        if np is not None and any(hasattr(x, 'shape') for x in col):
            # not robust; leave for custom usage
            pa_arrays[k] = pa.array(col)
        else:
            pa_arrays[k] = pa.array(col)
    return pa.table(pa_arrays)


def to_parquet_from_records(records: Iterable[dict], path: str, columns: Optional[Sequence[str]] = None,
                             row_group_size_bytes: int = 128 * 1024 * 1024, compression: str = 'zstd',
                             dict_encoding: bool = True) -> None:
    _require_arrow()
    table = to_arrow(records, columns=columns)
    pq.write_table(table, path, compression=compression, use_dictionary=dict_encoding,
                   row_group_size=row_group_size_bytes)


def table_to_parquet(table: Any, path: str, row_group_size_bytes: int = 128 * 1024 * 1024,
                     compression: str = 'zstd', dict_encoding: bool = True) -> None:
    _require_arrow()
    pq.write_table(table, path, compression=compression, use_dictionary=dict_encoding,
                   row_group_size=row_group_size_bytes)


def scan_parquet(path: str, columns: Optional[Sequence[str]] = None, filters: Optional[Any] = None) -> Any:
    _require_arrow()
    dataset = ds.dataset(path, format="parquet")
    return dataset.to_table(columns=columns, filter=filters)


def from_parquet_to_rows(path: str, columns: Optional[Sequence[str]] = None, filters: Optional[Any] = None):
    _require_arrow()
    table = scan_parquet(path, columns=columns, filters=filters)
    # Return list of dicts
    return table.to_pylist()
