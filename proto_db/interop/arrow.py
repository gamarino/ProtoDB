"""Apache Arrow and Parquet bridge (decision D42).

pyarrow is optional (extra ``parquet``) and imported lazily inside each
function; when it is missing every function raises ``ProtoNotSupportedError``
chained from the ``ImportError``.  Importing this module never imports pyarrow.

Functions:

- :func:`to_arrow` ``(source, columns=None, *, schema=None) -> pyarrow.Table``.
- :func:`vectors_fixed_size_list` ``(array, dim)``: ``FixedSizeList<float32>[dim]``.
- :func:`to_parquet`, :func:`scan_parquet`, :func:`from_parquet_to_rows`.

Sources (:func:`to_arrow`): an iterable of rows, a LINQ ``Queryable`` (it is
executed) or a ``QueryPlan`` (``execute()``).  A row is a mapping with string
keys (``dict``, ``FrozenDict``, the dicts produced by ``select``/``group_by``)
or a :class:`~proto_db.core.dbobject.DBObject` (its attributes).  ``str``,
``bytes`` and a mapping as the source are rejected.

Columns: ``columns`` selects and orders the columns; by default they are the
union of the rows' keys in first-seen order.  A key absent from a row, an
explicit ``None`` and ``MISSING`` all become null.  With ``schema`` (a
``pyarrow.Schema``) the columns are the schema's fields and each column is
converted to the field's type.

Value mapping (Python value after conversion -> Arrow type inferred by pyarrow):

========================================  ==========================================
ProtoDB value                             Arrow
========================================  ==========================================
``None``, ``MISSING``                     null
``bool`` / ``int`` / ``float``            bool / int64 / double (int beyond int64
                                          is an error: pass a decimal schema)
``str`` / ``bytes``                       string / binary
``Decimal``                               decimal128(p, s) (decimal256 when wider);
                                          NaN/Infinity rejected
``date`` / naive ``time``                 date32 / time64[us] (aware times are
                                          rejected: time64 has no time zone)
naive / aware ``datetime``                timestamp[us] / timestamp[us, tz] (tz of
                                          the column's first value; instants kept);
                                          naive and aware mixed at one position of
                                          a column are rejected
``timedelta``                             duration[us]
``UUID``                                  uuid (extension type)
``FrozenList``, ``list``, ``tuple``       list<...>
``FrozenDict``, ``dict``, ``DBObject``    struct<...> (string keys)
``MutableObject``, other atoms            rejected (``ProtoNotSupportedError``)
========================================  ==========================================

A column whose values pyarrow cannot convert to one type (e.g. ``int`` and
``str`` mixed), or holding values pyarrow would silently rewrite (naive and
aware datetimes mixed, aware times, non-finite Decimals), raises
``ProtoValidationError`` naming the column.  Vectors
stored as tuples become ``list<double>``; pass a schema with
``pyarrow.list_(pyarrow.float32(), dim)`` for a fixed-size list column.
"""
from __future__ import annotations

import numbers
import os
import sys
from collections.abc import Iterable, Iterator, Mapping, Sequence
from datetime import datetime, time
from decimal import Decimal

from ..core.dbobject import DBObject
from ..errors import ProtoNotSupportedError, ProtoValidationError
from ..query.linq import Queryable
from ..query.plans import QueryPlan
from ..values import MISSING, MutableObject, is_atom

__all__ = ["to_arrow", "vectors_fixed_size_list", "to_parquet", "scan_parquet", "from_parquet_to_rows"]

_INSTALL_HINT = ("pyarrow is required for the Arrow/Parquet bridge; install it with "
                 "pip install 'proto_db[parquet]' (or pip install pyarrow)")


def _pyarrow():
    try:
        import pyarrow
    except ImportError as e:
        raise ProtoNotSupportedError(_INSTALL_HINT) from e
    return pyarrow


def _parquet():
    _pyarrow()
    try:
        import pyarrow.parquet as pq
    except ImportError as e:
        raise ProtoNotSupportedError(_INSTALL_HINT) from e
    return pq


def _dataset():
    _pyarrow()
    try:
        import pyarrow.dataset as ds
    except ImportError as e:
        raise ProtoNotSupportedError(_INSTALL_HINT) from e
    return ds


# -- conversion of ProtoDB values -------------------------------------------------------------

def _describe_path(column: str, path: tuple) -> str:
    return repr(column) + "".join(p if p == "[]" else f".{p}" for p in path)


def _check_temporal(value, column: str, path: tuple, zones: dict) -> None:
    """Reject values pyarrow would silently rewrite: aware times, naive/aware datetimes mixed at one position."""
    if isinstance(value, time):
        if value.tzinfo is not None:
            raise ProtoValidationError(f"to_arrow: column {_describe_path(column, path)} holds an aware time "
                                       f"{value!r}; Arrow time64 has no time zone")
        return
    aware = value.tzinfo is not None and value.tzinfo.utcoffset(value) is not None
    seen = zones.setdefault(path, aware)
    if seen != aware:
        raise ProtoValidationError(f"to_arrow: column {_describe_path(column, path)} mixes naive and aware "
                                   f"datetimes; pyarrow would silently rewrite one kind into the other")


def _plain(value, column: str, path: tuple = (), zones: dict | None = None):
    """``value`` as plain Python for pyarrow (module docstring, "Value mapping").

    ``zones`` maps a position (``path`` inside the column) to whether its
    datetimes are aware, to reject mixtures (D42 fix round 1).
    """
    if zones is None:
        zones = {}
    if value is MISSING:
        return None
    if isinstance(value, (datetime, time)):
        _check_temporal(value, column, path, zones)
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ProtoValidationError(f"to_arrow: column {_describe_path(column, path)} holds the non-finite "
                                       f"Decimal {value}; Arrow decimals have no NaN or Infinity")
        return value
    if isinstance(value, DBObject):
        return {k: _plain(v, column, path + (k,), zones) for k, v in value._state().items()}
    if isinstance(value, MutableObject):
        raise ProtoNotSupportedError(f"to_arrow: column {column!r} holds a MutableObject handle "
                                     f"({value.name!r}); resolve it with a transaction first")
    if is_atom(value):
        raise ProtoNotSupportedError(f"to_arrow: column {column!r} holds a {type(value).__name__} atom; "
                                     f"convert it to a list or dict first")
    if isinstance(value, Mapping):
        out = {}
        for k, v in value.items():
            if not isinstance(k, str):
                raise ProtoValidationError(f"to_arrow: column {column!r} holds a mapping with a non-string key "
                                           f"{k!r}; Arrow structs need string keys")
            out[k] = _plain(v, column, path + (k,), zones)
        return out
    if isinstance(value, (list, tuple)):
        return [_plain(v, column, path + ("[]",), zones) for v in value]
    return value


def _record(row, index: int) -> Mapping:
    if isinstance(row, DBObject):
        return row._state()
    if isinstance(row, Mapping):
        for key in row:
            if not isinstance(key, str):
                raise ProtoValidationError(f"to_arrow: row {index} has a non-string key {key!r}")
        return row
    raise ProtoValidationError(f"to_arrow: row {index} is a {type(row).__name__}, not a mapping or DBObject "
                               f"(project rows with select({{...}}) first)")


def _iter_rows(source) -> Iterator:
    if isinstance(source, Queryable):
        return iter(source)
    if isinstance(source, QueryPlan):
        return source.execute()
    if isinstance(source, (str, bytes, bytearray, Mapping)) or not isinstance(source, Iterable):
        raise ProtoValidationError(f"to_arrow needs an iterable of rows, a Queryable or a QueryPlan, "
                                   f"not {type(source).__name__}")
    return iter(source)


def _check_columns(columns) -> list | None:
    if columns is None:
        return None
    if isinstance(columns, (str, bytes)) or not isinstance(columns, Sequence) \
            or not all(isinstance(c, str) for c in columns):
        raise ProtoValidationError(f"columns must be a sequence of column names, not {columns!r}")
    if len(set(columns)) != len(columns):
        raise ProtoValidationError(f"columns contains duplicate names: {list(columns)!r}")
    return list(columns)


def to_arrow(source, columns: Sequence[str] | None = None, *, schema=None):
    """Build a ``pyarrow.Table`` from rows, a Queryable or a QueryPlan (module docstring).

    ``columns`` selects and orders columns (default: union of the keys in
    first-seen order); ``schema`` (a ``pyarrow.Schema``) fixes names and types,
    and ``columns``, when also given, must equal its names.
    """
    pa = _pyarrow()
    columns = _check_columns(columns)
    if schema is not None:
        if not isinstance(schema, pa.Schema):
            raise ProtoValidationError(f"schema must be a pyarrow.Schema, not {type(schema).__name__}")
        if columns is not None and columns != schema.names:
            raise ProtoValidationError(f"columns {columns!r} differ from the schema's names {schema.names!r}")
        columns = list(schema.names)
    records = [_record(row, i) for i, row in enumerate(_iter_rows(source))]
    if columns is None:
        seen = {}
        for record in records:
            for key in record:
                seen.setdefault(key, None)
        columns = list(seen)
    arrays = []
    for name in columns:
        zones = {}
        values = [_plain(record.get(name, None), name, (), zones) for record in records]
        field_type = schema.field(name).type if schema is not None else None
        try:
            arrays.append(pa.array(values, type=field_type))
        except (pa.ArrowException, OverflowError) as e:
            raise ProtoValidationError(f"to_arrow: cannot convert column {name!r}: {e}") from e
    if schema is not None:
        return pa.Table.from_arrays(arrays, schema=schema)
    return pa.Table.from_arrays(arrays, names=columns)


def vectors_fixed_size_list(array, dim: int):
    """A ``FixedSizeList<float32>[dim]`` array from an ``(N, dim)`` buffer.

    Accepts a numpy ``float32`` array of shape ``(N, dim)`` (zero-copy when
    C-contiguous; other dtypes are rejected rather than copied silently -- use
    ``astype(numpy.float32)``) or an iterable of rows of ``dim`` real numbers
    (copied).  Shape or length mismatches raise ``ProtoValidationError``.
    """
    pa = _pyarrow()
    if not isinstance(dim, int) or isinstance(dim, bool) or dim < 1:
        raise ProtoValidationError(f"dim must be an int >= 1, not {dim!r}")
    np = sys.modules.get("numpy")
    if np is not None and isinstance(array, np.ndarray):
        if array.dtype != np.float32:
            raise ProtoValidationError(f"Expected a float32 array for vectors, got {array.dtype}; "
                                       f"use astype(numpy.float32)")
        if array.ndim != 2 or array.shape[1] != dim:
            raise ProtoValidationError(f"Expected an array of shape (N, {dim}), got {array.shape}")
        contiguous = np.ascontiguousarray(array)
        flat = pa.Array.from_buffers(pa.float32(), contiguous.size, [None, pa.py_buffer(contiguous)])
        return pa.FixedSizeListArray.from_arrays(flat, dim)
    flat_values = []
    for index, row in enumerate(array):
        row = list(row)
        if len(row) != dim:
            raise ProtoValidationError(f"Vector {index} has {len(row)} components, expected {dim}")
        for x in row:
            if not isinstance(x, numbers.Real) or isinstance(x, bool):
                raise ProtoValidationError(f"Vector {index} holds a non-numeric component {x!r}")
            flat_values.append(float(x))
    return pa.FixedSizeListArray.from_arrays(pa.array(flat_values, type=pa.float32()), dim)


def to_parquet(source, path: str | os.PathLike, columns: Sequence[str] | None = None, *, schema=None,
               compression: str = "zstd", row_group_size: int | None = None) -> None:
    """Write ``source`` (anything :func:`to_arrow` accepts, or a ``pyarrow.Table``) to a Parquet file.

    ``row_group_size`` is a number of rows (None: pyarrow's default);
    ``compression`` is passed to ``pyarrow.parquet.write_table``.
    """
    pa = _pyarrow()
    pq = _parquet()
    if row_group_size is not None and (not isinstance(row_group_size, int) or isinstance(row_group_size, bool)
                                       or row_group_size < 1):
        raise ProtoValidationError(f"row_group_size must be None or an int >= 1 (rows), not {row_group_size!r}")
    if isinstance(source, pa.Table):
        table = source if columns is None else source.select(_check_columns(columns))
    else:
        table = to_arrow(source, columns, schema=schema)
    pq.write_table(table, os.fspath(path), compression=compression, row_group_size=row_group_size)


def scan_parquet(path: str | os.PathLike, columns: Sequence[str] | None = None, filters=None):
    """Read a Parquet file or directory into a ``pyarrow.Table``.

    ``columns`` projects; ``filters`` is a ``pyarrow.dataset`` expression
    (e.g. ``pyarrow.dataset.field("id") >= 3``).
    """
    ds = _dataset()
    columns = _check_columns(columns)
    return ds.dataset(os.fspath(path), format="parquet").to_table(columns=columns, filter=filters)


def from_parquet_to_rows(path: str | os.PathLike, columns: Sequence[str] | None = None, filters=None) -> list:
    """Read a Parquet file into a list of dicts (arguments as :func:`scan_parquet`)."""
    return scan_parquet(path, columns=columns, filters=filters).to_pylist()
