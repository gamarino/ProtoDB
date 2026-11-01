"""Unit tests for the 2.0 Arrow/Parquet bridge, proto_db.interop.arrow (Task 8.1, D42).

Catches the 0.1 defects (proto_db/arrow_bridge.py): pyarrow imported eagerly
under ``except Exception`` (any import-time failure hidden behind "not
available"); a ``RuntimeError`` subclass outside the ProtoDB hierarchy;
columns built from each row's own keys, so rows with different keys produced
misaligned columns; ``row_group_size_bytes`` passed as a row count; vector
rows of the wrong length accepted.  It also checks the 2.0 value mapping
(FrozenDict/FrozenList, MISSING -> null, Decimal, datetimes, UUID, bytes),
DBObject and LINQ/QueryPlan sources, conversion errors naming the column,
and Parquet round trips.  pyarrow 26 installs on CPython 3.14t and keeps the
GIL disabled; the missing-pyarrow path is in test_arrow_missing.py.
"""
import uuid
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

import pytest

from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoNotSupportedError, ProtoValidationError
from proto_db.query.linq import F, from_collection
from proto_db.query.plans import ValuesScan
from proto_db.values import MISSING, FrozenDict, FrozenList, MutableObject
from proto_db.interop import arrow as bridge

pa = pytest.importorskip("pyarrow")
np = pytest.importorskip("numpy")

TZ = timezone(timedelta(hours=-3))
U = uuid.UUID("12345678-1234-5678-1234-567812345678")


# -- to_arrow ---------------------------------------------------------------------------------

def test_dict_rows_with_different_keys_align_columns():
    """0.1 appended each row's own keys, so columns had different lengths."""
    table = bridge.to_arrow([{"a": 1, "b": "x"}, {"b": "y", "c": True}, {"a": 3}])
    assert table.column_names == ["a", "b", "c"]
    assert table.to_pylist() == [{"a": 1, "b": "x", "c": None}, {"a": None, "b": "y", "c": True},
                                 {"a": 3, "b": None, "c": None}]


def test_columns_select_and_order():
    table = bridge.to_arrow([{"a": 1, "b": 2}, {"b": 3}], columns=["b", "z"])
    assert table.column_names == ["b", "z"]
    assert table.to_pylist() == [{"b": 2, "z": None}, {"b": 3, "z": None}]


def test_empty_sources():
    assert bridge.to_arrow([]).num_rows == 0
    table = bridge.to_arrow([], columns=["a"])
    assert table.num_rows == 0 and table.column_names == ["a"]


def test_value_mapping():
    row = {"none": None, "missing": MISSING, "flag": True, "i": 7, "f": 1.5, "s": "text", "b": b"\x00\x01",
           "dec": Decimal("12.50"), "d": date(2026, 11, 1), "t": time(10, 30, 1, 5),
           "naive": datetime(2026, 11, 1, 12, 0), "aware": datetime(2026, 11, 1, 12, 0, tzinfo=TZ),
           "td": timedelta(days=1, microseconds=3), "u": U,
           "fl": FrozenList([1, 2]), "fd": FrozenDict({"k": "v", "n": FrozenList([None])}), "tup": (1, 2)}
    table = bridge.to_arrow([row])
    types = {f.name: f.type for f in table.schema}
    assert pa.types.is_null(types["none"]) and pa.types.is_null(types["missing"])
    assert types["flag"] == pa.bool_() and types["i"] == pa.int64() and types["f"] == pa.float64()
    assert types["s"] == pa.string() and types["b"] == pa.binary()
    assert pa.types.is_decimal(types["dec"])
    assert types["d"] == pa.date32() and types["t"] == pa.time64("us")
    assert types["naive"] == pa.timestamp("us") and types["aware"] == pa.timestamp("us", tz="-03:00")
    assert types["td"] == pa.duration("us") and types["u"] == pa.uuid()
    assert pa.types.is_list(types["fl"]) and pa.types.is_struct(types["fd"]) and pa.types.is_list(types["tup"])
    back = table.to_pylist()[0]
    assert back["missing"] is None and back["dec"] == Decimal("12.50") and back["u"] == U
    assert back["aware"] == row["aware"] and back["fd"] == {"k": "v", "n": [None]} and back["tup"] == [1, 2]


def test_dbobject_rows_and_nested_records():
    rows = [DBObject(name="ann", age=30, address=DBObject(city="X")), DBObject(name="bob", tags=["a", "b"])]
    table = bridge.to_arrow(rows)
    assert table.to_pylist() == [{"name": "ann", "age": 30, "address": {"city": "X"}, "tags": None},
                                 {"name": "bob", "age": None, "address": None, "tags": ["a", "b"]}]


def test_queryable_and_plan_sources():
    data = [{"a": i, "b": str(i)} for i in range(10)]
    q = from_collection(data).where(F.a >= 7).order_by(F.a)
    assert bridge.to_arrow(q).to_pylist() == data[7:]
    assert bridge.to_arrow(ValuesScan(data)).num_rows == 10
    projected = from_collection(data).select({"x": "a"}).take(2)
    assert bridge.to_arrow(projected).to_pylist() == [{"x": 0}, {"x": 1}]


def test_conversion_errors_name_the_column():
    with pytest.raises(ProtoValidationError, match="'mixed'"):
        bridge.to_arrow([{"mixed": 1}, {"mixed": "one"}])
    with pytest.raises(ProtoValidationError, match="'big'"):
        bridge.to_arrow([{"big": 2 ** 70}])


@pytest.mark.parametrize("source", ["text", b"bytes", {"a": 1}, 42])
def test_invalid_sources_are_rejected(source):
    with pytest.raises(ProtoValidationError):
        bridge.to_arrow(source)


@pytest.mark.parametrize("row", [1, "row", (1, 2), [("a", 1)]])
def test_non_record_rows_are_rejected(row):
    with pytest.raises(ProtoValidationError, match="row 0"):
        bridge.to_arrow([row])


def test_unconvertible_values_are_rejected():
    with pytest.raises(ProtoNotSupportedError, match="MutableObject"):
        bridge.to_arrow([{"m": MutableObject("counter")}])
    with pytest.raises(ProtoValidationError):
        bridge.to_arrow([{"d": {1: "non-string key"}}])


def test_explicit_schema():
    schema = pa.schema([("id", pa.int32()), ("v", pa.list_(pa.float32(), 2))])
    table = bridge.to_arrow([{"id": 1, "v": (1.0, 2.0)}, {"id": 2, "v": FrozenList([3.0, 4.0])}], schema=schema)
    assert table.schema == schema
    with pytest.raises(ProtoValidationError, match="'id'"):
        bridge.to_arrow([{"id": "x", "v": None}], schema=schema)
    with pytest.raises(ProtoValidationError):
        bridge.to_arrow([{"id": 1}], schema=schema, columns=["id"])


# -- vectors ----------------------------------------------------------------------------------

def test_vectors_from_numpy_are_zero_copy():
    arr = np.arange(12, dtype=np.float32).reshape(4, 3)
    vectors = bridge.vectors_fixed_size_list(arr, 3)
    assert vectors.type == pa.list_(pa.float32(), 3) and len(vectors) == 4
    assert vectors.values.buffers()[1].address == arr.ctypes.data
    assert vectors.to_pylist()[1] == [3.0, 4.0, 5.0]


def test_vectors_from_lists():
    vectors = bridge.vectors_fixed_size_list([[1, 2], (3.5, 4)], 2)
    assert vectors.to_pylist() == [[1.0, 2.0], [3.5, 4.0]]
    assert len(bridge.vectors_fixed_size_list([], 2)) == 0


@pytest.mark.parametrize("array, dim", [([[1.0, 2.0], [3.0]], 2), ([[1.0]], 0), ([[1.0]], True),
                                        (np.zeros((2, 3), dtype=np.float64), 3),
                                        (np.zeros((2, 3), dtype=np.float32), 2),
                                        (np.zeros(6, dtype=np.float32), 3)])
def test_invalid_vectors_are_rejected(array, dim):
    with pytest.raises(ProtoValidationError):
        bridge.vectors_fixed_size_list(array, dim)


# -- parquet ----------------------------------------------------------------------------------

ROWS = [{"id": i, "name": f"n{i}", "price": Decimal(f"{i}.25"), "when": datetime(2026, 1, 1, i, tzinfo=TZ),
         "day": date(2026, 1, i + 1), "u": uuid.UUID(int=i), "tags": FrozenList(["x"] * i),
         "extra": MISSING if i % 2 else FrozenDict({"k": i})} for i in range(5)]


def _expected(rows):
    out = []
    for r in rows:
        d = dict(r)
        d["tags"] = list(d["tags"])
        d["extra"] = None if d["extra"] is MISSING else dict(d["extra"])
        out.append(d)
    return out


def test_parquet_round_trip(tmp_path):
    path = tmp_path / "rows.parquet"
    bridge.to_parquet(ROWS, path)
    assert bridge.from_parquet_to_rows(path) == _expected(ROWS)
    table = bridge.scan_parquet(path, columns=["id", "name"])
    assert table.column_names == ["id", "name"] and table.num_rows == 5


def test_parquet_filters(tmp_path):
    import pyarrow.dataset as ds
    path = tmp_path / "rows.parquet"
    bridge.to_parquet(ROWS, str(path), row_group_size=2)
    rows = bridge.from_parquet_to_rows(path, columns=["id"], filters=ds.field("id") >= 3)
    assert rows == [{"id": 3}, {"id": 4}]


def test_parquet_from_table_and_queryable(tmp_path):
    path = tmp_path / "t.parquet"
    bridge.to_parquet(pa.table({"a": [1, 2]}), path)
    assert bridge.from_parquet_to_rows(path) == [{"a": 1}, {"a": 2}]
    path2 = tmp_path / "q.parquet"
    bridge.to_parquet(from_collection(ROWS).where(F.id < 2), path2, columns=["id", "u"])
    assert bridge.from_parquet_to_rows(path2) == [{"id": 0, "u": uuid.UUID(int=0)}, {"id": 1, "u": uuid.UUID(int=1)}]


def test_parquet_row_group_size_is_rows(tmp_path):
    import pyarrow.parquet as pq
    path = tmp_path / "g.parquet"
    bridge.to_parquet([{"a": i} for i in range(10)], path, row_group_size=3)
    assert pq.ParquetFile(path).num_row_groups == 4


def test_parquet_missing_file_propagates(tmp_path):
    with pytest.raises(FileNotFoundError):
        bridge.from_parquet_to_rows(tmp_path / "absent.parquet")


# -- review fix round 1 -----------------------------------------------------------------------

@pytest.mark.parametrize("values", [
    [datetime(2026, 1, 1), datetime(2026, 1, 1, tzinfo=TZ)],
    [datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=3))), datetime(2026, 1, 1)],
])
def test_mixed_naive_and_aware_datetimes_are_rejected(values):
    with pytest.raises(ProtoValidationError, match="'when'.*naive and aware"):
        bridge.to_arrow([{"when": v} for v in values])
    with pytest.raises(ProtoValidationError, match="naive and aware"):
        bridge.to_arrow([{"rec": {"when": v}} for v in values])
    with pytest.raises(ProtoValidationError, match="naive and aware"):
        bridge.to_arrow([{"seq": [v]} for v in values])


def test_naive_and_aware_in_different_fields_are_fine():
    table = bridge.to_arrow([{"rec": {"a": datetime(2026, 1, 1), "b": datetime(2026, 1, 1, tzinfo=TZ)}}])
    assert table.to_pylist()[0]["rec"]["b"] == datetime(2026, 1, 1, tzinfo=TZ)


def test_aware_times_are_rejected():
    """Arrow time64 has no time zone: pyarrow drops tzinfo silently."""
    with pytest.raises(ProtoValidationError, match="'t'.*time zone"):
        bridge.to_arrow([{"t": time(1, tzinfo=TZ)}])


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity", "sNaN"])
def test_non_finite_decimals_are_rejected(value):
    with pytest.raises(ProtoValidationError, match=f"'price'.*{value}"):
        bridge.to_arrow([{"price": Decimal(value)}])


def test_importing_pyarrow_keeps_the_gil_disabled():
    import subprocess
    import sys
    import sysconfig
    if not sysconfig.get_config_var("Py_GIL_DISABLED"):
        pytest.skip("not a free-threaded build")
    code = ("import sys; before = sys._is_gil_enabled(); import proto_db.interop.arrow as b; "
            "b.to_arrow([{'a': 1}]); import pyarrow.parquet, pyarrow.dataset; print(before, sys._is_gil_enabled())")
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.split() == ["False", "False"]
