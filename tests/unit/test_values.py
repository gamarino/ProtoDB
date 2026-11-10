"""Unit tests for the ProtoDB value model."""
import pickle
import uuid
import sys
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

import pytest

from proto_db.errors import ProtoValidationError
from proto_db.values import MISSING, canonical, hash_key, is_atom, order_key, values_equal


class FakeAtom:
    def __init__(self, oid):
        self.oid = oid


def test_numeric_equality_is_exact_across_types():
    assert values_equal(1, 1.0)
    assert values_equal(1, Decimal("1"))
    assert values_equal(0.0, -0.0)
    assert not values_equal(Decimal("0.1"), 0.1)
    assert not values_equal(2**60 + 1, float(2**60))
    assert order_key(2**60 + 1) > order_key(float(2**60))


def test_bool_is_not_a_number():
    assert not values_equal(True, 1)
    assert not values_equal(False, 0)
    assert values_equal(True, True)


def test_large_integers_distinct():
    assert order_key(10**16 + 1) > order_key(10**16)


def test_rank_order():
    chain = [MISSING, None, False, True, -10**30, "", b"", date(2000, 1, 1),
             time(1, 2), time(1, 2, tzinfo=timezone.utc),
             datetime(2000, 1, 1), datetime(1900, 1, 1, tzinfo=timezone.utc),
             timedelta(0), uuid.UUID(int=0), (), FakeAtom(uuid.UUID(int=0))]
    keys = [order_key(v) for v in chain]
    assert keys == sorted(keys)
    assert len(set(keys)) == len(keys)
    assert order_key(None) < order_key(False) < order_key(-10**30) < order_key("") < order_key(b"")


def test_infinity_ordered_nan_rejected():
    assert order_key(float("-inf")) < order_key(-10**400) < order_key(10**400) < order_key(float("inf"))
    assert order_key(Decimal("Infinity")) == order_key(float("inf"))
    for bad in (float("nan"), Decimal("NaN"), Decimal("sNaN")):
        with pytest.raises(ProtoValidationError):
            order_key(bad)
    with pytest.raises(ProtoValidationError):
        order_key((1, float("nan")))


def test_unsupported_types():
    with pytest.raises(ProtoValidationError):
        canonical(object())
    with pytest.raises(ProtoValidationError):
        order_key(object())
    with pytest.raises(ProtoValidationError):
        canonical([1, object()])


def test_list_and_dict_storable_not_orderable():
    assert canonical([1, {"a": (2, b"x")}]) == [1, {"a": (2, b"x")}]
    assert canonical(bytearray(b"ab")) == b"ab"
    assert isinstance(canonical(bytearray(b"ab")), bytes)
    with pytest.raises(ProtoValidationError):
        order_key([1])
    with pytest.raises(ProtoValidationError):
        order_key({"a": 1})
    with pytest.raises(ProtoValidationError):
        order_key((1, [2]))


def test_naive_and_aware_datetime():
    naive = datetime(2020, 1, 1)
    aware = datetime(2020, 1, 1, tzinfo=timezone.utc)
    assert order_key(naive) < order_key(aware)
    assert not values_equal(naive, aware)
    plus1 = datetime(2020, 1, 1, 1, tzinfo=timezone(timedelta(hours=1)))
    assert values_equal(aware, plus1)
    assert hash_key(aware) == hash_key(plus1)


def test_time_naive_vs_aware():
    n = time(10, 0)
    a = time(10, 0, tzinfo=timezone.utc)
    assert order_key(n) < order_key(a)
    assert values_equal(a, time(11, 0, tzinfo=timezone(timedelta(hours=1))))
    assert order_key(time(23, 0, tzinfo=timezone(timedelta(hours=5)))) > order_key(time(0, 0, tzinfo=timezone.utc))


def test_date_vs_datetime_distinct():
    assert not values_equal(date(2020, 1, 1), datetime(2020, 1, 1))
    assert order_key(date(2020, 1, 1)) < order_key(datetime(1, 1, 1))


def test_tuples_elementwise():
    assert values_equal((1, "a"), (1.0, "a"))
    assert not values_equal((True,), (1,))
    assert order_key((1, 2)) < order_key((1, 2, 0))
    assert order_key((1, 2)) < order_key((2,))
    assert hash_key((1, 2)) == hash_key((1.0, Decimal(2)))


def test_atoms():
    a, b = FakeAtom(uuid.UUID(int=1)), FakeAtom(uuid.UUID(int=2))
    assert is_atom(a) and not is_atom(object()) and not is_atom(FakeAtom("x"))
    assert order_key(a) < order_key(b)
    assert values_equal(a, FakeAtom(uuid.UUID(int=1)))
    assert hash_key(a) == hash_key(FakeAtom(uuid.UUID(int=1)))


def test_missing_sentinel():
    assert repr(MISSING) == "MISSING"
    assert pickle.loads(pickle.dumps(MISSING)) is MISSING
    assert MISSING is not None
    assert order_key(MISSING) < order_key(None)
    assert not values_equal(MISSING, None)
    assert hash_key(MISSING) == hash_key(MISSING)


def test_nan_canonical_allowed():
    import math
    assert math.isnan(canonical(float("nan")))


def test_fold_and_gap_datetimes_use_resolved_instant():
    from zoneinfo import ZoneInfo
    m = ZoneInfo("Europe/Madrid")
    a0 = datetime(2020, 10, 25, 2, 30, tzinfo=m, fold=0)
    a1 = a0.replace(fold=1)
    b0 = a0.astimezone(timezone.utc)
    b1 = a1.astimezone(timezone.utc)
    assert b0 != b1
    for x, y in ((a0, b0), (a1, b1)):
        assert values_equal(x, y)
        assert hash_key(x) == hash_key(y)
    assert not values_equal(a0, a1)
    assert order_key(a0) < order_key(a1)
    # nonexistent (gap) local time: resolved with the fold-selected offset
    g = datetime(2020, 3, 29, 2, 30, tzinfo=m)
    assert values_equal(g, g.astimezone(timezone.utc))
    assert hash_key(g) == hash_key(g.astimezone(timezone.utc))


# -- process-stable hash_key (D24) ----------------------------------------------------------

_HASH_PROBE = r"""
import sys, uuid
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
sys.path.insert(0, sys.argv[1])
from proto_db.values import MISSING, hash_key
values = [MISSING, None, True, False, 0, 1, -7, 2**60 + 3, 10**40, 0.5, -2.25, Decimal("1.10"),
          float("inf"), Decimal("-Infinity"), "abc", "", "ñand\udc80", b"", b"\x00xyz",
          date(2024, 2, 29), time(10, 30), time(10, 30, tzinfo=timezone.utc),
          datetime(2020, 1, 1, 12), datetime(2020, 1, 1, 12, tzinfo=timezone(timedelta(hours=3))),
          timedelta(days=2, microseconds=5), uuid.UUID(int=12345), (1, "a", (b"x", None)), ()]
print([hash_key(v) for v in values])
"""


def test_hash_key_is_identical_across_processes_with_different_hash_seeds():
    """Persisted structures (HashDictionary buckets) need hashes that survive a restart."""
    import os
    import subprocess
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    outputs = set()
    for seed in ("1", "2", "12345"):
        env = dict(os.environ, PYTHONHASHSEED=seed)
        out = subprocess.run([sys.executable, "-c", _HASH_PROBE, root], env=env, check=True,
                             capture_output=True, text=True).stdout
        outputs.add(out)
    assert len(outputs) == 1, outputs


def test_hash_key_numbers_hash_by_exact_value():
    assert hash_key(1) == hash_key(1.0) == hash_key(Decimal(1)) == hash_key(Decimal("1.000"))
    assert hash_key(0.5) == hash_key(Decimal("0.5"))
    assert hash_key(-0.75) == hash_key(Decimal("-0.75"))
    assert hash_key(10**1000) == hash_key(Decimal("1E+1000"))
    assert hash_key(float("inf")) == hash_key(Decimal("Infinity"))
    assert hash_key(float("-inf")) == hash_key(Decimal("-Infinity"))
    assert hash_key(float("inf")) != hash_key(float("-inf"))
    assert hash_key(0.0) == hash_key(-0.0) == hash_key(0) == hash_key(Decimal("-0"))
    assert hash_key(True) != hash_key(1)
    # Huge exponents are reduced modularly, never expanded.
    hash_key(Decimal("1E+999999999"))
    hash_key(Decimal("1E-999999999"))


def test_hash_key_is_a_signed_64_bit_int():
    for v in ("x", 1, (1, 2), b"q", None):
        h = hash_key(v)
        assert type(h) is int and -(2**63) <= h < 2**63


# -- rank_bounds (Task 4.3, D30): range scans restricted to one type rank ------------------

def test_rank_bounds_bracket_exactly_one_rank():
    """Catches a RangeIndex range that leaks into a neighbouring rank (D28: ``None < 5`` is false)."""
    from proto_db.values import rank_bounds
    samples = [MISSING, None, False, True, -10**30, -1.5, 0, Decimal("2.5"), 10**30, float("inf"),
               float("-inf"), "", "zz", b"", b"\xff", date(1, 1, 1), date(9999, 12, 31),
               time(0, 0), time(23, 59, tzinfo=timezone.utc), datetime(2020, 1, 1),
               datetime(2020, 1, 1, tzinfo=timezone.utc), timedelta(days=-5), uuid.UUID(int=0),
               (), (1, "a"), ((),), FakeAtom(uuid.UUID(int=7))]
    for v in samples:
        low, high = rank_bounds(v)
        assert len(order_key(v)) == 2 and type(order_key(v)[0]) is int  # the invariant rank_bounds relies on
        for w in samples:
            k = order_key(w)
            same = k[0] == order_key(v)[0]
            assert (low < k < high) == same, (v, w)


def test_rank_bounds_rejects_unorderable_values():
    from proto_db.values import rank_bounds
    for bad in ([1], {"a": 1}, float("nan")):
        with pytest.raises(ProtoValidationError):
            rank_bounds(bad)
