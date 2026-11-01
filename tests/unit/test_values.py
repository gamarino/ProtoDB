"""Unit tests for the ProtoDB value model."""
import pickle
import uuid
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
