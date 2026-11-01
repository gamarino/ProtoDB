"""Strict structural equality for round-trip tests: same types, same exact values."""
import math
import struct
from datetime import date, datetime, time
from decimal import Decimal


def assert_exact(a, b, path="value"):
    """Raise AssertionError unless ``a`` and ``b`` are exactly the same value.

    Stricter than ``==``: types must match exactly, floats compare bit-for-bit
    (NaN, -0.0), Decimals keep their exponent, datetimes keep fold and tzinfo,
    dict item order is preserved, and atoms must be the same object.
    """
    assert type(a) is type(b), f"{path}: {type(a).__name__} != {type(b).__name__}"
    if isinstance(a, float):
        assert struct.pack("<d", a) == struct.pack("<d", b) or (math.isnan(a) and math.isnan(b)), \
            f"{path}: {a!r} != {b!r}"
    elif isinstance(a, Decimal):
        assert str(a) == str(b) and a.as_tuple() == b.as_tuple(), f"{path}: {a!r} != {b!r}"
    elif isinstance(a, (datetime, time)):
        assert a.replace(tzinfo=None) == b.replace(tzinfo=None), f"{path}: {a!r} != {b!r}"
        assert a.fold == b.fold, f"{path}: fold {a.fold} != {b.fold}"
        assert_tz(a.tzinfo, b.tzinfo, path)
    elif isinstance(a, date):
        assert a == b, f"{path}: {a!r} != {b!r}"
    elif isinstance(a, (list, tuple)):
        assert len(a) == len(b), f"{path}: length {len(a)} != {len(b)}"
        for i, (x, y) in enumerate(zip(a, b)):
            assert_exact(x, y, f"{path}[{i}]")
    elif isinstance(a, dict):
        assert len(a) == len(b), f"{path}: size {len(a)} != {len(b)}"
        for i, ((ka, va), (kb, vb)) in enumerate(zip(a.items(), b.items())):
            assert_exact(ka, kb, f"{path}.key{i}")
            assert_exact(va, vb, f"{path}[{ka!r}]")
    elif hasattr(a, "oid") and hasattr(type(a), "__type_name__"):
        assert a is b, f"{path}: atom {a!r} is not {b!r}"
    else:
        assert a == b, f"{path}: {a!r} != {b!r}"


def assert_tz(a, b, path):
    """Assert two tzinfo objects are equivalent: same type, same key or offset and name."""
    assert type(a) is type(b), f"{path}: tzinfo {a!r} != {b!r}"
    if a is None:
        return
    key = getattr(a, "key", None)
    if key is not None:
        assert key == b.key, f"{path}: zone {a.key} != {b.key}"
    else:
        assert a.utcoffset(None) == b.utcoffset(None) and a.tzname(None) == b.tzname(None), \
            f"{path}: tzinfo {a!r} != {b!r}"
