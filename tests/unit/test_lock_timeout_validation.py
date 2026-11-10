"""``lock_timeout`` and the per-call ``root_lock(timeout=...)`` are validated when given
(8.3d re-review 14; ruling R62).

NaN, infinities and values above ``threading.TIMEOUT_MAX`` used to be accepted and made every
later ``root_lock`` (so every commit) raise a raw ``ValueError`` or ``OverflowError``; ``True``
behaved as one second.  Every storage and ``ObjectSpace`` now refuse them, and zero or negative
waits, with ``ProtoValidationError`` at construction.
"""
import math
import threading

import pytest

from proto_db.errors import ProtoValidationError
from proto_db.storage.base import check_lock_timeout
from proto_db.storage.cloud import CloudStorage
from proto_db.storage.file import FileStorage
from proto_db.storage.memory import MemoryStorage
from proto_db.storage.tiered import TieredStorage
from proto_db.transactions import ObjectSpace
from tests.support.fake_object_store import FakeObjectStore

BAD = [True, False, math.nan, math.inf, -math.inf, 0, 0.0, -1, -0.5, threading.TIMEOUT_MAX * 2, 1e300,
       "1", None, 1 + 0j, [1]]
BAD_IDS = ["True", "False", "nan", "inf", "-inf", "0", "0.0", "-1", "-0.5", "above-TIMEOUT_MAX", "1e300",
           "str", "None", "complex", "list"]


def _makers(tmp_path):
    return {
        "MemoryStorage": lambda v: MemoryStorage(lock_timeout=v),
        "FileStorage": lambda v: FileStorage(str(tmp_path / "f"), lock_timeout=v),
        "CloudStorage": lambda v: CloudStorage(FakeObjectStore(), lock_timeout=v),
        "TieredStorage": lambda v: TieredStorage(str(tmp_path / "t"), FakeObjectStore(), lock_timeout=v,
                                                 ship_interval=3600.0, backoff=(0.0, 0.0), retries=0),
        "ObjectSpace": lambda v: ObjectSpace(MemoryStorage(), lock_timeout=v),
    }


@pytest.mark.parametrize("bad", BAD, ids=BAD_IDS)
def test_the_validator_refuses_bad_values(bad):
    with pytest.raises(ProtoValidationError, match="lock_timeout"):
        check_lock_timeout(bad)


@pytest.mark.parametrize("good", [0.001, 1, 30.0, threading.TIMEOUT_MAX])
def test_the_validator_accepts_finite_positive_values(good):
    assert check_lock_timeout(good) == good


@pytest.mark.parametrize("kind", ["MemoryStorage", "FileStorage", "CloudStorage", "TieredStorage", "ObjectSpace"])
@pytest.mark.parametrize("bad", BAD, ids=BAD_IDS)
def test_every_constructor_refuses_a_bad_lock_timeout(tmp_path, kind, bad):
    if kind == "ObjectSpace" and bad is None:
        pytest.skip("None is ObjectSpace's documented default (the storage's own lock_timeout)")
    with pytest.raises(ProtoValidationError, match="lock_timeout"):
        _makers(tmp_path)[kind](bad)
    if kind == "TieredStorage":
        assert not (tmp_path / "t").exists(), "the directory was touched before validation"


@pytest.mark.parametrize("kind", ["MemoryStorage", "FileStorage", "CloudStorage", "TieredStorage"])
@pytest.mark.parametrize("bad", [True, math.nan, math.inf, 0, -1, 1e300], ids=["True", "nan", "inf", "0", "-1", "1e300"])
def test_root_lock_refuses_a_bad_per_call_timeout_and_still_works(tmp_path, kind, bad):
    storage = _makers(tmp_path)[kind](5.0)
    try:
        with pytest.raises(ProtoValidationError, match="timeout"):
            storage.root_lock(timeout=bad)
        with storage.root_lock(timeout=1):                 # nothing was left held
            pass
        with storage.root_lock():
            pass
    finally:
        storage.close()
