"""BoundedCache: catches unbounded growth, wrong LRU eviction and races on the shared cache."""
import threading

import pytest

from proto_db.errors import ProtoValidationError
from proto_db.storage.cache import BoundedCache


def test_get_missing_returns_none():
    c = BoundedCache(100)
    assert c.get("x") is None
    assert len(c) == 0


def test_put_then_get():
    c = BoundedCache(100)
    c.put("a", b"12345")
    assert c.get("a") == b"12345"
    assert c.size_bytes == 5 and len(c) == 1


def test_evicts_least_recently_used():
    c = BoundedCache(10)
    c.put("a", b"aaaa")
    c.put("b", b"bbbb")
    assert c.get("a") == b"aaaa"  # "b" is now least recently used
    c.put("c", b"cccc")
    assert c.get("b") is None
    assert c.get("a") == b"aaaa" and c.get("c") == b"cccc"
    assert c.size_bytes <= 10


def test_replacing_a_key_updates_size():
    c = BoundedCache(10)
    c.put("a", b"aaaa")
    c.put("a", b"aaaaaaaa")
    assert c.size_bytes == 8 and len(c) == 1


def test_oversized_value_is_not_cached():
    c = BoundedCache(4)
    c.put("a", b"12")
    c.put("big", b"12345")
    assert c.get("big") is None
    assert c.get("a") == b"12"


def test_zero_capacity_caches_nothing():
    c = BoundedCache(0)
    c.put("a", b"1")
    assert c.get("a") is None


def test_invalidate_and_clear():
    c = BoundedCache(100)
    c.put("a", b"1")
    c.put("b", b"22")
    c.invalidate("a")
    c.invalidate("missing")
    assert c.get("a") is None and c.size_bytes == 2
    c.clear()
    assert len(c) == 0 and c.size_bytes == 0


def test_custom_sizeof():
    c = BoundedCache(3, sizeof=lambda v: 1)
    for k in "abcd":
        c.put(k, object())
    assert len(c) == 3 and c.get("a") is None


def test_hit_and_miss_counters():
    c = BoundedCache(100)
    c.put("a", b"1")
    c.get("a")
    c.get("b")
    assert (c.hits, c.misses) == (1, 1)


def test_invalid_capacity():
    with pytest.raises(ProtoValidationError):
        BoundedCache(-1)
    with pytest.raises(ProtoValidationError):
        BoundedCache("10")


def test_concurrent_use_stays_bounded_and_consistent():
    c = BoundedCache(64 * 100)
    errors = []

    def worker(t):
        for i in range(2000):
            key = (t * 7 + i) % 300
            value = bytes([key % 256]) * 64
            c.put(key, value)
            got = c.get(key)
            if got is not None and got != value:
                errors.append((key, got))
            if i % 97 == 0:
                c.invalidate((key + 1) % 300)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert c.size_bytes <= 64 * 100
    assert c.size_bytes == sum(64 for _ in range(len(c)))


def test_stats_snapshot_counts_hits_misses_and_size():
    """Catches counters that drift from the cache's behaviour (the benchmarks report them)."""
    c = BoundedCache(10)
    c.put("a", b"aaaa")
    c.get("a")
    c.get("zz")
    assert c.stats() == {"hits": 1, "misses": 1, "entries": 1, "bytes": 4, "capacity": 10,
                         "admissions": 1, "evictions": 0}
    c.put("b", b"bbbb")
    c.put("c", b"cccc")                 # evicts "a"
    assert (c.stats()["admissions"], c.stats()["evictions"], c.stats()["entries"]) == (3, 1, 2)


def test_file_storage_cache_stats_count_reads_after_reopen(tmp_path):
    """Catches a FileStorage.cache_stats() that does not reflect reads served by the page cache."""
    from proto_db.storage.file import FileStorage
    storage = FileStorage(str(tmp_path), durability="none")
    ptr = storage.push_blob(b"x" * 100)
    for _ in range(100):                  # fill past the first page: only complete pages are cached
        storage.push_blob(b"y" * 1000)
    storage.flush()
    storage.close()
    storage = FileStorage(str(tmp_path), durability="none")
    try:
        assert storage.cache_stats()["hits"] == 0
        assert storage.get_blob(ptr) == b"x" * 100
        before = storage.cache_stats()
        assert before["misses"] >= 1
        storage.get_blob(ptr)
        assert storage.cache_stats()["hits"] > before["hits"]
    finally:
        storage.close()
