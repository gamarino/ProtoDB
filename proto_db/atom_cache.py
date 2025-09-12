from __future__ import annotations

import sys
import threading
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Callable, Optional, Tuple

# Key type used for caches: (transaction_uuid, offset, optional_epoch)
AtomKey = Tuple[uuid.UUID, int, Optional[int]]
BytesKey = Tuple[uuid.UUID, int]


@dataclass
class CacheStats:
    hits: int = 0
    misses: int = 0
    puts: int = 0
    evictions: int = 0
    size_bytes: int = 0
    size_entries: int = 0
    singleflight_dedup: int = 0

    def as_dict(self) -> dict:
        total = self.hits + self.misses
        ratio = (self.hits / total) if total else 0.0
        return {
            "hits": self.hits,
            "misses": self.misses,
            "hit_ratio": ratio,
            "puts": self.puts,
            "evictions": self.evictions,
            "size_bytes": self.size_bytes,
            "size_entries": self.size_entries,
            "singleflight_dedup": self.singleflight_dedup,
        }


class StripingLock:
    def __init__(self, stripes: int = 64):
        self._locks = [threading.Lock() for _ in range(max(1, stripes))]

    def lock_for(self, key_hash: int) -> threading.Lock:
        return self._locks[key_hash % len(self._locks)]


class TwoQ:
    """
    Simple 2Q cache policy manager over a backing dict mapping key -> (value, size).
    We maintain two LRU queues of keys: probation (A1) and protected (Am).
    Admission: new items go into probation; on re-reference, promote to protected.
    Eviction: evict from probation first, then protected if needed.
    Limits enforced by number of entries and total bytes.
    """

    def __init__(self, max_entries: int, max_bytes: int, probation_ratio: float = 0.5):
        self.max_entries = max(0, int(max_entries))
        self.max_bytes = max(0, int(max_bytes))
        self.probation = OrderedDict()  # key -> None (we track order only)
        self.protected = OrderedDict()
        self.probation_target = max(0, int(self.max_entries * min(max(probation_ratio, 0.1), 0.9)))

    def on_get(self, key, is_present_in_store: bool, was_probation: bool) -> tuple[bool, bool]:
        """
        Update LRU queues on access. Returns (promote, touched).
        """
        if not is_present_in_store:
            return False, False
        if key in self.protected:
            self.protected.move_to_end(key)
            return False, True
        if key in self.probation:
            self.probation.pop(key, None)
            self.protected[key] = None
            return True, True
        # Present in store but not tracked (shouldn't happen), treat as probation miss
        return False, False

    def on_put(self, key):
        # New admission always into probation
        self.probation[key] = None

    def on_evict(self, key):
        # remove from either queue if present
        self.probation.pop(key, None)
        self.protected.pop(key, None)

    def ensure_capacity(self, current_entries: int, current_bytes: int, size_of_new: int,
                        evict_cb: Callable[[Any], None]) -> int:
        """
        Evict as needed to make space. Returns number of evicted entries.
        """
        evicted = 0
        # Hard limits 0 implies disabled cache
        if self.max_entries == 0 or self.max_bytes == 0:
            # evict everything via callback
            for k in list(self.probation.keys()):
                evict_cb(k); self.probation.pop(k, None); evicted += 1
            for k in list(self.protected.keys()):
                evict_cb(k); self.protected.pop(k, None); evicted += 1
            return evicted

        def over_limits(ent: int, byt: int) -> bool:
            return ent > self.max_entries or byt > self.max_bytes

        # While we'd be over limits after adding the new item, evict
        while over_limits(current_entries + 1, current_bytes + size_of_new):
            if self.probation:
                k, _ = self.probation.popitem(last=False)
                evict_cb(k)
                evicted += 1
                current_entries -= 1
            elif self.protected:
                k, _ = self.protected.popitem(last=False)
                evict_cb(k)
                evicted += 1
                current_entries -= 1
            else:
                break
        return evicted


def default_sizeof(obj: Any) -> int:
    # Shallow estimate; caller can pass a better estimator
    try:
        return sys.getsizeof(obj)
    except Exception:
        return 64


class SingleFlight:
    """Deduplicate concurrent loads by key."""

    def __init__(self):
        self._inflight: dict[Any, threading.Event] = {}
        self._lock = threading.Lock()

    def begin(self, key) -> Optional[threading.Event]:
        with self._lock:
            ev = self._inflight.get(key)
            if ev is None:
                ev = threading.Event()
                self._inflight[key] = ev
                return ev  # caller is leader; must set() when done and then remove
            else:
                return None  # follower; will wait on existing event

    def wait(self, key):
        with self._lock:
            ev = self._inflight.get(key)
        if ev is not None:
            ev.wait()

    def done(self, key):
        with self._lock:
            ev = self._inflight.pop(key, None)
            if ev is not None:
                ev.set()


class AtomBytesCache:
    def __init__(self, max_entries: int = 10000, max_bytes: int = 64 * 1024 * 1024, stripes: int = 64,
                 probation_ratio: float = 0.5):
        self._store: dict[BytesKey, tuple[memoryview, int]] = {}
        self._stats = CacheStats()
        self._locks = StripingLock(stripes)
        self._policy = TwoQ(max_entries, max_bytes, probation_ratio)

    def _key(self, txn: uuid.UUID, offset: int) -> BytesKey:
        return txn, offset

    def get(self, txn: uuid.UUID, offset: int) -> Optional[memoryview]:
        key = self._key(txn, offset)
        lk = self._locks.lock_for(hash(key))
        with lk:
            entry = self._store.get(key)
            if entry is None:
                self._stats.misses += 1
                return None
            mv, sz = entry
            promote, _ = self._policy.on_get(key, True, key in self._policy.probation)
            # Promote handled within on_get by moving between dicts
            self._stats.hits += 1
            return mv

    def put(self, txn: uuid.UUID, offset: int, data: bytes | memoryview):
        key = self._key(txn, offset)
        mv = memoryview(data)
        size = len(mv)
        lk = self._locks.lock_for(hash(key))
        with lk:
            # Evict as needed
            evicted = self._policy.ensure_capacity(self._stats.size_entries, self._stats.size_bytes, size,
                                                   evict_cb=lambda k: self._evict_key(k))
            self._stats.evictions += evicted
            self._store[key] = (mv, size)
            self._policy.on_put(key)
            self._stats.size_entries = len(self._store)
            self._stats.size_bytes += size
            self._stats.puts += 1

    def contains(self, txn: uuid.UUID, offset: int) -> bool:
        key = self._key(txn, offset)
        lk = self._locks.lock_for(hash(key))
        with lk:
            return key in self._store

    def _evict_key(self, key: BytesKey):
        entry = self._store.pop(key, None)
        if entry:
            _, sz = entry
            self._stats.size_bytes -= sz

    def stats(self) -> dict:
        return self._stats.as_dict()


class AtomObjectCache:
    def __init__(self, max_entries: int = 50000, max_bytes: int = 256 * 1024 * 1024, stripes: int = 64,
                 probation_ratio: float = 0.5, use_weak_for_probation: bool = False,
                 sizeof: Callable[[Any], int] = default_sizeof):
        self._store: dict[AtomKey, tuple[Any, int]] = {}
        self._stats = CacheStats()
        self._locks = StripingLock(stripes)
        self._policy = TwoQ(max_entries, max_bytes, probation_ratio)
        self._sizeof = sizeof
        self._use_weak = use_weak_for_probation  # Placeholder; not using weakref now for simplicity

    def _key(self, txn: uuid.UUID, offset: int, schema_epoch: Optional[int]) -> AtomKey:
        return txn, offset, schema_epoch

    def get(self, txn: uuid.UUID, offset: int, schema_epoch: Optional[int] = None) -> Optional[Any]:
        key = self._key(txn, offset, schema_epoch)
        lk = self._locks.lock_for(hash(key))
        with lk:
            entry = self._store.get(key)
            if entry is None:
                self._stats.misses += 1
                return None
            obj, sz = entry
            self._policy.on_get(key, True, key in self._policy.probation)
            self._stats.hits += 1
            return obj

    def put(self, txn: uuid.UUID, offset: int, obj: Any, schema_epoch: Optional[int] = None,
            size_bytes_est: Optional[int] = None):
        key = self._key(txn, offset, schema_epoch)
        size = size_bytes_est if size_bytes_est is not None else self._sizeof(obj)
        lk = self._locks.lock_for(hash(key))
        with lk:
            evicted = self._policy.ensure_capacity(self._stats.size_entries, self._stats.size_bytes, size,
                                                   evict_cb=lambda k: self._evict_key(k))
            self._stats.evictions += evicted
            self._store[key] = (obj, size)
            self._policy.on_put(key)
            self._stats.size_entries = len(self._store)
            self._stats.size_bytes += size
            self._stats.puts += 1

    def contains(self, txn: uuid.UUID, offset: int, schema_epoch: Optional[int] = None) -> bool:
        key = self._key(txn, offset, schema_epoch)
        lk = self._locks.lock_for(hash(key))
        with lk:
            return key in self._store

    def _evict_key(self, key: AtomKey):
        entry = self._store.pop(key, None)
        if entry:
            _, sz = entry
            self._stats.size_bytes -= sz

    def stats(self) -> dict:
        return self._stats.as_dict()


class AtomCacheBundle:
    """Convenience container to embed in storages and expose single-flight and stats."""

    def __init__(self,
                 enable_object_cache: bool = True,
                 enable_bytes_cache: bool = True,
                 object_max_entries: int = 50000,
                 object_max_bytes: int = 256 * 1024 * 1024,
                 bytes_max_entries: int = 10000,
                 bytes_max_bytes: int = 64 * 1024 * 1024,
                 stripes: int = 64,
                 probation_ratio: float = 0.5,
                 schema_epoch: Optional[int] = None):
        self.obj_cache = AtomObjectCache(object_max_entries, object_max_bytes, stripes, probation_ratio) \
            if enable_object_cache else None
        self.bytes_cache = AtomBytesCache(bytes_max_entries, bytes_max_bytes, stripes, probation_ratio) \
            if enable_bytes_cache else None
        self.schema_epoch = schema_epoch
        self.singleflight = SingleFlight()
        self._metrics_lock = threading.Lock()
        self.latencies = {
            "object_cache_ms": [],
            "bytes_cache_ms": [],
            "deserialize_ms": [],
        }

    def record_latency(self, key: str, ms: float):
        with self._metrics_lock:
            bucket = self.latencies.get(key)
            if bucket is not None:
                bucket.append(ms)

    def stats(self) -> dict:
        def pct(arr, p):
            if not arr:
                return 0.0
            arr2 = sorted(arr)
            k = int((p/100.0) * (len(arr2)-1))
            return float(arr2[k])
        return {
            "object_cache": self.obj_cache.stats() if self.obj_cache else {},
            "bytes_cache": self.bytes_cache.stats() if self.bytes_cache else {},
            "latency_ms": {
                "object_cache": {
                    "p50": pct(self.latencies["object_cache_ms"], 50),
                    "p95": pct(self.latencies["object_cache_ms"], 95),
                    "p99": pct(self.latencies["object_cache_ms"], 99),
                },
                "bytes_cache": {
                    "p50": pct(self.latencies["bytes_cache_ms"], 50),
                    "p95": pct(self.latencies["bytes_cache_ms"], 95),
                    "p99": pct(self.latencies["bytes_cache_ms"], 99),
                },
                "deserialize": {
                    "p50": pct(self.latencies["deserialize_ms"], 50),
                    "p95": pct(self.latencies["deserialize_ms"], 95),
                    "p99": pct(self.latencies["deserialize_ms"], 99),
                },
            },
        }
