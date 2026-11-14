"""Shared helpers of the benchmark workloads: data sets, object stores, SQLite and thread runners.

* :func:`records` -- the deterministic record set used by the lookup and range workloads;
* :class:`MemoryObjectStore` -- an in-memory :class:`~proto_db.storage.cloud.ObjectStore`
  with compare-and-swap writes and an injectable per-request latency (slept outside the
  lock, so concurrent requests overlap as they do against a real service);
* :class:`DirObjectStore` -- the same protocol over a directory, shared by subprocess peers;
* :func:`sqlite_connect` -- a SQLite connection with an explicit durability mode;
* :func:`run_threads` -- start callables on threads behind a barrier and time them.
"""
import fcntl
import os
import random
import sqlite3
import threading
import time
import uuid
from typing import Callable, List

from proto_db.storage.cloud import ObjectNotFoundError, PreconditionFailedError

CITIES = 100


def records(n: int, seed: int = 1) -> List[dict]:
    """``n`` records ``{id, code, city, score}``: ``id`` is ``0..n-1``; ``code`` a unique
    string; ``city`` one of :data:`CITIES` values; ``score`` a unique permutation of
    ``0..n-1`` (a total order for range and top-k checks)."""
    rng = random.Random(seed)
    scores = list(range(n))
    rng.shuffle(scores)
    return [{"id": i, "code": f"k{(i * 7919) % (n * 10):08d}-{i}", "city": f"city{i % CITIES:03d}",
             "score": scores[i]} for i in range(n)]


# -- object stores -----------------------------------------------------------------------------

class MemoryObjectStore:
    """Thread-safe in-memory object store with conditional writes and latency (module docstring)."""

    #: A refused write was never applied: nothing here re-sends a request (ruling R147a, D43).
    refusals_are_definitive = True

    def __init__(self, latency: float = 0.0):
        self.latency = latency
        self._lock = threading.Lock()
        self._objects = {}
        self._generation = 0
        self.failed_preconditions = 0
        self.requests = 0

    def _enter(self) -> None:
        if self.latency:
            time.sleep(self.latency)
        with self._lock:
            self.requests += 1

    def _write(self, key: str, data: bytes, etag, absent: bool) -> str:
        self._enter()
        with self._lock:
            current = self._objects.get(key)
            if (absent and current is not None) or (not absent and (current is None or current[1] != etag)):
                self.failed_preconditions += 1
                raise PreconditionFailedError(f"{key}: precondition failed")
            self._generation += 1
            new = str(self._generation)
            self._objects[key] = (bytes(data), new)
            return new

    def put_if_absent(self, key: str, data: bytes) -> str:
        """Create ``key``; ``PreconditionFailedError`` if it exists."""
        return self._write(key, data, None, True)

    def put_if_match(self, key: str, data: bytes, etag: str) -> str:
        """Replace ``key`` if its etag is ``etag``; ``PreconditionFailedError`` otherwise."""
        return self._write(key, data, etag, False)

    def get(self, key: str) -> tuple:
        """Return ``(data, etag)``; ``ObjectNotFoundError`` if absent."""
        self._enter()
        with self._lock:
            current = self._objects.get(key)
        if current is None:
            raise ObjectNotFoundError(f"{key} not found")
        return current

    def list(self, prefix: str) -> list:
        """The sorted keys starting with ``prefix``."""
        self._enter()
        with self._lock:
            return sorted(k for k in self._objects if k.startswith(prefix))

    def delete(self, key: str) -> None:
        """Delete ``key`` (absent: no error)."""
        self._enter()
        with self._lock:
            self._objects.pop(key, None)


class DirObjectStore:
    """Object store over a directory, safe across processes (``flock``-serialised conditional
    writes, atomic ``os.replace``); POSIX only.  ``latency`` seconds are slept per request."""

    _ETAG = 32

    def __init__(self, path: str, latency: float = 0.0):
        self._path = os.path.abspath(path)
        self.latency = latency
        os.makedirs(self._path, exist_ok=True)
        self._lock_path = os.path.join(self._path, ".lock")

    def _file(self, key: str) -> str:
        return os.path.join(self._path, key.replace("%", "%25").replace("/", "%2F"))

    def _locked(self) -> int:
        if self.latency:
            time.sleep(self.latency)
        fd = os.open(self._lock_path, os.O_RDWR | os.O_CREAT, 0o644)
        fcntl.flock(fd, fcntl.LOCK_EX)
        return fd

    @staticmethod
    def _unlock(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)

    def _read(self, key: str):
        try:
            with open(self._file(key), "rb") as f:
                raw = f.read()
        except FileNotFoundError:
            return None
        return raw[self._ETAG:], raw[:self._ETAG].decode("ascii")

    def _write(self, key: str, data: bytes) -> str:
        etag = uuid.uuid4().hex
        target = self._file(key)
        tmp = f"{target}.{etag}.tmp"
        with open(tmp, "wb") as f:
            f.write(etag.encode("ascii") + bytes(data))
        os.replace(tmp, target)
        return etag

    def put_if_absent(self, key: str, data: bytes) -> str:
        """Create ``key``; ``PreconditionFailedError`` if it exists."""
        fd = self._locked()
        try:
            if self._read(key) is not None:
                raise PreconditionFailedError(f"{key} exists")
            return self._write(key, data)
        finally:
            self._unlock(fd)

    def put_if_match(self, key: str, data: bytes, etag: str) -> str:
        """Replace ``key`` if its etag is ``etag``; ``PreconditionFailedError`` otherwise."""
        fd = self._locked()
        try:
            current = self._read(key)
            if current is None or current[1] != etag:
                raise PreconditionFailedError(f"{key}: etag mismatch")
            return self._write(key, data)
        finally:
            self._unlock(fd)

    def get(self, key: str) -> tuple:
        """Return ``(data, etag)``; ``ObjectNotFoundError`` if absent."""
        if self.latency:
            time.sleep(self.latency)
        current = self._read(key)
        if current is None:
            raise ObjectNotFoundError(f"{key} not found")
        return current

    def list(self, prefix: str) -> list:
        """The sorted keys starting with ``prefix``."""
        if self.latency:
            time.sleep(self.latency)
        keys = []
        for name in os.listdir(self._path):
            if name.startswith(".") or name.endswith(".tmp"):
                continue
            key = name.replace("%2F", "/").replace("%25", "%")
            if key.startswith(prefix):
                keys.append(key)
        return sorted(keys)

    def delete(self, key: str) -> None:
        """Delete ``key`` (absent: no error)."""
        fd = self._locked()
        try:
            os.remove(self._file(key))
        except FileNotFoundError:
            pass                    # deleting an absent key is not an error (S3 semantics)
        finally:
            self._unlock(fd)


# -- SQLite ------------------------------------------------------------------------------------

#: ProtoDB durability mode -> the SQLite ``synchronous`` setting giving the same guarantee
#: (WAL journal): ``fsync`` = FULL (fsync at every commit), ``flush`` = NORMAL (WAL not
#: fsynced at commit: survives a process crash, not a power loss), ``none`` = OFF.
SQLITE_SYNC = {"fsync": "FULL", "flush": "NORMAL", "none": "OFF"}


def sqlite_connect(path: str, durability: str = "none") -> sqlite3.Connection:
    """A connection in WAL mode with the ``synchronous`` level matching ``durability``."""
    conn = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
    if path != ":memory:":
        conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(f"PRAGMA synchronous={SQLITE_SYNC[durability]}")
    return conn


# -- threads -----------------------------------------------------------------------------------

def run_threads(workers: List[Callable[[], object]], timeout: float = 600.0) -> tuple:
    """Run every callable on its own thread, started together behind a barrier.

    Returns ``(elapsed_seconds, results)``; the first exception raised by a worker is
    re-raised after every thread has been joined.
    """
    barrier = threading.Barrier(len(workers) + 1)
    results = [None] * len(workers)
    errors = []

    def wrap(i, fn):
        barrier.wait()
        try:
            results[i] = fn()
        except BaseException as e:        # re-raised in the caller below
            errors.append(e)

    threads = [threading.Thread(target=wrap, args=(i, fn), name=f"bench-{i}") for i, fn in enumerate(workers)]
    for t in threads:
        t.start()
    barrier.wait()
    t0 = time.perf_counter()
    for t in threads:
        t.join(timeout)
    elapsed = time.perf_counter() - t0
    if any(t.is_alive() for t in threads):
        raise RuntimeError("benchmark threads did not finish in time")
    if errors:
        raise errors[0]
    return elapsed, results
