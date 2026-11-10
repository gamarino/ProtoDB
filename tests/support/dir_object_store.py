"""An object store in a local directory, shared by several processes (Task 8.3d).

``DirObjectStore(path)`` implements the :class:`proto_db.storage.cloud.ObjectStore`
protocol over files, so subprocess cluster peers can share one "remote" tier.  Each
object is one file holding a 32-hex-digit etag (a fresh UUID per write) followed by the
data, replaced atomically (``os.replace``); conditional writes are serialized across
processes by an exclusive ``flock`` on ``<path>/.lock``.  Keys map to file names with
``/`` encoded, so listing is a directory scan.  POSIX only (tests).
"""
import fcntl
import os
import uuid

from proto_db.storage.cloud import ObjectNotFoundError, PreconditionFailedError

_ETAG = 32


class DirObjectStore:
    """Object store over a directory, safe across processes (see the module docstring)."""

    def __init__(self, path: str):
        self._path = os.path.abspath(path)
        os.makedirs(self._path, exist_ok=True)
        self._lock_path = os.path.join(self._path, ".lock")

    def _file(self, key: str) -> str:
        return os.path.join(self._path, key.replace("%", "%25").replace("/", "%2F"))

    def _locked(self):
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
        return raw[_ETAG:], raw[:_ETAG].decode("ascii")

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
        """Return ``(data, etag)``; ``ObjectNotFoundError`` if ``key`` is absent."""
        current = self._read(key)
        if current is None:
            raise ObjectNotFoundError(f"{key} not found")
        return current

    def list(self, prefix: str) -> list:
        """Return the sorted keys starting with ``prefix``."""
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
            pass                    # deleting an absent key is not an error
        finally:
            self._unlock(fd)
