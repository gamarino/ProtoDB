"""An object store in memory, for the cluster chapter of the tutorial (``docs/tutorial/10-cluster.md``).

The five operations of an object store, the same ones ``S3ObjectStore`` and ``GCSObjectStore`` give; the
conditional writes are the only atomic ones. It is the class of ``docs/source/tutorials/cluster.rst``, kept
here so that the chapter stays short. A real deployment uses a real object store.
"""
import threading

from proto_db.storage.cloud import ObjectNotFoundError, PreconditionFailedError


class MemoryObjectStore:
    """The five operations of an object store, in memory."""

    def __init__(self):
        self._lock, self._objects, self._generation = threading.Lock(), {}, 0

    def _put(self, key, data):
        self._generation += 1
        self._objects[key] = (bytes(data), str(self._generation))
        return self._objects[key][1]

    def put_if_absent(self, key, data):
        with self._lock:
            if key in self._objects:
                raise PreconditionFailedError(key)
            return self._put(key, data)

    def put_if_match(self, key, data, etag):
        with self._lock:
            current = self._objects.get(key)
            if current is None or current[1] != etag:
                raise PreconditionFailedError(key)
            return self._put(key, data)

    def get(self, key):
        with self._lock:
            if key not in self._objects:
                raise ObjectNotFoundError(key)
            return self._objects[key]

    def list(self, prefix):
        with self._lock:
            return sorted(k for k in self._objects if k.startswith(prefix))

    def delete(self, key):
        with self._lock:
            self._objects.pop(key, None)
