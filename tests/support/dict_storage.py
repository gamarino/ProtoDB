"""Trivial in-memory atom store used until the real storage v2 exists (Phase 2).

Payloads are msgpack-encoded on push and decoded on get, so every test that goes
through this store also proves that codec output is msgpack-serializable.
"""
import threading
import uuid

import msgpack

from proto_db.core.atom import AtomPointer
from proto_db.errors import ProtoCorruptionError


class DictStorage:
    """Thread-safe dict of msgpack blobs keyed by ``AtomPointer``.

    Counters ``push_count`` and ``get_count`` let tests assert how often atoms
    were written and read.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._segment = uuid.uuid4()
        self._blobs = {}
        self._order = []
        self.push_count = 0
        self.get_count = 0

    def push_atom(self, payload: dict) -> AtomPointer:
        """Encode ``payload`` with msgpack, store it and return its pointer."""
        blob = msgpack.packb(payload, use_bin_type=True)
        with self._lock:
            ptr = AtomPointer(self._segment, len(self._order))
            self._blobs[ptr] = blob
            self._order.append(ptr)
            self.push_count += 1
        return ptr

    def get_atom(self, ptr: AtomPointer) -> dict:
        """Return the decoded payload stored at ``ptr``."""
        with self._lock:
            self.get_count += 1
            blob = self._blobs.get(ptr)
        if blob is None:
            raise ProtoCorruptionError(f"No atom at {ptr}")
        return msgpack.unpackb(blob, raw=False)

    def payloads_in_order(self):
        """Return every stored payload, in push order (decoded)."""
        with self._lock:
            order = list(self._order)
        return [self.get_atom(p) for p in order]
