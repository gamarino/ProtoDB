"""Count ``os.fsync`` / ``os.fdatasync`` calls by call site and by role, in-process.

ProtoDB calls ``os.fsync`` through the ``os`` module at call time (``file.py``,
``tiered.py``, ``logfile.py``, ``rootlog.py``), so replacing the two functions in ``os``
observes every sync the engine issues.  Each call is attributed to

* a **role**, from the calling thread's name: ``persister`` (``proto-persister-*``:
  background shipping to the object store), ``follower`` (``proto-cluster-worker-*``: a
  node handling peers' replication and release messages), ``grant`` (a transport thread:
  a node answering a lease request records its promise), ``rejoin``
  (``proto-cluster-rejoin-*``), else ``committer`` (the application threads committing on
  this node: the lease holder's path, local acceptance included);
* a **site**: the two innermost ``proto_db`` frames (``module.function:line <- caller``)
  and the outermost ``proto_db`` frame of the call (the entry point that led to the sync).

The probe is informational; the benchmark cross-checks its totals with ``strace -c``
(see ``docs/source/performance.rst``).  It adds a stack walk (a few microseconds) per sync,
negligible next to a sync on a disk.
"""
import os
import stat
import sys
import threading
from collections import Counter

_LOCK = threading.Lock()
_COUNTS = Counter()
_ORIGINAL = {}
_SYNCED = {}            # (st_dev, st_ino) -> file size at its last sync
_BYTES = Counter()      # role -> bytes made durable by syncs (growth of the synced file since its last sync)


def _role(name: str) -> str:
    if name.startswith("proto-persister"):
        return "persister"
    if name.startswith("proto-cluster-worker"):
        return "follower"
    if name.startswith("proto-cluster-rejoin"):
        return "rejoin"
    if name.startswith(("proto-cluster-tcp", "proto-cluster-inproc", "proto-cluster-net")):
        return "grant"
    return "committer"


def _sites() -> tuple:
    """``(site, entry)``: the two innermost proto_db frames (``inner <- caller``) and the
    outermost proto_db frame of the current call stack."""
    frame = sys._getframe(2)
    inner, outer = [], None
    while frame is not None:
        path = frame.f_code.co_filename
        idx = path.rfind(os.sep + "proto_db" + os.sep)
        if idx >= 0:
            mod = path[idx + len("proto_db") + 2:-3].replace(os.sep, ".")
            if len(inner) < 2:
                inner.append(f"{mod}.{frame.f_code.co_name}:{frame.f_lineno}")
            outer = f"{mod}.{frame.f_code.co_name}"
        frame = frame.f_back
    return (" <- ".join(inner) or "outside proto_db"), outer or "outside proto_db"


def _wrap(name: str):
    original = _ORIGINAL[name]

    def counted(fd):
        inner, outer = _sites()
        role = _role(threading.current_thread().name)
        key = (role, name, inner, outer)
        try:
            st = os.fstat(fd)
        except OSError:
            st = None
        with _LOCK:
            _COUNTS[key] += 1
            if st is not None and stat.S_ISREG(st.st_mode):
                # Append-only files: the bytes this sync makes durable are the growth since the
                # file's previous sync (a new file counts whole).
                ident = (st.st_dev, st.st_ino)
                _BYTES[role] += max(0, st.st_size - _SYNCED.get(ident, 0))
                _SYNCED[ident] = st.st_size
        return original(fd)
    counted.__wrapped__ = original
    return counted


def install() -> None:
    """Replace ``os.fsync`` and ``os.fdatasync`` with counting wrappers (idempotent)."""
    with _LOCK:
        for name in ("fsync", "fdatasync"):
            if name not in _ORIGINAL and hasattr(os, name):
                _ORIGINAL[name] = getattr(os, name)
                setattr(os, name, _wrap(name))


def uninstall() -> None:
    """Restore the original functions."""
    with _LOCK:
        for name, original in _ORIGINAL.items():
            setattr(os, name, original)
        _ORIGINAL.clear()


def reset() -> None:
    """Forget the counts (and the synced bytes; files keep their last synced size)."""
    with _LOCK:
        _COUNTS.clear()
        _BYTES.clear()


def synced_bytes() -> dict:
    """Bytes made durable by syncs since the last :func:`reset`, per role: for each synced
    regular file, its growth since its previous sync (exact for append-only files)."""
    with _LOCK:
        return dict(_BYTES)


def snapshot() -> list:
    """``[[role, call, inner_site, outer_site, count], ...]`` sorted by count, descending."""
    with _LOCK:
        return sorted(([*k, v] for k, v in _COUNTS.items()), key=lambda row: -row[-1])


def by_role(rows) -> dict:
    """Total syncs per role from :func:`snapshot` rows."""
    out = Counter()
    for role, _call, _inner, _outer, count in rows:
        out[role] += count
    return dict(out)
