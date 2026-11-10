"""Count atom decodes in this process (Task 9c CARRY: ruling R110 and the history-depth measurement).

``install()`` wraps :func:`proto_db.core.codec.decode_state`, the step every atom load from a
storage payload runs once (``Atom._load``, phase 2), and counts its calls.  ``count()`` is the
total so far.  The count is the number of atoms decoded from stored bytes, whether or not a
decoded-atom cache is enabled: with the cache it equals the cache's ``loads`` (plus any load
outside the cache), and with ``atom_cache_entries=0`` it is every decode the reads paid.

Counters are per thread (one list cell each, registered once under a lock), so counting adds
no shared write to the hot path on a free-threaded interpreter.  This is benchmark
instrumentation: it adds one Python call per decode to every setting measured with it alike.
"""
import threading

_cells = []
_lock = threading.Lock()
_local = threading.local()
_installed = False


def _cell() -> list:
    cell = getattr(_local, "cell", None)
    if cell is None:
        cell = [0]
        with _lock:
            _cells.append(cell)
        _local.cell = cell
    return cell


def install() -> None:
    """Start counting (idempotent)."""
    global _installed
    with _lock:
        if _installed:
            return
        _installed = True
    from proto_db.core import codec
    original = codec.decode_state

    def counting(data, load_ref):
        _cell()[0] += 1
        return original(data, load_ref)

    codec.decode_state = counting


def count() -> int:
    """Atom decodes counted so far, over every thread."""
    with _lock:
        return sum(c[0] for c in _cells)
