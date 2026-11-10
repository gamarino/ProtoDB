"""Exclusive ownership locks for local files that are repaired on open (8.3d re-review 11, I1;
ruling R59).

Opening a framed log (:func:`~proto_db.storage.logfile.open_log`) or a
:class:`~proto_db.storage.tiered.TieredStorage` directory truncates torn tails.  That is only
correct when no other live instance is writing the same files: a second instance would
truncate the first one's in-flight entry, the first would complete its write past the cut,
and the next open would trim the acknowledged entry as a torn tail.  Every owner that
truncates on open therefore holds an :class:`OwnerLock` for its whole lifetime, taken before
it reads or repairs anything and released when it is closed (or when its constructor fails).
A second open while the lock is held raises :class:`~proto_db.errors.ProtoLockingError`.

Mechanism (choice and justification):

* **Between processes**: an exclusive, non-blocking ``fcntl.flock`` on a dedicated lock file
  (``msvcrt.locking`` of its first byte on Windows).  ``flock`` was chosen over POSIX record
  locks (``fcntl.lockf`` / ``F_SETLK``) because POSIX locks belong to the *process*: a second
  lock request from the same process succeeds, and closing *any* descriptor of the file in
  that process silently drops the lock.  ``flock`` locks belong to the open file description,
  so they survive unrelated descriptors and, on local Linux file systems, also conflict
  between two instances of one process.  The descriptor is opened with ``O_CLOEXEC``, so a
  program started with ``exec`` never inherits the lock.
* **Within a process**: a process-wide registry of the lock files held, keyed by device and
  inode, refuses a second instance even where ``flock`` does not exclude descriptors of the
  same process (on NFS Linux emulates ``flock`` with POSIX locks; ``msvcrt.locking``
  semantics differ per platform).  Exclusion between instances of one process thus never
  depends on the file system.

Aliases (8.3d re-review 12, M3; ruling R60): the lock file path is resolved with
``os.path.realpath`` (owners derive it from the resolved path of what they protect, so a
symbolic link to a log or a directory names the same lock file), and the registry also holds
the device and inode of the protected *target* (the log file or the directory), so a hard
link to a log cannot be opened a second time in this process.  Between processes a hard link
is refused by :func:`~proto_db.storage.logfile.open_log`, which refuses a log file with more
than one link before it reads or repairs anything.  That holds only while the names share the
inode: a compaction renames a new file over the protected name, after which the other name is a
stale, independent copy of the log with one link, which is not detected and must never be
opened (D46 fix round 14, item 2).

The lock file holds no data and is never deleted (deleting it while held would let a second
instance lock a new file of the same name).  It is a separate file, not the log itself,
because log compactions replace the log file by a rename, which would leave a lock on the
old inode.

Platforms without either primitive: opening an owner raises
:class:`~proto_db.errors.ProtoNotSupportedError`, as :class:`~proto_db.storage.file.FileStorage`
already does (an unlocked open could destroy acknowledged data, so it is never done
silently).  Known limits, documented in D46: a process created with ``fork`` (without
``exec``) while a lock is held shares the open file description and thus the lock until the
owner closes; a lock on a network file system is only as reliable as that file system's lock
service.
"""
import os
import threading

from ..errors import ProtoLockingError, ProtoNotSupportedError, ProtoUnexpectedError, attach_secondary_errors

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows
    fcntl = None
try:
    import msvcrt
except ImportError:  # pragma: no cover - POSIX
    msvcrt = None

__all__ = ["OwnerLock", "locking_supported"]

_O_BINARY = getattr(os, "O_BINARY", 0)
_O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)

_held_lock = threading.Lock()
_held = set()                  # ("lock" | "target", st_dev, st_ino) of the lock files and targets held


def locking_supported() -> bool:
    """True when this platform provides an OS lock primitive :class:`OwnerLock` can use."""
    return fcntl is not None or msvcrt is not None


def _try_os_lock(fd: int) -> bool:
    """Take the exclusive OS lock on ``fd`` without blocking; False when another holds it."""
    if fcntl is not None:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        return True
    os.lseek(fd, 0, os.SEEK_SET)  # pragma: no cover - Windows
    try:  # pragma: no cover - Windows
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
    except PermissionError:  # pragma: no cover - Windows
        return False
    return True  # pragma: no cover - Windows


def _os_unlock(fd: int) -> None:
    """Release the OS lock held on ``fd``."""
    if fcntl is not None:
        fcntl.flock(fd, fcntl.LOCK_UN)
        return
    os.lseek(fd, 0, os.SEEK_SET)  # pragma: no cover - Windows
    msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)  # pragma: no cover - Windows


class OwnerLock:
    """An exclusive lock on ``path`` (a dedicated lock file, created if missing; its directory
    must exist), held from construction until :meth:`release` (see the module docstring).

    Args:
        path: the lock file (resolved with ``os.path.realpath``).
        what: a description of the owner for error messages (for example "the tiered storage
            directory /data/node1").
        target: the file or directory the lock protects; when it exists, its device and inode
            are registered too, so an alias of it (a hard link) is refused in this process.

    Raises ``ProtoLockingError`` when another instance, in this process or another one, holds
    the lock; ``ProtoNotSupportedError`` when the platform has no lock primitive;
    ``ProtoUnexpectedError`` when the lock file cannot be opened or locked.
    """

    def __init__(self, path: str, what: str, *, target=None):
        if not locking_supported():  # pragma: no cover - platforms without advisory locks
            raise ProtoNotSupportedError(
                f"No OS advisory file locking is available; {what} cannot be opened safely "
                f"(opening repairs files another instance could be writing)")
        self._path = os.path.realpath(os.fspath(path))
        self._what = what
        try:
            fd = os.open(self._path, os.O_RDWR | os.O_CREAT | _O_BINARY | _O_CLOEXEC, 0o644)
        except OSError as e:
            raise ProtoUnexpectedError(f"Cannot open the lock file {self._path} of {what}: {e}") from e
        try:
            st = os.fstat(fd)
            keys = [("lock", st.st_dev, st.st_ino)]
            if target is not None:
                try:
                    tst = os.stat(os.fspath(target))
                except FileNotFoundError:
                    tst = None              # not created yet: no alias of it can exist
                except OSError as e:
                    raise ProtoUnexpectedError(f"Cannot inspect {target} for {what}: {e}") from e
                if tst is not None:
                    keys.append(("target", tst.st_dev, tst.st_ino))
            keys = tuple(keys)
            with _held_lock:
                if keys[0] in _held:
                    raise self._busy("another instance in this process")
                if any(key in _held for key in keys[1:]):
                    raise self._busy(f"another instance in this process through another path to {target} "
                                     f"(a link)")
                try:
                    acquired = _try_os_lock(fd)
                except OSError as e:
                    raise ProtoUnexpectedError(f"Cannot lock {self._path} for {what}: {e}") from e
                if not acquired:
                    raise self._busy("another process")
                _held.update(keys)
        except BaseException as e:
            try:
                os.close(fd)
            except OSError as close_error:
                attach_secondary_errors(e, [close_error])
            raise
        self._fd = fd
        self._keys = keys
        self._released = False
        self._release_lock = threading.Lock()

    def track(self, fd: int) -> None:
        """Register the file open on ``fd`` as this lock's target, replacing the target
        registered before: called by an owner once it has created its file and after a
        compaction renamed a new file over it, so a hard link to the current file is refused
        in this process.  ``OSError`` from ``fstat`` propagates."""
        st = os.fstat(fd)
        key = ("target", st.st_dev, st.st_ino)
        with _held_lock:
            if self._released:
                return
            old = [k for k in self._keys if k[0] == "target"]
            _held.difference_update(old)
            _held.add(key)
            self._keys = tuple(k for k in self._keys if k[0] != "target") + (key,)

    def _busy(self, holder: str) -> ProtoLockingError:
        return ProtoLockingError(
            f"{self._what[:1].upper()}{self._what[1:]} is already open by {holder} (lock file {self._path} is "
            f"held); close that instance before opening it again (opening repairs files that the live "
            f"instance may be writing)")

    @property
    def path(self) -> str:
        """The lock file."""
        return self._path

    @property
    def held(self) -> bool:
        """True until :meth:`release`."""
        return not self._released

    def release(self) -> None:
        """Release the lock and close its descriptor.  Idempotent.  The registry entry is
        dropped and the descriptor closed even when unlocking fails (``ProtoUnexpectedError``);
        closing the descriptor releases the OS lock in any case."""
        with self._release_lock:
            if self._released:
                return
            self._released = True
        error = None
        try:
            _os_unlock(self._fd)
        except OSError as e:
            error = e
        try:
            os.close(self._fd)
        except OSError as e:
            error = error or e
        with _held_lock:
            _held.difference_update(self._keys)
        if error is not None:
            raise ProtoUnexpectedError(f"Releasing the lock {self._path} of {self._what} failed: {error}") from error
