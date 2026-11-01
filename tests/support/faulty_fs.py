"""Fault-injecting file-system adapter for FileStorage crash tests.

``FaultyFS(plan)`` wraps :class:`proto_db.storage.file.OsFileSystem` and is passed as
``FileStorage(path, _fs=FaultyFS(plan))``.  The ``plan.fail_at``-th call (1-based) of
operation ``plan.op`` raises :class:`InjectedFault` (an ``OSError``).  A failing
``write`` first writes half of its bytes, modelling a torn write.

The adapter also models what a power loss would keep, so a test can call
:meth:`FaultyFS.power_loss` after the fault (with the storage abandoned) and then reopen:

* file contents written after the file's last ``fsync`` are truncated away;
* files created since the last ``fsync_dir`` of their directory disappear;
* renames not followed by an ``fsync_dir`` are undone (the target gets back its old
  content, or disappears if it did not exist).

``power_loss(reordered=True)`` is the adversarial variant of a file system that may
persist metadata out of order: unsynced renames survive while unsynced file creations
are still lost.  Code that publishes a root only after the directory entries of the data
it references are durable passes both variants.

The model assumes the append/rewrite patterns FileStorage uses (segments are only
appended to, the temporary root file is rewritten from offset 0).
"""
import os
from dataclasses import dataclass, field
from typing import Literal

from proto_db.storage.file import OsFileSystem

OPS = ("write", "fsync", "rename", "dirsync")


class InjectedFault(OSError):
    """The OSError raised at the planned failure point."""


@dataclass
class FaultPlan:
    """Fail the ``fail_at``-th (1-based) call of ``op``; ``fail_at=0`` never fails."""

    fail_at: int
    op: Literal["write", "fsync", "rename", "dirsync"]
    counts: dict = field(default_factory=lambda: dict.fromkeys(OPS, 0))
    fired: bool = False

    def __post_init__(self):
        if self.op not in OPS:
            raise ValueError(f"unknown op {self.op!r}")

    def tick(self, op):
        """Count a call of ``op``; return True when it is the one that must fail."""
        self.counts[op] += 1
        if not self.fired and op == self.op and self.counts[op] == self.fail_at:
            self.fired = True
            return True
        return False


class FaultyFS(OsFileSystem):
    """OsFileSystem with planned failures and a power-loss model (see module docstring)."""

    def __init__(self, plan: FaultPlan = None):
        self.plan = plan if plan is not None else FaultPlan(0, "write")
        self._paths = {}            # fd -> path
        self._synced = {}           # path -> durable length
        self._new_files = {}        # dir -> set of paths created since the last dir fsync
        self._renames = {}          # dir -> list of (dst, previous content or None, src synced len)

    def _fail(self, op):
        if self.plan.tick(op):
            raise InjectedFault(5, f"injected {op} failure #{self.plan.fail_at}")

    def open(self, path, flags, mode=0o644):
        existed = os.path.exists(path)
        fd = super().open(path, flags, mode)
        self._paths[fd] = path
        if not existed:
            self._new_files.setdefault(os.path.dirname(path), set()).add(path)
            self._synced[path] = 0
        elif flags & os.O_TRUNC:
            self._synced[path] = 0
        else:
            self._synced.setdefault(path, os.path.getsize(path))
        return fd

    def write(self, fd, data, offset):
        if self.plan.tick("write"):
            half = bytes(data[: len(data) // 2])
            if half:
                super().write(fd, half, offset)
            raise InjectedFault(5, f"injected write failure #{self.plan.fail_at}")
        return super().write(fd, data, offset)

    def fsync(self, fd):
        self._fail("fsync")
        super().fsync(fd)
        path = self._paths[fd]
        self._synced[path] = os.fstat(fd).st_size

    def close(self, fd):
        self._paths.pop(fd, None)
        super().close(fd)

    def replace(self, src, dst):
        self._fail("rename")
        previous = None
        if os.path.exists(dst):
            with open(dst, "rb") as f:
                previous = f.read()
        super().replace(src, dst)
        self._renames.setdefault(os.path.dirname(dst), []).append((dst, previous))
        self._synced[dst] = self._synced.pop(src, 0)

    def fsync_dir(self, path):
        self._fail("dirsync")
        super().fsync_dir(path)
        self._new_files.pop(path, None)
        self._renames.pop(path, None)

    def power_loss(self, reordered: bool = False):
        """Rewrite the files on disk to what survives a power loss at this instant."""
        for directory, renames in ({} if reordered else self._renames).items():
            for dst, previous in reversed(renames):
                if previous is None:
                    if os.path.exists(dst):
                        os.unlink(dst)
                    self._synced.pop(dst, None)
                else:
                    with open(dst, "wb") as f:
                        f.write(previous)
                    self._synced[dst] = len(previous)
        for paths in self._new_files.values():
            for path in paths:
                if os.path.exists(path):
                    os.unlink(path)
                self._synced.pop(path, None)
        for path, length in self._synced.items():
            if os.path.exists(path) and os.path.getsize(path) > length:
                os.truncate(path, length)
        self._renames.clear()
        self._new_files.clear()
