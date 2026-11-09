"""A power-loss model for whole directories, recorded from the ``fsync`` calls of the process
(Task 8.3e, defect D-SAFE-2).

``PowerLoss(monkeypatch)`` wraps ``os.fsync`` for the duration of a test and records, at each
call, what that call makes durable:

* a regular file: its whole contents, keyed by inode (a later rename keeps the inode);
* a directory: its entries (name -> inode).

``writeback(root)`` models the kernel's periodic write-back of dirty pages (seconds after a
write, in a real system): every file and directory under ``root`` becomes durable as it is now.
It lets a test make *old* state durable without an ``fsync`` -- the case where a power loss
keeps an older entry of a log (the rejoined marker) and drops a newer one (a promise).

``power_loss(root)`` rewrites ``root`` (which must not be in use: close the storage first)
to what a power loss keeps under the strictest legal model: every directory holds the
entries it had at its last durable point, and each file the contents of its inode at the
inode's last durable point (empty when it never had one).  Entries created after their
directory's last durable point disappear; renames after it are undone.  A directory never
made durable keeps nothing.  ``root`` itself is assumed durable (it existed at a
write-back).

Limits of the model (8.3e review, M2) -- what it cannot generate, and why the tests built on it
remain meaningful:

* **Unsynced data is all or nothing per file.**  A file keeps exactly its contents at its last
  durable point: no partial write-back, no page written back out of order, no file extended
  with zeros (a size update that reached the disk before its data).  Real file systems can do
  all three.  For the acceptor log this does not matter: entries are CRC-framed, a torn or
  zero-filled tail is trimmed at open (:mod:`proto_db.storage.logfile`), and only committed
  entries may lie past the last sync (ruling R67).  For data, such partial states are handled by
  code these tests do not reach: a torn own segment is trimmed by
  ``TieredStorage._repair_own_segment`` before it is shipped, copies of other nodes' segments
  are CRC-walked, and a short or corrupt store object falls back to peers and parts.  Those
  paths have their own tests (``test_log_torn_tails.py``, ``test_self_validating_framing.py``).
* **A directory sync records its whole listing**, including entries whose files were never
  synced; after the power loss they reappear empty.  This is realistic (the entry is durable,
  the data is not).
* **Only ``os.fsync`` is wrapped.**  An ``os.fdatasync`` (or ``O_SYNC`` write) would not be
  recorded, so the model would drop data the real system keeps -- conservative: it can only
  report a missing sync that is not missing, never hide one.  proto_db calls ``os.fsync`` only.
* ``root`` is assumed durable, and the model is per process: every fsync recorded is one this
  process made (subprocess peers are not modelled).
"""
import os
import stat


class PowerLoss:
    """Record what ``fsync`` makes durable and replay a power loss (see the module docstring)."""

    def __init__(self, monkeypatch):
        self._seq = 0
        self.files = {}          # (dev, ino) -> (seq, bytes)
        self.dirs = {}           # realpath -> (seq, {name: (dev, ino)})
        self.syncs = 0
        real_fsync = os.fsync

        def fsync(fd):
            real_fsync(fd)
            self._record_fd(fd)

        monkeypatch.setattr(os, "fsync", fsync)

    def _next(self) -> int:
        self._seq += 1
        return self._seq

    @staticmethod
    def _entries(path: str) -> dict:
        out = {}
        for name in os.listdir(path):
            st = os.lstat(os.path.join(path, name))
            out[name] = (st.st_dev, st.st_ino)
        return out

    def _record_fd(self, fd: int) -> None:
        self.syncs += 1
        st = os.fstat(fd)
        path = os.readlink(f"/proc/self/fd/{fd}")
        if stat.S_ISDIR(st.st_mode):
            self.dirs[os.path.realpath(path)] = (self._next(), self._entries(path))
        elif stat.S_ISREG(st.st_mode):
            with open(f"/proc/self/fd/{fd}", "rb") as f:
                self.files[(st.st_dev, st.st_ino)] = (self._next(), f.read())

    def writeback(self, root: str) -> None:
        """Make everything under ``root`` durable as it is now (the kernel's write-back)."""
        for dirpath, dirnames, filenames in os.walk(root):
            self.dirs[os.path.realpath(dirpath)] = (self._next(), self._entries(dirpath))
            for name in filenames:
                path = os.path.join(dirpath, name)
                st = os.lstat(path)
                if stat.S_ISREG(st.st_mode):
                    with open(path, "rb") as f:
                        self.files[(st.st_dev, st.st_ino)] = (self._next(), f.read())

    def durable_contents(self, path: str):
        """The bytes a power loss would keep for the file now at ``path`` (``None``: no record)."""
        st = os.lstat(path)
        entry = self.files.get((st.st_dev, st.st_ino))
        return None if entry is None else entry[1]

    def power_loss(self, root: str) -> None:
        """Rewrite ``root`` to what a power loss keeps (see the module docstring)."""
        root = os.path.realpath(root)
        self._restore_dir(root)

    def _restore_dir(self, path: str) -> None:
        durable = self.dirs.get(path)
        keep = durable[1] if durable is not None else {}
        current = self._entries(path)
        # The contents of every inode that will be named here, read before anything is removed.
        contents = {}
        for name, key in keep.items():
            entry = self.files.get(key)
            contents[name] = entry[1] if entry is not None else b""
        for name in current:
            full = os.path.join(path, name)
            if os.path.isdir(full) and not os.path.islink(full):
                if name in keep:
                    self._restore_dir(os.path.realpath(full))
                else:
                    _remove_tree(full)
            else:
                os.unlink(full)
        for name, key in keep.items():
            full = os.path.join(path, name)
            if os.path.isdir(full):
                continue
            # A snapshot: live nodes' fsync calls add entries meanwhile, and iterating a dict
            # that another thread changes raises RuntimeError (Task 8.3n fix round 2).
            if (path + os.sep + name) in self.dirs or any(d.startswith(full + os.sep) for d in tuple(self.dirs)):
                os.mkdir(full)
                self._restore_dir(full)
                continue
            with open(full, "wb") as f:
                f.write(contents[name])


def _remove_tree(path: str) -> None:
    for dirpath, dirnames, filenames in os.walk(path, topdown=False):
        for name in filenames:
            os.unlink(os.path.join(dirpath, name))
        for name in dirnames:
            os.rmdir(os.path.join(dirpath, name))
    os.rmdir(path)
