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
import itertools
import os
import stat
import threading


class PowerLoss:
    """Record what ``fsync`` makes durable and replay a power loss (see the module docstring)."""

    def __init__(self, monkeypatch):
        self._seq = 0
        self.files = {}          # file id -> (seq, bytes)
        self.dirs = {}           # realpath -> (seq, {name: file id})
        self.syncs = 0
        # A file id names one inode for its lifetime.  An inode number alone does not: ext4
        # gives a freed inode's number to the next file created (CI, Task 11.1: a new segment
        # was restored with the bytes of a removed file that had held its inode number, "does
        # not start with its segment header but holds other data").  So the model maps
        # (dev, ino) to a fresh id, and drops that mapping when the inode's last name goes
        # (unlink, remove, or a rename over it): the next file with that number gets a new id,
        # with no durable contents until it is synced.  A directory record keeps the old id,
        # so an unlink that was never made durable still brings the old file back intact.
        self._ids = {}
        self._id_seq = itertools.count(1)
        self._ids_lock = threading.Lock()
        real_fsync = os.fsync
        real_unlink, real_remove, real_replace, real_rename = os.unlink, os.remove, os.replace, os.rename

        def fsync(fd):
            real_fsync(fd)
            self._record_fd(fd)

        def freeing(real, path_arg, dir_fd_arg):
            def call(*args, **kw):
                path = args[path_arg] if len(args) > path_arg else kw.get(("path", "dst")[path_arg])
                dir_fd = kw.get(dir_fd_arg)
                try:
                    st = os.lstat(path, dir_fd=dir_fd) if dir_fd is not None else os.lstat(path)
                except (FileNotFoundError, NotADirectoryError, TypeError):
                    st = None
                result = real(*args, **kw)
                if st is not None and stat.S_ISREG(st.st_mode) and st.st_nlink <= 1:
                    with self._ids_lock:
                        self._ids.pop((st.st_dev, st.st_ino), None)
                return result
            return call

        monkeypatch.setattr(os, "fsync", fsync)
        monkeypatch.setattr(os, "unlink", freeing(real_unlink, 0, "dir_fd"))
        monkeypatch.setattr(os, "remove", freeing(real_remove, 0, "dir_fd"))
        monkeypatch.setattr(os, "replace", freeing(real_replace, 1, "dst_dir_fd"))
        monkeypatch.setattr(os, "rename", freeing(real_rename, 1, "dst_dir_fd"))

    def _id(self, dev: int, ino: int) -> int:
        """The file id of the inode ``(dev, ino)`` now (see ``__init__``)."""
        with self._ids_lock:
            key = (dev, ino)
            fid = self._ids.get(key)
            if fid is None:
                fid = self._ids[key] = next(self._id_seq)
            return fid

    def _ided(self, entries: dict) -> dict:
        return {name: self._id(*key[:2]) for name, key in entries.items()}

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
            self.dirs[os.path.realpath(path)] = (self._next(), self._ided(self._entries(path)))
        elif stat.S_ISREG(st.st_mode):
            with open(f"/proc/self/fd/{fd}", "rb") as f:
                self.files[self._id(st.st_dev, st.st_ino)] = (self._next(), f.read())

    #: Attempts at a consistent image in :meth:`writeback` before it gives up.
    WRITEBACK_ATTEMPTS = 200

    def writeback(self, root: str) -> None:
        """Make everything under ``root`` durable as it is now (the kernel's write-back).

        Live nodes rename, create and remove files while the image is taken (a log compaction
        renames ``roots.log.tmp`` over ``roots.log``; Task 11.1, A3 run 43).  The image must be
        one point in time, or it may hold neither the old nor the new file, a state no disk can
        reach.  So it is taken as a stable capture: list every directory with each entry's
        inode, size and modification time, read every regular file, list again; the image is
        recorded only when both listings are equal and every file read matches its listing
        (otherwise, or when a file vanishes, the capture restarts).  Equal listings around the
        reads mean no rename, creation, removal or write happened between them, so the image is
        the state at that moment.  ``AssertionError`` after ``WRITEBACK_ATTEMPTS`` restarts."""
        for _ in range(self.WRITEBACK_ATTEMPTS):
            image = self._stable_image(root)
            if image is not None:
                listing, contents = image
                for dirpath, entries in listing.items():
                    self.dirs[dirpath] = (self._next(), self._ided(entries))
                for key, data in contents.items():
                    self.files[self._id(*key)] = (self._next(), data)
                return
        raise AssertionError(f"writeback: no consistent image of {root} in {self.WRITEBACK_ATTEMPTS} attempts")

    @staticmethod
    def _listing(root: str):
        """``{realpath: {name: (dev, ino, mode, size, mtime_ns)}}`` of every directory under
        ``root``, or ``None`` when an entry vanished while it was listed."""
        out = {}
        try:
            for dirpath, dirnames, filenames in os.walk(root):
                entries = {}
                for name in os.listdir(dirpath):
                    st = os.lstat(os.path.join(dirpath, name))
                    entries[name] = (st.st_dev, st.st_ino, st.st_mode, st.st_size, st.st_mtime_ns)
                out[os.path.realpath(dirpath)] = entries
        except FileNotFoundError:
            return None
        return out

    def _stable_image(self, root: str):
        """One capture attempt: ``(listing, {(dev, ino): bytes})`` or ``None`` (restart)."""
        before = self._listing(root)
        if before is None:
            return None
        contents = {}
        for dirpath, entries in before.items():
            for name, (dev, ino, mode, size, mtime) in entries.items():
                if not stat.S_ISREG(mode):
                    continue
                try:
                    with open(os.path.join(dirpath, name), "rb") as f:
                        st = os.fstat(f.fileno())
                        data = f.read()
                except FileNotFoundError:
                    return None
                if (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns) != (dev, ino, size, mtime) \
                        or len(data) != size:
                    return None
                contents[(dev, ino)] = data
        return (before, contents) if self._listing(root) == before else None

    def durable_contents(self, path: str):
        """The bytes a power loss would keep for the file now at ``path`` (``None``: no record)."""
        st = os.lstat(path)
        entry = self.files.get(self._id(st.st_dev, st.st_ino))
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


class FrozenPowerLoss(PowerLoss):
    """:class:`PowerLoss` that stops recording ``fsync`` calls under a dead node's directory
    (a host without power syncs nothing; closing its space must not make anything durable)."""

    def __init__(self, monkeypatch):
        super().__init__(monkeypatch)
        self.frozen = frozenset()

    # ``frozen`` is replaced, never changed in place (Task 8.3n fix round 2): the dead node's
    # straggling threads read it in ``fsync`` while the test thread freezes a directory, and
    # iterating a set that another thread changes raises ``RuntimeError`` -- it once fenced
    # a live node's persister in the middle of a run (free-threaded 3.14t, GIL off).
    def freeze(self, path: str) -> None:
        self.frozen = self.frozen | {path}

    def thaw_all(self) -> None:
        self.frozen = frozenset()

    @staticmethod
    def _entries(path: str) -> dict:
        # The live nodes create and remove files while a directory sync is recorded: an entry
        # removed between the listing and its lstat is simply not in the recorded listing.
        out = {}
        for name in os.listdir(path):
            try:
                st = os.lstat(os.path.join(path, name))
            except FileNotFoundError:
                continue
            out[name] = (st.st_dev, st.st_ino)
        return out

    def _record_fd(self, fd: int) -> None:
        try:
            path = os.path.realpath(os.readlink(f"/proc/self/fd/{fd}"))
        except OSError:
            path = ""
        if any(path == d or path.startswith(d + "/") for d in self.frozen):
            return
        super()._record_fd(fd)
