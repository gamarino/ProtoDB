"""Regression test of the power-loss model (Task 11.1), for CI job 131949937344 (ext4): "Own segment
... does not start with its segment header but holds other data" after a modelled power loss.  ext4
gives a freed inode's number to the next file created.  Simulated deterministically (the development
disk is ZFS): file A is synced and removed; the new segment B, never synced, is reported with A's
inode number (os.lstat and os.fstat rewritten for B).  After the power loss B must hold its own
durable contents (none: it was never synced) or nothing -- never A's bytes, which no disk can show
under B's name."""
import os

from tests.support import power_loss

HEADER = b"SEGHDR-B"


def test_a_reused_inode_number_never_gives_a_new_file_an_old_files_bytes(tmp_path, monkeypatch):
    d = tmp_path / "n1"
    d.mkdir()
    fs = power_loss.PowerLoss(monkeypatch)
    real_lstat, real_fstat = os.lstat, os.fstat
    a = d / "old.tmp"
    with open(a, "wb") as f:
        f.write(b"OTHER FILE DATA " * 8)
        f.flush()
        os.fsync(f.fileno())                     # A durable
    ino_a = real_lstat(a).st_ino
    fd = os.open(d, os.O_RDONLY)
    os.fsync(fd)                                 # the directory durable with A
    os.unlink(a)                                 # A removed (the unlink itself never made durable)
    b = d / "c76f.seg"
    with open(b, "wb") as f:
        f.write(HEADER + b"records...")          # B created, never synced
    ino_b = real_lstat(b).st_ino

    def reuse(st):
        if st.st_ino != ino_b:
            return st
        t = list(st[:10])
        t[1] = ino_a                             # ext4's inode reuse, simulated
        return os.stat_result(t)
    monkeypatch.setattr(os, "lstat", lambda *a, **kw: reuse(real_lstat(*a, **kw)))
    monkeypatch.setattr(os, "fstat", lambda *a, **kw: reuse(real_fstat(*a, **kw)))
    os.fsync(fd)                                 # the directory durable with B (B's data not)
    os.close(fd)
    monkeypatch.setattr(os, "lstat", real_lstat)
    monkeypatch.setattr(os, "fstat", real_fstat)
    fs.power_loss(str(d))                        # d itself is the durable root of the model
    contents = {n: (d / n).read_bytes() for n in sorted(os.listdir(d))}
    assert contents.get("c76f.seg") in (b"", None), \
        f"the never-synced segment was restored with another file's bytes: {contents.get('c76f.seg')[:32]!r}"
