"""Regression test of the power-loss model (Task 11.1): PowerLoss.writeback while a live node's log
compaction renames ``roots.log.tmp`` over ``roots.log`` (A3 run 43, GIL on: FileNotFoundError on
roots.log.tmp).  The rename is made to happen deterministically inside the capture, at two points:
while the directory is being listed, and while the files are being read.  writeback must not raise,
and the image must be one point in time: after a power loss, ``roots.log`` is there and holds the
pre-rename or the post-rename bytes, and a post-rename image has no ``roots.log.tmp``."""
import builtins
import os

import pytest

from tests.support import power_loss


def _node(tmp_path):
    d = tmp_path / "n0"
    d.mkdir()
    (d / "roots.log").write_bytes(b"old-log")
    (d / "roots.log.tmp").write_bytes(b"new-log")
    return d


def _check(d):
    names = sorted(os.listdir(d))
    contents = {n: (d / n).read_bytes() for n in names}
    assert "roots.log" in contents, f"roots.log missing after the power loss: {contents}"
    assert contents["roots.log"] in (b"old-log", b"new-log"), contents
    if contents["roots.log"] == b"new-log":
        assert "roots.log.tmp" not in contents, f"post-rename image still names the tmp file: {contents}"
    else:
        assert contents.get("roots.log.tmp") == b"new-log", f"pre-rename image without its tmp file: {contents}"


@pytest.mark.parametrize("when", ["while listing", "while reading"])
def test_writeback_takes_one_point_in_time_across_a_rename(tmp_path, monkeypatch, when):
    d = _node(tmp_path)
    fs = power_loss.PowerLoss(monkeypatch)
    done = []

    def rename_once():
        if not done:
            done.append(1)
            os.replace(d / "roots.log.tmp", d / "roots.log")      # the compaction's rename

    real_lstat, real_open = os.lstat, builtins.open

    def racing_lstat(path, *a, **kw):
        if when == "while listing" and str(path).endswith("roots.log.tmp"):
            rename_once()
        return real_lstat(path, *a, **kw)

    def racing_open(path, *a, **kw):
        if when == "while reading" and str(path).endswith(os.sep + "roots.log"):
            rename_once()
        return real_open(path, *a, **kw)
    monkeypatch.setattr(power_loss.os, "lstat", racing_lstat)
    monkeypatch.setattr(power_loss, "open", racing_open, raising=False)
    fs.writeback(str(tmp_path))
    monkeypatch.setattr(power_loss.os, "lstat", real_lstat)
    monkeypatch.delattr(power_loss, "open", raising=False)
    assert done, "the planned rename did not happen inside the capture"
    fs.power_loss(str(tmp_path))
    _check(d)
