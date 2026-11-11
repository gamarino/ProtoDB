#!/usr/bin/env python3
"""Build the C1 exercise: a scratch copy of the repository with the bug planted.

Usage, from anywhere inside the repository::

    python docs/kit/exercise-c1/make_exercise.py <destination directory>

The destination must not exist yet, or be empty, and must be outside the repository.  The script
never writes into the repository: it exports the committed tree (``git archive HEAD``) into the
destination, then edits only the copy:

* it plants the bug: an uncertain lease takes the recovery round's result as its commit base,
  alone, as the code did before commit 276a256 (8.3n re-review 2, finding C1);
* it puts back the comments and docstrings of that time, so that the code does not describe its
  own fix;
* it removes what states the solution: ``docs/``, ``CHANGELOG.md``, the C1 regression tests
  (``tests/unit/test_cluster_recovery_base.py``), the plant catalogue
  (``tests/support/plant_defects.py``) and the kit's own test (``tests/docs/test_kit.py``);
* it copies this exercise's ``README.md`` and its test into ``exercise_c1/`` (never the solution).

Every edit must match the exported text exactly once, and every edited file is checked before any
is written; when the code has moved, the script stops with an error naming the edit, and no
planted file is written.

Then, in the destination::

    python -m pytest exercise_c1/test_c1_exercise.py      # fails: an acknowledged commit is lost
"""
import os
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))

#: Paths removed from the copy: they state the fix.
REMOVED = ("docs", "CHANGELOG.md", "tests/unit/test_cluster_recovery_base.py", "tests/support/plant_defects.py",
           "tests/docs/test_kit.py")

#: Files of this exercise copied into ``exercise_c1/`` (never SOLUTION.md).
HANDED_OUT = ("README.md", "test_c1_exercise.py")

_T = "proto_db/transactions.py"
_C = "proto_db/cluster/coordinator.py"

#: ``(path, old, new)``: each ``old`` must occur exactly once in the exported file.
EDITS = (
    # The bug: the key check of the fix is gone.
    (_T,
     "        known, current, label = coordinator.recover_root()\n"
     "        if _phase_1_floor(lease) and (not known or compare_roots(lease.root_term, lease.root, label, current) > 0):\n"
     "            # Re-review 2 of 8.3n, C1: the recovery answers promised nothing and may predate\n"
     "            # a root accepted afterwards; the phase-1 root may not (see the docstring).\n"
     "            known, current, label = True, lease.root, lease.root_term\n",
     "        known, current, label = coordinator.recover_root()\n"),
    # The docstrings and comments of the time, which do not describe the fix.
    (_T,
     "    else the higher, in :func:`~proto_db.cluster.rootlog.compare_roots` order, of the lease's\n"
     "    root -- its phase-1 value, the floor (:func:`_phase_1_floor`, R127): the recovery query\n"
     "    promises nothing, so its result alone may predate a root accepted afterwards -- and the\n"
     "    result of a recovery round.  When neither knows any root (a cluster started on empty\n",
     "    else the result of a recovery round.  When no node of the answering majority knows any\n"
     "    root (a cluster started on empty\n"),
    (_T, "instead of issuing the same floor base again (R133).", "instead of issuing the same base again (R133)."),
    (_T,
     "      afterwards, and the phase-1 floor of the next term is the highest root those members\n"
     "      accepted.  A live acceptor of R either holds R's records itself, or accepted R alone\n"
     "      after ``majority`` copies existed, at least one of them on a live member; so a floor\n"
     "      that reports R again can re-commit it.\n"
     "    So the stall the floor otherwise caused",
     "      afterwards.  A live acceptor of R either holds R's records itself, or accepted R alone\n"
     "      after ``majority`` copies existed, at least one of them on a live member; so a base\n"
     "      that reports R again can re-commit it.\n"
     "    So the stall such a base otherwise caused"),
    (_C,
     "    (:meth:`ClusterCoordinator.recover_root`): the roots of a majority of peers.  Its base\n"
     "    never orders below the lease's own root, the phase-1 value its granters reported after\n"
     "    promising the term: the recovery answers promise nothing and can miss a root accepted\n"
     "    after they were given (re-review 2 of 8.3n, C1).\n",
     "    (:meth:`ClusterCoordinator.recover_root`): the roots of a majority of peers.\n"),
    (_C,
     "        this node's.  The query carries ballot 0 and promises nothing: an answering voter may\n"
     "        accept a root of an older term after it answered, so the result orders at or above\n"
     "        every root a majority accepted *before* the intersecting member answered, and may miss\n"
     "        one accepted after (re-review 2 of 8.3n, C1).  A commit's base therefore never comes\n"
     "        from this result alone: :func:`proto_db.transactions._lease_base` floors it at the\n"
     "        lease's phase-1 root, whose granters promised the term before they replied.  With\n",
     "        this node's.  Every committed root was acknowledged by a majority, which intersects\n"
     "        the answering majority, so the result is the newest committed root or a root built on\n"
     "        it.  With\n"),
    ("tests/support/manual_cluster.py",
     "        :func:`~proto_db.transactions._lease_base`: the lease's root when certain, else the\n"
     "        higher of it (the phase-1 root) and the root a recovery round answered by\n"
     "        ``responders`` returns (re-review 2 of 8.3n, C1).  Raises",
     "        :func:`~proto_db.transactions._lease_base`: the lease's root when certain, else the\n"
     "        root a recovery round answered by ``responders`` returns.  Raises"),
    ("tests/unit/test_cluster_transactions.py",
     "    The round ends at a majority of witnesses.  Its answers promise nothing (ballot 0), so it\n"
     "    may miss a root accepted after they were given; a commit's base is therefore floored at\n"
     "    the lease's phase-1 root (re-review 2 of 8.3n, C1).  Here",
     "    The round ends at a majority of witnesses.  Here"),
)

#: ``(path, first line of the cut, first line kept after it)``: a block removed whole.
CUTS = (
    (_T, "def _phase_1_floor(lease) -> bool:\n", "def _guard_error("),
)


def _repo_root() -> str:
    out = subprocess.run(["git", "-C", HERE, "rev-parse", "--show-toplevel"], capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"make_exercise: not inside a git checkout ({out.stderr.strip()})")
    return out.stdout.strip()


def _replace_once(text: str, old: str, new: str, where: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"make_exercise: an edit of {where} matches {count} times, not once; the code moved "
                         f"since the exercise was written.  Edit text:\n{old}")
    return text.replace(old, new)


def plant(dest: str) -> None:
    """Apply :data:`EDITS` and :data:`CUTS` to the copy in ``dest``."""
    texts = {}

    def load(path):
        if path not in texts:
            with open(os.path.join(dest, path), encoding="utf-8") as f:
                texts[path] = f.read()
        return texts[path]

    for path, old, new in EDITS:
        texts[path] = _replace_once(load(path), old, new, path)
    for path, first, kept in CUTS:
        text = load(path)
        if text.count(first) != 1 or text.count(kept) != 1:
            raise SystemExit(f"make_exercise: the cut in {path} from {first!r} to {kept!r} does not match once")
        start, end = text.index(first), text.index(kept)
        if end <= start:
            raise SystemExit(f"make_exercise: the cut in {path} ends before it starts")
        texts[path] = text[:start] + text[end:]
    for path, text in texts.items():
        if "_phase_1_floor" in text:
            raise SystemExit(f"make_exercise: {path} still names the fix after planting")
    for path, text in texts.items():           # every check passed: now write
        with open(os.path.join(dest, path), "w", encoding="utf-8") as f:
            f.write(text)


def main(argv) -> int:
    if len(argv) != 1:
        print(__doc__)
        return 2
    root = os.path.realpath(_repo_root())
    dest = os.path.realpath(argv[0])
    if dest == root or dest.startswith(root + os.sep):
        sys.exit(f"make_exercise: {dest} is inside the repository; choose a directory outside it")
    if os.path.exists(dest) and os.listdir(dest):
        sys.exit(f"make_exercise: {dest} is not empty")
    os.makedirs(dest, exist_ok=True)
    archive = subprocess.run(["git", "-C", root, "archive", "--format=tar", "HEAD"], capture_output=True, check=True)
    subprocess.run(["tar", "-x", "-C", dest], input=archive.stdout, check=True)
    for path in REMOVED:
        full = os.path.join(dest, path)
        if os.path.isdir(full):
            shutil.rmtree(full)
        elif os.path.exists(full):
            os.remove(full)
    plant(dest)
    os.makedirs(os.path.join(dest, "exercise_c1"))
    for name in HANDED_OUT:
        shutil.copy(os.path.join(HERE, name), os.path.join(dest, "exercise_c1", name))
    print(f"The exercise is ready in {dest}.\n"
          f"  cd {dest}\n"
          f"  python -m pytest exercise_c1/test_c1_exercise.py\n"
          f"Read exercise_c1/README.md first.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
