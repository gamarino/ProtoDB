"""Export a commit for measurement: ``python -m benchmarks.export COMMIT DEST``.

Measurements are taken from a clean ``git archive`` export of the commit under test, never
from a working tree (Task 9c).  The export is written to ``DEST`` (which must not exist) with
a ``BENCH_SOURCE.json`` at its root holding the full commit hash and a SHA-256 of every
exported file; the harness re-verifies those digests when it starts and records
``git.commit`` and ``git.clean`` in the results JSON (:func:`benchmarks.harness.source_revision`).
Run the suite from the export's directory so that ``proto_db`` and ``benchmarks`` are
imported from it.
"""
import io
import json
import os
import subprocess
import sys
import tarfile

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def export(commit: str, dest: str) -> dict:
    """Write ``git archive COMMIT`` into ``dest`` plus its source record; return the record."""
    from benchmarks.harness import SOURCE_FILE, tree_digests
    full = subprocess.run(["git", "rev-parse", "--verify", f"{commit}^{{commit}}"], cwd=REPO, check=True,
                          capture_output=True, text=True).stdout.strip()
    if os.path.exists(dest):
        raise SystemExit(f"{dest} exists; choose a new directory")
    data = subprocess.run(["git", "archive", "--format=tar", full], cwd=REPO, check=True,
                          capture_output=True).stdout
    os.makedirs(dest)
    with tarfile.open(fileobj=io.BytesIO(data)) as tar:
        names = [m.name for m in tar.getmembers() if m.isfile()]
        tar.extractall(dest, filter="data")
    record = {"commit": full, "method": "git archive", "files": tree_digests(dest, sorted(names))}
    with open(os.path.join(dest, SOURCE_FILE), "w", encoding="utf-8") as f:
        json.dump(record, f, indent=0, sort_keys=True)
    return record


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit(__doc__.splitlines()[0])
    rec = export(sys.argv[1], os.path.abspath(sys.argv[2]))
    print(f"exported {rec['commit']} ({len(rec['files'])} files) to {sys.argv[2]}")
