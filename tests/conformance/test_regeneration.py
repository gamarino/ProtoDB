"""The generator still reproduces the committed vectors (review M-3 of Task 7; opt-in).

``test_conformance.py`` checks the committed files and never regenerates them, so a change in
what the writer does (when an oid is drawn, the order of records, a segment roll) leaves the
committed vectors valid but no longer what ``generate.py`` writes: the parents vectors drifted
that way from ``ab91eca`` to ``655dd6d``.  This test runs the generator into a fresh directory, in
a separate process, and compares its pinned digests with ``vectors.sha256``.  It writes several
megabytes and takes seconds, so it runs only when ``PROTODB_REGEN_VECTORS=1``.

Catches: committed vectors the generator no longer reproduces.
"""
import os
import subprocess
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]

pytestmark = [pytest.mark.timeout(300),
              pytest.mark.skipif(os.environ.get("PROTODB_REGEN_VECTORS") != "1",
                                 reason="opt-in: set PROTODB_REGEN_VECTORS=1 to regenerate the vectors")]

_SCRIPT = """
import sys
from pathlib import Path
import tests.conformance.generate as g
out = Path(sys.argv[1])
g.VECTORS = out / "vectors"
g.MANIFEST = out / "manifest.json"
g.PINS = out / "vectors.sha256"
g.main()
"""


def test_the_generator_reproduces_the_committed_vectors(tmp_path):
    out = tmp_path / "regenerated"
    out.mkdir()
    subprocess.run([sys.executable, "-c", _SCRIPT, str(out)], check=True, cwd=str(REPO), timeout=280)
    assert (out / "vectors.sha256").read_text() == (HERE / "vectors.sha256").read_text(), (
        "generate.py no longer reproduces the committed vectors: regenerate them (python -m "
        "tests.conformance.generate), explain the change, and update the digest in format-spec.rst")
