"""``python -m benchmarks``: run the ProtoDB performance suite (see :mod:`benchmarks.harness`)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from benchmarks.harness import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
