# Test discovery helper for unittest when run from repository root.
# Ensure the project root is on sys.path so that 'proto_db' is importable
# and relative imports inside proto_db.tests resolve with a known package.
import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
