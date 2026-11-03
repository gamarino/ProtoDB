# Ensure the project root is on sys.path so that 'proto_db' and the 'tests' package are importable
# when pytest is run from the repository root.
import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
