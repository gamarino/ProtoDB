# Helper to run unittest discovery ensuring package context
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

if __name__ == '__main__':
    # Discover tests under proto_db/tests as package imports
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=os.path.join(ROOT, 'proto_db', 'tests'), top_level_dir=ROOT)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
