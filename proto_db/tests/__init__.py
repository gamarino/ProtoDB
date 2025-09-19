# Make tests a package so relative imports (e.g., from ..module import X) work under unittest discovery.
# Also expose load_tests to allow `python -m unittest proto_db.tests` to discover tests.
import os
import unittest

def load_tests(loader, tests, pattern):
    start_dir = os.path.dirname(__file__)
    # Default pattern to test_*.py if none provided
    pattern = pattern or 'test_*.py'
    return loader.discover(start_dir=start_dir, pattern=pattern, top_level_dir=os.path.dirname(os.path.dirname(start_dir)))
