import unittest

# Gracefully skip if Hypothesis is not available in the environment
try:
    from hypothesis import given, settings, strategies as st
    HYP_AVAILABLE = True
except Exception:  # pragma: no cover
    HYP_AVAILABLE = False

from ..db_access import ObjectSpace
from ..memory_storage import MemoryStorage
from ..dictionaries import Dictionary
from ..sets import Set, CountedSet
from ..lists import List


if not HYP_AVAILABLE:
    # Define no-op decorators and minimal st to avoid NameError at import time
    def given(*args, **kwargs):
        def _wrap(fn):
            return fn
        return _wrap
    def settings(*args, **kwargs):
        def _wrap(fn):
            return fn
        return _wrap
    class _DummySt:
        def lists(self, *a, **k): return []
        def tuples(self, *a, **k): return []
        def sampled_from(self, *a, **k): return []
        def integers(self, *a, **k): return []
        def text(self, *a, **k): return []
    st = _DummySt()

@unittest.skipUnless(HYP_AVAILABLE, "hypothesis not installed")
class TestCollectionsProperties(unittest.TestCase):
    def setUp(self):
        # Use fast in-memory storage for property tests
        self.space = ObjectSpace(MemoryStorage())
        self.db = self.space.new_database("MemDB")

    def tearDown(self):
        self.space.close()

    @given(st.lists(st.tuples(st.sampled_from(["add", "remove"]), st.integers() | st.text()), min_size=1, max_size=50))
    @settings(deadline=None)
    def test_set_behaves_like_python_set(self, ops):
        tr = self.db.new_transaction()
        pset = Set(transaction=tr)
        py = set()
        for op, val in ops:
            if op == "add":
                pset = pset.add(val)
                py.add(val)
            else:
                pset = pset.remove_at(val)
                py.discard(val)
            # After each operation compare
            self.assertEqual(pset.count, len(py))
            self.assertEqual(set(pset.as_iterable()), py)
        tr.abort()  # Nothing persisted

    @given(st.lists(st.tuples(st.sampled_from(["add", "remove"]), st.integers(min_value=0, max_value=10)), min_size=1, max_size=50))
    @settings(deadline=None)
    def test_counted_set_behaves_like_multiset(self, ops):
        from collections import Counter
        tr = self.db.new_transaction()
        cs = CountedSet(transaction=tr)
        model = Counter()
        for op, val in ops:
            if op == "add":
                cs = cs.add(val)
                model[val] += 1
            else:
                cs = cs.remove_at(val)
                if model[val] > 0:
                    model[val] -= 1
                    if model[val] == 0:
                        del model[val]
            # Validate content and counts
            self.assertEqual(set(cs.as_iterable()), set(model.keys()))
            self.assertEqual(cs.count, len(model.keys()))
            self.assertEqual(cs.total_count, sum(model.values()))
            for k in model:
                self.assertEqual(cs.get_count(k), model[k])
        tr.abort()

    @given(st.lists(st.tuples(st.sampled_from(["set", "remove"]), st.text(min_size=1, max_size=8), st.integers() | st.text()), min_size=1, max_size=50))
    @settings(deadline=None)
    def test_dictionary_behaves_like_python_dict(self, ops):
        tr = self.db.new_transaction()
        d = Dictionary(transaction=tr)
        model = {}
        for op, k, v in ops:
            if op == "set":
                d = d.set_at(k, v)
                model[k] = v
            else:
                d = d.remove_at(k)
                model.pop(k, None)
            self.assertEqual(d.count, len(model))
            self.assertEqual(set(k for k, _ in d.as_iterable()), set(model.keys()))
            # Validate values
            for k in model.keys():
                self.assertEqual(d.get_at(k), model[k])
        tr.abort()

    @given(st.lists(st.integers(min_value=0, max_value=100), min_size=1, max_size=80))
    @settings(deadline=None)
    def test_list_insert_remove_balance_and_equality(self, values):
        tr = self.db.new_transaction()
        lst = List(transaction=tr)
        py = []
        # We'll derive operations from values: even -> insert, odd -> remove (when possible)
        for n in values:
            if (n % 2 == 0) or len(py) == 0:
                # insert at computed index
                idx = n % (len(py) + 1)
                lst = lst.insert_at(idx, n)
                py.insert(idx, n)
            else:
                idx = n % len(py)
                lst = lst.remove_at(idx)
                py.pop(idx)
            # Validate after each step
            self.assertEqual(lst.count, len(py))
            self.assertEqual(list(lst.as_iterable()), py)
            bal = lst._balance()
            self.assertGreaterEqual(bal, -1)
            self.assertLessEqual(bal, 1)
        tr.abort()
