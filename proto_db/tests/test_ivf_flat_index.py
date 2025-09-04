import unittest

from proto_db.vectors import Vector
from proto_db.vector_index import ExactVectorIndex, IVFFlatIndex


class TestIVFFlatIndex(unittest.TestCase):
    def _make_data(self):
        vecs = [
            Vector.from_list([1.0, 0.0], normalize=True),
            Vector.from_list([0.0, 1.0], normalize=True),
            Vector.from_list([0.7, 0.7], normalize=True),
            Vector.from_list([0.9, 0.1], normalize=True),
            Vector.from_list([0.1, 0.9], normalize=True),
        ]
        ids = [f"id{i}" for i in range(len(vecs))]
        return vecs, ids

    def test_build_and_search_knn_matches_exact_with_full_probe(self):
        vecs, ids = self._make_data()
        exact = ExactVectorIndex(metric='cosine')
        exact.build(vecs, ids, metric='cosine')
        ivf = IVFFlatIndex(metric='cosine', nlist=2, nprobe=2, page_size=2)
        ivf.build(vecs, ids, metric='cosine', params={'nlist': 2, 'nprobe': 2, 'page_size': 2})

        q = Vector.from_list([1.0, 0.0], normalize=True)
        exact_res = exact.search(q, k=3)
        ivf_res = ivf.search(q, k=3, params={'nprobe': 2})
        self.assertEqual([i for i, _ in exact_res[:1]], [i for i, _ in ivf_res[:1]])

    def test_add_remove_and_stats(self):
        vecs, ids = self._make_data()
        ivf = IVFFlatIndex(metric='cosine', nlist=2, nprobe=2, page_size=2)
        ivf.build(vecs, ids, metric='cosine')
        # Add new vector near [1,0]
        new_id = 'newA'
        ivf.add(new_id, Vector.from_list([0.99, 0.01], normalize=True))
        res = ivf.search(Vector.from_list([1.0, 0.0], normalize=True), k=2)
        self.assertIn(new_id, [i for i, _ in res])
        # Remove it and ensure not present
        ivf.remove(new_id)
        res2 = [i for i, _ in ivf.search(Vector.from_list([1.0, 0.0], normalize=True), k=3)]
        self.assertNotIn(new_id, res2)
        st = ivf.stats()
        self.assertEqual(st['backend'], 'ivf-flat')
        self.assertGreaterEqual(st['nlist'], 1)
        self.assertGreaterEqual(st['page_size'], 1)

    def test_save_and_load(self):
        import os, tempfile, json
        vecs, ids = self._make_data()
        ivf = IVFFlatIndex(metric='cosine', nlist=2, nprobe=2, page_size=2)
        ivf.build(vecs, ids, metric='cosine')
        with tempfile.TemporaryDirectory() as d:
            prefix = os.path.join(d, 'idx')
            ivf.save(prefix)
            # Load into a new index
            ivf2 = IVFFlatIndex(metric='cosine')
            ivf2.load(prefix)
            q = Vector.from_list([1.0, 0.0], normalize=True)
            a = ivf.search(q, k=3)
            b = ivf2.search(q, k=3)
            self.assertEqual([i for i, _ in a], [i for i, _ in b])


if __name__ == '__main__':
    unittest.main()
