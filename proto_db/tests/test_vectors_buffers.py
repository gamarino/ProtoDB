import unittest

from proto_db.vectors import Vector

try:
    import numpy as np
except Exception:  # pragma: no cover
    np = None


class TestVectorBuffers(unittest.TestCase):
    def test_from_buffer_memoryview(self):
        import array
        arr = array.array('f', [1.0, 2.0, 3.5])
        v = Vector.from_buffer(memoryview(arr), dtype='float32', copy='auto')
        self.assertEqual(v.dim, 3)
        self.assertAlmostEqual(v.data[2], 3.5, places=5)
        mv = v.as_buffer()
        self.assertEqual(mv.nbytes, 3 * 4)

    @unittest.skipIf(np is None, "numpy not available")
    def test_as_numpy_zero_copy(self):
        a = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        v = Vector.from_buffer(a, copy='auto')
        arr = v.as_numpy(copy=False)
        self.assertEqual(arr.dtype, np.float32)
        self.assertEqual(arr.shape[0], 3)
        # Ensure values match
        self.assertTrue(np.allclose(arr, a.astype(np.float32)))


if __name__ == '__main__':
    unittest.main()
