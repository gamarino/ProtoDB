import unittest

try:
    import pyarrow as pa  # type: ignore
    have_arrow = True
except Exception:  # pragma: no cover
    have_arrow = False

from proto_db.arrow_bridge import to_arrow, table_to_parquet, ArrowNotAvailable


class TestArrowParquetBridge(unittest.TestCase):
    def test_to_arrow_no_dep(self):
        if not have_arrow:
            with self.assertRaises(ArrowNotAvailable):
                _ = to_arrow([{"a": 1}], columns=["a"])  # type: ignore
        else:
            tbl = to_arrow([{"a": 1}, {"a": 2}], columns=["a"])  # type: ignore
            self.assertEqual(tbl.num_rows, 2)
            self.assertIn("a", tbl.column_names)


if __name__ == '__main__':
    unittest.main()
