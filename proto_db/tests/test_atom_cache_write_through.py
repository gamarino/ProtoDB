import unittest
import uuid

from proto_db.db_access import ObjectSpace
from unittest.mock import Mock, MagicMock
from proto_db.standalone_file_storage import StandaloneFileStorage
from proto_db.db_access import Database
from proto_db.common import AtomPointer


class TestWriteThroughCache(unittest.TestCase):
    def test_write_through_on_save_then_read_hit(self):
        # Setup storage with caches enabled (StandaloneFileStorage integrates caches on read)
        mock_bp = Mock()
        mock_bp.get_new_wal = MagicMock(return_value=(uuid.uuid4(), 0))
        mock_bp.write_streamer = MagicMock()
        mock_bp.get_reader = MagicMock()
        mock_bp.close_wal = MagicMock()
        storage = StandaloneFileStorage(block_provider=mock_bp)
        space = ObjectSpace(storage=storage)
        db = space.new_database('CacheDB')

        # Transaction 1: create and persist a literal as a root to force save
        tr1 = db.new_transaction()
        lit = tr1.get_literal("hello-cache")
        # Persist by setting as a named root
        tr1.set_root_object('greeting', lit)
        tr1.commit()

        # After commit, literal should have a stable pointer and be present in caches via write-through
        ap: AtomPointer = lit.atom_pointer
        self.assertIsNotNone(ap)

        # Record cache stats before the read
        bundle = storage._atom_caches
        obj_stats_before = bundle.obj_cache.stats() if bundle.obj_cache else None
        hits_before = obj_stats_before["hits"] if obj_stats_before else 0

        # Transaction 2: read the same literal by pointer
        tr2 = db.new_transaction()
        same_lit = tr2.read_object('Literal', ap)
        # Touch it to force a _load from storage/caches
        same_lit._load()
        self.assertEqual(str(same_lit), "hello-cache")

        # Validate that the object cache recorded a hit
        obj_stats_after = bundle.obj_cache.stats() if bundle.obj_cache else None
        hits_after = obj_stats_after["hits"] if obj_stats_after else 0

        self.assertGreaterEqual(hits_after, hits_before + 1, "Expected a cache hit on read-after-write")


if __name__ == '__main__':
    unittest.main()
