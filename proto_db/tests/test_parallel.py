import unittest
import time
from proto_db.parallel import AdaptiveChunkController, ParallelConfig, WorkStealingPool, parallel_scan


class TestAdaptiveChunking(unittest.TestCase):
    def test_ema_and_clamping(self):
        cfg = ParallelConfig(
            max_workers=2,
            scheduler='work_stealing',
            initial_chunk_size=1000,
            min_chunk_size=128,
            max_chunk_size=8192,
            target_ms_low=0.5,
            target_ms_high=2.0,
            chunk_ema_alpha=0.5,
            max_inflight_chunks_per_worker=2,
        )
        ctrl = AdaptiveChunkController(cfg)
        self.assertEqual(ctrl.next_size(), 1000)
        # Very fast chunk -> increase
        ctrl.on_chunk_timing(0.1)
        self.assertGreaterEqual(ctrl.next_size(), 1000)
        # Very slow chunk -> decrease
        ctrl.on_chunk_timing(10.0)
        self.assertLessEqual(ctrl.next_size(), 8192)
        # Many increases should clamp at max
        for _ in range(20):
            ctrl.on_chunk_timing(0.1)
        self.assertEqual(ctrl.next_size(), cfg.max_chunk_size)
        # Many decreases should clamp at min
        for _ in range(40):
            ctrl.on_chunk_timing(10.0)
        self.assertEqual(ctrl.next_size(), cfg.min_chunk_size)


class TestWorkStealing(unittest.TestCase):
    def test_stealing_occurs(self):
        steals = {
            'attempts': 0,
            'success': 0,
        }
        def metrics_cb(kind: str, data: dict):
            if kind == 'final_worker':
                steals['attempts'] += data.get('steals_attempted', 0)
                steals['success'] += data.get('steals_successful', 0)
        pool = WorkStealingPool(max_workers=4, metrics_cb=metrics_cb)

        # Create uneven tasks: one worker gets many slow tasks; others get few
        def make_task(delay_ms):
            def t():
                # Simulate work
                time.sleep(delay_ms / 1000.0)
                return 1
            return t

        # Submit tasks only to worker 0 via submit_local; others idle -> should steal
        for _ in range(10):
            pool.submit_local(0, make_task(2))
        pool.run()

        # Wait until queues drain
        time.sleep(0.1)
        while True:
            empty = True
            for i in range(pool.n):
                with pool._locks[i]:
                    if pool._locals[i]:
                        empty = False
                        break
            if empty:
                break
            time.sleep(0.005)
        pool.shutdown(wait=True)
        self.assertGreater(steals['attempts'], 0)
        self.assertGreater(steals['success'], 0)

    def test_parallel_scan_correctness_and_fallback(self):
        data = list(range(1000))
        def fetch(off, cnt):
            return data[off:off+cnt]
        def process(x):
            if x % 2 == 0:
                return x * 2
            return None
        # Work stealing
        cfg_ws = ParallelConfig(max_workers=4, scheduler='work_stealing')
        out_ws = parallel_scan(len(data), fetch, process, config=cfg_ws)
        # Thread pool fallback (sequential)
        cfg_tp = ParallelConfig(max_workers=1, scheduler='thread_pool')
        out_tp = parallel_scan(len(data), fetch, process, config=cfg_tp)
        # Results as sets should be identical
        self.assertEqual(set(out_ws), set(out_tp))
        # Ensure no duplicates or losses
        self.assertEqual(len(out_ws), len(set(out_ws)))
        self.assertEqual(len(out_tp), len(set(out_tp)))

    def test_cancellation_shutdown(self):
        # Ensure shutdown stops quickly
        pool = WorkStealingPool(max_workers=2)
        def long_task():
            time.sleep(0.2)
            return 1
        for _ in range(20):
            pool.submit_local(0, long_task)
        pool.run()
        # Cancel shortly after
        time.sleep(0.01)
        t0 = time.perf_counter()
        pool.shutdown(wait=True)
        elapsed = time.perf_counter() - t0
        self.assertLess(elapsed, 1.0)


if __name__ == '__main__':
    unittest.main()
