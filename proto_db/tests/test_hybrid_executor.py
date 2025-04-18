import unittest
import asyncio
import concurrent.futures
import time
import threading
from unittest.mock import MagicMock, patch

from ..hybrid_executor import HybridExecutor, sync_task, async_task


class TestHybridExecutor(unittest.TestCase):
    """
    Unit tests for the HybridExecutor class.

    These tests verify the functionality of the HybridExecutor, which manages both
    synchronous and asynchronous tasks using dedicated thread pools.
    """

    def setUp(self):
        """
        Set up common resources for the tests.
        """
        # Create a HybridExecutor with a small number of workers for testing
        self.executor = HybridExecutor(base_num_workers=2, sync_multiplier=1)

    def tearDown(self):
        """
        Clean up resources after each test.
        """
        if hasattr(self, 'executor') and self.executor:
            self.executor.shutdown()

    def test_initialization_default_params(self):
        """
        Test that HybridExecutor initializes correctly with default parameters.
        """
        executor = HybridExecutor()
        self.assertIsNotNone(executor._sync_executor)
        self.assertEqual(len(executor._async_loops), executor._num_async_workers)
        self.assertEqual(len(executor._async_threads), executor._num_async_workers)
        self.assertEqual(executor._sync_multiplier, 2)  # Default value
        executor.shutdown()

    def test_initialization_custom_params(self):
        """
        Test that HybridExecutor initializes correctly with custom parameters.
        """
        executor = HybridExecutor(base_num_workers=3, sync_multiplier=1.5)
        self.assertIsNotNone(executor._sync_executor)
        self.assertEqual(len(executor._async_loops), 3)
        self.assertEqual(len(executor._async_threads), 3)
        self.assertEqual(executor._sync_multiplier, 1.5)
        self.assertEqual(executor._num_sync_workers, 4)  # 3 * 1.5 = 4.5, rounded to 4 (banker's rounding)
        executor.shutdown()

    def test_initialization_invalid_params(self):
        """
        Test that HybridExecutor raises appropriate exceptions for invalid parameters.
        """
        # Test with negative base_num_workers
        with self.assertRaises(ValueError):
            HybridExecutor(base_num_workers=-1)

        # Test with sync_multiplier < 1
        with self.assertRaises(ValueError):
            HybridExecutor(sync_multiplier=0.5)

    def test_submit_sync_task(self):
        """
        Test that submitting a synchronous task works correctly.
        """
        # Define a simple synchronous task
        def simple_sync_task():
            return "sync result"

        # Submit the task and get the result
        future = self.executor.submit(simple_sync_task)
        result = future.result(timeout=5)

        self.assertEqual(result, "sync result")

    def test_submit_async_task(self):
        """
        Test that submitting an asynchronous task works correctly.
        """
        # Define a simple asynchronous task
        async def simple_async_task():
            await asyncio.sleep(0.1)
            return "async result"

        # Submit the task and get the result
        future = self.executor.submit(simple_async_task)
        result = future.result(timeout=5)

        self.assertEqual(result, "async result")

    def test_submit_coroutine_object(self):
        """
        Test that submitting a coroutine object works correctly.
        """
        # Create a coroutine object
        async def coro_func():
            await asyncio.sleep(0.1)
            return "coroutine result"

        coro = coro_func()

        # Submit the coroutine and get the result
        future = self.executor.submit(coro)
        result = future.result(timeout=5)

        self.assertEqual(result, "coroutine result")

    def test_concurrent_tasks(self):
        """
        Test that multiple tasks can be executed concurrently.
        """
        # Define tasks that will run concurrently
        def sync_task_with_delay(delay):
            time.sleep(delay)
            return f"sync after {delay}s"

        async def async_task_with_delay(delay):
            await asyncio.sleep(delay)
            return f"async after {delay}s"

        # Submit multiple tasks
        start_time = time.time()
        futures = [
            self.executor.submit(sync_task_with_delay, 0.5),
            self.executor.submit(sync_task_with_delay, 0.5),
            self.executor.submit(async_task_with_delay, 0.5),
            self.executor.submit(async_task_with_delay, 0.5)
        ]

        # Wait for all tasks to complete
        results = [future.result(timeout=5) for future in futures]
        end_time = time.time()

        # Verify that all tasks completed successfully
        self.assertEqual(len(results), 4)
        self.assertIn("sync after 0.5s", results)
        self.assertIn("async after 0.5s", results)

        # Verify that tasks ran concurrently (total time should be less than sum of individual times)
        # Allow some overhead for task scheduling and execution
        self.assertLess(end_time - start_time, 1.5)  # Should be much less than 2s (4 tasks * 0.5s)

    def test_shutdown(self):
        """
        Test that shutdown properly cleans up resources.
        """
        executor = HybridExecutor(base_num_workers=1)

        # Submit a task to ensure the executor is working
        future = executor.submit(lambda: "test")
        result = future.result(timeout=5)
        self.assertEqual(result, "test")

        # Shutdown the executor
        executor.shutdown()

        # Verify that resources are cleaned up
        for loop in executor._async_loops:
            self.assertTrue(loop.is_closed())

        # Verify that we can't submit new tasks after shutdown
        with self.assertRaises(RuntimeError):
            executor.submit(lambda: "should fail")

    def test_context_manager(self):
        """
        Test that the executor works correctly as a context manager.
        """
        results = []

        with HybridExecutor(base_num_workers=1) as executor:
            # Submit tasks within the context
            future1 = executor.submit(lambda: "task1")
            future2 = executor.submit(lambda: "task2")

            # Get results
            results.append(future1.result(timeout=5))
            results.append(future2.result(timeout=5))

        # Verify that tasks completed successfully
        self.assertEqual(results, ["task1", "task2"])

        # Verify that the executor is shut down after exiting the context
        with self.assertRaises(RuntimeError):
            executor.submit(lambda: "should fail")

    def test_error_handling_sync_task(self):
        """
        Test that errors in synchronous tasks are properly propagated.
        """
        # Define a task that raises an exception
        def failing_sync_task():
            raise ValueError("Sync task error")

        # Submit the task
        future = self.executor.submit(failing_sync_task)

        # Verify that the exception is propagated
        with self.assertRaises(ValueError) as context:
            future.result(timeout=5)

        self.assertEqual(str(context.exception), "Sync task error")

    def test_error_handling_async_task(self):
        """
        Test that errors in asynchronous tasks are properly propagated.
        """
        # Define a task that raises an exception
        async def failing_async_task():
            await asyncio.sleep(0.1)
            raise ValueError("Async task error")

        # Submit the task
        future = self.executor.submit(failing_async_task)

        # Verify that the exception is propagated
        with self.assertRaises(ValueError) as context:
            future.result(timeout=5)

        self.assertEqual(str(context.exception), "Async task error")

    def test_round_robin_distribution(self):
        """
        Test that async tasks are distributed in a round-robin fashion.
        """
        # Create an executor with 2 async workers
        executor = HybridExecutor(base_num_workers=2)

        # Track which loop each task is assigned to
        loop_assignments = {}

        # Mock the run_coroutine_threadsafe function to track loop assignments
        original_run_coroutine_threadsafe = asyncio.run_coroutine_threadsafe

        def mock_run_coroutine_threadsafe(coro, loop):
            task_id = id(coro)
            loop_id = id(loop)
            loop_assignments[task_id] = loop_id
            return original_run_coroutine_threadsafe(coro, loop)

        # Apply the mock
        with patch('asyncio.run_coroutine_threadsafe', side_effect=mock_run_coroutine_threadsafe):
            # Submit multiple async tasks
            async def simple_task(task_id):
                await asyncio.sleep(0.1)
                return f"Task {task_id}"

            futures = [executor.submit(simple_task, i) for i in range(4)]

            # Wait for all tasks to complete
            results = [future.result(timeout=5) for future in futures]

        # Verify that all tasks completed successfully
        self.assertEqual(len(results), 4)

        # Verify that tasks were distributed across both loops
        unique_loops = set(loop_assignments.values())
        self.assertEqual(len(unique_loops), 2)

        # Cleanup
        executor.shutdown()

    def test_example_tasks(self):
        """
        Test the example tasks provided in the hybrid_executor.py file.
        """
        # Submit the example tasks
        future1 = self.executor.submit(sync_task, "test_sync", 0.1)
        future2 = self.executor.submit(async_task, "test_async", 0.1)

        # Get the results
        result1 = future1.result(timeout=5)
        result2 = future2.result(timeout=5)

        # Verify the results
        self.assertEqual(result1, "Sync task test_sync: Completed after 0.1s")
        self.assertEqual(result2, "Async task test_async: Completed after 0.1s")


if __name__ == "__main__":
    unittest.main()
