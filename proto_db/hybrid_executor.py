import asyncio
import concurrent.futures
import threading
import time
import inspect
import logging
import os
from itertools import cycle
import math # Used for rounding

# Basic logging setup (configure as needed in your application)
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')

class HybridExecutor:
    """
    A concurrent executor that manages both synchronous and asynchronous tasks
    using dedicated, configurable pools of OS threads.

    Features:
    - Unified `submit` method for both regular (`def`) and async (`async def`) functions.
    - Separate thread pools:
        - Sync Pool: Uses `concurrent.futures.ThreadPoolExecutor` for blocking or
          CPU-bound synchronous tasks. Well-suited for `no-GIL` CPU parallelism.
        - Async Pool: Uses N dedicated OS threads, each running an independent
          `asyncio` event loop for efficient I/O-bound asynchronous tasks.
    - Configurable pool sizes:
        - Async Pool Size (N): Based on `base_num_workers` or defaults to `os.cpu_count()`.
        - Sync Pool Size (K*N): Calculated using `sync_multiplier` (K, default 2)
          applied to the async pool size (N). Allows over-provisioning threads
          to compensate for blocking I/O in synchronous tasks.
    - Round-robin distribution of async tasks across available event loops.
    - Thread-safe submission mechanism.
    - Graceful shutdown of both pools.
    - Context manager support (`with` statement).

    Use Cases:
    - Applications mixing legacy/blocking sync code with modern async code.
    - Gradual migration from sync to async architectures.
    - Libraries needing internal async I/O execution without controlling the
      caller's event loop, while potentially exposing a sync API.
    """
    def __init__(self, base_num_workers: int | None = None, sync_multiplier: float | int = 2):
        """
        Initializes the HybridExecutor.

        Args:
            base_num_workers (int, optional):
                The base number of workers. This directly determines the number
                of threads and event loops in the asynchronous pool (N).
                If None (default), N is set to `os.cpu_count()` if detectable,
                otherwise falls back to a sensible default (e.g., 4).
            sync_multiplier (float | int, optional):
                The multiplier (K) used to determine the number of synchronous workers,
                relative to the number of asynchronous workers (N).
                Formula: num_sync_workers = max(1, int(round(sync_multiplier * num_async_workers))).
                Must be >= 1. Defaults to 2, providing twice as many sync threads
                as async threads by default to compensate for potential sync I/O blocking.
        """
        # --- Validate sync_multiplier (K) ---
        if not isinstance(sync_multiplier, (int, float)) or sync_multiplier < 1:
             # We allow K >= 1, so setting K=1 means sync pool size == async pool size.
             raise ValueError(f"sync_multiplier (K) must be a number >= 1, got: {sync_multiplier}")
        self._sync_multiplier = sync_multiplier # Store K for logging

        # --- Determine Async Pool Size (N) ---
        num_cores = os.cpu_count()
        if base_num_workers is None:
            if num_cores:
                # Default N to number of CPU cores if detectable
                self._num_async_workers = num_cores
                logging.info(f"HybridExecutor: base_num_workers not specified. Using N={self._num_async_workers} (CPU cores) async workers.")
            else:
                # Fallback if os.cpu_count() fails
                self._num_async_workers = 4 # A sensible fallback value
                logging.warning(f"HybridExecutor: Could not determine CPU cores. Using fallback N={self._num_async_workers} async workers.")
        else:
            # Use user-specified N
            if base_num_workers <= 0:
                 raise ValueError("base_num_workers must be greater than 0")
            self._num_async_workers = base_num_workers
            logging.info(f"HybridExecutor: Using specified N={self._num_async_workers} async workers.")

        # --- Determine Sync Pool Size (K*N) ---
        # Calculate K*N, ensuring at least 1 sync worker for functionality.
        # Rounding handles non-integer K, int() converts, max(1,...) ensures minimum.
        self._num_sync_workers = max(1, int(round(self._sync_multiplier * self._num_async_workers)))
        logging.info(f"HybridExecutor: Using K={self._sync_multiplier}. Calculated {self._num_sync_workers} sync workers (max(1, int(round(K*N)))).")

        # === Initialize Sync Pool ===
        # Standard thread pool for synchronous tasks.
        # This pool directly benefits from no-GIL for CPU-bound Python tasks.
        self._sync_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self._num_sync_workers, # Set size to K*N
            thread_name_prefix='SyncWorker'
        )

        # === Initialize Async Pool ===
        # We need lists to hold the loops, threads, and startup signals for the async workers.
        self._async_loops: list[asyncio.AbstractEventLoop] = []
        self._async_threads: list[threading.Thread] = []
        self._async_threads_started_events: list[threading.Event] = []
        self._async_loop_iterator = None # For round-robin distribution
        self._async_submit_lock = threading.Lock() # Protects the iterator during submit

        logging.info(f"HybridExecutor: Creating N={self._num_async_workers} async worker threads/loops...")
        # Create N async worker threads.
        for i in range(self._num_async_workers):
            # Each async worker thread needs its own independent event loop.
            loop = asyncio.new_event_loop()
            # Each thread needs an event to signal when its loop has started.
            started_event = threading.Event()
            # Create the thread, targeting the _run_async_loop method.
            # Pass the specific loop and event for this thread.
            # Daemon=True allows the program to exit even if these threads are running
            # (though graceful shutdown via shutdown() is preferred).
            thread = threading.Thread(
                target=self._run_async_loop,
                args=(loop, started_event),
                name=f'AsyncLoopThread-{i}',
                daemon=True
            )
            # Store references
            self._async_loops.append(loop)
            self._async_threads.append(thread)
            self._async_threads_started_events.append(started_event)
            # Start the thread; it will run _run_async_loop.
            thread.start()

        # Wait for all async threads to confirm their event loops are running.
        # This prevents submitting tasks before the loops are ready.
        logging.info(f"HybridExecutor: Waiting for all N={self._num_async_workers} AsyncLoopThreads to start...")
        for i, event in enumerate(self._async_threads_started_events):
            event.wait() # Block until the event is set by the corresponding thread.
            logging.debug(f"HybridExecutor: AsyncLoopThread-{i} started.")

        # Initialize the round-robin iterator over the list of async loops.
        # This must be done *after* the loops list is populated.
        if self._async_loops:
             self._async_loop_iterator = cycle(self._async_loops)
        else:
             # Edge case: N=0 (shouldn't happen with validation base_num_workers > 0)
             logging.warning("HybridExecutor: No async worker threads were created.")

        logging.info(f"HybridExecutor initialized. {self._num_sync_workers} sync workers and {self._num_async_workers} async workers ready.")

    def _run_async_loop(self, loop: asyncio.AbstractEventLoop, started_event: threading.Event):
        """
        Target function executed by each thread in the asynchronous pool.
        Manages a single, independent asyncio event loop for its lifetime.

        Args:
            loop: The specific asyncio event loop this thread should manage.
            started_event: The threading.Event this thread should set once the
                           loop is running.
        """
        thread_name = threading.current_thread().name
        logging.debug(f"Starting event loop in {thread_name}...")
        try:
            # Set the provided loop as the current event loop *for this thread*.
            # This is crucial for asyncio functions used within this thread.
            asyncio.set_event_loop(loop)
            # Signal that the loop is set up and about to run.
            started_event.set()
            # Run the event loop indefinitely, processing scheduled coroutines.
            # This call blocks until loop.stop() is called.
            loop.run_forever()
        except Exception as e:
             # Log unexpected errors during loop execution
             logging.error(f"Unexpected error in {thread_name} during run_forever: {e}", exc_info=True)
        finally:
            # --- Graceful Cleanup Sequence for this specific loop ---
            logging.info(f"Closing event loop in {thread_name}...")
            try:
                # 1. Cancel all remaining tasks in *this specific loop*.
                # Use loop=loop to ensure we only get tasks for this loop.
                tasks = asyncio.all_tasks(loop=loop)
                if tasks:
                    logging.debug(f"{thread_name}: Cancelling {len(tasks)} pending tasks...")
                    for task in tasks:
                        task.cancel()
                    # Wait for cancellations to be processed. Requires running the loop briefly.
                    loop.run_until_complete(
                        asyncio.gather(*tasks, return_exceptions=True)
                    )
                    logging.debug(f"{thread_name}: Pending tasks cancelled.")

                # 2. Shut down any pending asynchronous generators.
                logging.debug(f"{thread_name}: Shutting down async generators...")
                loop.run_until_complete(loop.shutdown_asyncgens())
                logging.debug(f"{thread_name}: Async generators shut down.")

            except Exception as e:
                 # Log errors during cleanup but don't prevent loop closing.
                 logging.error(f"Error during event loop cleanup in {thread_name}: {e}")
            finally:
                 # 3. Close the loop itself.
                 # Check if closed already to prevent errors.
                 if not loop.is_closed():
                      loop.close()
                      logging.info(f"Event loop in {thread_name} closed.")

    def submit(self, fn, *args, **kwargs) -> concurrent.futures.Future:
        """
        Submits a callable (synchronous or asynchronous) for execution.

        - If `fn` is a regular function (`def`), it's submitted to the synchronous
          thread pool (`ThreadPoolExecutor`).
        - If `fn` is an async function (`async def`) or a coroutine object,
          it's submitted to one of the asynchronous worker threads/loops using
          a round-robin strategy.

        Args:
            fn: The callable to execute.
            *args: Positional arguments for `fn`.
            **kwargs: Keyword arguments for `fn`.

        Returns:
            concurrent.futures.Future: A Future representing the pending execution.
                                       This Future object can be used to get the
                                       result or check for exceptions, regardless
                                       of which pool executed the task.
        """
        # Check if the submitted callable is an async function or coroutine object.
        if inspect.iscoroutinefunction(fn) or inspect.iscoroutine(fn):
            # --- Handle Asynchronous Task ---
            if not self._async_loop_iterator:
                 # Safety check: cannot submit async tasks if no async workers exist.
                 raise RuntimeError("HybridExecutor has no async workers configured to run the task.")

            # If it's already a coroutine object, use it directly.
            # Otherwise, call the async function to create the coroutine object.
            if inspect.iscoroutine(fn):
                 coro = fn
            else:
                 coro = fn(*args, **kwargs)

            # Select the next async event loop using thread-safe round-robin.
            with self._async_submit_lock:
                # cycle() ensures the iterator loops back to the beginning.
                target_loop = next(self._async_loop_iterator)

            # Optional: Log which loop is getting the task.
            try:
                 loop_index = self._async_loops.index(target_loop)
                 thread_name = f"AsyncLoopThread-{loop_index}"
            except ValueError:
                 thread_name = "target loop" # Should not happen if setup is correct
            logging.debug(f"Submitting async task {getattr(fn, '__name__', 'coroutine')} to {thread_name}")

            # Schedule the coroutine on the chosen loop from this (potentially different) thread.
            # `run_coroutine_threadsafe` is essential:
            # 1. It's thread-safe.
            # 2. It schedules the coro on the target loop running in another thread.
            # 3. It returns a `concurrent.futures.Future` which integrates seamlessly.
            future = asyncio.run_coroutine_threadsafe(coro, target_loop)
            return future
        else:
            # --- Handle Synchronous Task ---
            logging.debug(f"Submitting sync task {fn.__name__} to the sync ThreadPoolExecutor")
            # Submit the regular function to the standard ThreadPoolExecutor.
            future = self._sync_executor.submit(fn, *args, **kwargs)
            return future

    def shutdown(self, wait: bool = True):
        """
        Shuts down the executor gracefully.

        Stops all asynchronous event loops, waits for all worker threads (async and sync)
        to finish, and releases resources.

        Args:
            wait (bool): If True (default), wait for all pending tasks (submitted
                         before shutdown) in the synchronous pool to complete.
                         Async tasks currently running will be cancelled during loop cleanup.
                         If False, signal threads to stop but don't wait for task completion.
        """
        logging.info("Initiating HybridExecutor shutdown...")

        # --- Shutdown Async Pool ---
        if self._async_loops:
             # 1. Request all async loops to stop.
             # `call_soon_threadsafe` is needed to schedule `loop.stop()` from this
             # (potentially external) thread onto each loop's own thread.
             logging.info(f"Requesting stop for {len(self._async_loops)} async event loops...")
             for i, loop in enumerate(self._async_loops):
                 if loop.is_running():
                      logging.debug(f"Sending stop request to loop {i}...")
                      loop.call_soon_threadsafe(loop.stop)
                 else:
                      logging.debug(f"Loop {i} was not running.")

             # 2. Wait for all async worker threads to terminate.
             # They will exit after their `loop.run_forever()` returns (due to `loop.stop()`)
             # and they complete the cleanup in the `finally` block of `_run_async_loop`.
             logging.info(f"Waiting for {len(self._async_threads)} AsyncLoopThreads to finish...")
             for i, thread in enumerate(self._async_threads):
                 thread.join() # Wait for the thread to completely finish.
                 logging.debug(f"AsyncLoopThread-{i} finished.")
             logging.info("All AsyncLoopThreads finished.")
        else:
             logging.info("No async worker threads to shut down.")

        # --- Shutdown Sync Pool ---
        # Use the standard shutdown method of ThreadPoolExecutor.
        logging.info(f"Shutting down sync ThreadPoolExecutor ({self._num_sync_workers} workers, wait={wait})...")
        self._sync_executor.shutdown(wait=wait)
        logging.info("Sync ThreadPoolExecutor shut down.")

        logging.info("HybridExecutor shutdown complete.")

    # --- Context Manager Protocol ---
    def __enter__(self):
        """Enter the runtime context related to this object."""
        # Allows using the executor with a 'with' statement.
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the runtime context related to this object."""
        # Ensures shutdown() is called automatically when exiting the 'with' block,
        # even if exceptions occurred within the block.
        self.shutdown()

# --- Example Tasks (unchanged, for demonstration) ---
def sync_task(task_id, duration):
    """Example synchronous task simulating blocking work."""
    logging.info(f"Sync task {task_id}: Starting (will sleep for {duration}s).")
    time.sleep(duration)
    result = f"Sync task {task_id}: Completed after {duration}s"
    logging.info(result)
    return result

async def async_task(task_id, duration):
    """Example asynchronous task simulating non-blocking I/O."""
    logging.info(f"Async task {task_id}: Starting (will await asyncio.sleep for {duration}s).")
    await asyncio.sleep(duration) # Simulates waiting for I/O cooperatively
    result = f"Async task {task_id}: Completed after {duration}s"
    logging.info(result)
    return result

# --- Example Usage (demonstrates configuration and submission) ---
if __name__ == "__main__":
    # Setup logging to see executor messages and task outputs
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s [%(levelname)s] %(message)s')

    print("\n--- Example 1: Using defaults (base=CPU cores, k=2) ---")
    # This will typically create N async workers (where N=CPU cores)
    # and 2*N sync workers.
    try:
        with HybridExecutor() as executor1:
             print(f"Executor 1 Info: Async Workers={executor1._num_async_workers}, Sync Workers={executor1._num_sync_workers}, K={executor1._sync_multiplier}")
             futures1 = [
                 executor1.submit(sync_task, "S1.1", 1.5),
                 executor1.submit(async_task, "A1.1", 2.0),
                 executor1.submit(sync_task, "S1.2", 0.5),
                 executor1.submit(async_task, "A1.2", 1.0)
             ]
             print("Executor 1: Tasks submitted. Waiting for results...")
             for future in concurrent.futures.as_completed(futures1):
                 try:
                      print(f"Executor 1: Result received -> {future.result()}")
                 except Exception as e:
                      logging.error(f"Executor 1: Task raised an exception: {e}")
             print("Executor 1: All tasks likely completed.")
    except Exception as e:
         logging.error(f"Error during Executor 1 execution: {e}", exc_info=True)


    print("\n--- Example 2: Specifying base workers and sync multiplier (k=1) ---")
    # Explicitly set N=4 async workers and K=1 (so 1*4=4 sync workers)
    try:
        with HybridExecutor(base_num_workers=4, sync_multiplier=1) as executor2:
            print(f"Executor 2 Info: Async Workers={executor2._num_async_workers}, Sync Workers={executor2._num_sync_workers}, K={executor2._sync_multiplier}")
            futures2 = [
                 executor2.submit(sync_task, "S2.1", 1.0),
                 executor2.submit(async_task, "A2.1", 1.2),
                 executor2.submit(sync_task, "S2.2", 1.1), # Will run concurrently if cores allow
                 executor2.submit(async_task, "A2.2", 0.8) # Will run concurrently on async loops
            ]
            print("Executor 2: Tasks submitted. Waiting for results...")
            for future in concurrent.futures.as_completed(futures2):
                 try:
                      print(f"Executor 2: Result received -> {future.result()}")
                 except Exception as e:
                      logging.error(f"Executor 2: Task raised an exception: {e}")
            print("Executor 2: All tasks likely completed.")
    except Exception as e:
         logging.error(f"Error during Executor 2 execution: {e}", exc_info=True)

    print("\n--- Example program finished ---")