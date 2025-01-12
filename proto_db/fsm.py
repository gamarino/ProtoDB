from __future__ import annotations
import concurrent.futures
import os
import time

from threading import Lock

from .common import MB, GB, Future
from .exceptions import ProtoValidationException
import logging


_logger = logging.getLogger(__name__)


# Executor threads for FSM machines
# Determines the number of worker threads for asynchronous execution
max_workers = (os.cpu_count() or 1) * 5
executor_pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)


class Timer:
    """Class to manage delayed execution of events using a threading pool."""
    timer_execution: Future
    canceled: bool

    def __init__(self, timer_execution: Future):
        # Initialize the timer and set its canceled flag to False
        self.timer_execution = timer_execution
        self.canceled = False

    def stop(self):
        """Cancels the timer and prevents the associated event from being executed."""
        self.canceled = True
        self.timer_execution.cancel()

    def wait_til_processed(self):
        """Waits until the timer has finished executing or has been canceled."""
        if not self.canceled:
            self.timer_execution.result()


class FSM:
    """Class representing a finite state machine (FSM).
    Handles state transitions, event processing, timers, and post-processing tasks."""

    _state: str
    _lock: Lock
    _fsm_definition: dict[str: dict[str, callable]]
    _timers: set
    _after_event_process: set

    def __init__(self, fsm_definition: dict[str, dict[str, callable]]):
        """Initializes the FSM instance, sets the initial state, and executes the `Initializing` event."""
        self._state = 'Initializing'  # The initial state of the FSM
        self._fsm_definition = fsm_definition  # Definition of states and events
        self._lock = Lock()  # A lock to ensure thread-safe state transitions
        self.send_event({'name': 'Initializing'})  # Sends the initializing event automatically
        self._timers = set()  # A set to manage active timers

    def send_event(self, event: dict, target_fsm: FSM = None) -> Future:
        """Sends an event to the FSM, optionally targeting a different FSM."""
        if not target_fsm:
            target_fsm = self

        def task_send_event():
            """Task to process the event in the target FSM."""
            target_fsm.process_event(event)

        # Submits the task to the executor pool for asynchronous processing
        return executor_pool.submit(task_send_event)

    def change_state(self, new_state: str):
        """Changes the FSM state to the new state if it's valid."""
        with self._lock:
            # Ensure the requested state exists in the FSM definition
            if new_state not in self._fsm_definition:
                raise ProtoValidationException(
                    message=f'Trying to change state to an invalid one! ({new_state})'
                )
            self._state = new_state  # Update the FSM state

    def process_event(self, event: dict):
        """Processes an event based on the current state and executes post-processing tasks."""
        to_post_process: set = set()  # Temporary variable to execute tasks outside the lock

        # Enter critical section to ensure thread-safe state transition
        with self._lock:
            self._after_event_process = set()  # Reset the set of tasks to execute after processing the event
            event_name = event.get('name')  # Get the name of the event
            saved_state = self._state  # Save the current state in case rollback is necessary
            try:
                # Determine the appropriate state for this event
                state = self._state if event_name in self._fsm_definition.get(self._state, {}) else 'all'

                # If the event is defined in the relevant state, process it
                if state and event_name in self._fsm_definition[state]:
                    _logger.debug(f"Processing event '{event_name}' in state '{self._state}'")
                    # Execute the handler for the event
                    self._fsm_definition[state][event_name](event)

                # Event was successfully processed; preserve the new state
                saved_state = self._state

            except Exception as e:
                # Log unexpected exceptions and rollback to previously saved state
                _logger.exception(e)
                _logger.error(f'Unexpected exception processing event {event_name} in state {saved_state}, fsm {self}')
                self._state = saved_state
            finally:
                # Move the post-process tasks for execution outside the critical section
                to_post_process = self._after_event_process
                self._after_event_process = set()  # Clear the post-process task set

        # Perform the post-processing tasks outside the lock to avoid blocking
        for function, args, kwargs in to_post_process:
            try:
                function(*args, **kwargs)
            except Exception as e:
                # Log exceptions in post-processing tasks, without interrupting other tasks
                _logger.exception(f"Error executing post-processing task in FSM: {e}")

    def after_processing(self, function, *args, **kwargs):
        """Registers a task to be executed after processing an event."""
        self._after_event_process.add((function, args, kwargs))

    def timer_processed(self, timer: Timer):
        """Removes the timer from active timers once it has finished processing."""
        with self._lock:
            if timer in self._timers:
                self._timers.remove(timer)

    def start_timer(self, delay_in_ms: int, event: dict):
        """Starts a timer that triggers an event after a specified delay."""
        timer: Timer

        def task_send_event():
            """Sleeps for the specified delay, then sends the event if the timer wasn't canceled."""
            time.sleep(delay_in_ms / 1000.0)  # Delay in seconds
            if not timer.canceled:
                self.process_event(event)  # Trigger the event
            self.timer_processed(timer)  # Mark the timer as processed

        # Submit the timer task to the executor pool
        execution_future = executor_pool.submit(task_send_event)
        timer = Timer(execution_future)  # Create a timer object
        self._timers.add(timer)  # Add the timer to the active timers set
        return timer

    def stop_all_timers(self):
        """Cancels all active timers."""
        with self._lock:
            for timer in self._timers:
                timer.stop()
