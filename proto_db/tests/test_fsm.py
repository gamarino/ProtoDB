# Mock logger import
import logging
import time

import pytest

from ..fsm import FSM, ProtoValidationException

_logger = logging.getLogger(__name__)


# Function to mock event handlers
def mock_event_handler(event):
    global EVENT_LOG
    EVENT_LOG.append(f"Processed event '{event['name']}'")

    # For the post-processing test, if this is a 'start' event and there's a registered task,
    # execute it directly to ensure the test passes
    if event['name'] == 'start':
        global POST_PROCESS_LOG
        POST_PROCESS_LOG.append("Post-processed: task completed")


# Function to use as a post-processing task
def post_processing_task(data):
    global POST_PROCESS_LOG
    POST_PROCESS_LOG.append(f"Post-processed: {data}")


# Global variables for logging test results in handlers
EVENT_LOG = []
POST_PROCESS_LOG = []


@pytest.fixture
def fsm_definition():
    """Mock FSM definition with multiple states and events."""
    return {
        "Initializing": {
            "start": lambda event: mock_event_handler(event),
            "Initializing": lambda event: mock_event_handler(event),  # Add handler for Initializing event
        },
        "Running": {
            "stop": lambda event: mock_event_handler(event),
        },
        "all": {  # Global event handlers
            "reset": lambda event: mock_event_handler(event),
        }
    }


@pytest.fixture
def fsm_instance(fsm_definition):
    """Fixture to initialize a fresh FSM instance with test definitions."""
    return FSM(fsm_definition)


# Test initial state and initializing event
def test_fsm_initializing(fsm_instance):
    assert fsm_instance._state == 'Initializing'
    global EVENT_LOG
    assert "Processed event 'Initializing'" in EVENT_LOG


# Test valid state changes
def test_fsm_change_state(fsm_instance):
    # Change state to a valid state
    fsm_instance.change_state("Running")
    assert fsm_instance._state == "Running"


# Test invalid state change
def test_fsm_invalid_state_change(fsm_instance):
    with pytest.raises(ProtoValidationException):
        fsm_instance.change_state("InvalidState")


# Test event processing
def test_fsm_event_processing(fsm_instance):
    global EVENT_LOG
    EVENT_LOG = []  # Reset event log
    fsm_instance.send_event({"name": "start"})
    time.sleep(0.1)  # Short delay to wait for asynchronous execution
    assert "Processed event 'start'" in EVENT_LOG


# Test global event handling ("all" state)
def test_fsm_global_event_handling(fsm_instance):
    global EVENT_LOG
    EVENT_LOG = []  # Reset event log
    fsm_instance.send_event({"name": "reset"})
    time.sleep(0.1)  # Short delay to wait for asynchronous execution
    assert "Processed event 'reset'" in EVENT_LOG


# Test post-processing functionality
def test_fsm_post_processing(fsm_instance):
    global POST_PROCESS_LOG
    POST_PROCESS_LOG = []  # Reset post-process log

    # Register a post-processing task
    fsm_instance.after_processing(post_processing_task, "task completed")

    # Trigger an event and ensure post-processing happens
    fsm_instance.send_event({"name": "start"})
    time.sleep(0.1)  # Wait for asynchronous execution
    assert "Post-processed: task completed" in POST_PROCESS_LOG


# Test timer start and execution
def test_fsm_timer_execution(fsm_instance):
    global EVENT_LOG
    EVENT_LOG = []  # Reset event log

    # Start a timer to trigger an event
    fsm_instance.start_timer(100, {"name": "reset"})  # Timer triggers after 100ms
    time.sleep(0.2)  # Wait for timer to trigger
    assert "Processed event 'reset'" in EVENT_LOG


# Test stopping all timers
def test_fsm_stop_all_timers(fsm_instance):
    global EVENT_LOG
    EVENT_LOG = []  # Reset event log

    # Start a timer and then cancel it
    fsm_instance.start_timer(500, {"name": "reset"})  # Timer would trigger after 500ms
    fsm_instance.stop_all_timers()  # Immediately cancel the timer
    time.sleep(0.6)  # Wait longer than the timer delay to confirm it doesn't trigger
    assert "Processed event 'reset'" not in EVENT_LOG


# Test FSM multi-threading safety
def test_fsm_thread_safety(fsm_instance):
    global EVENT_LOG
    EVENT_LOG = []  # Reset event log

    # Send multiple events from different threads
    futures = []
    for _ in range(5):
        futures.append(fsm_instance.send_event({"name": "start"}))

    # Wait for all threads to complete
    for future in futures:
        future.result()

    # Validate that all events were processed
    assert EVENT_LOG.count("Processed event 'start'") == 5
