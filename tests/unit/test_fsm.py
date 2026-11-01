"""Unit tests for the 2.0 finite state machine, proto_db.runtime.fsm (Task 8.1, D41).

Catches the 0.1 defects (proto_db/fsm.py): package code importing the test
module under pytest to fake an ``Initializing`` log entry; handler exceptions
logged and swallowed; events of one FSM processed concurrently by a shared
pool (handlers racing each other, order lost); a handler calling
``change_state`` (or ``send_event``) blocking on the non-reentrant FSM lock;
post-processing tasks kept in a set (order lost, duplicates dropped, tasks
registered outside a handler leaking into an arbitrary later event); timers
sleeping on pool threads.  It checks one worker per FSM, ordered and
serialised handlers, re-entrant calls, error delivery to the event future,
the ``on_error`` hook or ``join()``/``stop()``, clean timer cancellation and
a clean stop with no leaked thread.
"""
import threading
import time

import pytest

from proto_db.errors import ProtoTimeoutError, ProtoUserError, ProtoValidationError
from proto_db.runtime.fsm import FSM, Timer

WAIT = 10  # seconds; generous bound so a deadlock fails the test instead of hanging it


def _settle(baseline: int, timeout: float = 5.0) -> int:
    deadline = time.monotonic() + timeout
    while threading.active_count() > baseline and time.monotonic() < deadline:
        time.sleep(0.005)
    return threading.active_count()


class Recorder:
    """Builds FSM definitions whose handlers record the events they receive."""

    def __init__(self):
        self.log = []
        self.lock = threading.Lock()

    def handler(self, label=None):
        def handle(event):
            with self.lock:
                self.log.append(label or event["name"])
        return handle


@pytest.fixture
def rec():
    return Recorder()


@pytest.fixture
def fsms():
    created = []
    yield created
    for fsm in created:
        fsm.stop(raise_errors=False)


def make(fsms, definition, **kwargs):
    fsm = FSM(definition, **kwargs)
    fsms.append(fsm)
    return fsm


def test_initializing_event_is_processed_at_construction(fsms, rec):
    fsm = make(fsms, {"Initializing": {"Initializing": rec.handler()}})
    fsm.join(timeout=WAIT)
    assert fsm.state == "Initializing"
    assert rec.log == ["Initializing"]


def test_change_state(fsms, rec):
    fsm = make(fsms, {"Initializing": {}, "Running": {}})
    fsm.change_state("Running")
    assert fsm.state == "Running"
    with pytest.raises(ProtoValidationError):
        fsm.change_state("Nowhere")
    with pytest.raises(ProtoValidationError):
        fsm.change_state("all")


@pytest.mark.parametrize("definition", [[], {"S": []}, {"S": {"e": 42}}, {1: {}}, {"S": {2: print}}])
def test_invalid_definitions_are_rejected(definition):
    with pytest.raises(ProtoValidationError):
        FSM(definition)


def test_event_dispatch_state_handler_wins_over_all(fsms, rec):
    fsm = make(fsms, {"Initializing": {"start": rec.handler("state-start")},
                      "all": {"start": rec.handler("all-start"), "reset": rec.handler("all-reset")}})
    assert fsm.send_event({"name": "start"}).result(timeout=WAIT) is None
    fsm.send_event({"name": "reset"}).result(timeout=WAIT)
    fsm.send_event({"name": "unknown"}).result(timeout=WAIT)  # no handler: processed, nothing happens
    assert rec.log == ["state-start", "all-reset"]


def test_handler_receives_the_event(fsms):
    seen = []
    fsm = make(fsms, {"Initializing": {"go": seen.append}})
    event = {"name": "go", "data": 7}
    fsm.send_event(event).result(timeout=WAIT)
    assert seen == [event]


@pytest.mark.parametrize("event", [None, "go", {}, {"name": ""}, {"name": 3}])
def test_invalid_events_are_rejected_at_send(fsms, event):
    fsm = make(fsms, {"Initializing": {}})
    with pytest.raises(ProtoValidationError):
        fsm.send_event(event)


def test_one_worker_per_fsm_processes_events_in_order(fsms):
    threads, names = [], []

    def handle(event):
        threads.append(threading.get_ident())
        names.append(event["n"])
    fsm = make(fsms, {"all": {"e": handle}})
    futures = [fsm.send_event({"name": "e", "n": i}) for i in range(200)]
    for f in futures:
        f.result(timeout=WAIT)
    assert names == list(range(200))
    assert len(set(threads)) == 1 and threads[0] != threading.get_ident()


def test_handlers_never_overlap_with_concurrent_senders(fsms):
    active = [0]
    overlaps = [0]
    count = [0]

    def handle(event):
        active[0] += 1
        if active[0] > 1:
            overlaps[0] += 1
        time.sleep(0.0001)
        count[0] += 1
        active[0] -= 1
    fsm = make(fsms, {"all": {"e": handle}})

    def sender():
        for _ in range(50):
            fsm.send_event({"name": "e"})
    senders = [threading.Thread(target=sender) for _ in range(8)]
    for t in senders:
        t.start()
    for t in senders:
        t.join()
    fsm.join(timeout=WAIT)
    assert count[0] == 400 and overlaps[0] == 0


def test_reentrant_change_state_and_send_event_complete(fsms, rec):
    """The 0.1 deadlock: a handler calling change_state while the FSM lock was held."""
    holder = {}

    def start(event):
        fsm = holder["fsm"]
        fsm.change_state("Running")
        assert fsm.state == "Running"
        fsm.send_event({"name": "tick"})
    holder["fsm"] = fsm = make(fsms, {"Initializing": {"start": start}, "Running": {"tick": rec.handler()}})
    fsm.send_event({"name": "start"}).result(timeout=WAIT)
    fsm.join(timeout=WAIT)
    assert fsm.state == "Running" and rec.log == ["tick"]


def test_waiting_on_own_event_from_a_handler_raises_instead_of_deadlocking(fsms):
    holder = {}

    def start(event):
        holder["fsm"].send_event({"name": "other"}).result()
    holder["fsm"] = fsm = make(fsms, {"all": {"start": start, "other": lambda e: None}},
                               on_error=lambda *a: None)
    with pytest.raises(ProtoUserError, match="deadlock"):
        fsm.send_event({"name": "start"}).result(timeout=WAIT)


def test_handler_exception_goes_to_the_future_rolls_back_and_is_raised_by_join(fsms, rec):
    holder = {}

    def boom(event):
        holder["fsm"].change_state("Running")
        raise ValueError("handler failed")
    holder["fsm"] = fsm = make(fsms, {"Initializing": {"boom": boom, "ok": rec.handler()}, "Running": {}})
    with pytest.raises(ValueError, match="handler failed") as info:
        fsm.send_event({"name": "boom"}).result(timeout=WAIT)
    assert any("'boom'" in note for note in info.value.__notes__)
    assert fsm.state == "Initializing"  # rolled back
    fsm.send_event({"name": "ok"}).result(timeout=WAIT)  # the worker survived
    assert rec.log == ["ok"]
    with pytest.raises(ValueError, match="handler failed"):
        fsm.join(timeout=WAIT)
    fsm.join(timeout=WAIT)  # reported once


def test_several_errors_first_raised_others_noted(fsms):
    def fail(event):
        raise KeyError(event["n"])
    fsm = make(fsms, {"all": {"e": fail}})
    for i in range(3):
        fsm.send_event({"name": "e", "n": i})
    with pytest.raises(KeyError) as info:
        fsm.join(timeout=WAIT)
    assert info.value.args == (0,)
    assert any("2 other" in note for note in info.value.__notes__)


def test_error_hook_receives_errors_and_join_does_not_raise(fsms):
    received = []
    fsm = make(fsms, {"all": {"e": lambda e: 1 / 0}}, on_error=lambda f, ev, exc: received.append((f, ev, exc)))
    fsm.send_event({"name": "e"})
    fsm.join(timeout=WAIT)
    assert len(received) == 1 and received[0][0] is fsm and isinstance(received[0][2], ZeroDivisionError)


def test_error_hook_failure_is_raised_by_join(fsms):
    def hook(f, ev, exc):
        raise RuntimeError("hook failed")
    fsm = make(fsms, {"all": {"e": lambda e: 1 / 0}}, on_error=hook)
    fsm.send_event({"name": "e"})
    with pytest.raises(RuntimeError, match="hook failed"):
        fsm.join(timeout=WAIT)


def test_on_error_must_be_callable():
    with pytest.raises(ProtoValidationError):
        FSM({"S": {}}, on_error=3)


def test_after_processing_runs_after_the_handler_in_order(fsms):
    log = []
    holder = {}

    def start(event):
        fsm = holder["fsm"]
        fsm.after_processing(log.append, "post-1")
        fsm.after_processing(log.append, "post-1")  # duplicates are kept
        fsm.after_processing(lambda value=None: log.append(value), value="post-2")
        log.append("handler")
    holder["fsm"] = fsm = make(fsms, {"all": {"start": start}})
    fsm.send_event({"name": "start"}).result(timeout=WAIT)
    assert log == ["handler", "post-1", "post-1", "post-2"]


def test_after_processing_outside_a_handler_is_rejected(fsms):
    fsm = make(fsms, {"all": {}})
    with pytest.raises(ProtoUserError):
        fsm.after_processing(print)


def test_post_task_error_is_reported(fsms):
    holder = {}

    def start(event):
        holder["fsm"].after_processing(lambda: 1 / 0)
    holder["fsm"] = fsm = make(fsms, {"all": {"start": start}})
    with pytest.raises(ZeroDivisionError):
        fsm.send_event({"name": "start"}).result(timeout=WAIT)
    with pytest.raises(ZeroDivisionError):
        fsm.join(timeout=WAIT)


def test_target_fsm(fsms, rec):
    a = make(fsms, {"all": {}})
    b = make(fsms, {"all": {"ping": rec.handler()}})
    a.send_event({"name": "ping"}, target_fsm=b).result(timeout=WAIT)
    assert rec.log == ["ping"]


# -- timers -----------------------------------------------------------------------------------

def test_timer_fires_after_its_delay(fsms, rec):
    fsm = make(fsms, {"all": {"timeout": rec.handler()}})
    t0 = time.monotonic()
    timer = fsm.start_timer(50, {"name": "timeout"})
    assert isinstance(timer, Timer)
    timer.wait_til_processed(timeout=WAIT)
    assert time.monotonic() - t0 >= 0.045
    assert rec.log == ["timeout"] and timer.fired and not timer.canceled
    assert timer.stop() is False  # too late


def test_stopped_timer_never_fires(fsms, rec):
    fsm = make(fsms, {"all": {"timeout": rec.handler()}})
    timer = fsm.start_timer(100, {"name": "timeout"})
    assert timer.stop() is True and timer.canceled
    timer.wait_til_processed(timeout=WAIT)  # returns at once
    time.sleep(0.2)
    fsm.join(timeout=WAIT)
    assert rec.log == []


def test_stop_all_timers(fsms, rec):
    fsm = make(fsms, {"all": {"timeout": rec.handler()}})
    timers = [fsm.start_timer(80, {"name": "timeout"}) for _ in range(5)]
    fsm.stop_all_timers()
    time.sleep(0.15)
    fsm.join(timeout=WAIT)
    assert rec.log == [] and all(t.canceled for t in timers)


def test_timers_fire_in_deadline_order(fsms, rec):
    fsm = make(fsms, {"all": {"a": rec.handler(), "b": rec.handler(), "c": rec.handler()}})
    tc = fsm.start_timer(90, {"name": "c"})
    fsm.start_timer(10, {"name": "a"})
    fsm.start_timer(50, {"name": "b"})
    tc.wait_til_processed(timeout=WAIT)
    assert rec.log == ["a", "b", "c"]


def test_timer_handler_error_is_raised_by_wait(fsms):
    fsm = make(fsms, {"all": {"t": lambda e: [][1]}}, on_error=lambda *a: None)
    timer = fsm.start_timer(1, {"name": "t"})
    with pytest.raises(IndexError):
        timer.wait_til_processed(timeout=WAIT)


@pytest.mark.parametrize("delay", [-1, "5", None, float("nan")])
def test_invalid_timer_delay(fsms, delay):
    fsm = make(fsms, {"all": {}})
    with pytest.raises(ProtoValidationError):
        fsm.start_timer(delay, {"name": "t"})


# -- lifecycle --------------------------------------------------------------------------------

def test_stop_drains_queued_events_and_joins_the_worker(rec):
    baseline = threading.active_count()
    fsm = FSM({"all": {"e": rec.handler()}})
    for _ in range(20):
        fsm.send_event({"name": "e"})
    timer = fsm.start_timer(5000, {"name": "e"})
    fsm.stop()
    assert len(rec.log) == 20 and timer.canceled
    assert threading.active_count() == baseline
    with pytest.raises(ProtoUserError):
        fsm.send_event({"name": "e"})
    with pytest.raises(ProtoUserError):
        fsm.start_timer(1, {"name": "e"})
    fsm.stop()  # idempotent


def test_stop_raises_pending_errors():
    fsm = FSM({"all": {"e": lambda e: 1 / 0}})
    fsm.send_event({"name": "e"})
    with pytest.raises(ZeroDivisionError):
        fsm.stop()


def test_context_manager_stops(rec):
    baseline = threading.active_count()
    with FSM({"all": {"e": rec.handler()}}) as fsm:
        fsm.send_event({"name": "e"})
    assert rec.log == ["e"] and _settle(baseline) == baseline


def test_stop_from_a_handler_does_not_deadlock():
    baseline = threading.active_count()
    holder = {}
    fsm = holder["fsm"] = FSM({"all": {"quit": lambda e: holder["fsm"].stop()}})
    fsm.send_event({"name": "quit"}).result(timeout=WAIT)
    fsm.stop()
    assert _settle(baseline) == baseline


def test_join_from_a_handler_is_rejected(fsms):
    holder = {}
    fsm = holder["fsm"] = make(fsms, {"all": {"j": lambda e: holder["fsm"].join()}}, on_error=lambda *a: None)
    with pytest.raises(ProtoUserError):
        fsm.send_event({"name": "j"}).result(timeout=WAIT)


def test_join_timeout(fsms):
    gate = threading.Event()
    fsm = make(fsms, {"all": {"block": lambda e: gate.wait(WAIT)}})
    fsm.send_event({"name": "block"})
    with pytest.raises(ProtoTimeoutError):
        fsm.join(timeout=0.05)
    gate.set()
    fsm.join(timeout=WAIT)


def test_change_state_from_many_threads(fsms):
    states = {f"S{i}": {} for i in range(8)}
    fsm = make(fsms, {**states, "all": {"e": lambda e: None}})

    def worker(i):
        for _ in range(200):
            fsm.change_state(f"S{i}")
            fsm.send_event({"name": "e"})
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    fsm.join(timeout=WAIT)
    assert fsm.state in states
