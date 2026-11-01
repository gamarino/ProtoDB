"""Finite state machine with one worker thread per machine (decision D41).

``FSM(definition, *, on_error=None, name=None)``: ``definition`` maps state
names to ``{event name: handler}`` dicts; the pseudo-state ``"all"`` holds
handlers that apply in every state.  A handler is called as
``handler(event)``; handlers that need the machine capture it (closures).
The machine starts in state ``"Initializing"`` and immediately queues the
event ``{"name": "Initializing"}``.

Semantics (D41):

- **One worker.** Each machine owns one worker thread that processes its event
  queue (and its timers) in order; handlers of one machine never run
  concurrently.  :meth:`FSM.send_event` validates the event, queues it and
  returns an :class:`EventFuture` resolved when the event has been processed.
  The handler is the current state's handler for the event name, else the
  ``"all"`` handler, else none (the event is consumed with no effect).
- **Re-entrancy.** No lock is held while a handler runs, so a handler may call
  :meth:`FSM.change_state`, :meth:`FSM.send_event` (the event is processed
  after the current one), :meth:`FSM.start_timer`, :meth:`FSM.after_processing`
  and :meth:`FSM.stop`.  Waiting from a handler for an unfinished event of the
  same machine (``EventFuture.result``, :meth:`FSM.join`,
  ``Timer.wait_til_processed``) would deadlock and raises ``ProtoUserError``.
- **Errors are never swallowed.** If a handler (or a post-processing task)
  raises, the state is restored to the state before the event (handler
  failures only), the exception gets a note naming the machine, event and
  state, and it is set on the event's future.  It is then delivered to
  ``on_error(fsm, event, exc)`` when given; otherwise it is kept and re-raised
  by the next :meth:`FSM.join` or :meth:`FSM.stop` (the first one, with the
  others listed in a note and kept as objects, see
  :func:`proto_db.errors.secondary_errors`), unless a caller retrieved it
  through the event's future first (it is then consumed, reported once).
  An exception raised by ``on_error`` itself is kept and re-raised the same way.  The worker keeps processing later events.
- **Timers** are kept in a heap served by the worker: a due timer queues its
  event.  :meth:`Timer.stop` cancels a timer that has not fired (returns True);
  :meth:`FSM.stop` cancels them all.
- **Stop.** :meth:`FSM.stop` refuses new events and timers, processes the
  events already queued, joins the worker (unless called from a handler) and
  re-raises pending errors.  Use the machine as a context manager or call
  ``stop()``: the worker is a daemon thread, and a machine that is never
  stopped keeps it alive.
"""
from __future__ import annotations

import heapq
import itertools
import logging
import math
import threading
import time
from collections import deque
from collections.abc import Callable, Mapping
from concurrent.futures import CancelledError, Future

from ..errors import ProtoTimeoutError, ProtoUserError, ProtoValidationError, attach_secondary_errors

__all__ = ["FSM", "Timer", "EventFuture", "ALL_STATES", "INITIAL_STATE"]

_logger = logging.getLogger(__name__)

ALL_STATES = "all"
INITIAL_STATE = "Initializing"
_MAX_LISTED_ERRORS = 10
_names = itertools.count(1)


def _validate_definition(definition) -> dict:
    if not isinstance(definition, Mapping):
        raise ProtoValidationError("FSM definition must be a mapping of state names to event handler mappings")
    for state, handlers in definition.items():
        if not isinstance(state, str) or not state:
            raise ProtoValidationError(f"FSM state names must be non-empty strings, not {state!r}")
        if not isinstance(handlers, Mapping):
            raise ProtoValidationError(f"Event handlers for state {state!r} must be a mapping of event names "
                                       f"to callables")
        for event, handler in handlers.items():
            if not isinstance(event, str) or not event:
                raise ProtoValidationError(f"Event names in state {state!r} must be non-empty strings, "
                                           f"not {event!r}")
            if not callable(handler):
                raise ProtoValidationError(f"Handler for event {event!r} in state {state!r} must be callable")
    return {state: dict(handlers) for state, handlers in definition.items()}


def _validate_event(event) -> str:
    if not isinstance(event, Mapping):
        raise ProtoValidationError(f"An FSM event must be a mapping with a 'name', not {event!r}")
    name = event.get("name")
    if not isinstance(name, str) or not name:
        raise ProtoValidationError(f"An FSM event needs a non-empty string 'name', got {name!r}")
    return name


class EventFuture(Future):
    """The outcome of one event: ``None`` once processed, or the handler's exception.

    A ``concurrent.futures.Future``; waiting on an unfinished event from the
    worker of the same machine raises ``ProtoUserError`` (it would deadlock).
    """

    def __init__(self, fsm: "FSM"):
        super().__init__()
        self._fsm = fsm

    def _check_wait(self) -> None:
        if not self.done() and self._fsm._on_worker():
            raise ProtoUserError("Waiting from a handler for an unprocessed event of the same FSM would deadlock")

    def result(self, timeout=None):
        """As ``Future.result``; raises ``ProtoUserError`` instead of deadlocking (class docstring).

        Retrieving a handler error here consumes it: :meth:`FSM.join` / :meth:`FSM.stop`
        do not re-raise it.
        """
        self.exception(timeout)
        return super().result(0)

    def exception(self, timeout=None):
        """As ``Future.exception``; raises ``ProtoUserError`` instead of deadlocking (class docstring).

        A returned handler error is consumed (see :meth:`result`).
        """
        self._check_wait()
        exc = super().exception(timeout)
        if exc is not None:
            self._fsm._consume(exc)
        return exc


class Timer:
    """A delayed event created by :meth:`FSM.start_timer`.

    ``fired`` / ``canceled`` tell what happened; :meth:`stop` cancels it if it
    has not fired; :meth:`wait_til_processed` waits for its event.
    """

    def __init__(self, fsm: "FSM", deadline: float, event: Mapping):
        self._fsm = fsm
        self.deadline = deadline
        self.event = event
        self.future = EventFuture(fsm)
        self._status = "pending"  # guarded by the FSM's condition

    @property
    def fired(self) -> bool:
        """True once the timer has queued its event."""
        return self._status == "fired"

    @property
    def canceled(self) -> bool:
        """True if the timer was stopped before firing."""
        return self._status == "canceled"

    def stop(self) -> bool:
        """Cancel the timer; True if this prevented it from firing, False if it had already fired or stopped."""
        return self._fsm._cancel_timer(self)

    def wait_til_processed(self, timeout: float | None = None) -> None:
        """Wait until the timer's event is processed (return at once if canceled).

        Re-raises the handler's exception; ``timeout`` expiry raises ``ProtoTimeoutError``.
        """
        if self.canceled:
            return
        self.future._check_wait()
        try:
            self.future.result(timeout)
        except CancelledError:
            return  # stopped concurrently, before firing: nothing to wait for
        except TimeoutError as e:
            raise ProtoTimeoutError(f"Timer event {self.event.get('name')!r} not processed within {timeout} s") \
                from e


class FSM:
    """A finite state machine served by one worker thread (module docstring, "Semantics").

    ``on_error(fsm, event, exc)`` (optional) receives handler errors;
    without it they are re-raised by :meth:`join` / :meth:`stop`.
    ``name`` labels the worker thread and error notes.
    """

    def __init__(self, fsm_definition: Mapping, *, on_error: Callable | None = None, name: str | None = None):
        self._fsm_definition = _validate_definition(fsm_definition)
        if on_error is not None and not callable(on_error):
            raise ProtoValidationError(f"on_error must be callable, not {on_error!r}")
        self._on_error = on_error
        self.name = name if name is not None else f"fsm-{next(_names)}"
        self._cond = threading.Condition(threading.Lock())
        self._state = INITIAL_STATE
        self._queue: deque = deque()
        self._timers: list = []  # heap of (deadline, seq, Timer)
        self._timer_seq = itertools.count()
        self._enqueued = 0  # items appended to the queue (events and fired timers)
        self._processed = 0  # items taken from the queue and finished
        self._stopping = False
        self._pending_errors: list[BaseException] = []
        self._post: list | None = None  # post-processing tasks of the event being handled (worker only)
        self._thread = threading.Thread(target=self._run, name=f"proto-{self.name}", daemon=True)
        self._thread.start()
        self.send_event({"name": INITIAL_STATE})

    # -- public API ---------------------------------------------------------------------------

    @property
    def state(self) -> str:
        """The current state."""
        with self._cond:
            return self._state

    def __repr__(self):
        return f"FSM({self.name!r}, state={self.state!r})"

    def __enter__(self) -> "FSM":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()

    def change_state(self, new_state: str) -> None:
        """Set the current state; ``ProtoValidationError`` if it is not a defined state (``"all"`` is not one)."""
        if new_state == ALL_STATES or new_state not in self._fsm_definition:
            raise ProtoValidationError(f"Trying to change state to an invalid one! ({new_state!r})")
        with self._cond:
            self._state = new_state

    def send_event(self, event: Mapping, target_fsm: "FSM | None" = None) -> EventFuture:
        """Queue ``event`` (a mapping with a non-empty ``"name"``) on this machine or on ``target_fsm``.

        Returns an :class:`EventFuture`.  Raises ``ProtoUserError`` once the
        target machine is stopping.
        """
        if target_fsm is not None and target_fsm is not self:
            return target_fsm.send_event(event)
        _validate_event(event)
        future = EventFuture(self)
        with self._cond:
            if self._stopping:
                raise ProtoUserError(f"FSM {self.name!r} is stopped; it accepts no new events")
            self._queue.append((event, future))
            self._enqueued += 1
            self._cond.notify_all()
        return future

    def after_processing(self, function: Callable, *args, **kwargs) -> None:
        """From a handler: run ``function(*args, **kwargs)`` after the handler returns, in registration order.

        Post-processing tasks run on the worker after the handler succeeded;
        the first one that raises stops the others and is reported like a
        handler error (without rolling back the state).  Calling this outside
        a handler raises ``ProtoUserError``.
        """
        if not callable(function):
            raise ProtoValidationError(f"after_processing needs a callable, not {function!r}")
        if not self._on_worker() or self._post is None:
            raise ProtoUserError("after_processing can only be called from an event handler of this FSM")
        self._post.append((function, args, kwargs))

    def start_timer(self, delay_in_ms, event: Mapping) -> Timer:
        """Queue ``event`` after ``delay_in_ms`` milliseconds (a finite number >= 0); returns a :class:`Timer`."""
        if not isinstance(delay_in_ms, (int, float)) or isinstance(delay_in_ms, bool) \
                or not math.isfinite(delay_in_ms) or delay_in_ms < 0:
            raise ProtoValidationError(f"Timer delay must be a finite number of milliseconds >= 0, "
                                       f"not {delay_in_ms!r}")
        _validate_event(event)
        timer = Timer(self, time.monotonic() + delay_in_ms / 1000.0, event)
        with self._cond:
            if self._stopping:
                raise ProtoUserError(f"FSM {self.name!r} is stopped; it accepts no new timers")
            heapq.heappush(self._timers, (timer.deadline, next(self._timer_seq), timer))
            self._cond.notify_all()
        return timer

    def stop_all_timers(self) -> None:
        """Cancel every timer that has not fired."""
        with self._cond:
            cancelled = self._cancel_all_locked()
        self._cancel_futures(cancelled)

    def join(self, timeout: float | None = None) -> None:
        """Wait until every event queued before this call is processed, then re-raise pending errors.

        Events queued after the call (e.g. by a handler that resends itself) and
        pending timers are not waited for.  Expiry of ``timeout`` raises
        ``ProtoTimeoutError``; calling it from a handler raises ``ProtoUserError``.
        """
        if self._on_worker():
            raise ProtoUserError("join() called from a handler of the same FSM would deadlock")
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            target = self._enqueued
            while self._processed < target and self._thread.is_alive():
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise ProtoTimeoutError(f"FSM {self.name!r} did not become idle within {timeout} s")
                self._cond.wait(remaining)
        self._raise_pending()

    def stop(self, *, raise_errors: bool = True) -> None:
        """Refuse new events and timers, cancel timers, process queued events and join the worker.

        Idempotent.  From a handler it only requests the stop (the worker
        finishes the queue after the handler returns).  With ``raise_errors``
        (default) pending errors are re-raised; ``raise_errors=False`` leaves
        them for a later :meth:`join`.
        """
        with self._cond:
            self._stopping = True
            cancelled = self._cancel_all_locked()
            self._cond.notify_all()
        self._cancel_futures(cancelled)
        if self._on_worker():
            return
        self._thread.join()
        if raise_errors:
            self._raise_pending()

    # -- worker -------------------------------------------------------------------------------

    def _on_worker(self) -> bool:
        return threading.current_thread() is self._thread

    def _cancel_timer(self, timer: Timer) -> bool:
        with self._cond:
            cancelled = self._mark_cancelled(timer)
            self._cond.notify_all()
        if cancelled:
            self._cancel_futures([timer])
        return cancelled

    @staticmethod
    def _mark_cancelled(timer: Timer) -> bool:
        """Under the lock: mark a pending timer cancelled (its future is cancelled after the lock is released)."""
        if timer._status != "pending":
            return False
        timer._status = "canceled"
        return True

    def _cancel_all_locked(self) -> list:
        cancelled = [timer for _, _, timer in self._timers if self._mark_cancelled(timer)]
        self._timers.clear()
        return cancelled

    @staticmethod
    def _cancel_futures(timers: list) -> None:
        """Cancel the futures of cancelled timers; never called with the lock held (done-callbacks run here)."""
        for timer in timers:
            timer.future.cancel()

    def _next_item(self):
        """Block until an event is available (timers included); None when stopping with an empty queue."""
        with self._cond:
            while True:
                now = time.monotonic()
                while self._timers and self._timers[0][0] <= now:
                    _, _, timer = heapq.heappop(self._timers)
                    if timer._status == "pending":
                        timer._status = "fired"
                        self._queue.append((timer.event, timer.future))
                        self._enqueued += 1
                if self._queue:
                    return self._queue.popleft()
                if self._stopping:
                    self._cond.notify_all()
                    return None
                timeout = self._timers[0][0] - now if self._timers else None
                self._cond.wait(timeout)

    def _run(self) -> None:
        while True:
            item = self._next_item()
            if item is None:
                return
            try:
                self._process(*item)
            finally:
                with self._cond:
                    self._processed += 1
                    self._cond.notify_all()

    def _process(self, event: Mapping, future: EventFuture) -> None:
        if not future.set_running_or_notify_cancel():
            return
        name = event["name"]
        with self._cond:
            saved = self._state
        handler = self._fsm_definition.get(saved, {}).get(name)
        if handler is None:
            handler = self._fsm_definition.get(ALL_STATES, {}).get(name)
        error = None
        self._post = []
        try:
            if handler is not None:
                _logger.debug("FSM %s: event %r in state %r", self.name, name, saved)
                handler(event)
        except BaseException as e:  # reported below: future, then on_error or join()/stop()
            error = e
            with self._cond:
                self._state = saved
        post, self._post = self._post, None
        if error is None:
            for function, args, kwargs in post:
                try:
                    function(*args, **kwargs)
                except BaseException as e:  # reported below like a handler error
                    error = e
                    break
        if error is None:
            future.set_result(None)
            return
        error.add_note(f"FSM {self.name!r}: raised while processing event {name!r} in state {saved!r}")
        if self._on_error is None:
            self._keep(error)  # before set_exception, so that retrieval through the future can consume it
            future.set_exception(error)
            return
        future.set_exception(error)
        self._report(event, error)

    def _report(self, event: Mapping, error: BaseException) -> None:
        try:
            self._on_error(self, event, error)
        except BaseException as hook_error:  # kept and re-raised by join()/stop()
            hook_error.add_note(f"FSM {self.name!r}: raised by on_error while reporting {error!r}")
            self._keep(hook_error)

    def _keep(self, error: BaseException) -> None:
        with self._cond:
            self._pending_errors.append(error)

    def _consume(self, error: BaseException) -> None:
        """Forget a pending error that a caller retrieved through its event future."""
        with self._cond:
            self._pending_errors = [e for e in self._pending_errors if e is not error]

    def _raise_pending(self) -> None:
        with self._cond:
            errors, self._pending_errors = self._pending_errors, []
        if not errors:
            return
        first, others = errors[0], errors[1:]
        if others:
            listed = "; ".join(repr(e) for e in others[:_MAX_LISTED_ERRORS])
            more = "; ..." if len(others) > _MAX_LISTED_ERRORS else ""
            first.add_note(f"FSM {self.name!r}: {len(others)} other handler error(s): {listed}{more}")
        attach_secondary_errors(first, others)
        raise first
