"""RecordingIndex: an immutable index double that records every hook call it receives.

It implements the index hook names of ruling "Index hooks" (``build``, ``empty``,
``with_added``, ``with_removed``, ``with_replaced``) and keeps the calls as an
``events`` tuple: ``("build", elements_tuple)``, ``("add", e)``, ``("remove", e)``,
``("replace", old, new)``.  Every hook returns a NEW RecordingIndex; the receiver
never changes.  It is an atom, so it persists with the collection it belongs to.

:class:`RootRecordingExtractor` (spec section 12 item 3, P-C) is a key extractor registered under
a name (``KeySpec.extractor(name)``) that records, per call, whether the calling thread held the
space root (``Transaction._holding_root_lock()``): a test asserts that it ran and that no call saw
the root held, a reapplication over a changed root included.
"""
import threading
from collections import Counter

from proto_db.core.atom import Atom
from proto_db.errors import ProtoUserError
from proto_db.values import order_key


class RecordingIndex(Atom):
    """Index double: ``name`` and the ``events`` it has seen, oldest first."""

    __type_name__ = "tests.support.RecordingIndex"

    def __init__(self, name, events=(), *, context=None):
        super().__init__(context=context)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "events", tuple(events))

    def __setattr__(self, name, value):
        raise ProtoUserError("RecordingIndex is immutable")

    def _plus(self, event):
        return RecordingIndex(self.name, self.events + (event,))

    def empty(self):
        """Return an index with the same name and no events."""
        return RecordingIndex(self.name)

    def build(self, elements):
        """Fold ``elements`` onto this index (as a fold of ``with_added`` would): records a build event.

        Building a non-empty index keeps its previous content, so a collection
        that forgets to call ``empty()`` first ends up with a stale index.
        """
        return self._plus(("build", tuple(elements)))

    def with_added(self, element):
        """Record an element addition."""
        return self._plus(("add", element))

    def with_removed(self, element):
        """Record an element removal."""
        return self._plus(("remove", element))

    def with_replaced(self, old, new):
        """Record an element replacement."""
        return self._plus(("replace", old, new))

    def events_since(self, older: "RecordingIndex") -> tuple:
        """Events recorded after ``older`` (which must be a prefix of this index's history)."""
        n = len(older.events)
        assert self.events[:n] == older.events, "index history was rewritten"
        return self.events[n:]

    def live(self, key=order_key) -> Counter:
        """Multiset (by ``key(element)``) of the elements this index believes are in the collection.

        Raises AssertionError when a remove/replace names an element the index does not hold
        (a stale index).
        """
        live = Counter()
        for event in self.events:
            kind = event[0]
            if kind == "build":
                live.update(key(e) for e in event[1])
            elif kind == "add":
                live[key(event[1])] += 1
            elif kind == "remove":
                k = key(event[1])
                assert live[k] > 0, f"index saw a remove of {event[1]!r} it does not hold"
                live[k] -= 1
            elif kind == "replace":
                k = key(event[1])
                assert live[k] > 0, f"index saw a replace of {event[1]!r} it does not hold"
                live[k] -= 1
                live[key(event[2])] += 1
            else:  # pragma: no cover - defensive
                raise AssertionError(f"unknown event {event!r}")
        return +live


class RootRecordingExtractor:
    """``key(element)`` as an extractor named ``name`` that records, per call, whether the calling
    thread held the space root.  A context manager: registered on entry, unregistered on exit."""

    def __init__(self, name: str, key=lambda element: element):
        self.name = name
        self._key = key
        self._lock = threading.Lock()
        self.calls: list = []

    def __call__(self, element):
        from proto_db.transactions import Transaction
        held = Transaction._holding_root_lock()
        with self._lock:
            self.calls.append(held)
        return self._key(element)

    @property
    def key_spec(self):
        """The key spec naming this extractor."""
        from proto_db.indexing.keyspec import KeySpec
        return KeySpec.extractor(self.name)

    def held_calls(self) -> int:
        """How many calls ran with the space root held (must be 0)."""
        with self._lock:
            return sum(1 for held in self.calls if held)

    def reset(self) -> None:
        """Forget the calls recorded so far."""
        with self._lock:
            self.calls.clear()

    def __enter__(self):
        from proto_db.indexing.keyspec import register_extractor
        register_extractor(self.name, self)
        return self

    def __exit__(self, *exc):
        from proto_db.indexing.keyspec import unregister_extractor
        unregister_extractor(self.name)
        return False
