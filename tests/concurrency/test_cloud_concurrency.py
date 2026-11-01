"""Two CloudStorage instances over one object store: no lost update under real parallelism (Task 8.3).

Each instance stands for a separate process: its ``root_lock`` excludes only its own
threads, so commits from the two instances race on the root object and only the
compare-and-swap of ``publish_root`` arbitrates.  8 threads (4 per instance) run
``Database.run`` increments of:

* a plain ``int`` root (read set: a lost CAS race is a conflict, retried);
* a ``CountedSet`` receiving blind adds (intent merge; a lost CAS race is retried and
  the merge replays the intent on the winner's version);

and again over stores that fail at random (5 % and 20 % of operations) (transient errors, timeouts before and
after a write is applied), which exercises the unknown-outcome resolution under
contention: a doubled or lost commit shows up as a wrong count.

Catches: lost updates across instances, commits applied twice after an unknown
outcome, and deadlocks between the in-process lock and the CAS retry path.
"""
import pytest

from proto_db.storage.cloud import CloudStorage
from proto_db.transactions import ObjectSpace
from tests.concurrency.harness import Attempts, report, run_threads
from tests.support.fake_object_store import FakeObjectStore

THREADS = 8
PER_THREAD = 60
TOTAL = THREADS * PER_THREAD
RUN_KW = dict(retries=10_000, backoff=(0.001, 0.05))

pytestmark = [pytest.mark.freethreaded, pytest.mark.timeout(120)]


def _spaces(store, **kw):
    """Two independent spaces (one CloudStorage each) over ``store``."""
    return [ObjectSpace(CloudStorage(store, backoff=(0.0, 0.001), **kw)) for _ in range(2)]


def _hammer(spaces, fn, attempts):
    dbs = [s.open_database("db") for s in spaces]

    def worker_for(db):
        def worker():
            for _ in range(PER_THREAD):
                attempts.run(db, fn, **RUN_KW)
        return worker

    return run_threads([worker_for(dbs[i % 2]) for i in range(THREADS)])


def _read(space, fn):
    tr = space.open_database("db").new_transaction()
    try:
        return fn(tr)
    finally:
        tr.abort()


@pytest.mark.parametrize("faults", [0.0, 0.05, 0.2], ids=["reliable", "flaky5", "flaky20"])
def test_plain_int_counter_across_instances(request, faults):
    store = FakeObjectStore(random_faults=faults, seed=11)
    spaces = _spaces(store, retries=50)
    db = spaces[0].new_database("db")
    with db.new_transaction() as tr:
        tr.set_root_object("n", 0)
    attempts = Attempts()

    def increment(tr):
        tr.set_root_object("n", tr.get_root_object("n") + 1)

    elapsed = _hammer(spaces, increment, attempts)
    fresh = ObjectSpace(CloudStorage(store, retries=50, backoff=(0.0, 0.001)))
    assert _read(fresh, lambda tr: tr.get_root_object("n")) == TOTAL
    assert attempts.commits == TOTAL
    assert fresh.open_database("db").history()[0].version == 1 + TOTAL
    report(request.node.name, elapsed, attempts)
    for s in spaces + [fresh]:
        s.close()


def test_counted_set_blind_adds_across_instances(request):
    store = FakeObjectStore()
    spaces = _spaces(store)
    db = spaces[0].new_database("db")
    with db.new_transaction() as tr:
        tr.set_root_object("hits", tr.new_counted_set().add("seed"))
    attempts = Attempts()

    def hit(tr):
        tr.set_root_object("hits", tr.new_counted_set().add("hit"))

    elapsed = _hammer(spaces, hit, attempts)
    hits = _read(spaces[1], lambda tr: tr.get_root_object("hits"))
    assert hits.count_of("hit") == TOTAL and hits.count_of("seed") == 1
    assert attempts.commits == TOTAL
    report(request.node.name, elapsed, attempts)
    for s in spaces:
        s.close()
