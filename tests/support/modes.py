"""One space in each commit mode of the precondition model (spec section 12: standalone, group
commit and cluster; Task 14).

``space_in_mode(mode, tmp_path, **space_kw)`` is a context manager yielding an open
:class:`~proto_db.transactions.ObjectSpace`:

* ``"standalone"``: a space over a :class:`~proto_db.storage.memory.MemoryStorage`; every
  commit is alone in its group round, so it publishes the pre-pass candidate whenever the root
  did not move.
* ``"group"``: the same space, with every group round forced to hold two queued commits:
  before a round runs, a *filler* commit (root ``n`` of the database ``filler``, from another
  thread) is queued and joins the round first (:func:`_grouped_round`).  Each commit of the test
  is then the round's second entry: the root it finds is the filler's, never its pre-pass's
  R0, so it is validated by the diff R0 -> R1 (R65) and reapplied on R1 (R32), whatever the
  test's sequence.  Deterministic: the round waits until the filler is queued (a condition, no
  sleep).
* ``"cluster"``: a 3-node in-process cluster over tiered storages (:mod:`tests.support.
  tiered_cluster`, :mod:`tests.support.cluster`); the space is node ``n0``'s, so every commit
  is built under ``n0``'s write permission (R38).

The fixtures ``mode`` (parametrized over :data:`MODES`), ``space_any_mode`` and
``db_any_mode`` (the database ``db`` of that space) are imported by the test modules that run in
every mode; a module's ``SPACE_KW`` dict, when present, is passed to the space.
"""
import contextlib
import itertools
import threading

import pytest

from proto_db import transactions
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace

MODES = ("standalone", "group", "cluster")
#: The database the group mode's filler commits write to (never the test's own).
FILLER_DATABASE = "filler"

_filler_thread = threading.local()


def _grouped_round(real_round, space, counter):
    """A replacement for ``transactions._standalone_round`` that queues one filler commit and
    runs it first in the round, before the commits the round took."""
    filler_db = space.open_database(FILLER_DATABASE)

    def fill(outcome):
        _filler_thread.active = True
        try:
            filler_db.run(lambda tr: tr.set_root_object("n", next(counter)))
            outcome.append("landed")
        except BaseException as e:     # reported by the round below, never swallowed
            outcome.append(e)
        finally:
            _filler_thread.active = False

    def grouped(storage, lock_timeout, batch):
        if getattr(_filler_thread, "active", False) or storage is not space.storage:
            return real_round(storage, lock_timeout, batch)
        group = batch[0].group
        outcome = []
        filler = threading.Thread(target=fill, args=(outcome,), name="group-mode-filler")
        filler.start()
        with group.cond:
            # The filler queues behind this running round (group.running is set): wait for it,
            # then take it into this round, first.
            group.cond.wait_for(lambda: group.queue or not filler.is_alive())
            queued, group.queue = group.queue, []
        # Group mode must never degrade silently to a one-commit round (Task 14 fix round 1):
        # the filler is queued here, and its commit lands in this round.
        assert queued, f"group mode: the filler commit was not queued ({outcome!r})"
        try:
            return real_round(storage, lock_timeout, queued + batch)
        finally:
            filler.join()
            assert outcome == ["landed"], f"group mode: the filler commit did not land ({outcome!r})"
    return grouped


@contextlib.contextmanager
def space_in_mode(mode: str, tmp_path, **space_kw):
    """An open space in ``mode`` (module docstring); closed (and the cluster stopped) on exit."""
    if mode not in MODES:
        raise ValueError(f"Unknown mode {mode!r}; expected one of {MODES}")
    if mode == "cluster":
        from tests.support.tiered_cluster import TieredCluster
        with TieredCluster(tmp_path, 3, space_kw=space_kw) as tc:
            yield tc.spaces["n0"]
        return
    space = ObjectSpace(MemoryStorage(), **space_kw)
    try:
        if mode == "standalone":
            yield space
            return
        space.new_database(FILLER_DATABASE)
        real = transactions._standalone_round
        transactions._standalone_round = _grouped_round(real, space, itertools.count(1))
        try:
            yield space
        finally:
            transactions._standalone_round = real
    finally:
        space.close()


@pytest.fixture(params=MODES)
def mode(request):
    """The commit mode of the test (standalone, group, cluster)."""
    return request.param


@pytest.fixture
def space_any_mode(request, mode, tmp_path):
    """A space in ``mode``, with the test module's ``SPACE_KW``."""
    with space_in_mode(mode, tmp_path, **getattr(request.module, "SPACE_KW", {})) as space:
        yield space


@pytest.fixture
def db_any_mode(space_any_mode):
    """The database ``db`` of a space in each mode."""
    return space_any_mode.new_database("db")
