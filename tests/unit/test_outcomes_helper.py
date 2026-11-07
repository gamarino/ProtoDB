"""The commit-token helper of the cluster tests (ruling R114, N1; Task 8.3n P1, P3, P4) catches each
defect it exists for."""
import pytest

from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from tests.support.outcomes import (CommitTokens, Snapshot, assert_outcomes_resolved, observe_members,
                                    snapshot_of, write_token)


def _tokens(acked=(), unknown=()):
    t = CommitTokens()
    for tok in acked:
        t.acked(tok)
    for tok in unknown:
        t.unknown(tok)
    return t


def _readers(views):
    return {m: (lambda v=frozenset(v): v) for m, v in views.items()}


def test_resolved_outcomes_pass_and_return_the_tokens_present():
    t = _tokens(acked={"a", "b"}, unknown={"u", "w"})
    assert assert_outcomes_resolved(_readers({"n0": {"a", "b", "u"}, "n1": {"a", "b", "u"}}), t, 0.1).present \
        == {"a", "b", "u"}


def test_an_unknown_that_took_effect_cannot_hide_a_lost_acknowledged_commit():
    """The reviewer's planted pair: the count is right, the tokens are not."""
    t = _tokens(acked={"a", "b"}, unknown={"u"})
    with pytest.raises(AssertionError, match="acknowledged commits lost"):
        assert_outcomes_resolved(_readers({"n0": {"a", "u"}, "n1": {"a", "u"}}), t, 0.1)


def test_an_unknown_resolved_differently_on_two_members_fails():
    t = _tokens(acked={"a"}, unknown={"u"})
    with pytest.raises(AssertionError, match="not resolved alike"):
        assert_outcomes_resolved(_readers({"n0": {"a", "u"}, "n1": {"a"}}), t, 0.1)


def test_a_token_no_commit_reported_fails():
    t = _tokens(acked={"a"})
    with pytest.raises(AssertionError, match="no acknowledged or unknown commit wrote"):
        assert_outcomes_resolved(_readers({"n0": {"a", "x"}, "n1": {"a", "x"}}), t, 0.1)


def test_members_that_never_converge_fail():
    t = _tokens(acked={"a"}, unknown={"u"})
    with pytest.raises(AssertionError):
        assert_outcomes_resolved(_readers({"n0": {"a", "u"}, "n1": {"a", "v"}}), t, 0.1)


def _sequence(*views):
    """A reader that returns ``views`` in turn, then the last one forever."""
    views = [frozenset(v) for v in views]

    def read():
        return views.pop(0) if len(views) > 1 else views[0]
    return read


def test_a_commit_observed_and_then_removed_from_history_fails():
    """P1: the unknown commit ``u`` became visible on n0, then vanished everywhere (a successor
    built on a base without it).  The final state alone looks resolved (absent everywhere)."""
    t = _tokens(acked={"a"}, unknown={"u"})
    readers = {"n0": _sequence({"a", "u"}, {"a"}), "n1": _sequence({"a"})}
    observe_members(readers, t)                      # during the run: n0 shows u
    with pytest.raises(AssertionError, match="history later lost"):
        assert_outcomes_resolved(readers, t, 0.1)


def test_an_observation_made_while_waiting_counts_too():
    """P1: the helper's own reads are observations: u shows on n0 in its first poll only."""
    t = _tokens(acked={"a"}, unknown={"u"})
    readers = {"n0": _sequence({"a", "u"}, {"a"}), "n1": _sequence({"a", "v"}, {"a"})}
    with pytest.raises(AssertionError, match="history later lost"):
        assert_outcomes_resolved(readers, t, 0.5)


def test_an_unknown_that_was_never_seen_may_resolve_absent():
    t = _tokens(acked={"a"}, unknown={"u"})
    readers = {"n0": _sequence({"a"}), "n1": _sequence({"a"})}
    observe_members(readers, t)
    assert assert_outcomes_resolved(readers, t, 0.1).present == {"a"}


def test_an_empty_run_fails():
    """P3: no acknowledged commit at all, whatever the members show."""
    with pytest.raises(AssertionError, match="proves nothing"):
        assert_outcomes_resolved(_readers({"n0": set(), "n1": set()}), CommitTokens(), 0.1)
    with pytest.raises(AssertionError, match="proves nothing"):
        assert_outcomes_resolved(_readers({"n0": set(), "n1": set()}), _tokens(unknown={"u"}), 0.1)
    t = _tokens(acked={"a"})
    with pytest.raises(AssertionError, match="proves nothing"):
        assert_outcomes_resolved(_readers({"n0": {"a"}}), t, 0.1, min_acked=2)


def test_the_value_comes_from_the_snapshot_the_tokens_came_from():
    """P4: ``snapshot_of`` reads the tokens and the counter in one transaction.  A commit that
    lands between the two reads (here made by the value function itself, the worst case) is in
    neither, so the counter still equals the tokens shown; the helper returns, per member, the
    value of the snapshot whose tokens it judged."""
    space = ObjectSpace(MemoryStorage())
    try:
        db = space.new_database("db")
        t = CommitTokens()

        def commit():
            tok = t.new("n0")

            def bump(tr):
                tr.set_root_object("n", (tr.get_root_object("n") or 0) + 1)
                write_token(tr, tok)
            db.run(bump)
            t.acked(tok)
        for _ in range(3):
            commit()

        def counter_after_a_commit(tr):
            commit()                                 # lands after the tokens were read
            return tr.get_root_object("n")
        snap = snapshot_of(db, counter_after_a_commit)
        assert snap.value == len(snap.tokens) == 3
        res = assert_outcomes_resolved({"n0": lambda: snapshot_of(db, lambda tr: tr.get_root_object("n"))}, t, 0.1)
        assert res.values == {"n0": 4} and len(res.present) == 4
    finally:
        space.close()


def test_blind_tokens_merge_and_a_token_applied_twice_fails():
    """``add_blind_token`` reads nothing, so two transactions adding concurrently both commit
    (merged); a token present twice fails the read."""
    from tests.support.outcomes import add_blind_token, blind_snapshot_of
    space = ObjectSpace(MemoryStorage())
    try:
        db = space.new_database("db")
        t1, t2 = db.new_transaction(), db.new_transaction()
        add_blind_token(t1, "a")
        add_blind_token(t2, "b")
        t1.commit()
        t2.commit()                                  # no conflict: nothing was read
        assert blind_snapshot_of(db).tokens == {"a", "b"}
        db.run(lambda tr: add_blind_token(tr, "a"))  # planted: "a" applied a second time
        with pytest.raises(AssertionError, match="applied more than once"):
            blind_snapshot_of(db)
    finally:
        space.close()


def test_commit_once_applies_a_step_exactly_once_across_unknown_outcomes():
    """Review I6: ``commit_once`` turns an unknown outcome into a retry only when the step's
    effect is absent from a committed snapshot.  A first attempt that took effect but reported
    unknown is not applied again; one that took no effect is applied by the retry."""
    from proto_db.errors import ProtoOutcomeUnknownError
    from tests.support.outcomes import commit_once
    space = ObjectSpace(MemoryStorage())
    try:
        db = space.new_database("db")
        real_run = db.run
        calls = []

        def flaky_run(fn, **kw):
            calls.append(1)
            if len(calls) == 1:
                real_run(fn, **kw)                     # applied ...
                raise ProtoOutcomeUnknownError("planted: applied, reported unknown")
            if len(calls) == 3:
                raise ProtoOutcomeUnknownError("planted: nothing applied, reported unknown")
            return real_run(fn, **kw)
        db.run = flaky_run

        def incr(tr):
            tr.set_root_object("n", (tr.get_root_object("n") or 0) + 1)
        assert commit_once(db, incr) == 1             # one unknown met, the effect kept once
        assert commit_once(db, incr) == 1             # the unknown took nothing: applied by the retry
        tr = db.new_transaction()
        assert tr.get_root_object("n") == 2
        tr.abort()
    finally:
        space.close()



def test_observing_reads_during_the_body_and_raises_what_a_read_found():
    """Review M1: the observer reads members while the body runs, records what they show (a
    token seen mid-run counts as observed), and an error a read raised fails the block."""
    import time
    import pytest
    from tests.support.outcomes import CommitTokens, Snapshot, observing
    tokens = CommitTokens()
    t = tokens.new("n0")
    tokens.unknown(t)
    with observing({"n0": lambda: Snapshot(frozenset({t}))}, tokens, interval=0.001) as obs:
        time.sleep(0.05)
    assert obs.reads > 0 and t in tokens.observed

    def broken():
        raise AssertionError("token applied twice")
    with pytest.raises(AssertionError, match="applied twice"):
        with observing({"n0": broken}, tokens, interval=0.001):
            time.sleep(0.05)
