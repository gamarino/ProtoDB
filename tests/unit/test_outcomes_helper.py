"""The commit-token helper of the cluster tests (ruling R114, N1) catches each defect it exists for."""
import pytest

from tests.support.outcomes import CommitTokens, assert_outcomes_resolved


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
    assert assert_outcomes_resolved(_readers({"n0": {"a", "b", "u"}, "n1": {"a", "b", "u"}}), t, 0.1) \
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
