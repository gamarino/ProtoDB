"""Task 6 fixture: the first module of the ``reg6.Opp`` chain, and the parent name ``reg6.Thread``."""
from proto_db.registry import module

base = module("reg6_base")


@base.extends("reg6.Thread")
class Thread:
    channel = "thread"

    def win(self, trace):
        trace.append("thread")
        # The end of the chain: a further definition is a miss, which is None (R150).
        assert self._super().win is None
        return trace


@base.extends("reg6.Opp", parents=("reg6.Thread",))
class Opp:
    stage = "base-default"
    level = "base"

    def win(self, trace):
        trace.append("base")
        return self._super().win(trace)

    def title(self):
        return "base title"

    def describe(self):
        # Late binding: ``self.title`` resolves from the start, so a later override wins.
        return f"{self.title()} ({self.level})"
