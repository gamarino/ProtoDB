"""Task 6 fixture: the second module of the ``reg6.Opp`` chain."""
from proto_db.registry import module

mid = module("reg6_mid", depends=("reg6_base",))


@mid.extends("reg6.Opp")
class Opp:
    stage = "mid-default"

    def win(self, trace):
        trace.append("mid")
        return self._super().win(trace)

    def title(self):
        return "mid title"

    def promote(self, stage):
        return self.with_(stage=f"{stage}->won")

    def me(self):
        return self
