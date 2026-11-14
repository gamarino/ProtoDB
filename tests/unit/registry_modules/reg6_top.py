"""Task 6 fixture: the last module of the ``reg6.Opp`` chain, with the ``_super`` misuses."""
from proto_db.registry import module

top = module("reg6_top", depends=("reg6_mid",))


@top.extends("reg6.Opp")
class Opp:
    level = "top"

    def win(self, trace):
        trace.append("top")
        return self._super().win(trace)

    def super_stage(self):
        return self._super().stage

    def super_items(self):
        sup = self._super()
        return sup["stage"], "stage" in sup, "absent" in sup, sup.absent

    def from_closure(self):
        def inner():
            return self._super().stage
        return inner()

    def from_genexpr(self):
        return list(self._super().stage for _ in range(1))

    def generator_method(self):
        yield self._super().stage

    def on_other(self, other):
        return other._super().stage

    def store_super(self):
        return self.with_(kept=self._super())

    async def async_win(self, trace):
        # A coroutine method is a method: _super() works in it (ruling R176).
        trace.append("async top")
        return self._super().win(trace)
