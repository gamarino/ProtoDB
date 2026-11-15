"""Final review I5 fixture: one method wrapped without ``functools.wraps`` whose body calls
``_super()``: the body's code object is not indexed, so the call is refused explicitly."""
from proto_db.registry import module

plain = module("reg6_unwrapped_super", depends=("reg6_base",))


def bare(fn):
    def wrapper(self, *args, **kwargs):
        return fn(self, *args, **kwargs)
    return wrapper


@plain.extends("reg6.Opp")
class Opp:
    @bare
    def title(self):
        return "bare " + self._super().title()
