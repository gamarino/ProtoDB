"""Final review I5 fixture: a decorator without ``functools.wraps`` on two methods: both
definitions share the wrapper's code object, which ``_super`` cannot tell apart."""
from proto_db.registry import module

plain = module("reg6_unwrapped")


def bare(fn):
    def wrapper(self, *args, **kwargs):
        return fn(self, *args, **kwargs)
    return wrapper


@plain.extends("reg6.Unwrapped")
class Unwrapped:
    @bare
    def first(self):
        return "first"

    @bare
    def second(self):
        return "second"
