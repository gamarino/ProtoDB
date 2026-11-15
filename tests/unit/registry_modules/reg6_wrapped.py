"""Final review I5 fixture: methods wrapped by a ``functools.wraps`` decorator, one decorator on
two methods (one wrapper code object), and ``_super()`` inside a wrapped body; ``reg6_wrapped_top``
overrides one of them with a wrapped method that supers into this one."""
import functools

from proto_db.registry import module

wrapped = module("reg6_wrapped")

CALLS = []


def logged(fn):
    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        CALLS.append(fn.__name__)
        return fn(self, *args, **kwargs)
    return wrapper


@wrapped.extends("reg6.Wrapped")
class Wrapped:
    @logged
    def first(self):
        return "first"

    @logged
    def second(self):
        return "second"

    @logged
    @logged
    def twice(self):
        return ["wrapped"]
