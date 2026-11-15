"""Final review I5 fixture: a wrapped override whose body calls ``_super()``."""
from proto_db.registry import module
from tests.unit.registry_modules.reg6_wrapped import logged

top = module("reg6_wrapped_top", depends=("reg6_wrapped",))


@top.extends("reg6.Wrapped")
class Wrapped:
    @logged
    def twice(self):
        return ["top"] + self._super().twice()
