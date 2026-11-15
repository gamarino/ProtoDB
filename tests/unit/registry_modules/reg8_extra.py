"""Task 8 fixture: a module over ``reg8_base`` that overrides ``reg8.Thing.greet``."""
from proto_db.registry import module

extra = module("reg8_extra", depends=("reg8_base",))


@extra.extends("reg8.Thing")
class Thing:
    def greet(self):
        return "extra+" + self._super().greet()
