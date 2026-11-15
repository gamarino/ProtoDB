"""An extension that defines a property: refused when the module is imported."""
from proto_db.registry import module

bad = module("reg_bad_property")


@bad.extends("reg.Bad")
class Bad:
    @property
    def size(self):
        return 1
