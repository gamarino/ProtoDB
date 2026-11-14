"""Task 7 fix round fixture: a plain module installed while ``reg7_importcommit`` is imported."""
from proto_db.registry import module

handle = module("reg7_y")


@handle.extends("reg7.Y_only")
class YOnly:
    v = 1
