"""Task 7 fixture: a module without dependencies that introduces the registry name ``reg7.Partner``."""
from proto_db.registry import module

base = module("reg7_base")


@base.extends("reg7.Partner")
class Partner:
    kind = "partner"
