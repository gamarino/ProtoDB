"""Task 7 fixture: one of four independent modules installed concurrently.  Each introduces its
own registry name and extends the shared name ``reg7.Shared``, so concurrent installs race on
the catalog entry of a name they all introduce."""
from proto_db.registry import module

handle = module("reg7_a")


@handle.extends("reg7.Only_a")
class Only:
    origin = "reg7_a"


@handle.extends("reg7.Shared")
class Shared:
    seen_a = True
