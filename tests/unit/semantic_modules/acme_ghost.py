"""The registry module ``acme_ghost`` (no dependencies): the type ``acme.ghost.Thing``, declared in the
decorator spelling only, so the catalog describes it while no Python class of this process is registered
for it (a semantic session cannot create one: ``ProtoNotSupportedError``)."""
from proto_db.registry import module
from proto_db.semantic import Field

ghost = module("acme_ghost")


@ghost.extends("acme.ghost.Thing")
class ThingDeclaration:
    name = Field(str, meaning="The thing's name")
