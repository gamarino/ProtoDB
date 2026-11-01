"""Minimal PersistenceContext (ruling R3) for tests: saves and loads atoms directly."""
from proto_db.core.atom import Atom, AtomPointer


class DirectContext:
    """PersistenceContext over any object offering ``push_atom`` and ``get_atom``.

    ``save`` adopts atoms that have no context yet; ``load`` returns a lazy stub.
    """

    def __init__(self, storage):
        self.storage = storage

    def save(self, atom: Atom) -> AtomPointer:
        """Persist ``atom`` (children first) and return its pointer."""
        if atom.context is None:
            atom._bind(self)
        return atom._save()

    def load(self, ptr: AtomPointer, type_name: str) -> Atom:
        """Return a lazy stub of the atom stored at ``ptr``."""
        return Atom._stub(self, ptr, type_name)
