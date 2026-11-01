"""ProtoDB 2.0 index framework (spec section 8.3).

``proto_db.indexing`` is the permanent name of this package (ruling R16): the
spec's ``proto_db.indexes`` collides with the 0.1 module ``proto_db/indexes.py``.
Index classes persist under their own stable ``__type_name__``
(``proto_db.indexing.Index`` for the abstract base, D29).

``KeySpec`` and ``register_extractor`` live in :mod:`.keyspec`; ``Index`` and
``element_identity`` in :mod:`.base`.  The latter are resolved lazily (PEP 562)
because :mod:`proto_db.query.terms` depends on :mod:`.keyspec` while
:mod:`.base` depends on the terms: importing this package must not import
:mod:`.base` eagerly, or importing the terms first would form a cycle.
"""
from .keyspec import ELEMENT, EXTRACTOR, PATH, KeySpec, register_extractor

__all__ = ["KeySpec", "register_extractor", "PATH", "ELEMENT", "EXTRACTOR", "Index", "element_identity"]

_LAZY = {"Index", "element_identity"}


def __getattr__(name):
    if name in _LAZY:
        from . import base
        return getattr(base, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
