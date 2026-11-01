"""ProtoDB 2.0 index framework (spec section 8.3).

This package is ``proto_db.indexing`` rather than the spec's ``proto_db.indexes``
because the 0.1 module ``proto_db/indexes.py`` must stay until Task 8.5 (ruling
R4) and a package of the same name would shadow it (D29).  Persisted type names
already use the ``proto_db.indexes.`` prefix, so a later rename changes no data.

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
