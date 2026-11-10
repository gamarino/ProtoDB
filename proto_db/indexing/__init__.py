"""ProtoDB 2.0 index framework (spec section 8.3).

``proto_db.indexing`` is the permanent name of this package (ruling R16): the
spec's ``proto_db.indexes`` collided with the 0.1 module ``proto_db/indexes.py`` (removed in Task 8.5).
Index classes persist under their own stable ``__type_name__``
(``proto_db.indexing.Index`` for the abstract base, D29).

``KeySpec`` and ``register_extractor`` live in :mod:`.keyspec`; ``Index`` and
``element_identity`` in :mod:`.base`; ``EqualityIndex`` in :mod:`.equality` and
``RangeIndex`` in :mod:`.range` (D30); ``VectorIndex`` and ``near_oracle`` in
:mod:`.vector`, its backends in :mod:`.vector_backends` (D39).  All but the first two are resolved lazily (PEP 562)
because :mod:`proto_db.query.terms` depends on :mod:`.keyspec` while
:mod:`.base` depends on the terms: importing this package must not import
:mod:`.base` eagerly, or importing the terms first would form a cycle.
"""
from .keyspec import (ELEMENT, EXTRACTOR, PATH, KeySpec, extractor_generation, extractor_version,
                      register_extractor, unregister_extractor)

__all__ = ["KeySpec", "register_extractor", "unregister_extractor", "extractor_generation",
           "extractor_version", "PATH", "ELEMENT",
           "EXTRACTOR", "Index", "element_identity", "EqualityIndex", "RangeIndex", "VectorIndex", "near_oracle"]

#: Lazily resolved names and their submodules.
_LAZY = {"Index": "base", "element_identity": "base", "EqualityIndex": "equality", "RangeIndex": "range",
         "VectorIndex": "vector", "near_oracle": "vector"}


def __getattr__(name):
    if name in _LAZY:
        from importlib import import_module
        return getattr(import_module(f".{_LAZY[name]}", __name__), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
