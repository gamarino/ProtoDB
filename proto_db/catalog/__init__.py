"""The semantic catalog (format 6, feature ``catalog``; ``docs/source/format-spec.rst``, "Semantic catalog
(format 6)").

:mod:`proto_db.catalog.atoms` holds the catalog's built-in atom types, the records every implementation
reads and writes; :mod:`proto_db.catalog.model` converts between those records and the neutral model, plain
data whose items use the field names of the format, and gives a catalog's canonical form (ruling R258).
"""
