"""The semantic model (decisions D63 to D72; ``docs/source/format-spec.rst``, "Semantic catalog (format 6)").

- :mod:`proto_db.semantic.expressions` holds the expression language of the catalog: its checker,
  its evaluator, its translation to the query planner and the Python text form;
- :mod:`proto_db.semantic.descriptors` holds the declarations: ``Field``, ``Enum``, ``Computed``,
  ``Aggregate``, ``Relation``, ``RelationTo``, ``Role``, ``Invariant``, ``intent`` and ``Certified``;
- :mod:`proto_db.semantic.classes` maps them onto the rc4 registry: semantic ``DBObject`` classes,
  ``Extension``, ``Relationship`` and the intent invoker (``intent_context``);
- :mod:`proto_db.semantic.compose` composes the catalog from the modules' contributions;
- :mod:`proto_db.semantic.store` is the catalog in the database: written at install, verified on use;
- :mod:`proto_db.semantic.session` is the semantic session (``semantic(transaction or snapshot)``);
- :mod:`proto_db.semantic.entities` holds its entities and relations (cells, links, relationship atoms,
  maintained keys and aggregates, the versions and members it gives) and its query engine.

The names below are the user-facing API, in the brief's spellings.
"""
from .classes import Extension, IntentContext, Relationship, intent_context
from .descriptors import Aggregate, Certified, Computed, Enum, Field, Invariant, Relation, RelationTo, Role, intent
from .entities import Members, Version
from .session import SemanticSession, semantic

__all__ = ["semantic", "SemanticSession", "Version", "Members", "Field", "Enum", "Computed", "Aggregate", "Relation",
           "RelationTo", "Role", "Relationship", "Extension", "Invariant", "intent", "intent_context", "IntentContext", "Certified"]
