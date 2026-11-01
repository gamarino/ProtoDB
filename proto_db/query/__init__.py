"""ProtoDB 2.0 query layer (spec section 10): expressions, physical plans, planner and front ends.

:mod:`.terms` holds the term model and scan oracle (D28), :mod:`.expressions`
the boolean expression layer and the 0.1 prefix syntax compiler,
:mod:`.plans` the physical plan nodes (D36), :mod:`.planner` the planner (D37)
and :mod:`.linq` the LINQ front end (D38).  This package imports only
:mod:`.terms` and :mod:`.expressions` eagerly (so ``Term`` is registered as an
``Expression`` whenever terms are used); :mod:`.plans` is imported by the
index framework, which it must not import back at module level.
"""
from .expressions import And, Expression, Not, Or, Term, evaluate

__all__ = ["Term", "Expression", "And", "Or", "Not", "evaluate"]
