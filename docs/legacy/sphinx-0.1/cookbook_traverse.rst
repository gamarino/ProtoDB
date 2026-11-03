Hierarchical and Graph Traversal with .traverse()
=================================================

ProtoBase exposes a native way to walk hierarchies and simple graphs directly from the LINQ-like API via the ``.traverse()`` operator, powered by the ``RecursivePlan`` in the query engine. This removes the need for manual Python loops when navigating relations such as manager chains, folder trees, or peer links, and keeps the traversal in the query pipeline so you can chain additional filters or projections.

When to use it
--------------
- Navigate "up" a reference chain (each node points to a single parent-like reference).
- Navigate "down" a collection attribute (each node has an iterable of children-like references).
- Combine with ``where()``, ``select()``, and other operators before/after traversal.
- Avoid infinite loops in the presence of cycles: traversal uses a canonical identity set to visit each node at most once.

Basic model example
-------------------

Suppose we model employees as DBObjects with a ``manager`` reference and an optional list of ``reports``:

.. code-block:: python

   from proto_db.common import DBObject
   from proto_db.lists import List

   ceo = DBObject(transaction=tr, name="CEO")
   vp1 = DBObject(transaction=tr, name="VP1")._setattr('manager', ceo)
   vp2 = DBObject(transaction=tr, name="VP2")._setattr('manager', ceo)
   dir1 = DBObject(transaction=tr, name="Director1")._setattr('manager', vp1)
   mgr1 = DBObject(transaction=tr, name="Manager1")._setattr('manager', dir1)
   emp1 = DBObject(transaction=tr, name="Employee1")._setattr('manager', mgr1)

   ceo = ceo._setattr('reports', List(transaction=tr).append_last(vp1).append_last(vp2))
   vp1 = vp1._setattr('reports', List(transaction=tr).append_last(dir1))

Traversing upward (manager chain)
---------------------------------

.. code-block:: python

   from proto_db.linq import from_collection, F

   q = (from_collection(employees_set)
        .where(F.name == 'Employee1')
        .traverse('manager', direction='up', strategy='dfs'))

   names = [e.name for e in q]  # ['Manager1', 'Director1', 'VP1', 'CEO']

Traversing downward (reports tree, breadth-first)
-------------------------------------------------

.. code-block:: python

   q = (from_collection(employees_set)
        .where(F.name == 'CEO')
        .traverse('reports', direction='down', strategy='bfs'))

   names = [e.name for e in q]  # includes 'VP1', 'VP2', then 'Director1'

Limiting depth and including the start node
-------------------------------------------

.. code-block:: python

   # Limit traversal to two hops
   short_chain = (from_collection(employees_set)
                  .where(F.name == 'Employee1')
                  .traverse('manager', max_depth=2))
   assert [e.name for e in short_chain] == ['Manager1', 'Director1']

   # Include the starting node in the output
   with_start = (from_collection(employees_set)
                 .where(F.name == 'Manager1')
                 .traverse('manager', include_start_node=True))
   # Produces: Manager1, Director1, VP1, CEO

Cycle safety
------------

If a cycle exists (e.g., ``nodeA.peer -> nodeB`` and ``nodeB.peer -> nodeA``), traversal will visit each node at most once using a canonical identity hash under the hood and will terminate naturally.

Parameters
----------

``traverse(relation_attr, direction='up', max_depth=-1, strategy='dfs', include_start_node=False)``

- relation_attr (str): attribute name holding the relation to follow (e.g., ``'manager'`` or ``'reports'``).
- direction (str):
  - ``'up'``: follow a single reference attribute.
  - ``'down'``: iterate an attribute assumed to be a collection of references.
- max_depth (int): maximum expansion depth; ``-1`` means unlimited.
- strategy (str): ``'dfs'`` for depth-first or ``'bfs'`` for breadth-first search.
- include_start_node (bool): whether to emit the starting node(s) before traversing.

Chaining after traversal
------------------------

The traversal is part of the lazy query pipeline, so you can continue chaining operators. For example:

.. code-block:: python

   q = (from_collection(employees_set)
        .where(F.name == 'Employee1')
        .traverse('manager')
        .where(F.name.contains('VP')))
   assert [e.name for e in q] == ['VP1']

Implementation notes
--------------------

- The LINQ operator builds a ``RecursivePlan`` when the source is a ProtoBase plan/collection, enabling planner integration.
- Cycle prevention relies on ``proto_db.common.canonical_hash`` of each node.
- ``include_start_node`` seeds the visited set with start nodes so they are not re-emitted later.
