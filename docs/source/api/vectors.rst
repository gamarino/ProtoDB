Vector Support
==============

.. module:: proto_db.vectors

ProtoBase provides a lightweight Vector type and an exact in-memory vector index suitable as a fallback for similarity search.

Vector
------

.. autoclass:: proto_db.vectors.Vector
   :members:
   :special-members: __init__

Utility Functions
-----------------

- ``cosine_similarity(a, b)``: compute cosine similarity between iterables of floats.
- ``l2_distance(a, b)``: compute Euclidean distance between iterables of floats.

Vector Indexes
--------------

.. module:: proto_db.vector_index

.. autoclass:: proto_db.vector_index.VectorIndex
   :members:

.. autoclass:: proto_db.vector_index.ExactVectorIndex
   :members:

Operators
---------

The query system recognizes a similarity-threshold operator for linear evaluation:

- ``near[]``: match records whose vector field is near a query vector above a threshold for cosine (or within a distance threshold for l2).

Example:

.. code-block:: python

    # Filter records with cosine similarity >= 0.8 to query vector
    expr = Expression.compile(['emb', 'near[]', [1.0, 0.0, 0.0], 0.8])
    plan = WherePlan(filter=expr, based_on=base_plan, transaction=tr)
    out = list(plan.execute())

Notes
-----

- This phase provides a correctness baseline (linear evaluation). Future phases can attach ANN indexes (e.g., HNSW) to collections and enable pushdown.
