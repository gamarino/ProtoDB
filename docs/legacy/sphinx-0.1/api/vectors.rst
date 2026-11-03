Vector Support
==============

.. module:: proto_db.vectors

ProtoBase provides a lightweight Vector type and vector indexes (exact and optional ANN) for similarity search that integrate with the cooperative query optimizer.

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

.. autoclass:: proto_db.vector_index.HNSWVectorIndex
   :members:

Notes on backends
-----------------
- HNSWVectorIndex requires the optional 'hnswlib' (and numpy) dependency. If it is not available, it transparently falls back to the exact implementation.
- HNSW supports parameters M, efConstruction and efSearch. Use save(path_prefix)/load(path_prefix) to persist and reload the index and its metadata (id mapping, metric, params).

Query Integration (ANN pushdown)
--------------------------------

Vector indexes implement the ``QueryableIndex`` interface so they can participate in planning. When you add a vector index to a collection and express a ``Near`` term in a WherePlan, the optimizer builds a specialized ``VectorSearchPlan`` that executes a top-k or threshold search in the index and returns results as a collection.

Typical setup using ``IndexDefinition`` and ``HNSWVectorIndex``:

.. code-block:: python

    from proto_db.indexes import IndexDefinition
    from proto_db.vector_index import HNSWVectorIndex
    from proto_db.queries import WherePlan, Expression

    # Suppose each record has an embedding at key 'emb'
    idx_def = IndexDefinition(
        name='emb',
        extractor=lambda rec: rec['emb'],  # produce the vector per record
        index_class=HNSWVectorIndex,
        index_params={'metric': 'cosine', 'M': 16, 'efConstruction': 200, 'efSearch': 64},
    )

    # Build the index on a List/Set
    people = people.add_index(idx_def)

    # Express a vector similarity predicate using the Near operator via Expression.compile
    # Cosine threshold 0.8; optionally you may include k as a third value
    expr = Expression.compile(['emb', 'near[]', [0.1, 0.2, 0.3], 0.8])
    plan = WherePlan(filter=expr, based_on=people.as_query_plan(), transaction=tr)

    # Optimize: the planner detects the vector index and returns a VectorSearchPlan (or an AndMerge with residuals)
    opt = plan.optimize()
    print(opt.explain())

    # Execute and page efficiently (execute returns a DBCollections, e.g., List)
    coll = opt.execute()
    page1 = coll.slice(0, 10)  # O(1) slice for pagination
    for rec in page1.as_iterable():
        ...

Operators
---------

The query system provides a similarity operator for use in expressions:

- ``near[]``: match records whose vector field is near a query vector above a threshold for cosine (or within a distance threshold for l2). It can be used by vector indexes to create a ``VectorSearchPlan``.

Example (linear fallback when no index is present):

.. code-block:: python

    # Filter records with cosine similarity >= 0.8 to query vector
    expr = Expression.compile(['emb', 'near[]', [1.0, 0.0, 0.0], 0.8])
    plan = WherePlan(filter=expr, based_on=base_plan, transaction=tr)
    out = list(plan.execute().as_iterable())

Notes
-----

- With a configured vector index, ``WherePlan.optimize()`` pushes down ``Near`` terms to a specialized ``VectorSearchPlan``.
- Results are first-class collections, so paginating with ``slice()`` is O(1) and requires no re-execution.
- If no index is present, evaluation falls back to a linear scan for correctness.
