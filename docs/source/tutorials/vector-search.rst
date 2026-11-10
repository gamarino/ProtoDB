Vector search
=============

A vector index finds the elements whose embedding is nearest to a query vector. It is the third built-in
index kind and works like the other two: it belongs to a collection, every mutator keeps it current, and
it takes part in the same planner, so ``near`` can be combined with ordinary conditions. This tutorial needs
numpy (``pip install "proto_db[vectors]"``). Every code block runs in the test suite.

Documents with embeddings
-------------------------

The embeddings here are random vectors from a seeded generator, so the page is deterministic. In an
application they come from an embedding model; ProtoDB stores and indexes the vectors you give it and
never computes them.

.. code-block:: python

   import math
   import random

   from proto_db import F, MemoryStorage, ObjectSpace, VectorIndex, from_collection

   DIM = 8
   TOPICS = ("sports", "music", "science")
   rng = random.Random(7)
   docs = [(i, TOPICS[i % 3], tuple(rng.uniform(-1, 1) for _ in range(DIM))) for i in range(600)]

   def cosine(a, b):
       dot = sum(x * y for x, y in zip(a, b))
       return dot / (math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b)))

   space = ObjectSpace(MemoryStorage())
   db = space.new_database("docs")
   with db.new_transaction() as tr:
       index = VectorIndex("by_vec", "vec", DIM, metric="cosine")        # backend "exact" by default
       collection = tr.new_list().add_index(index)
       for i, topic, vec in docs:
           collection = collection.append_last(tr.new_object(i=i, topic=topic, vec=vec))
       tr.set_root_object("docs", collection)

   tr = db.new_transaction()
   docs_now = tr.get_root_object("docs")
   print(len(docs_now), docs_now.index_names())
   tr.abort()

.. code-block:: text
   :caption: Output

   600 ['by_vec']

``VectorIndex(name, path, dim, metric, backend)`` fixes the dimension, the metric (``"cosine"``, ``"l2"`` or
``"ip"``, the inner product) and the backend. A document whose ``vec`` is missing or is not ``dim``
finite numbers is kept in the collection but is never returned by a search.

Nearest neighbours
------------------

.. code-block:: python

   def plan_of(query):
       names, node = [], query.explain()
       while node:
           names.append(node["plan"])
           node = node["children"][0] if node.get("children") else None
       return " <- ".join(names)

   tr = db.new_transaction()
   coll = tr.get_root_object("docs")
   target = docs[42][2]
   nearest = from_collection(coll).where(F.vec.near(target, k=5))
   print(plan_of(nearest))
   print("5 nearest to document 42:", [d.i for d in nearest.to_list()])

   ranked = sorted(docs, key=lambda d: -cosine(target, d[2]))      # brute force, for the check
   print("same as brute force:", [d.i for d in nearest.to_list()] == [i for i, _, _ in ranked[:5]])
   tr.abort()

.. code-block:: text
   :caption: Output

   VectorSearch <- VectorExact
   5 nearest to document 42: [42, 370, 439, 309, 94]
   same as brute force: True

The ``exact`` backend ranks every vector, in bulk with numpy, then re-scores every candidate that could
matter with exact arithmetic, so its answer equals brute force bit for bit. Results come nearest first.
Document 42 is the first neighbour of itself.

Combining ``near`` with conditions
----------------------------------

"The 3 nearest *music* documents" is not "the 3 nearest documents, then keep the music ones": the second
can return fewer than 3. ProtoDB hands the other conditions to the index, which finds the k nearest among
the documents that satisfy them:

.. code-block:: python

   tr = db.new_transaction()
   coll = tr.get_root_object("docs")
   music = from_collection(coll).where(F.vec.near(target, k=3) & (F.topic == "music"))
   got = [d.i for d in music.to_list()]
   expected = [i for i, topic, _ in ranked if topic == "music"][:3]
   print("3 nearest music documents:", got, got == expected)

   close = from_collection(coll).where(F.vec.near(target, threshold=0.6)).to_list()
   print(len(close), "documents with cosine similarity >= 0.6:",
         {d.i for d in close} == {i for i, _, v in docs if cosine(target, v) >= 0.6})
   tr.abort()

.. code-block:: text
   :caption: Output

   3 nearest music documents: [370, 439, 94] True
   26 documents with cosine similarity >= 0.6: True

Give ``k``, a ``threshold``, or both ("the k nearest among those at least this similar").

The IVF-Flat backend
--------------------

Exact search reads every vector, so it grows linearly with the collection. ``ivf_flat`` clusters the
vectors (deterministic k-means, stored in the index) and searches only the ``nprobe`` nearest clusters of
``nlist``. It trades recall for speed, and with ``nprobe == nlist`` it is exact.

.. code-block:: python

   def recall(nprobe, queries=40):
       with db.new_transaction() as tr:
           ivf = tr.new_list().add_index(VectorIndex("ivf", "vec", DIM, metric="cosine",
                                                     backend="ivf_flat", nlist=16, nprobe=nprobe))
           for i, topic, vec in docs:
               ivf = ivf.append_last(tr.new_object(i=i, topic=topic, vec=vec))
           tr.set_root_object("ivf", ivf, replace=True)
       tr = db.new_transaction()
       coll, hits = tr.get_root_object("ivf"), 0
       for q in range(queries):
           vec = docs[q * 13][2]
           truth = {i for i, _, _ in sorted(docs, key=lambda d: -cosine(vec, d[2]))[:10]}
           found = {d.i for d in from_collection(coll).where(F.vec.near(vec, k=10)).to_list()}
           hits += len(truth & found)
       tr.abort()
       return hits / (queries * 10)

   results = {nprobe: recall(nprobe) for nprobe in (1, 4, 16)}
   for nprobe, value in results.items():
       print(f"nlist=16 nprobe={nprobe:2}: recall@10 = {value:.2f}")
   assert results[16] == 1.0 and results[1] < results[16]      # nprobe == nlist is exact

.. code-block:: text
   :caption: Sample output (the partial-probe recalls differ from run to run)

   nlist=16 nprobe= 1: recall@10 = 0.62
   nlist=16 nprobe= 4: recall@10 = 0.88
   nlist=16 nprobe=16: recall@10 = 1.00

The figures above are measured by the block (the block asserts only that ``nprobe == nlist`` is exact; the
others differ from run to run, as the training of the clusters is not reproduced identically between
processes), on random vectors, which have no cluster structure for the
index to exploit. Treat them as an illustration of the trade, not as a benchmark. Pick ``nlist`` and ``nprobe`` by measuring recall on your own data, as this block
does. The vector benchmark results (latency and recall by backend and size) are in :doc:`../performance`.

.. code-block:: python

   space.close()

Limits and choices
------------------

* ``exact`` is the default and the right choice up to the size where its scan time is acceptable for your
  latency budget; it needs no training and is never approximate.
* ``hnsw`` is optional (``pip install "proto_db[hnsw]"``). ``hnswlib`` re-enables the GIL on free-threaded
  CPython when it is imported, so it costs the thread scalability the rest of ProtoDB has, and its graph is
  rebuilt from the stored vectors in each process, not stored. It is the better choice for very large
  collections at low latency, when you accept those costs.
* A dedicated vector database, or a library such as FAISS, is better than ProtoDB when vector search is the
  whole workload: they typically offer more index types, quantisation and GPU execution. ProtoDB's advantage is that
  the vectors live in the same transactions, versions and queries as the records they describe, so a
  document and its embedding commit together and a past version answers with its past embeddings.
* A version of the index is built by path copying like any other, so changing one vector does not rewrite
  the others.
