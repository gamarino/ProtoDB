#!/usr/bin/env python3
"""Vector similarity search: a VectorIndex on a collection, k-nearest queries combined with filters.

Run from the repository root (needs numpy: ``pip install "proto_db[vectors]"``)::

    python examples/vector_search.py

Documents carry a small embedding.  An exact index and a persistent IVF-Flat index
answer ``near`` queries; the script checks the results against a brute-force search
and exits with an error if they differ.
"""
import math
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proto_db import F, MemoryStorage, ObjectSpace, VectorIndex, from_collection  # noqa: E402

DIM = 8
TOPICS = ("sports", "music", "science")


def cosine(a, b) -> float:
    """Cosine similarity of two vectors."""
    dot = sum(x * y for x, y in zip(a, b))
    return dot / (math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b)))


def main() -> None:
    """Index 300 random embeddings and compare indexed k-NN answers with a brute-force search."""
    rng = random.Random(7)
    docs = [(i, TOPICS[i % 3], tuple(rng.uniform(-1, 1) for _ in range(DIM))) for i in range(300)]
    space = ObjectSpace(MemoryStorage())
    try:
        db = space.new_database("docs")
        with db.new_transaction() as tr:
            coll = tr.new_list() \
                .add_index(VectorIndex("by_vec", "vec", DIM, metric="cosine")) \
                .add_index(VectorIndex("by_vec_ivf", "vec", DIM, metric="cosine", backend="ivf_flat",
                                       nlist=8, nprobe=8))
            for i, topic, vec in docs:
                coll = coll.append_last(tr.new_object(i=i, topic=topic, vec=vec))
            tr.set_root_object("docs", coll)

        tr = db.new_transaction()
        coll = tr.get_root_object("docs")
        query = docs[42][2]

        nearest = from_collection(coll).where(F.vec.near(query, k=5))
        print("plan:", nearest.explain()["plan"])
        got = [d.i for d in nearest.to_list()]
        expected = [i for i, _, _ in sorted(docs, key=lambda d: -cosine(query, d[2]))[:5]]
        print("5 nearest to doc 42:", got)
        assert got == expected, (got, expected)

        # k nearest among the documents of one topic: the filter applies before choosing the k.
        music = from_collection(coll).where(F.vec.near(query, k=3) & (F.topic == "music")).to_list()
        expected_music = [i for i, topic, vec in sorted(docs, key=lambda d: -cosine(query, d[2]))
                          if topic == "music"][:3]
        print("3 nearest music docs:", [d.i for d in music])
        assert [d.i for d in music] == expected_music

        # Everything within a similarity threshold.
        close = from_collection(coll).where(F.vec.near(query, threshold=0.6)).to_list()
        expected_close = {i for i, _, vec in docs if cosine(query, vec) >= 0.6}
        print(f"{len(close)} docs with cosine similarity >= 0.6")
        assert {d.i for d in close} == expected_close
        tr.abort()
    finally:
        space.close()
    print("vector_search: OK")


if __name__ == "__main__":
    main()
