#!/usr/bin/env python3
"""9. Vector search

Generated from docs/tutorial/09-vector-search.md by ``python tests/docs/test_markdown_tutorials.py --write-scripts``:
the code below is the chapter's code blocks, verbatim and in order. After the blocks of each output the chapter
shows, ``_expect`` compares what they printed with it, so the script checks every figure the chapter shows,
exits with status 1 on the first difference, and prints "09_vector_search: OK" at the end. Edit the chapter, not this file.

Run it from anywhere: python examples/tutorial/09_vector_search.py
"""
import os as _os
import sys as _sys

_HERE = _os.path.dirname(_os.path.abspath(__file__))
_REPO = _os.path.normpath(_os.path.join(_HERE, *[_os.pardir] * 2))
_sys.path[:0] = [_REPO, _os.path.join(_REPO, "examples", "tutorial")]


class _Tee:
    """Standard output that also keeps what was printed since the last check."""

    def __init__(self, stream):
        self.stream, self.kept = stream, []

    def write(self, text):
        self.kept.append(text)
        return self.stream.write(text)

    def flush(self):
        self.stream.flush()


_sys.stdout = _Tee(_sys.stdout)


def _expect(shown):
    """Exit with status 1 unless the code since the last check printed ``shown`` (the chapter's Output)."""
    printed = "".join(_sys.stdout.kept)
    _sys.stdout.kept.clear()
    def normal(text):
        return "\n".join(line.rstrip() for line in text.strip().splitlines())
    if normal(printed) != normal(shown):
        _sys.stderr.write(f"--- the chapter shows ---\n{normal(shown)}\n--- the code printed ---\n{normal(printed)}\n")
        _sys.exit(1)


def _sample():
    """The chapter shows what the code since the last check printed as a sample of one run: not compared."""
    _sys.stdout.kept.clear()


# -- 09-vector-search.md, line 15 ----------------------------------------
import math
import random

from proto_db import F, MemoryStorage, ObjectSpace, VectorIndex, from_collection

DIM = 8
rng = random.Random(11)
TOPICS = {name: [rng.uniform(-1, 1) for _ in range(DIM)] for name in ("licences", "hardware", "services")}

def embedding(topic):
    return tuple(x + rng.gauss(0, 0.35) for x in TOPICS[topic])

def cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    return dot / (math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b)))

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
rows = [(n, list(TOPICS)[n % 3], ("lead", "qualified", "proposal", "won")[n % 4]) for n in range(600)]
with db.new_transaction() as tr:
    deals = tr.new_dictionary().add_index(VectorIndex("by_meaning", "vec", DIM, metric="cosine"))
    for number, topic, stage in rows:
        deals = deals.set_at(number, tr.new_object(number=number, topic=topic, stage=stage, vec=embedding(topic)))
    tr.set_root_object("opportunities", deals)

tr = db.new_transaction()
deals = tr.get_root_object("opportunities")
print(len(deals), deals.index_names(), deals.get_index("by_meaning").explain()["kind"])
tr.abort()
_expect("""
600 ['by_meaning'] VectorIndex
""")

# -- 09-vector-search.md, line 59 ----------------------------------------
def plan_of(query):
    names, node = [], query.explain()
    while node:
        names.append(node["plan"])
        node = node["children"][0] if node.get("children") else None
    return " <- ".join(names)

tr = db.new_transaction()
deals = tr.get_root_object("opportunities")
target = deals.get_at(42).vec
like_42 = from_collection(deals).where(F.vec.near(target, k=5))
found = [d.number for d in like_42.to_list()]
brute = [n for n, _ in sorted(((n, cosine(target, deals.get_at(n).vec)) for n in range(600)), key=lambda p: -p[1])][:5]
print(plan_of(like_42), "|", found, "| same as brute force:", found == brute)
print("topics:", {deals.get_at(n).topic for n in found})
_expect("""
VectorSearch <- VectorExact | [42, 378, 384, 573, 348] | same as brute force: True
topics: {'licences'}
""")

# -- 09-vector-search.md, line 93 ----------------------------------------
won_like_42 = from_collection(deals).where(F.vec.near(target, k=3) & (F.stage == "won"))
got = [d.number for d in won_like_42.to_list()]
expected = [n for n in sorted(range(600), key=lambda n: -cosine(target, deals.get_at(n).vec))
            if deals.get_at(n).stage == "won"][:3]
print(got, [deals.get_at(n).stage for n in got], got == expected)
close = from_collection(deals).where(F.vec.near(target, threshold=0.9)).to_list()
print(len(close), "with cosine similarity >= 0.9:",
      {d.number for d in close} == {n for n in range(600) if cosine(target, deals.get_at(n).vec) >= 0.9})
tr.abort()
space.close()
_expect("""
[87, 327, 183] ['won', 'won', 'won'] True
2 with cosine similarity >= 0.9: True
""")

print("09_vector_search: OK")
