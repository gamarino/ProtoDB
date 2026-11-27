#!/usr/bin/env python3
"""5. Range, vector and custom indexes

Generated from docs/tutorial/queries/05-range-vector-and-custom-indexes.md by ``python tests/docs/test_markdown_tutorials.py --write-scripts``:
the code below is the chapter's code blocks, verbatim and in order. After the blocks of each output the chapter
shows, ``_expect`` compares what they printed with it, so the script checks every figure the chapter shows,
exits with status 1 on the first difference, and prints "05_range_vector_and_custom_indexes: OK" at the end. Edit the chapter, not this file.

Run it from anywhere: python examples/tutorial/queries/05_range_vector_and_custom_indexes.py
"""
import os as _os
import sys as _sys

_HERE = _os.path.dirname(_os.path.abspath(__file__))
_REPO = _os.path.normpath(_os.path.join(_HERE, *[_os.pardir] * 3))
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


# -- 05-range-vector-and-custom-indexes.md, line 7 ----------------------------------------
from proto_db import F, MemoryStorage, ObjectSpace, RangeIndex, from_collection

import erp_app
from erp_app import plan_of

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
erp_app.seed(db)
tr = db.new_transaction()
deals = tr.get_root_object("opportunities")

# -- 05-range-vector-and-custom-indexes.md, line 25 ----------------------------------------
amounts = lambda query: sorted(d.amount for d in query.to_list())
for label, condition in [
    ("between(1000, 1001)", F.amount.between(1000, 1001)),
    ("between_open", F.amount.between_open(1000, 1001)),
    ("between_left_open", F.amount.between_left_open(1000, 1001)),
    ("between_right_open", F.amount.between_right_open(1000, 1001)),
    ('range(1000, 1001, "[)")', F.amount.range(1000, 1001, "[)")),
    ("amount < 1001", F.amount < 1001),
]:
    query = from_collection(deals).where(condition)
    print(f"{label:24} {plan_of(query):12} {amounts(query)}")
_expect("""
between(1000, 1001)      IndexRange   [1000, 1001]
between_open             IndexRange   []
between_left_open        IndexRange   [1001]
between_right_open       IndexRange   [1000]
range(1000, 1001, "[)")  IndexRange   [1000]
amount < 1001            IndexRange   [1000]
""")

# -- 05-range-vector-and-custom-indexes.md, line 58 ----------------------------------------
with db.new_transaction() as wtr:
    wtr.set_root_object("opportunities", wtr.get_root_object("opportunities").add_index(RangeIndex("by_title", "title")))
tr.abort()
tr = db.new_transaction()
deals = tr.get_root_object("opportunities")

prefix = "opportunity 99"
as_filter = from_collection(deals).where(F.title.startswith(prefix))
as_range = from_collection(deals).where(F.title.range(prefix, prefix[:-1] + chr(ord(prefix[-1]) + 1), "[)"))
print(plan_of(as_filter), as_filter.count(), "|", plan_of(as_range), as_range.count(),
      "| same rows:", {d.number for d in as_filter.to_list()} == {d.number for d in as_range.to_list()})
_expect("""
PythonFilter <- CollectionScan 11 | IndexRange 11 | same rows: True
""")

# -- 05-range-vector-and-custom-indexes.md, line 85 ----------------------------------------
import random

from proto_db import VectorIndex

rng = random.Random(5)
centres = [[rng.uniform(-1, 1) for _ in range(4)] for _ in range(3)]
with db.new_transaction() as wtr:
    notes = wtr.new_dictionary().add_index(VectorIndex("by_meaning", "vec", 4, metric="cosine"))
    for n in range(300):
        vec = tuple(x + rng.gauss(0, 0.3) for x in centres[n % 3])
        notes = notes.set_at(n, wtr.new_object(number=n, topic=n % 3, amount=1000 + (n * 37) % 9000, vec=vec))
    wtr.set_root_object("notes", notes)

notes = db.new_transaction().get_root_object("notes")
probe = notes.get_at(7).vec
for label, condition in [
    ("near, k=4", F.vec.near(probe, k=4)),
    ("near, k=4, amount >= 8000", F.vec.near(probe, k=4) & (F.amount >= 8000)),
    ("near, threshold 0.99", F.vec.near(probe, threshold=0.99)),
]:
    query = from_collection(notes).where(condition)
    found = query.to_list()
    print(f"{label:28} {plan_of(query):30} {[d.number for d in found]} topics {sorted({d.topic for d in found})}")
_expect("""
near, k=4                    VectorSearch <- VectorExact    [7, 130, 22, 67] topics [1]
near, k=4, amount >= 8000    VectorSearch <- VectorExact    [208, 214, 232, 211] topics [1]
near, threshold 0.99         VectorSearch <- VectorExact    [7, 130, 22, 67, 154, 208, 214, 46, 31, 295, 79, 124] topics [1]
""")

# -- 05-range-vector-and-custom-indexes.md, line 130 ----------------------------------------
from proto_db import EqualityIndex, KeySpec, register_extractor

register_extractor("stage_and_band", lambda deal: (deal.stage, deal.amount // 1000), version="1")

with db.new_transaction() as wtr:
    wtr.set_root_object("opportunities", wtr.get_root_object("opportunities")
                        .add_index(EqualityIndex("by_stage_band", KeySpec.extractor("stage_and_band"))))
tr.abort()
tr = db.new_transaction()
deals = tr.get_root_object("opportunities")
both = from_collection(deals).where(F["@stage_and_band"] == ("proposal", 9))
apart = from_collection(deals).where((F.stage == "proposal") & (F.amount.between(9000, 9999)))
print(plan_of(both), both.count(), "|", plan_of(apart), apart.count())
_expect("""
IndexLookup 36 | Filter <- IndexRange 36
""")

# -- 05-range-vector-and-custom-indexes.md, line 184 ----------------------------------------
tr.abort()
space.close()

print("05_range_vector_and_custom_indexes: OK")
