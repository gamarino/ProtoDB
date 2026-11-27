#!/usr/bin/env python3
"""4. Indexes and queries

Generated from docs/tutorial/04-indexes-and-queries.md by ``python tests/docs/test_markdown_tutorials.py --write-scripts``:
the code below is the chapter's code blocks, verbatim and in order. After the blocks of each output the chapter
shows, ``_expect`` compares what they printed with it, so the script checks every figure the chapter shows,
exits with status 1 on the first difference, and prints "04_indexes_and_queries: OK" at the end. Edit the chapter, not this file.

Run it from anywhere: python examples/tutorial/04_indexes_and_queries.py
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


# -- 04-indexes-and-queries.md, line 14 ----------------------------------------
from proto_db import F, MemoryStorage, ObjectSpace, from_collection

import erp_app

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
erp_app.seed(db, indexes=False)

def plan_of(query):
    """The node names of a query's plan, from the root down."""
    names, node = [], query.explain()
    while node:
        names.append(node["plan"])
        node = node["children"][0] if node.get("children") else None
    return " <- ".join(names)

tr = db.new_transaction()
deals = tr.get_root_object("opportunities")
proposals = from_collection(deals).where(F.stage == "proposal")
print(plan_of(proposals), "->", proposals.count(), "of", len(deals))
tr.abort()
_expect("""
Filter <- CollectionScan -> 333 of 1000
""")

# -- 04-indexes-and-queries.md, line 52 ----------------------------------------
from proto_db import EqualityIndex, RangeIndex

with db.new_transaction() as tr:
    deals = (tr.get_root_object("opportunities")
             .add_index(EqualityIndex("by_stage", "stage"))
             .add_index(RangeIndex("by_amount", "amount"))
             .add_index(EqualityIndex("by_owner", "owner")))
    tr.set_root_object("opportunities", deals)

tr = db.new_transaction()
deals = tr.get_root_object("opportunities")
print(deals.index_names())
for label, query in [
    ("stage == proposal", from_collection(deals).where(F.stage == "proposal")),
    ("amount >= 9000", from_collection(deals).where(F.amount >= 9000)),
    ("owned by Ann", from_collection(deals).where(F.owner == erp_app.user("ann"))),
    ("proposal and amount >= 9000", from_collection(deals).where((F.stage == "proposal") & (F.amount >= 9000))),
]:
    print(f"{label:28} {plan_of(query):24} {query.count()} rows")
tr.abort()
_expect("""
['by_amount', 'by_owner', 'by_stage']
stage == proposal            IndexLookup              333 rows
amount >= 9000               IndexRange               108 rows
owned by Ann                 IndexLookup              334 rows
proposal and amount >= 9000  Filter <- IndexRange     36 rows
""")

# -- 04-indexes-and-queries.md, line 100 ----------------------------------------
from proto_db import Agg

tr = db.new_transaction()
deals = tr.get_root_object("opportunities")
top = (from_collection(deals).where(F.stage == "proposal")
       .order_by(F.amount, descending=True).then_by(F.number).take(3)
       .select({"number": F.number, "amount": F.amount}))
print(top.to_list())
per_owner = (from_collection(deals)
             .group_by(F.owner, {"owner": Agg.key(), "n": Agg.count(), "largest": Agg.max(F.amount)})
             .to_list())
print(sorted((row["owner"].get(tr).name, row["n"], row["largest"]) for row in per_owner))
print(erp_app.pipeline(deals))
tr.abort()
_expect("""
[{'number': 242, 'amount': 9954}, {'number': 485, 'amount': 9945}, {'number': 728, 'amount': 9936}]
[('Ann Lee', 334, 9991), ('Bob Ruiz', 333, 9917), ('Cy Park', 333, 9954)]
[('lead', 334, 1808821), ('proposal', 333, 1795500), ('qualified', 333, 1783179)]
""")

# -- 04-indexes-and-queries.md, line 133 ----------------------------------------
tr = db.new_transaction()
plan = from_collection(tr.get_root_object("opportunities")).where(F.amount.between(9000, 9100)).explain()
print(plan["plan"], plan["index"]["index"], plan["index"]["kind"], "| estimated rows:", plan["estimate"]["rows"])
tr.abort()
_expect("""
IndexRange by_amount RangeIndex | estimated rows: 11.0
""")

# -- 04-indexes-and-queries.md, line 156 ----------------------------------------
with db.new_transaction() as tr:
    pager = tr.pager("opportunities", where=[["stage", "==", "proposal"]], order_by=[("amount", True), "number"],
                     page_size=100)
    print(pager.rows, "rows in", pager.pages, "pages; the first starts with", pager.page(0)[0].number)
space.close()
_expect("""
333 rows in 4 pages; the first starts with 242
""")

print("04_indexes_and_queries: OK")
