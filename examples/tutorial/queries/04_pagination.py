#!/usr/bin/env python3
"""4. Pagination

Generated from docs/tutorial/queries/04-pagination.md by ``python tests/docs/test_markdown_tutorials.py --write-scripts``:
the code below is the chapter's code blocks, verbatim and in order. After the blocks of each output the chapter
shows, ``_expect`` compares what they printed with it, so the script checks every figure the chapter shows,
exits with status 1 on the first difference, and prints "04_pagination: OK" at the end. Edit the chapter, not this file.

Run it from anywhere: python examples/tutorial/queries/04_pagination.py
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


# -- 04-pagination.md, line 7 ----------------------------------------
from proto_db import F, MemoryStorage, ObjectSpace, from_collection

import erp_app
from erp_app import plan_of

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
erp_app.seed(db)

# -- 04-pagination.md, line 23 ----------------------------------------
def page(k, size=5):
    with db.new_transaction() as tr:
        query = (from_collection(tr.get_root_object("opportunities"))
                 .order_by(F.amount, descending=True).skip(k * size).take(size))
        return plan_of(query), [(d.number, d.amount) for d in query.to_list()]

for k in (0, 1, 150):
    print(k, *page(k))
_expect("""
0 Limit <- IndexOrderedScan [(243, 9991), (486, 9982), (729, 9973), (972, 9964), (242, 9954)]
1 Limit <- IndexOrderedScan [(485, 9945), (728, 9936), (971, 9927), (241, 9917), (484, 9908)]
150 Limit <- IndexOrderedScan [(542, 3054), (785, 3045), (55, 3035), (298, 3026), (541, 3017)]
""")

# -- 04-pagination.md, line 52 ----------------------------------------
with db.new_transaction() as tr:
    pager = tr.pager("opportunities", where=[["stage", "==", "proposal"], ["amount", ">=", 5000]],
                     order_by=[("amount", True), "number"], page_size=50)
    first = [(d.number, d.amount) for d in pager.page(0)][:3]
    token = pager.token()
    print(pager.rows, "rows,", pager.pages, "pages; first rows:", first, "| token:", type(token).__name__)

erp_app.advance(db, 242, "won", "cy")                 # the best proposal is won meanwhile

resumed = db.resume_pager(token)                      # a later request, or another process
print("page 0 again:", [(d.number, d.amount) for d in resumed.page(0)][:3])
print("last page:", len(resumed.page(resumed.pages - 1)), "rows")
with db.new_transaction() as tr:
    print("a new query now finds", from_collection(tr.get_root_object("opportunities"))
          .where((F.stage == "proposal") & (F.amount >= 5000)).count(), "rows")
_expect("""
180 rows, 4 pages; first rows: [(242, 9954), (485, 9945), (728, 9936)] | token: bytes
page 0 again: [(242, 9954), (485, 9945), (728, 9936)]
last page: 30 rows
a new query now finds 179 rows
""")

# -- 04-pagination.md, line 89 ----------------------------------------
with db.new_transaction() as tr:
    deals = tr.get_root_object("opportunities")
    keys = deals.keys(after=995)
    print("keys from 995:", list(keys))
    print("key at position 10:", deals.key_at_position(10), "| position of key 500:", deals.position_of(500))
_expect("""
keys from 995: [996, 997, 998, 999]
key at position 10: 10 | position of key 500: 500
""")

# -- 04-pagination.md, line 121 ----------------------------------------
space.close()

print("04_pagination: OK")
