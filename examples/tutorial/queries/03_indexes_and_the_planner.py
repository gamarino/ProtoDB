#!/usr/bin/env python3
"""3. Indexes and the planner

Generated from docs/tutorial/queries/03-indexes-and-the-planner.md by ``python tests/docs/test_markdown_tutorials.py --write-scripts``:
the code below is the chapter's code blocks, verbatim and in order. After the blocks of each output the chapter
shows, ``_expect`` compares what they printed with it, so the script checks every figure the chapter shows,
exits with status 1 on the first difference, and prints "03_indexes_and_the_planner: OK" at the end. Edit the chapter, not this file.

Run it from anywhere: python examples/tutorial/queries/03_indexes_and_the_planner.py
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


# -- 03-indexes-and-the-planner.md, line 8 ----------------------------------------
from proto_db import EqualityIndex, F, MemoryStorage, ObjectSpace, RangeIndex, from_collection

import erp_app
from erp_app import plan_of

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
erp_app.seed(db)                                      # by_stage, by_amount and by_owner on the opportunities
tr = db.new_transaction()
deals = tr.get_root_object("opportunities")
print(deals.index_names())
for name in deals.index_names():
    print(deals.get_index(name).explain())
_expect("""
['by_amount', 'by_owner', 'by_stage']
{'index': 'by_amount', 'kind': 'RangeIndex', 'key': 'amount', 'entries': 1000, 'keys': 1000, 'unorderable': 0}
{'index': 'by_owner', 'kind': 'EqualityIndex', 'key': 'owner', 'entries': 1000, 'keys': 3, 'unorderable': 0}
{'index': 'by_stage', 'kind': 'EqualityIndex', 'key': 'stage', 'entries': 1000, 'keys': 3, 'unorderable': 0}
""")

# -- 03-indexes-and-the-planner.md, line 42 ----------------------------------------
with db.new_transaction() as small_tr:
    small = small_tr.new_dictionary().add_index(EqualityIndex("by_stage", "stage"))
    for n in range(3):
        small = small.set_at(n, small_tr.new_object(number=n, stage=erp_app.STAGES[n % 3]))
    small_tr.set_root_object("small", small)

small = tr.database.new_transaction().get_root_object("small")
for label, source in (("3 opportunities", small), ("1000 opportunities", deals)):
    query = from_collection(source).where(F.stage != "lead")
    plan = query.explain()
    print(f"{label:19} {plan_of(query):26} estimated rows {plan['estimate']['rows']:6.1f}, cost {plan['estimate']['cost']:7.1f}")
_expect("""
3 opportunities     Filter <- CollectionScan   estimated rows    1.5, cost     6.0
1000 opportunities  IndexComplement            estimated rows  666.0, cost  1004.0
""")

# -- 03-indexes-and-the-planner.md, line 73 ----------------------------------------
def show(query):
    def walk(node, depth):
        what = {k: v for k, v in node.items() if k not in ("plan", "children", "estimate")}
        if "index" in what:
            what["index"] = what["index"]["index"]
        print("  " * depth + f"{node['plan']}  rows~{node['estimate']['rows']:.0f}  {what}")
        for child in node.get("children", []):
            walk(child, depth + 1)
    walk(query.explain(), 0)

show(from_collection(deals).where((F.stage == "proposal") & (F.amount >= 9000)).order_by(F.amount).take(3))
_expect("""
Limit  rows~3  {'limit': 3, 'offset': 0}
  Sort  rows~54  {'keys': [{'key': 'amount', 'descending': False}]}
    Filter  rows~54  {'predicate': {'path': 'stage', 'op': '==', 'value': 'proposal'}}
      IndexRange  rows~108  {'index': 'by_amount', 'term': {'path': 'amount', 'op': '>=', 'value': 9000}}
""")

# -- 03-indexes-and-the-planner.md, line 106 ----------------------------------------
for label, condition in (("stage == proposal", F.stage == "proposal"), ("amount >= 9000", F.amount >= 9000),
                         ("stage in (lead, won)", F.stage.in_(["lead", "won"])), ("stage != lead", F.stage != "lead"),
                         ("owner is Ann", F.owner == erp_app.user("ann"))):
    query = from_collection(deals).where(condition)
    print(f"{label:22} {plan_of(query):26} estimated {query.explain()['estimate']['rows']:6.1f}, actual {query.count()}")
_expect("""
stage == proposal      IndexLookup                estimated  333.0, actual 333
amount >= 9000         IndexRange                 estimated  108.0, actual 108
stage in (lead, won)   IndexLookup                estimated  334.0, actual 334
stage != lead          IndexComplement            estimated  666.0, actual 666
owner is Ann           IndexLookup                estimated  334.0, actual 334
""")

# -- 03-indexes-and-the-planner.md, line 134 ----------------------------------------
for label, query in (("ordered by amount, first 5", from_collection(deals).order_by(F.amount).take(5)),
                     ("ordered by title, first 5", from_collection(deals).order_by(F.title).take(5))):
    print(f"{label:28} {plan_of(query):34} {[d.number for d in query.to_list()]}")
_expect("""
ordered by amount, first 5   Limit <- IndexOrderedScan          [0, 973, 730, 487, 244]
ordered by title, first 5    Limit <- Sort <- CollectionScan    [0, 1, 10, 100, 101]
""")

# -- 03-indexes-and-the-planner.md, line 155 ----------------------------------------
query = from_collection(deals).where(F.stage == "lead")
print(query.explain_count()["plan"], "<-", query.explain_count()["children"][0]["plan"], "|", query.count())
_expect("""
Count <- IndexLookup | 334
""")

# -- 03-indexes-and-the-planner.md, line 174 ----------------------------------------
from proto_db import KeySpec, register_extractor

register_extractor("amount_band", lambda deal: deal.amount // 1000, version="1")

with db.new_transaction() as wtr:
    with_band = wtr.get_root_object("opportunities").add_index(
        EqualityIndex("by_band", KeySpec.extractor("amount_band")))
    wtr.set_root_object("opportunities", with_band)

tr.abort()
tr = db.new_transaction()
deals = tr.get_root_object("opportunities")
band_9 = from_collection(deals).where(F["@amount_band"] == 9)
print(plan_of(band_9), band_9.count(), "opportunities between 9,000 and 9,999")
_expect("""
IndexLookup 108 opportunities between 9,000 and 9,999
""")

# -- 03-indexes-and-the-planner.md, line 205 ----------------------------------------
from proto_db import ProtoValidationError

with db.new_transaction() as wtr:
    wtr.set_root_object("logins", wtr.new_dictionary().add_index(EqualityIndex("by_login", "login", unique=True))
                        .set_at(1, wtr.new_object(login="ann")))
wtr = db.new_transaction()
try:
    wtr.get_root_object("logins").set_at(2, wtr.new_object(login="ann"))
except ProtoValidationError:
    print("a second 'ann' is refused: ProtoValidationError")
wtr.abort()
tr.abort()
space.close()
_expect("""
a second 'ann' is refused: ProtoValidationError
""")

print("03_indexes_and_the_planner: OK")
