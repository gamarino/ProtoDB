#!/usr/bin/env python3
"""1. Building queries

Generated from docs/tutorial/queries/01-building-queries.md by ``python tests/docs/test_markdown_tutorials.py --write-scripts``:
the code below is the chapter's code blocks, verbatim and in order. After the blocks of each output the chapter
shows, ``_expect`` compares what they printed with it, so the script checks every figure the chapter shows,
exits with status 1 on the first difference, and prints "01_building_queries: OK" at the end. Edit the chapter, not this file.

Run it from anywhere: python examples/tutorial/queries/01_building_queries.py
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


# -- 01-building-queries.md, line 13 ----------------------------------------
from proto_db import F, MISSING, MemoryStorage, ObjectSpace, ProtoUserError, from_collection

import erp_app
from erp_app import plan_of

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
erp_app.seed(db)
tr = db.new_transaction()
deals = tr.get_root_object("opportunities")
print(len(deals), "opportunities; one of them:", deals.get_at(7).title, deals.get_at(7).stage, deals.get_at(7).amount)
_expect("""
1000 opportunities; one of them: opportunity 7 qualified 1259
""")

# -- 01-building-queries.md, line 38 ----------------------------------------
def numbers(query, limit=6):
    return [d.number for d in query.order_by(F.number).take(limit).to_list()]

queries = {
    "stage == 'won' (none yet)":         from_collection(deals).where(F.stage == "won"),
    "amount > 9990":                     from_collection(deals).where(F.amount > 9990),
    "stage in (lead, proposal)":         from_collection(deals).where(F.stage.in_(["lead", "proposal"])),
    "amount between 5000 and 5010":      from_collection(deals).where(F.amount.between(5000, 5010)),
    "lead and amount < 1100":            from_collection(deals).where((F.stage == "lead") & (F.amount < 1100)),
    "amount < 1010 or amount > 9990":    from_collection(deals).where((F.amount < 1010) | (F.amount > 9990)),
    "not a lead, amount < 1100":         from_collection(deals).where(~(F.stage == "lead") & (F.amount < 1100)),
}
for label, query in queries.items():
    print(f"{label:34} {query.count():4}  first: {numbers(query)}")
_expect("""
stage == 'won' (none yet)             0  first: []
amount > 9990                         1  first: [243]
stage in (lead, proposal)           667  first: [0, 2, 3, 5, 6, 8]
amount between 5000 and 5010          1  first: [838]
lead and amount < 1100                4  first: [0, 489, 732, 975]
amount < 1010 or amount > 9990        3  first: [0, 243, 973]
not a lead, amount < 1100            10  first: [1, 2, 244, 245, 487, 488]
""")

# -- 01-building-queries.md, line 73 ----------------------------------------
print("previous is None:", from_collection(deals).where(F.previous.is_none()).count(),
      "| fax is None:", from_collection(deals).where(F.fax.is_none()).count(),
      "| fax > 0:", from_collection(deals).where(F.fax > 0).count())
_expect("""
previous is None: 1000 | fax is None: 1000 | fax > 0: 0
""")

# -- 01-building-queries.md, line 91 ----------------------------------------
try:
    (F.stage == "lead") and (F.amount > 5000)           # Python would silently keep only one side
except ProtoUserError as error:
    print("and:", type(error).__name__)
try:
    F.stage in ["lead", "won"]                          # compares each item and asks for a truth value
except ProtoUserError as error:
    print("in: ", type(error).__name__, "- write F.stage.in_([...])")
_expect("""
and: ProtoUserError
in:  ProtoUserError - write F.stage.in_([...])
""")

# -- 01-building-queries.md, line 115 ----------------------------------------
for label, query in [
    ("== a value (index)", from_collection(deals).where(F.amount == 9991)),
    ("startswith", from_collection(deals).where(F.title.startswith("opportunity 99"))),
    ("a lambda", from_collection(deals).where(lambda d: d.amount % 1000 == 0)),
    ("amount * 2 > 19900", from_collection(deals).where(F.amount * 2 > 19900)),
]:
    print(f"{label:20} {plan_of(query):32} {query.count()} rows")
_expect("""
== a value (index)   IndexLookup                      1 rows
startswith           PythonFilter <- CollectionScan   11 rows
a lambda             PythonFilter <- CollectionScan   1 rows
amount * 2 > 19900   PythonFilter <- CollectionScan   5 rows
""")

# -- 01-building-queries.md, line 142 ----------------------------------------
top = from_collection(deals).where(F.amount > 9950).order_by(F.number)
print(top.select(F.number).to_list())
print(top.select({"n": F.number, "stage": F.stage}).take(2).to_list())
print(top.select(lambda d: f"#{d.number}: {d.amount}").take(2).to_list())
print(from_collection(deals).where(F.number == 3).select(F.fax).to_list(), MISSING is not None)
_expect("""
[242, 243, 486, 729, 972]
[{'n': 242, 'stage': 'proposal'}, {'n': 243, 'stage': 'lead'}]
['#242: 9954', '#243: 9991']
[MISSING] True
""")

# -- 01-building-queries.md, line 168 ----------------------------------------
ranked = (from_collection(deals).where(F.stage == "proposal")
          .order_by(F.amount, descending=True).then_by(F.number))
print([(d.number, d.amount) for d in ranked.take(3).to_list()])
print([(d.number, d.amount) for d in ranked.skip(3).take(3).to_list()])
try:
    from_collection(deals).where(F.amount > 0).then_by(F.number)
except ProtoUserError as error:
    print(error)
_expect("""
[(242, 9954), (485, 9945), (728, 9936)]
[(971, 9927), (239, 9843), (482, 9834)]
then_by must directly follow order_by or then_by
""")

# -- 01-building-queries.md, line 192 ----------------------------------------
proposals = from_collection(deals).where(F.stage == "proposal").order_by(F.amount)
print(proposals.count(), proposals.first().number, proposals.first_or_none(lambda d: d.amount > 99_999))
print(sum(1 for _ in proposals), proposals.count(lambda d: d.amount >= 9000))
print(proposals)
_expect("""
333 974 None
333 36
Queryable(Dictionary, ['where', 'order'])
""")

# -- 01-building-queries.md, line 212 ----------------------------------------
rows = [{"login": "ann", "deals": 334}, {"login": "bob", "deals": 333}, {"login": "cy", "deals": 333}]
print(from_collection(rows).where(F.deals > 333).select(F.login).to_list())
tr.abort()
space.close()
_expect("""
['ann']
""")

print("01_building_queries: OK")
