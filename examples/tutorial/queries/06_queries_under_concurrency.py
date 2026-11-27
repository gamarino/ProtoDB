#!/usr/bin/env python3
"""6. Queries under concurrency

Generated from docs/tutorial/queries/06-queries-under-concurrency.md by ``python tests/docs/test_markdown_tutorials.py --write-scripts``:
the code below is the chapter's code blocks, verbatim and in order. After the blocks of each output the chapter
shows, ``_expect`` compares what they printed with it, so the script checks every figure the chapter shows,
exits with status 1 on the first difference, and prints "06_queries_under_concurrency: OK" at the end. Edit the chapter, not this file.

Run it from anywhere: python examples/tutorial/queries/06_queries_under_concurrency.py
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


# -- 06-queries-under-concurrency.md, line 14 ----------------------------------------
from proto_db import F, MemoryStorage, ObjectSpace, ProtoConflictError, from_collection

import erp_app
from erp_app import plan_of

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
erp_app.seed(db)

def after(read, change):
    reader = db.new_transaction()
    read(reader.get_root_object("opportunities"))
    reader.set_root_object("report", "done")
    with db.new_transaction() as writer:
        deals = writer.get_root_object("opportunities")
        writer.set_root_object("opportunities", change(writer, deals))
    try:
        reader.commit()
        return "committed"
    except ProtoConflictError as error:
        unit = error.unit
        index = f" of index {unit.index!r}" if unit.index else ""
        key = f" {unit.key!r}" if unit.key is not None and unit.kind.name != "RANGE" else ""
        return f"conflict on {unit.kind.name}{key}{index}"

def stage_of(number, stage):
    return lambda tr, deals: deals.set_at(number, deals.get_at(number).with_(stage=stage))

def amount_of(number, amount):
    return lambda tr, deals: deals.set_at(number, deals.get_at(number).with_(amount=amount))

def add_deal(number):
    return lambda tr, deals: deals.set_at(number, erp_app.opportunity_record(tr, number, {
        login: erp_app.user(login) for login in erp_app.LOGINS}))

with db.new_transaction() as tr:
    deals = tr.get_root_object("opportunities")
    print(deals.get_at(3).stage, deals.get_at(4).stage, deals.get_at(5).stage, "| amounts of 242 and 7:",
          deals.get_at(242).amount, deals.get_at(7).amount)
_expect("""
lead qualified proposal | amounts of 242 and 7: 9954 1259
""")

# -- 06-queries-under-concurrency.md, line 67 ----------------------------------------
leads = lambda deals: from_collection(deals).where(F.stage == "lead").count()
print(plan_of(from_collection(db.new_transaction().get_root_object("opportunities")).where(F.stage == "lead")))
print("a qualified deal is lost:   ", after(leads, stage_of(4, "lost")))
print("a lead's amount changes:    ", after(leads, amount_of(3, 2000)))
print("a lead is qualified:        ", after(leads, stage_of(3, "qualified")))
print("a deal becomes a lead:      ", after(leads, stage_of(5, "lead")))
_expect("""
IndexLookup
a qualified deal is lost:    committed
a lead's amount changes:     conflict on INDEX_GROUP 'lead' of index 'by_stage'
a lead is qualified:         conflict on INDEX_GROUP 'lead' of index 'by_stage'
a deal becomes a lead:       conflict on INDEX_GROUP 'lead' of index 'by_stage'
""")

# -- 06-queries-under-concurrency.md, line 94 ----------------------------------------
large = lambda deals: from_collection(deals).where(F.amount >= 9900).count()
print("a small deal changes:         ", after(large, stage_of(7, "won")))
print("a small deal grows into range:", after(large, amount_of(7, 9950)))
print("a large deal is won:          ", after(large, stage_of(242, "won")))
print("a new small deal:             ", after(large, add_deal(5000)))
_expect("""
a small deal changes:          committed
a small deal grows into range: conflict on RANGE of index 'by_amount'
a large deal is won:           conflict on RANGE of index 'by_amount'
a new small deal:              committed
""")

# -- 06-queries-under-concurrency.md, line 116 ----------------------------------------
by_title = lambda deals: from_collection(deals).where(F.title == "opportunity 9").count()
by_lambda = lambda deals: from_collection(deals).where(lambda d: d.amount % 1000 == 0).count()
print(plan_of(from_collection(db.new_transaction().get_root_object("opportunities")).where(F.title == "x")))
print("title scan, another deal changes: ", after(by_title, stage_of(7, "won")))
print("a lambda, another deal changes:   ", after(by_lambda, stage_of(8, "won")))
print("len(), another deal changes:      ", after(len, stage_of(9, "won")))
_expect("""
Filter <- CollectionScan
title scan, another deal changes:  conflict on WHOLE
a lambda, another deal changes:    conflict on WHOLE
len(), another deal changes:       conflict on WHOLE
""")

# -- 06-queries-under-concurrency.md, line 139 ----------------------------------------
from proto_db import EqualityIndex

with db.new_transaction() as tr:
    tr.set_root_object("tiny", tr.new_dictionary().add_index(EqualityIndex("by_stage", "stage"))
                       .set_at(1, tr.new_object(stage="lead")))

def on_tiny(read, change):
    reader = db.new_transaction()
    read(reader.get_root_object("tiny"))
    reader.set_root_object("report", "done")
    with db.new_transaction() as writer:
        writer.set_root_object("tiny", change(writer, writer.get_root_object("tiny")))
    try:
        reader.commit()
        return "committed"
    except ProtoConflictError as error:
        return f"conflict on {error.unit.kind.name}"

lead = lambda source: from_collection(source).where(F.stage == "lead")
print("1 element:    ", plan_of(lead(db.new_transaction().get_root_object("tiny"))),
      "|", on_tiny(lambda t: lead(t).count(), lambda tr, t: t.set_at(2, tr.new_object(stage="lost"))))
print("1000 elements:", plan_of(lead(db.new_transaction().get_root_object("opportunities"))),
      "|", after(lambda d: lead(d).count(), stage_of(4, "lost")))
print("!= on 1000:   ", plan_of(from_collection(db.new_transaction().get_root_object("opportunities")).where(F.stage != "lead")),
      "|", after(lambda d: from_collection(d).where(F.stage != "lead").count(), amount_of(3, 2500)))
_expect("""
1 element:     Filter <- CollectionScan | conflict on WHOLE
1000 elements: IndexLookup | committed
!= on 1000:    IndexComplement | conflict on WHOLE
""")

# -- 06-queries-under-concurrency.md, line 188 ----------------------------------------
walk = lambda deals: list(deals.keys(after=990))
window = lambda deals: from_collection(deals).skip(10).take(5).to_list()
counted = lambda deals: from_collection(deals).skip(10).take(5).count()
print("keys(after=990), key 10 changes:     ", after(walk, stage_of(10, "won")))
print("keys(after=990), key 995 changes:    ", after(walk, stage_of(995, "won")))
print("skip(10).take(5), key 3 changes:     ", after(window, stage_of(3, "won")))
print("skip(10).take(5), key 500 changes:   ", after(window, stage_of(500, "won")))
print("the same window counted, key 500:    ", after(counted, stage_of(501, "won")))
_expect("""
keys(after=990), key 10 changes:      committed
keys(after=990), key 995 changes:     conflict on RANGE
skip(10).take(5), key 3 changes:      conflict on RANGE
skip(10).take(5), key 500 changes:    committed
the same window counted, key 500:     conflict on WHOLE
""")

# -- 06-queries-under-concurrency.md, line 217 ----------------------------------------
def explain_only(deals):
    from_collection(deals).where(F.title == "opportunity 9").explain()   # a scan's explain shows its row count

def many_keys(deals):
    for number in range(1100):                                          # more than 1024 fine reads
        deals.get_at(number)

print("explain() of a scan:      ", after(explain_only, stage_of(12, "won")))
print("1100 get_at, key 1099 too:", after(many_keys, add_deal(5001)))
print("from a Snapshot:          ", end=" ")
reader = db.new_transaction()
snapshot = db.snapshot_at(reader.snapshot_ref)
from_collection(snapshot.get_root_object("opportunities")).where(F.title == "opportunity 9").count()
reader.set_root_object("report", "from a snapshot")
with db.new_transaction() as writer:
    deals = writer.get_root_object("opportunities")
    writer.set_root_object("opportunities", deals.set_at(13, deals.get_at(13).with_(stage="won")))
reader.commit()
print("committed")
space.close()
_expect("""
explain() of a scan:       conflict on WHOLE
1100 get_at, key 1099 too: conflict on WHOLE
from a Snapshot:           committed
""")

print("06_queries_under_concurrency: OK")
