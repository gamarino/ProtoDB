#!/usr/bin/env python3
"""6. Time: as-of reads, audits and stable pages

Generated from docs/tutorial/06-time.md by ``python tests/docs/test_markdown_tutorials.py --write-scripts``:
the code below is the chapter's code blocks, verbatim and in order. After the blocks of each output the chapter
shows, ``_expect`` compares what they printed with it, so the script checks every figure the chapter shows,
exits with status 1 on the first difference, and prints "06_time: OK" at the end. Edit the chapter, not this file.

Run it from anywhere: python examples/tutorial/06_time.py
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


# -- 06-time.md, line 11 ----------------------------------------
import datetime as dt
import time

from proto_db import F, MemoryStorage, ObjectSpace, from_collection

import erp_app

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
erp_app.seed(db)

def work_on(number, stage, who, **changes):
    erp_app.advance(db, number, stage, who, **changes)
    time.sleep(0.01)                                  # commits get distinct timestamps on a fast machine

before_week = dt.datetime.now(dt.timezone.utc)
time.sleep(0.02)
work_on(3, "qualified", "ann")
work_on(3, "proposal", "ann", amount=7500)
work_on(3, "won", "bob")
work_on(4, "lost", "cy")
mid_week = dt.datetime.now(dt.timezone.utc)
time.sleep(0.02)
work_on(6, "qualified", "ann")
print(len(db.history()), "versions:", [entry.version for entry in db.history()])
_expect("""
7 versions: [6, 5, 4, 3, 2, 1, 0]
""")

# -- 06-time.md, line 51 ----------------------------------------
def as_of(moment):
    """The database as it was at ``moment``."""
    for entry in db.history():                        # newest first
        if entry.committed_at <= moment:
            return db.snapshot_at(entry)
    raise LookupError("the database did not exist yet")

now = db.new_transaction()
print("now:     ", erp_app.pipeline(now.get_root_object("opportunities")))
print("mid-week:", erp_app.pipeline(as_of(mid_week).get_root_object("opportunities")))
print("before:  ", erp_app.pipeline(as_of(before_week).get_root_object("opportunities")))
now.abort()
_expect("""
now:      [('lead', 332, 1806488), ('lost', 1, 1148), ('proposal', 333, 1795500), ('qualified', 333, 1783253), ('won', 1, 7500)]
mid-week: [('lead', 333, 1807710), ('lost', 1, 1148), ('proposal', 333, 1795500), ('qualified', 332, 1782031), ('won', 1, 7500)]
before:   [('lead', 334, 1808821), ('proposal', 333, 1795500), ('qualified', 333, 1783179)]
""")

# -- 06-time.md, line 82 ----------------------------------------
def audit(deal):
    """Every version of an opportunity, oldest first: (stage, amount, changed_by)."""
    trail = []
    while deal is not None:
        trail.append((deal.stage, deal.amount, deal.changed_by))
        deal = deal.previous
    return list(reversed(trail))

tr = db.new_transaction()
for row in audit(tr.get_root_object("opportunities").get_at(3)):
    print(row)
tr.abort()
_expect("""
('lead', 1111, 'import')
('qualified', 1111, 'ann')
('proposal', 7500, 'ann')
('won', 7500, 'bob')
""")

# -- 06-time.md, line 109 ----------------------------------------
with db.new_transaction() as tr:
    tr.set_mutable("user:ann", tr.get_mutable("user:ann").with_(name="Ann Lee-Smith"))

tr = db.new_transaction()
deal = tr.get_root_object("opportunities").get_at(3)
print("owner now:", deal.owner.get(tr).name, "| before the week:", as_of(before_week).get_mutable("user:ann").name)
tr.abort()
_expect("""
owner now: Ann Lee-Smith | before the week: Ann Lee
""")

# -- 06-time.md, line 135 ----------------------------------------
def top_open(source):
    return (from_collection(source).where(F.stage != "won")
            .order_by(F.amount, descending=True).then_by(F.number))

def offset_page(k, size=250):
    with db.new_transaction() as tr:                  # a fresh snapshot per page, as a web request would
        return [d.number for d in top_open(tr.get_root_object("opportunities")).skip(k * size).take(size).to_list()]

with db.new_transaction() as tr:
    truth = [d.number for d in top_open(tr.get_root_object("opportunities")).to_list()]
seen = offset_page(0)
for number in seen[:40]:                              # Cy wins the 40 best deals meanwhile
    erp_app.advance(db, number, "won", "cy")
seen += offset_page(1)
print(len(seen), "rows in two offset pages;", len(set(truth[:500]) - set(seen)), "of the first 500 never shown")
_expect("""
500 rows in two offset pages; 40 of the first 500 never shown
""")

# -- 06-time.md, line 162 ----------------------------------------
with db.new_transaction() as tr:
    pager = tr.pager("opportunities", where=[["stage", "!=", "won"]], order_by=[("amount", True), "number"],
                     page_size=250)
    report = [d.number for d in pager.page(0)]
    token = pager.token()
    rows_at_start = pager.rows

for number in report[:40]:                            # the 40 best deals of this report are won meanwhile
    erp_app.advance(db, number, "won", "cy")

resumed = db.resume_pager(token)                      # a later request
for k in range(1, resumed.pages):
    report += [d.number for d in resumed.page(k)]
print(len(report), "rows,", len(set(report)), "distinct,", rows_at_start, "at the start")
space.close()
_expect("""
959 rows, 959 distinct, 959 at the start
""")

print("06_time: OK")
