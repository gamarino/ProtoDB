#!/usr/bin/env python3
"""7. Performance pitfalls

Generated from docs/tutorial/queries/07-performance-pitfalls.md by ``python tests/docs/test_markdown_tutorials.py --write-scripts``:
the code below is the chapter's code blocks, verbatim and in order. After the blocks of each output the chapter
shows, ``_expect`` compares what they printed with it, so the script checks every figure the chapter shows,
exits with status 1 on the first difference, and prints "07_performance_pitfalls: OK" at the end. Edit the chapter, not this file.

Run it from anywhere: python examples/tutorial/queries/07_performance_pitfalls.py
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


# -- 07-performance-pitfalls.md, line 21 ----------------------------------------
import os
import tempfile
import time

from proto_db import F, FileStorage, ObjectSpace, from_collection

import erp_app
from erp_app import plan_of

workdir = tempfile.TemporaryDirectory(prefix="proto_db-tutorial-")   # this chapter's own directory
home = os.getcwd()
os.chdir(workdir.name)                                # its files stay out of yours

space = ObjectSpace(FileStorage("perf-space"))
erp_app.seed(space.new_database("erp"), opportunities=5000)
space.close()

def cold(read):
    """Open the space afresh, run ``read`` on the opportunities; return (result, atoms decoded, seconds)."""
    fresh = ObjectSpace(FileStorage("perf-space"))
    tr = fresh.open_database("erp").new_transaction()
    deals = tr.get_root_object("opportunities")
    before, start = fresh.atom_cache_stats()["loads"], time.perf_counter()
    result = read(deals)
    elapsed = time.perf_counter() - start
    loads = fresh.atom_cache_stats()["loads"] - before
    tr.abort()
    fresh.close()
    return result, loads, elapsed

with ObjectSpace(FileStorage("perf-space")) as opened:
    print(len(opened.open_database("erp").new_transaction().get_root_object("opportunities")), "opportunities")
_expect("""
5000 opportunities
""")

# -- 07-performance-pitfalls.md, line 66 ----------------------------------------
shapes = {
    "amount == 9991 (range index)":     lambda d: from_collection(d).where(F.amount == 9991).count(),
    "title == ... (no index: a scan)":  lambda d: from_collection(d).where(F.title == "opportunity 4242").count(),
    "a lambda (a PythonFilter)":        lambda d: from_collection(d).where(lambda o: o.amount == 9991).count(),
    "title.startswith (a PythonFilter)": lambda d: from_collection(d).where(F.title.startswith("opportunity 4242")).count(),
}
measured = {label: cold(read) for label, read in shapes.items()}
for label, (rows, loads, _) in measured.items():
    print(f"{label:36} {rows:3} rows, {loads:6} atoms decoded")
_expect("""
amount == 9991 (range index)           1 rows,     16 atoms decoded
title == ... (no index: a scan)        1 rows,  10007 atoms decoded
a lambda (a PythonFilter)              1 rows,  10007 atoms decoded
title.startswith (a PythonFilter)      1 rows,  10007 atoms decoded
""")

# -- 07-performance-pitfalls.md, line 87 ----------------------------------------
for label, (_, _, elapsed) in measured.items():
    print(f"{label:36} {elapsed * 1000:8.1f} ms (cold)")
_sample()

# -- 07-performance-pitfalls.md, line 110 ----------------------------------------
for label, read in (("order_by(amount).take(10)", lambda d: from_collection(d).order_by(F.amount).take(10).to_list()),
                    ("order_by(title).take(10)", lambda d: from_collection(d).order_by(F.title).take(10).to_list())):
    rows, loads, _ = cold(read)
    print(f"{label:28} {len(rows)} rows, {loads:6} atoms decoded")
_expect("""
order_by(amount).take(10)    10 rows,     35 atoms decoded
order_by(title).take(10)     10 rows,  10007 atoms decoded
""")

# -- 07-performance-pitfalls.md, line 130 ----------------------------------------
for label, read in (("len(collection)", len), ("query count, no condition", lambda d: from_collection(d).count()),
                    ("count of a lookup", lambda d: from_collection(d).where(F.stage == "lead").count())):
    rows, loads, _ = cold(read)
    print(f"{label:28} {rows:5}, {loads:6} atoms decoded")
_expect("""
len(collection)               5000,      0 atoms decoded
query count, no condition     5000,      7 atoms decoded
count of a lookup             1667,      9 atoms decoded
""")

# -- 07-performance-pitfalls.md, line 156 ----------------------------------------
def update_cost(indexes):
    path = f"writes-{indexes}"
    store = ObjectSpace(FileStorage(path, durability="none"))
    db = store.new_database("erp")
    erp_app.seed(db, opportunities=5000, indexes=indexes)
    size, start = sum(os.path.getsize(os.path.join(path, n)) for n in os.listdir(path)), time.perf_counter()
    for n in range(200):
        with db.new_transaction() as tr:
            deals = tr.get_root_object("opportunities")
            tr.set_root_object("opportunities", deals.set_at(n, deals.get_at(n).with_(amount=1234, stage="won")))
    elapsed = time.perf_counter() - start
    grown = sum(os.path.getsize(os.path.join(path, n)) for n in os.listdir(path)) - size
    store.close()
    return elapsed / 200, grown / 200

plain, indexed = update_cost(False), update_cost(True)
assert indexed[1] > plain[1]                         # the indexes' paths are written too
print(f"no index:  {plain[0] * 1e6:6.0f} us and {plain[1]:6.0f} bytes per update")
print(f"3 indexes: {indexed[0] * 1e6:6.0f} us and {indexed[1]:6.0f} bytes per update")
_sample()

# -- 07-performance-pitfalls.md, line 208 ----------------------------------------
_, loads_ordered, _ = cold(lambda d: from_collection(d).order_by(F.amount).skip(4000).take(10).to_list())
_, loads_sorted, _ = cold(lambda d: from_collection(d).order_by(F.title).skip(4000).take(10).to_list())
print(f"page at offset 4000: {loads_ordered} atoms over the amount index, {loads_sorted} sorting by title")

os.chdir(home)
workdir.cleanup()                                     # removes only the directory this chapter created
_expect("""
page at offset 4000: 40 atoms over the amount index, 10007 sorting by title
""")

print("07_performance_pitfalls: OK")
