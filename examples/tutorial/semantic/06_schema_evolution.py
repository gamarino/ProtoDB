#!/usr/bin/env python3
"""6. Schema evolution

Generated from docs/tutorial/semantic-model/06-schema-evolution.md by ``python tests/docs/test_markdown_tutorials.py --write-scripts``:
the code below is the chapter's code blocks, verbatim and in order. After the blocks of each output the chapter
shows, ``_expect`` compares what they printed with it, so the script checks every figure the chapter shows,
exits with status 1 on the first difference, and prints "06_schema_evolution: OK" at the end. Edit the chapter, not this file.

Run it from anywhere: python examples/tutorial/semantic/06_schema_evolution.py
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


# -- 06-schema-evolution.md, line 24 ----------------------------------------
from decimal import Decimal

from proto_db import MemoryStorage, ObjectSpace, ProtoValidationError
from proto_db.semantic import semantic

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
db.run(lambda tr: tr.install_module("erp_model"))

def load(tr):
    s = semantic(tr)
    ann = s.create("erp.User", login="ann", name="Ann Lee")
    pat = s.create("erp.Person", name="Pat Doe", company="Globex")
    deals = [s.create("erp.Opportunity", number=n, title=f"deal {n}", amount=Decimal(amount), stage=stage,
                      owner=ann, contact=pat)
             for n, amount, stage in ((1, "12000", "proposal"), (2, "3000", "won"), (3, "5000", "won"))]
    return ann, pat, deals

ann, pat, deals = db.run(load)
db.run(lambda tr: tr.install_module("erp_regions"))

tr = db.new_transaction()
s = semantic(tr)
print(tr.installed_modules())
print("ann won:", s.get(ann).won, "| stored:", s.get(ann).record.has_defined_attr("won"))
print("regions:", [s.get(deal).region for deal in deals])
tr.abort()
_expect("""
('base', 'erp_model', 'erp_regions')
ann won: 8000 | stored: False
regions: [None, None, None]
""")

# -- 06-schema-evolution.md, line 68 ----------------------------------------
db.run(lambda tr: semantic(tr).update(deals[2], amount=Decimal("5500")))
tr = db.new_transaction()
s = semantic(tr)
print("ann won:", s.get(ann).won, "| stored:", s.get(ann).record.has_defined_attr("won"))
tr.abort()
_expect("""
ann won: 8500 | stored: True
""")

# -- 06-schema-evolution.md, line 88 ----------------------------------------
def create_without(tr):
    semantic(tr).create("erp.Opportunity", number=4, title="deal 4", amount=Decimal(1), owner=ann, contact=pat)

try:
    db.run(create_without)
except ProtoValidationError as error:
    print("a new one without a region:", str(error).split(": ", 1)[1])
db.run(lambda tr: semantic(tr).update(deals[0], title="deal 1, revised"))
print("an old one changed without a region: committed")
_expect("""
a new one without a region: 'erp.Opportunity' has the required fields ['region'] with no value; required fields are given at creation
an old one changed without a region: committed
""")

# -- 06-schema-evolution.md, line 118 ----------------------------------------
def backfill(tr):
    s = semantic(tr)
    changed = 0
    for opportunity in s.get(ann).opportunities:
        if opportunity.region is None:                        # .entity: the handle of the version's cell
            s.update(opportunity.entity, region="south" if opportunity.number % 2 else "north")
            changed += 1
    return changed

print("backfilled:", db.run(backfill))
tr = db.new_transaction()
s = semantic(tr)
print([(s.get(deal).number, s.get(deal).region) for deal in deals])
tr.abort()
_expect("""
backfilled: 3
[(1, 'south'), (2, 'north'), (3, 'south')]
""")

# -- 06-schema-evolution.md, line 159 ----------------------------------------
space.close()

print("06_schema_evolution: OK")
