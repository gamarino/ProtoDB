#!/usr/bin/env python3
"""5. Modules and composition

Generated from docs/tutorial/semantic-model/05-modules-and-composition.md by ``python tests/docs/test_markdown_tutorials.py --write-scripts``:
the code below is the chapter's code blocks, verbatim and in order. After the blocks of each output the chapter
shows, ``_expect`` compares what they printed with it, so the script checks every figure the chapter shows,
exits with status 1 on the first difference, and prints "05_modules_and_composition: OK" at the end. Edit the chapter, not this file.

Run it from anywhere: python examples/tutorial/semantic/05_modules_and_composition.py
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


# -- 05-modules-and-composition.md, line 26 ----------------------------------------
from datetime import date
from decimal import Decimal

from proto_db import MemoryStorage, ObjectSpace
from proto_db.semantic import semantic

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
db.run(lambda tr: tr.install_module("erp_model"))

def load(tr):
    s = semantic(tr)
    ann = s.create("erp.User", login="ann", name="Ann Lee")
    pat = s.create("erp.Person", name="Pat Doe", company="Globex")
    deal = s.create("erp.Opportunity", number=1, title="ERP licences", amount=Decimal("12000"), owner=ann, contact=pat)
    return ann, pat, deal

ann, pat, deal = db.run(load)

def opportunity_attributes():
    tr = db.new_transaction()
    described = semantic(tr).describe("erp.Opportunity")
    tr.abort()
    return {a["name"]: (a.get("label", {}).get("und"), a["provenance"]) for a in described["attributes"]}

before = opportunity_attributes()
db.run(lambda tr: tr.install_module("erp_forecast"))
after = opportunity_attributes()

tr = db.new_transaction()
print(tr.installed_modules())
tr.abort()
for name in ("title", "close_date"):
    print(f"{name:11} before {str(before.get(name)):34} after {after[name]}")
_expect("""
('base', 'erp_model', 'erp_forecast')
title       before ('Title', ['erp_model'])           after ('Subject', ['erp_model', 'erp_forecast'])
close_date  before None                               after (None, ['erp_forecast'])
""")

# -- 05-modules-and-composition.md, line 81 ----------------------------------------
def plan(tr):
    s = semantic(tr)
    print("before setting it:", s.get(deal).close_date)
    s.update(deal, close_date=date(2026, 12, 15))
    print("after:", s.get(deal).close_date)

db.run(plan)
_expect("""
before setting it: None
after: 2026-12-15
""")

# -- 05-modules-and-composition.md, line 103 ----------------------------------------
def key_account(tr):
    s = semantic(tr)
    acme = s.create("erp.KeyAccount", name="Lee Wong", company="Acme", tier="gold")
    s.create("erp.Opportunity", number=2, title="Support contract", amount=Decimal("40000"), owner=ann, contact=acme)
    return acme

acme = db.run(key_account)
tr = db.new_transaction()
s = semantic(tr)
described = s.describe("erp.KeyAccount")
print("supertypes:", described["supertypes"])
for attribute in described["attributes"]:
    print(f"  {attribute['name']:14} inherited from {str(attribute.get('inherited_from')):11} provenance {attribute['provenance']}")
print("the key account's deals:", [(o.number, o.contact.tier) for o in s.get(acme).opportunities])
tr.abort()
_expect("""
supertypes: ['erp.Person']
  name           inherited from erp.Person  provenance ['erp_model']
  company        inherited from erp.Person  provenance ['erp_model']
  opportunities  inherited from erp.Person  provenance ['erp_model']
  quotes         inherited from erp.Person  provenance ['erp_model']
  tier           inherited from None        provenance ['erp_forecast']
the key account's deals: [(2, 'gold')]
""")

# -- 05-modules-and-composition.md, line 144 ----------------------------------------
db.run(lambda tr: tr.install_module("erp_labels"))
tr = db.new_transaction()
s = semantic(tr)
print([s.get(deal).label(), s.get(deal).currency])
tr.abort()
space.close()
_expect("""
['#1 ERP licences (lead)', 'ARS']
""")

print("05_modules_and_composition: OK")
