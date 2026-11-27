#!/usr/bin/env python3
"""7. History

Generated from docs/tutorial/semantic-model/07-history.md by ``python tests/docs/test_markdown_tutorials.py --write-scripts``:
the code below is the chapter's code blocks, verbatim and in order. After the blocks of each output the chapter
shows, ``_expect`` compares what they printed with it, so the script checks every figure the chapter shows,
exits with status 1 on the first difference, and prints "07_history: OK" at the end. Edit the chapter, not this file.

Run it from anywhere: python examples/tutorial/semantic/07_history.py
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


# -- 07-history.md, line 10 ----------------------------------------
import base64
from decimal import Decimal

from proto_db import MemoryStorage, ObjectSpace, ProtoUserError
from proto_db.semantic import semantic

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
db.run(lambda tr: tr.install_module("erp_model"))

def load(tr):
    s = semantic(tr)
    ann = s.create("erp.User", login="ann", name="Ann Lee")
    pat = s.create("erp.Person", name="Pat Doe", company="Globex")
    deal = s.create("erp.Opportunity", number=1, title="ERP licences", amount=Decimal("12000"), owner=ann, contact=pat)
    return ann, deal

ann, deal = db.run(load)
db.run(lambda tr: semantic(tr).invoke(deal, "move_to", stage="qualified"))
db.run(lambda tr: tr.install_module("erp_labels"))                  # behaviour arrives
db.run(lambda tr: semantic(tr).invoke(deal, "move_to", stage="proposal"))
db.run(lambda tr: tr.install_module("erp_crm"))                     # and is refined
db.run(lambda tr: semantic(tr).update(deal, probability=Decimal("0.5")))

for entry in reversed(db.history()):                                # oldest first
    if entry.version < 2:                                           # the empty database, the bare install
        continue
    s = semantic(db.snapshot_at(entry))
    print(entry.version, s.get(deal).stage, s.catalog()["module_set"])
_expect("""
2 lead ['base', 'erp_model']
3 qualified ['base', 'erp_model']
4 qualified ['base', 'erp_labels', 'erp_model']
5 proposal ['base', 'erp_labels', 'erp_model']
6 proposal ['base', 'erp_crm', 'erp_labels', 'erp_model']
7 proposal ['base', 'erp_crm', 'erp_labels', 'erp_model']
""")

# -- 07-history.md, line 58 ----------------------------------------
def as_of(version):
    return db.snapshot_at(next(entry for entry in db.history() if entry.version == version))

for version in (5, 7):
    s = semantic(as_of(version))
    print(version, s.get(deal).stage, "|", s.get(deal).label())
tr = db.new_transaction()
print("now", semantic(tr).get(deal).label())
tr.abort()
_expect("""
5 proposal | #1 ERP licences (proposal)
7 proposal | #1 ERP licences (proposal), expected 6000.0 USD
now #1 ERP licences (proposal), expected 6000.0 USD
""")

# -- 07-history.md, line 86 ----------------------------------------
old = as_of(5)
record = old.get_mutable(deal.name)
print("today's modules: ", record.label())
with old:
    print("the modules of then:", old.get_mutable(deal.name).label())
_expect("""
today's modules:  #1 ERP licences (proposal), expected 1200.0 USD
the modules of then: #1 ERP licences (proposal)
""")

# -- 07-history.md, line 106 ----------------------------------------
db.run(lambda tr: tr.install_module("erp_forecast"))              # version 8: a new field
for version in (7, 8):
    attributes = [a["name"] for a in semantic(as_of(version)).describe("erp.Opportunity")["attributes"]]
    print(version, attributes[-3:])
print("close_date at version 7:", semantic(as_of(7)).get(deal).close_date)
_expect("""
7 ['owner', 'contact', 'quotes']
8 ['contact', 'quotes', 'close_date']
close_date at version 7: None
""")

# -- 07-history.md, line 132 ----------------------------------------
tr = db.new_transaction()
read = semantic(tr).version()
tr.abort()
db.run(lambda tr: semantic(tr).update(deal, probability=Decimal("0.9")))

again = semantic(db.snapshot_at(base64.b64decode(read["ref"])))
print("version", read["number"], "probability then:", again.get(deal).probability)
try:
    again.update(deal, probability=Decimal("0.1"))
except ProtoUserError:
    print("writing through a snapshot: ProtoUserError")
space.close()
_expect("""
version 8 probability then: 0.5
writing through a snapshot: ProtoUserError
""")

print("07_history: OK")
