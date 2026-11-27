#!/usr/bin/env python3
"""8. Extensible models and the runtime registry

Generated from docs/tutorial/08-extensible-models.md by ``python tests/docs/test_markdown_tutorials.py --write-scripts``:
the code below is the chapter's code blocks, verbatim and in order. After the blocks of each output the chapter
shows, ``_expect`` compares what they printed with it, so the script checks every figure the chapter shows,
exits with status 1 on the first difference, and prints "08_extensible_models: OK" at the end. Edit the chapter, not this file.

Run it from anywhere: python examples/tutorial/08_extensible_models.py
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


# -- 08-extensible-models.md, line 15 ----------------------------------------
from proto_db import MemoryStorage, ObjectSpace

storage = MemoryStorage()
space = ObjectSpace(storage)
db = space.new_database("erp")

with db.new_transaction() as tr:
    globex = tr.new_object(company="Globex", city="Rosario", currency="ARS")
    pat = tr.new_object(parents=(globex,), name="Pat Doe", city="Funes")
    print(pat.name, pat.city, pat.company, pat.currency)         # own, own (shadows), inherited, inherited
    print(pat.has_defined_attr("city"), pat.has_defined_attr("company"), "company" in pat)
_expect("""
Pat Doe Funes Globex ARS
True False True
""")

# -- 08-extensible-models.md, line 63 ----------------------------------------
from decimal import Decimal

from proto_db.semantic import semantic

db.run(lambda tr: tr.install_module("erp_model"))

def load(tr):
    s = semantic(tr)
    ann = s.create("erp.User", login="ann", name="Ann Lee")
    pat = s.create("erp.Person", name="Pat Doe", company="Globex")
    return s.create("erp.Opportunity", number=1, title="ERP licences", amount=Decimal("12000"),
                    probability=Decimal("0.5"), owner=ann, contact=pat)

deal = db.run(load)
with db.new_transaction() as tr:
    print(tr.installed_modules(), "| has a label:", semantic(tr).get(deal).label is not None)

db.run(lambda tr: tr.install_module("erp_labels"))
with db.new_transaction() as tr:
    opportunity = semantic(tr).get(deal)
    print(tr.installed_modules())
    print(opportunity.label(), "|", opportunity.currency)
_expect("""
('base', 'erp_model') | has a label: False
('base', 'erp_model', 'erp_labels')
#1 ERP licences (lead) | ARS
""")

# -- 08-extensible-models.md, line 114 ----------------------------------------
other = ObjectSpace(storage)                          # another process, opened before erp_crm is installed
other_db = other.open_database("erp")
with other_db.new_transaction() as tr:
    print("other process:", semantic(tr).get(deal).label())

with db.new_transaction() as tr:
    before_crm = tr.snapshot_ref                      # remembered for the as-of read below

db.run(lambda tr: tr.install_module("erp_crm"))
with db.new_transaction() as tr:
    print("this process: ", semantic(tr).get(deal).label())
    print([extension.module for extension in tr.registry.chain("erp.Opportunity")])
_expect("""
other process: #1 ERP licences (lead)
this process:  #1 ERP licences (lead), expected 6000.0 USD
['erp_crm', 'erp_labels', 'erp_model']
""")

# -- 08-extensible-models.md, line 150 ----------------------------------------
from proto_db import ProtoValidationError

with other_db.new_transaction() as tr:
    record = tr.get_mutable(deal.name)                # the stored record of the entity's cell
    print("own data needs no registry:", record.title)
    try:
        semantic(tr).get(deal).label()
    except ProtoValidationError as error:
        print("ProtoValidationError:", str(error).split(";")[0])

other.reload_registry("erp")
with other_db.new_transaction() as tr:
    print("after the reload:", semantic(tr).get(deal).label())
_expect("""
own data needs no registry: ERP licences
ProtoValidationError: Database 'erp': registry built for modules ['base', 'erp_model', 'erp_labels'] but the database lists ['base', 'erp_model', 'erp_labels', 'erp_crm']
after the reload: #1 ERP licences (lead), expected 6000.0 USD
""")

# -- 08-extensible-models.md, line 186 ----------------------------------------
old = db.snapshot_at(before_crm)
record = old.get_mutable(deal.name)                   # the opportunity as stored in that version
print("old data, today's modules:  ", record.label())
with old:
    print("old data, the modules of then:", old.get_mutable(deal.name).label())
print("a semantic session over the snapshot:", semantic(old).get(deal).label())
space.close()
other.close()
_expect("""
old data, today's modules:   #1 ERP licences (lead), expected 6000.0 USD
old data, the modules of then: #1 ERP licences (lead)
a semantic session over the snapshot: #1 ERP licences (lead)
""")

print("08_extensible_models: OK")
