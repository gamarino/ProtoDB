#!/usr/bin/env python3
"""1. Declaring a model

Generated from docs/tutorial/semantic-model/01-declaring-a-model.md by ``python tests/docs/test_markdown_tutorials.py --write-scripts``:
the code below is the chapter's code blocks, verbatim and in order. After the blocks of each output the chapter
shows, ``_expect`` compares what they printed with it, so the script checks every figure the chapter shows,
exits with status 1 on the first difference, and prints "01_declaring_a_model: OK" at the end. Edit the chapter, not this file.

Run it from anywhere: python examples/tutorial/semantic/01_declaring_a_model.py
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


# -- 01-declaring-a-model.md, line 62 ----------------------------------------
from proto_db import MemoryStorage, ObjectSpace
from proto_db.semantic import semantic

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
db.run(lambda tr: tr.install_module("erp_model"))

tr = db.new_transaction()
print(tr.installed_modules())
s = semantic(tr)
for entity in s.list_entities():
    print(f"{entity['type_name']:16} {entity['meaning']}")
_expect("""
('base', 'erp_model')
erp.Group        A group of users that share permissions
erp.Membership   A user belongs to a group, in a role
erp.Opportunity  A possible sale to a customer, owned by a user, moving through stages
erp.Person       A contact at a customer company
erp.Quote        An offer sent to a contact for an opportunity, with the terms of that moment
erp.User         A person who signs in to the ERP and owns opportunities
""")

# -- 01-declaring-a-model.md, line 92 ----------------------------------------
print([entity["type_name"] for entity in s.list_entities("deal")], [entity["type_name"] for entity in s.list_entities("sale")])
_expect("""
['erp.Opportunity'] ['erp.Opportunity']
""")

# -- 01-declaring-a-model.md, line 109 ----------------------------------------
described = s.describe("erp.Opportunity")
print(described["meaning"], "| also called:", described["synonyms"])
for attribute in described["attributes"]:
    kind = attribute.get("kind") or f"relation to {attribute['target']} ({attribute['cardinality']})"
    details = {k: attribute[k] for k in ("value_type", "required", "default", "values", "unit") if k in attribute}
    print(f"  {attribute['name']:12} {kind:36} {details}")
print("intents:", [i["name"] for i in described["intents"]], "| constraints:", [c["name"] for c in described["constraints"]])
_expect("""
A possible sale to a customer, owned by a user, moving through stages | also called: ['deal']
  number       field                                {'value_type': 'int', 'required': True}
  title        field                                {'value_type': 'str', 'required': True}
  stage        field                                {'value_type': 'str', 'default': 'lead', 'values': ['lead', 'qualified', 'proposal', 'won', 'lost']}
  amount       field                                {'value_type': 'decimal', 'required': True, 'unit': 'currency:company'}
  probability  field                                {'value_type': 'decimal', 'default': Decimal('0.1')}
  expected     computed                             {'unit': 'currency:company'}
  owner        relation to erp.User (one)           {}
  contact      relation to erp.Person (one)         {}
  quotes       relation to erp.Quote (many)         {}
intents: ['move_to'] | constraints: ['probability_range']
""")

# -- 01-declaring-a-model.md, line 143 ----------------------------------------
from decimal import Decimal

def load(tr):
    s = semantic(tr)
    ann = s.create("erp.User", login="ann", name="Ann Lee")
    pat = s.create("erp.Person", name="Pat Doe", company="Globex")
    return s.create("erp.Opportunity", number="seven", title="ERP licences", stage="maybe",
                    amount=Decimal("12000"), owner=ann, contact=pat)

odd = db.run(load)
tr.abort()
tr = db.new_transaction()
print(semantic(tr).get(odd).stage, semantic(tr).get(odd).number)
tr.abort()
space.close()
_expect("""
maybe seven
""")

print("01_declaring_a_model: OK")
