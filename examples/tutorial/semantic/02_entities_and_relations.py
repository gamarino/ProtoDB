#!/usr/bin/env python3
"""2. Entities and relations

Generated from docs/tutorial/semantic-model/02-entities-and-relations.md by ``python tests/docs/test_markdown_tutorials.py --write-scripts``:
the code below is the chapter's code blocks, verbatim and in order. After the blocks of each output the chapter
shows, ``_expect`` compares what they printed with it, so the script checks every figure the chapter shows,
exits with status 1 on the first difference, and prints "02_entities_and_relations: OK" at the end. Edit the chapter, not this file.

Run it from anywhere: python examples/tutorial/semantic/02_entities_and_relations.py
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


# -- 02-entities-and-relations.md, line 10 ----------------------------------------
from decimal import Decimal

from proto_db import MemoryStorage, ObjectSpace, ProtoValidationError
from proto_db.semantic import semantic

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
db.run(lambda tr: tr.install_module("erp_model"))

def load(tr):
    s = semantic(tr)
    ann = s.create("erp.User", login="ann", name="Ann Lee")
    bob = s.create("erp.User", login="bob", name="Bob Ruiz")
    pat = s.create("erp.Person", name="Pat Doe", company="Globex")
    licences = s.create("erp.Opportunity", number=1, title="ERP licences", amount=Decimal("12000"),
                        owner=ann, contact=pat)
    return ann, bob, pat, licences

ann, bob, pat, licences = db.run(load)
print(type(licences).__name__, licences.name.split("#")[0])

tr = db.new_transaction()
s = semantic(tr)
deal = s.get(licences)
print(type(deal).__name__, deal.number, deal.title, deal.stage, deal.probability)
print("owner:", deal.owner.login, "| contact:", deal.contact.name, deal.contact.company)
tr.abort()
_expect("""
MutableObject erp.Opportunity
Version 1 ERP licences lead 0.1
owner: ann | contact: Pat Doe Globex
""")

# -- 02-entities-and-relations.md, line 56 ----------------------------------------
def change(tr):
    s = semantic(tr)
    s.update(licences, title="ERP licences, 40 seats", probability=Decimal("0.4"))
    try:
        s.update(licences, owner=bob)
    except ProtoValidationError as error:
        print("update(owner=...):", str(error).split(": ", 1)[1])

db.run(change)
_expect("""
update(owner=...): 'owner' is a role of 'erp.Opportunity'; change it with link and unlink
""")

# -- 02-entities-and-relations.md, line 80 ----------------------------------------
def reassign(tr):
    s = semantic(tr)
    s.unlink(licences, "owner", ann)
    s.link(licences, "owner", bob)

db.run(reassign)

tr = db.new_transaction()
s = semantic(tr)
print("owner:", s.get(licences).owner.login)
print("ann's:", [o.number for o in s.get(ann).opportunities], "| bob's:", [o.number for o in s.get(bob).opportunities])
print("navigate:", s.navigate(licences, "owner").login, type(s.navigate(bob, "opportunities")).__name__)
tr.abort()
_expect("""
owner: bob
ann's: [] | bob's: [1]
navigate: bob Members
""")

# -- 02-entities-and-relations.md, line 107 ----------------------------------------
import re

def wrong(tr, action):
    try:
        action(semantic(tr))
    except ProtoValidationError as error:                     # hide the random part of cell names
        return re.sub(r"#[0-9a-f]{32}", "#...", str(error).split(": ", 1)[-1])

for label, action in (("a second owner", lambda s: s.link(licences, "owner", ann)),
                      ("a person as owner", lambda s: s.create("erp.Opportunity", number=2, title="x",
                                                               amount=Decimal(1), owner=pat, contact=pat))):
    tr = db.new_transaction()
    print(f"{label:18}", wrong(tr, action))
    tr.abort()
_expect("""
a second owner     the end 'owner' of 'erp.Opportunity' is linked to 'erp.User#...'; unlink it first
a person as owner  the target 'erp.Person#...' is a 'erp.Person', not a 'erp.User' (or one of its subtypes)
""")

# -- 02-entities-and-relations.md, line 149 ----------------------------------------
def groups(tr):
    s = semantic(tr)
    sales = s.create("erp.Group", name="sales")
    s.relate("erp.Membership", user=ann, group=sales, role="manager")
    s.relate("erp.Membership", user=bob, group=sales)
    return sales

sales = db.run(groups)
tr = db.new_transaction()
s = semantic(tr)
print("sales:", [(m.user.login, m.role) for m in s.get(sales).memberships])
print("ann:", [(m.group.name, m.role) for m in s.get(ann).memberships])
tr.abort()
_expect("""
sales: [('ann', 'manager'), ('bob', 'member')]
ann: [('sales', 'manager')]
""")

# -- 02-entities-and-relations.md, line 181 ----------------------------------------
db.run(lambda tr: semantic(tr).relate("erp.Quote", opportunity=licences, recipient=pat, amount=Decimal("12000")))
db.run(lambda tr: semantic(tr).update(pat, company="Initech"))       # the contact moves
db.run(lambda tr: semantic(tr).relate("erp.Quote", opportunity=licences, recipient=pat, amount=Decimal("11500")))

tr = db.new_transaction()
s = semantic(tr)
for quote in s.get(licences).quotes:                                  # ordered: the role says ordered=True
    print(quote.amount, "sent to", quote.recipient.name, "at", quote.recipient.company)
print("the contact now:", s.get(licences).contact.company)
tr.abort()
_expect("""
12000 sent to Pat Doe at Globex
11500 sent to Pat Doe at Initech
the contact now: Initech
""")

# -- 02-entities-and-relations.md, line 208 ----------------------------------------
tr = db.new_transaction()
s = semantic(tr)
answer = s.query({"from": ["via", ["cell", sales.name], "memberships"],
                  "where": ["==", ["attr", "role"], ["const", "member"]],
                  "select": ["user.login", "role"]})
print(answer["rows"], "| version", answer["version"]["number"], "| plan", answer["explain"]["plan"]["plan"])
quotes = s.query({"from": ["via", ["cell", licences.name], "quotes"], "select": ["amount", "recipient.company"]})
print(quotes["rows"])
total = s.query({"from": ["via", ["cell", sales.name], "memberships", "user", "opportunities"],
                 "aggregate": ["sum", ["attr", "amount"]]})
print(total["rows"], [term["name"] for term in total["terms"]])
tr.abort()
space.close()
_expect("""
[{'user.login': 'bob', 'role': 'member'}] | version 8 | plan CollectionScan
[{'amount': Decimal('12000'), 'recipient.company': 'Globex'}, {'amount': Decimal('11500'), 'recipient.company': 'Initech'}]
[{'sum': Decimal('12000')}] ['memberships', 'user', 'opportunities', 'amount']
""")

print("02_entities_and_relations: OK")
