#!/usr/bin/env python3
"""3. Modelling

Generated from docs/tutorial/03-modelling.md by ``python tests/docs/test_markdown_tutorials.py --write-scripts``:
the code below is the chapter's code blocks, verbatim and in order. After the blocks of each output the chapter
shows, ``_expect`` compares what they printed with it, so the script checks every figure the chapter shows,
exits with status 1 on the first difference, and prints "03_modelling: OK" at the end. Edit the chapter, not this file.

Run it from anywhere: python examples/tutorial/03_modelling.py
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


# -- 03-modelling.md, line 22 ----------------------------------------
from proto_db import MemoryStorage, MutableObject, ObjectSpace

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")

with db.new_transaction() as tr:
    ann = tr.new_mutable("user:ann", tr.new_object(login="ann", name="Ann Lee"))
    bob = tr.new_mutable("user:bob", tr.new_object(login="bob", name="Bob Ruiz"))
    tr.set_root_object("groups", tr.new_dictionary().set_at("sales", tr.new_set().add("ann").add("bob")))
    tr.set_root_object("persons", tr.new_dictionary().set_at(7, tr.new_object(name="Pat Doe", company="Globex")))
    deal = tr.new_object(number=1, title="ERP licences", stage="lead", amount=12_000,
                         owner=ann,                      # an entity: the handle of the cell
                         contact=7,                      # a person, by key
                         changed_by="import", previous=None)
    tr.set_root_object("opportunities", tr.new_dictionary().set_at(1, deal))

def move(number, stage, who, **changes):
    """The next version of an opportunity: a new record that remembers the one it replaces."""
    def work(tr):
        deals = tr.get_root_object("opportunities")
        current = deals.get_at(number)
        revised = current.with_(previous=current, stage=stage, changed_by=who, **changes)
        tr.set_root_object("opportunities", deals.set_at(number, revised))
    db.run(work)

move(1, "qualified", "ann")
move(1, "proposal", "bob", amount=11_500)
with db.new_transaction() as tr:                         # a correction of the user, committed once
    tr.set_mutable("user:ann", tr.get_mutable("user:ann").with_(name="Ann Lee-Smith"))

tr = db.new_transaction()
deal = tr.get_root_object("opportunities").get_at(1)
print("owner now:", deal.owner.get(tr).name)
history = []
while deal is not None:
    history.append((deal.stage, deal.amount, deal.changed_by))
    deal = deal.previous
print("history:", list(reversed(history)))
tr.abort()
_expect("""
owner now: Ann Lee-Smith
history: [('lead', 12000, 'import'), ('qualified', 12000, 'ann'), ('proposal', 11500, 'bob')]
""")

# -- 03-modelling.md, line 85 ----------------------------------------
import erp_app

shop = space.new_database("shop")
erp_app.seed(shop)
tr = shop.new_transaction()
print(erp_app.pipeline(tr.get_root_object("opportunities")))
print(sorted(tr.get_root_object("groups").get_at("sales")), tr.get_mutable("user:bob").name)
tr.abort()
_expect("""
[('lead', 334, 1808821), ('proposal', 333, 1795500), ('qualified', 333, 1783179)]
['ann', 'bob'] Bob Ruiz
""")

# -- 03-modelling.md, line 152 ----------------------------------------
from decimal import Decimal

from proto_db.semantic import semantic

model = space.new_database("model")
model.run(lambda tr: tr.install_module("erp_model"))

def load(tr):
    s = semantic(tr)
    sales = s.create("erp.Group", name="sales")
    ann = s.create("erp.User", login="ann", name="Ann Lee")
    s.relate("erp.Membership", user=ann, group=sales, role="manager")
    pat = s.create("erp.Person", name="Pat Doe", company="Globex")
    for number, amount, stage in ((1, "12000", "proposal"), (2, "3000", "lead"), (3, "8000", "lost")):
        s.create("erp.Opportunity", number=number, title=f"deal {number}", amount=Decimal(amount),
                 stage=stage, probability=Decimal("0.5"), owner=ann, contact=pat)
    return ann

ann = model.run(load)                                 # the handle of Ann's cell

tr = model.new_transaction()
s = semantic(tr)
user = s.get(ann)
print(user.name, "| groups:", [(m.group.name, m.role) for m in user.memberships])
print("pipeline:", user.pipeline, "| expected:", sorted(o.expected for o in user.opportunities))
print("owner of each:", sorted((o.number, o.owner.login) for o in user.opportunities))
print(s.describe("erp.Opportunity")["meaning"])
tr.abort()
_expect("""
Ann Lee | groups: [('sales', 'manager')]
pipeline: 15000 | expected: [Decimal('1500.0'), Decimal('4000.0'), Decimal('6000.0')]
owner of each: [(1, 'ann'), (2, 'ann'), (3, 'ann')]
A possible sale to a customer, owned by a user, moving through stages
""")

# -- 03-modelling.md, line 196 ----------------------------------------
space.close()

print("03_modelling: OK")
