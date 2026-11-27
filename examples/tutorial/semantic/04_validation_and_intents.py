#!/usr/bin/env python3
"""4. Validation and intents

Generated from docs/tutorial/semantic-model/04-validation-and-intents.md by ``python tests/docs/test_markdown_tutorials.py --write-scripts``:
the code below is the chapter's code blocks, verbatim and in order. After the blocks of each output the chapter
shows, ``_expect`` compares what they printed with it, so the script checks every figure the chapter shows,
exits with status 1 on the first difference, and prints "04_validation_and_intents: OK" at the end. Edit the chapter, not this file.

Run it from anywhere: python examples/tutorial/semantic/04_validation_and_intents.py
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


# -- 04-validation-and-intents.md, line 8 ----------------------------------------
import re
from decimal import Decimal

from proto_db import MemoryStorage, ObjectSpace, ProtoConstraintError, ProtoValidationError
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

def refused(action):
    """Run ``action`` on a session of a new transaction; the refusal, with the random part of cell names hidden."""
    tr = db.new_transaction()
    try:
        action(semantic(tr))
        tr.commit()
        return "accepted"
    except (ProtoValidationError, ProtoConstraintError) as error:
        if tr.is_active:                                      # a refused operation; a refused commit ended it
            tr.abort()
        return f"{type(error).__name__}: " + re.sub(r"#[0-9a-f]{32}", "#...", str(error))

# -- 04-validation-and-intents.md, line 43 ----------------------------------------
cases = {
    "a required field missing": lambda s: s.create("erp.Opportunity", number=2, amount=Decimal(1), owner=ann, contact=pat),
    "a name the type does not declare": lambda s: s.update(deal, colour="red"),
    "a computed attribute written": lambda s: s.update(deal, expected=Decimal(5)),
    "a probability of 2": lambda s: s.update(deal, probability=Decimal(2)),
}
for label, action in cases.items():
    print(f"{label:34}", refused(action))
_expect("""
a required field missing           ProtoValidationError: create('erp.Opportunity'): 'erp.Opportunity' has the required fields ['title'] with no value; required fields are given at creation
a name the type does not declare   ProtoValidationError: update('erp.Opportunity#...'): 'erp.Opportunity' has no attribute or role 'colour'
a computed attribute written       ProtoValidationError: update('erp.Opportunity#...'): 'expected' of 'erp.Opportunity' is computed; it is never written
a probability of 2                 ProtoConstraintError: The probability 2 is not between 0 and 1
""")

# -- 04-validation-and-intents.md, line 83 ----------------------------------------
def linked_later(s):
    second = s.create("erp.Opportunity", number=2, title="CRM", amount=Decimal("3000"))
    s.link(second, "owner", ann)
    s.link(second, "contact", pat)

print("linked in the same transaction:", refused(linked_later))
print("an owner removed:              ", refused(lambda s: s.unlink(deal, "owner", ann)))
_expect("""
linked in the same transaction: accepted
an owner removed:               ProtoConstraintError: The opportunity erp.Opportunity#... has no owner (cardinality one)
""")

# -- 04-validation-and-intents.md, line 118 ----------------------------------------
def advance(tr):
    s = semantic(tr)
    s.get(deal).move_to("proposal")                           # a call on a version
    return s.invoke(deal, "move_to", stage="won").stage       # the same, by name

print("now:", db.run(advance))
print("once won:", refused(lambda s: s.invoke(deal, "move_to", stage="lead")))
print("no such intent:", refused(lambda s: s.invoke(deal, "close")))
_expect("""
now: won
once won: ProtoConstraintError: The intent 'move_to' does not apply to 'erp.Opportunity#...': erp.Opportunity.move_to.requires is false on its current version (Move the opportunity to another stage; a won or lost opportunity is closed)
no such intent: ProtoValidationError: The intent 'close' of 'erp.Opportunity#...': 'erp.Opportunity' has no intent named 'close'
""")

# -- 04-validation-and-intents.md, line 145 ----------------------------------------
print("a direct update:", refused(lambda s: s.update(deal, stage="lead")))
space.close()
_expect("""
a direct update: accepted
""")

print("04_validation_and_intents: OK")
