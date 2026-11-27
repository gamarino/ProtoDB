#!/usr/bin/env python3
"""3. Computed attributes and aggregates

Generated from docs/tutorial/semantic-model/03-computed-and-aggregates.md by ``python tests/docs/test_markdown_tutorials.py --write-scripts``:
the code below is the chapter's code blocks, verbatim and in order. After the blocks of each output the chapter
shows, ``_expect`` compares what they printed with it, so the script checks every figure the chapter shows,
exits with status 1 on the first difference, and prints "03_computed_and_aggregates: OK" at the end. Edit the chapter, not this file.

Run it from anywhere: python examples/tutorial/semantic/03_computed_and_aggregates.py
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


# -- 03-computed-and-aggregates.md, line 9 ----------------------------------------
from decimal import Decimal

from proto_db import MemoryStorage, ObjectSpace, ProtoConflictError
from proto_db.semantic import semantic

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
db.run(lambda tr: tr.install_module("erp_model"))

def load(tr):
    s = semantic(tr)
    ann = s.create("erp.User", login="ann", name="Ann Lee")
    pat = s.create("erp.Person", name="Pat Doe", company="Globex")
    deals = [s.create("erp.Opportunity", number=n, title=f"deal {n}", amount=Decimal(amount), stage=stage,
                      probability=Decimal(p), owner=ann, contact=pat)
             for n, amount, stage, p in ((1, "12000", "proposal", "0.5"), (2, "3000", "lead", "0.1"),
                                         (3, "8000", "lost", "0"))]
    return ann, pat, deals

ann, pat, (first, second, third) = db.run(load)

def show(label):
    tr = db.new_transaction()
    s = semantic(tr)
    user = s.get(ann)
    print(f"{label:28} pipeline {user.pipeline:>6} | expected {sorted(o.expected for o in user.opportunities)}")
    tr.abort()

show("after the load")
_expect("""
after the load               pipeline  15000 | expected [Decimal('0'), Decimal('300.0'), Decimal('6000.0')]
""")

# -- 03-computed-and-aggregates.md, line 54 ----------------------------------------
tr = db.new_transaction()
expected = next(a for a in semantic(tr).describe("erp.Opportunity")["attributes"] if a["name"] == "expected")
print(expected["kind"], expected["expression"]["ast"])
tr.abort()
_expect("""
computed ['*', ['attr', 'amount'], ['attr', 'probability']]
""")

# -- 03-computed-and-aggregates.md, line 76 ----------------------------------------
db.run(lambda tr: semantic(tr).update(first, amount=Decimal("15000")))
show("first deal raised to 15000")
db.run(lambda tr: semantic(tr).update(second, stage="lost"))
show("second deal lost")
db.run(lambda tr: semantic(tr).update(third, stage="qualified"))
show("third deal revived")

def unassign(tr):
    s = semantic(tr)
    s.unlink(third, "owner", ann)
    bob = s.create("erp.User", login="bob", name="Bob Ruiz")
    s.link(third, "owner", bob)
    return bob

bob = db.run(unassign)
show("third deal handed to bob")
tr = db.new_transaction()
print("bob's pipeline:", semantic(tr).get(bob).pipeline, "| stored on ann's record:",
      semantic(tr).get(ann).record.has_defined_attr("pipeline"))
tr.abort()
_expect("""
first deal raised to 15000   pipeline  18000 | expected [Decimal('0'), Decimal('300.0'), Decimal('7500.0')]
second deal lost             pipeline  15000 | expected [Decimal('0'), Decimal('300.0'), Decimal('7500.0')]
third deal revived           pipeline  23000 | expected [Decimal('0'), Decimal('300.0'), Decimal('7500.0')]
third deal handed to bob     pipeline  15000 | expected [Decimal('300.0'), Decimal('7500.0')]
bob's pipeline: 8000 | stored on ann's record: True
""")

# -- 03-computed-and-aggregates.md, line 120 ----------------------------------------
writers = [db.new_transaction() for _ in range(2)]
semantic(writers[0]).update(first, amount=Decimal("16000"))
semantic(writers[1]).update(second, amount=Decimal("3500"))
writers[0].commit()
try:
    writers[1].commit()
except ProtoConflictError as error:
    print("second commit:", error.unit.kind.name, "on", error.unit.key.split("#")[0])

attempts = []
def raise_second(tr):
    attempts.append(1)
    semantic(tr).update(second, amount=Decimal("3500"))
db.run(raise_second)
print("through Database.run:", len(attempts), "attempt")
show("both changes in")
space.close()
_expect("""
second commit: CELL on erp.User
through Database.run: 1 attempt
both changes in              pipeline  16000 | expected [Decimal('350.0'), Decimal('8000.0')]
""")

print("03_computed_and_aggregates: OK")
