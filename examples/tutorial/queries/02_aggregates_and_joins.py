#!/usr/bin/env python3
"""2. Aggregates, joins and relations

Generated from docs/tutorial/queries/02-aggregates-and-joins.md by ``python tests/docs/test_markdown_tutorials.py --write-scripts``:
the code below is the chapter's code blocks, verbatim and in order. After the blocks of each output the chapter
shows, ``_expect`` compares what they printed with it, so the script checks every figure the chapter shows,
exits with status 1 on the first difference, and prints "02_aggregates_and_joins: OK" at the end. Edit the chapter, not this file.

Run it from anywhere: python examples/tutorial/queries/02_aggregates_and_joins.py
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


# -- 02-aggregates-and-joins.md, line 7 ----------------------------------------
from proto_db import Agg, F, MemoryStorage, ObjectSpace, from_collection

import erp_app
from erp_app import plan_of

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
erp_app.seed(db)
tr = db.new_transaction()
deals, persons, groups = (tr.get_root_object(name) for name in ("opportunities", "persons", "groups"))
print(len(deals), "opportunities,", len(persons), "persons,", len(groups), "groups")
_expect("""
1000 opportunities, 200 persons, 3 groups
""")

# -- 02-aggregates-and-joins.md, line 33 ----------------------------------------
by_stage = (from_collection(deals)
            .group_by(F.stage, {"stage": Agg.key(), "n": Agg.count(), "total": Agg.sum(F.amount),
                                "average": Agg.avg(F.amount), "smallest": Agg.min(F.amount)})
            .order_by(F.stage))
for row in by_stage.to_list():
    print(row)
print(plan_of(by_stage))
_expect("""
{'stage': 'lead', 'n': 334, 'total': 1808821, 'average': 5415.631736526946, 'smallest': 1000}
{'stage': 'proposal', 'n': 333, 'total': 1795500, 'average': 5391.891891891892, 'smallest': 1038}
{'stage': 'qualified', 'n': 333, 'total': 1783179, 'average': 5354.891891891892, 'smallest': 1001}
Sort <- Project <- GroupBy <- CollectionScan
""")

# -- 02-aggregates-and-joins.md, line 56 ----------------------------------------
big_contacts = (from_collection(deals)
                .group_by(F.contact, {"contact": Agg.key(), "total": Agg.sum(F.amount), "numbers": Agg.collect(F.number)})
                .where(F.total >= 32_500).order_by(F.total, descending=True))
print(big_contacts.to_list())
first_group = from_collection(deals).where(F.amount > 9980).group_by(F.stage).order_by(F.key).first()
print(first_group["key"], [d.number for d in first_group["items"]])
_expect("""
[{'contact': 43, 'total': 32955, 'numbers': (43, 243, 443, 643, 843)}, {'contact': 42, 'total': 32770, 'numbers': (42, 242, 442, 642, 842)}, {'contact': 41, 'total': 32585, 'numbers': (41, 241, 441, 641, 841)}]
lead [486, 243]
""")

# -- 02-aggregates-and-joins.md, line 78 ----------------------------------------
large = from_collection(deals).where(F.amount >= 9950).order_by(F.number)
pairs = large.join(persons, F.contact, F.number).select(lambda pair: (pair[0].number, pair[1].name, pair[1].company))
print(pairs.to_list())
print(plan_of(pairs))

with_owner = large.join(list(erp_app.USERS), lambda d: d.owner.name.split(":")[1], F(), kind="left")
print([(d.number, login) for d, login in with_owner.to_list()][:3])
_expect("""
[(242, 'person 42', 'company 2'), (243, 'person 43', 'company 3'), (486, 'person 86', 'company 6'), (729, 'person 129', 'company 9'), (972, 'person 172', 'company 12')]
Project <- Join <- Sort <- IndexRange
[(242, 'cy'), (243, 'ann'), (486, 'ann')]
""")

# -- 02-aggregates-and-joins.md, line 106 ----------------------------------------
rows = (from_collection(deals).where(F.amount >= 9960).order_by(F.number)
        .select(lambda d: (d.number, d.owner.get(tr).name, persons.get_at(d.contact).company)))
print(rows.to_list())

per_owner = (from_collection(deals).where(F.amount >= 9000)
             .group_by(F.owner, {"owner": Agg.key(), "n": Agg.count()}).to_list())
print(sorted((row["owner"].get(tr).name, row["n"]) for row in per_owner))
_expect("""
[(243, 'Ann Lee', 'company 3'), (486, 'Ann Lee', 'company 6'), (729, 'Ann Lee', 'company 9'), (972, 'Ann Lee', 'company 12')]
[('Ann Lee', 36), ('Bob Ruiz', 36), ('Cy Park', 36)]
""")

# -- 02-aggregates-and-joins.md, line 133 ----------------------------------------
memberships = (from_collection(groups.items())
               .select_many(lambda item: [(item[0], login) for login in sorted(item[1])]))
print(memberships.to_list())
print(from_collection(groups.values()).select_many(F()).distinct().order_by(F()).to_list())
tr.abort()
space.close()
_expect("""
[('managers', 'ann'), ('sales', 'ann'), ('sales', 'bob'), ('support', 'cy')]
['ann', 'bob', 'cy']
""")

print("02_aggregates_and_joins: OK")
