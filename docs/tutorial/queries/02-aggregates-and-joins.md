# 2. Aggregates, joins and relations

This chapter groups and aggregates, joins two collections, follows relations between records, and flattens
nested collections. These operators run after the planner's access path: the planner chooses how to read
(chapter 3), and the operators of this chapter are plan nodes stacked on top of that read.

```python
from proto_db import Agg, F, MemoryStorage, ObjectSpace, from_collection

import erp_app
from erp_app import plan_of

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
erp_app.seed(db)
tr = db.new_transaction()
deals, persons, groups = (tr.get_root_object(name) for name in ("opportunities", "persons", "groups"))
print(len(deals), "opportunities,", len(persons), "persons,", len(groups), "groups")
```

Output:

```text
1000 opportunities, 200 persons, 3 groups
```

## Grouping and aggregating

`group_by(key, aggregations)` yields one dictionary per group, in first-seen order. The aggregations are
`Agg.key()` (the group's key), `Agg.count()`, `Agg.sum(path)`, `Agg.avg(path)`, `Agg.min(path)`,
`Agg.max(path)` and `Agg.collect(path)`:

```python
by_stage = (from_collection(deals)
            .group_by(F.stage, {"stage": Agg.key(), "n": Agg.count(), "total": Agg.sum(F.amount),
                                "average": Agg.avg(F.amount), "smallest": Agg.min(F.amount)})
            .order_by(F.stage))
for row in by_stage.to_list():
    print(row)
print(plan_of(by_stage))
```

Output:

```text
{'stage': 'lead', 'n': 334, 'total': 1808821, 'average': 5415.631736526946, 'smallest': 1000}
{'stage': 'proposal', 'n': 333, 'total': 1795500, 'average': 5391.891891891892, 'smallest': 1038}
{'stage': 'qualified', 'n': 333, 'total': 1783179, 'average': 5354.891891891892, 'smallest': 1001}
Sort <- Project <- GroupBy <- CollectionScan
```

A group is a row like any other, so a `where` after `group_by` filters groups (SQL's `HAVING`), and
`order_by` sorts them. Without aggregations, each group is `{"key": k, "items": (...)}`, its items in the order
the plan read them (here by amount, from the range index):

```python
big_contacts = (from_collection(deals)
                .group_by(F.contact, {"contact": Agg.key(), "total": Agg.sum(F.amount), "numbers": Agg.collect(F.number)})
                .where(F.total >= 32_500).order_by(F.total, descending=True))
print(big_contacts.to_list())
first_group = from_collection(deals).where(F.amount > 9980).group_by(F.stage).order_by(F.key).first()
print(first_group["key"], [d.number for d in first_group["items"]])
```

Output:

```text
[{'contact': 43, 'total': 32955, 'numbers': (43, 243, 443, 643, 843)}, {'contact': 42, 'total': 32770, 'numbers': (42, 242, 442, 642, 842)}, {'contact': 41, 'total': 32585, 'numbers': (41, 241, 441, 641, 841)}]
lead [486, 243]
```

## Joins

`join(other, left_key, right_key, kind)` is an equi-join: it yields `(left, right)` pairs whose keys are equal.
`kind="left"` keeps a left element without a match, paired with `MISSING`. An opportunity's `contact` is a
person's `number`:

```python
large = from_collection(deals).where(F.amount >= 9950).order_by(F.number)
pairs = large.join(persons, F.contact, F.number).select(lambda pair: (pair[0].number, pair[1].name, pair[1].company))
print(pairs.to_list())
print(plan_of(pairs))

with_owner = large.join(list(erp_app.USERS), lambda d: d.owner.name.split(":")[1], F(), kind="left")
print([(d.number, login) for d, login in with_owner.to_list()][:3])
```

Output:

```text
[(242, 'person 42', 'company 2'), (243, 'person 43', 'company 3'), (486, 'person 86', 'company 6'), (729, 'person 129', 'company 9'), (972, 'person 172', 'company 12')]
Project <- Join <- Sort <- IndexRange
[(242, 'cy'), (243, 'ann'), (486, 'ann')]
```

The right side can be any source: a collection, a query or a plain list. A join is a hash join: each run reads
the whole right side once into a table, then streams the left side through it, in the left side's order. It
does not use the right side's indexes, so filter the right side when you can, and put the smaller side on the
right.

## Following relations

In the plain model a relation is an attribute that names another record: a key (`contact`), or the handle of
an entity's cell (`owner`). Following it is a read, and the handle reads the entity's **current** value:

```python
rows = (from_collection(deals).where(F.amount >= 9960).order_by(F.number)
        .select(lambda d: (d.number, d.owner.get(tr).name, persons.get_at(d.contact).company)))
print(rows.to_list())

per_owner = (from_collection(deals).where(F.amount >= 9000)
             .group_by(F.owner, {"owner": Agg.key(), "n": Agg.count()}).to_list())
print(sorted((row["owner"].get(tr).name, row["n"]) for row in per_owner))
```

Output:

```text
[(243, 'Ann Lee', 'company 3'), (486, 'Ann Lee', 'company 6'), (729, 'Ann Lee', 'company 9'), (972, 'Ann Lee', 'company 12')]
[('Ann Lee', 36), ('Bob Ruiz', 36), ('Cy Park', 36)]
```

Grouping by the handle groups by entity identity, however often the user's record changes, and an
`EqualityIndex` on the handle answers "the opportunities of this user" (chapter 3). In the semantic model a
relation is declared with its inverse and navigable from both sides; its queries are in the
[semantic model tutorial](../semantic-model/02-entities-and-relations.md).

## Flattening

`select_many(path)` flattens a list, a tuple or a collection held by each element; with `with_parent=True` it
yields `(parent, item)` pairs. `distinct()` keeps the first occurrence of each value:

```python
memberships = (from_collection(groups.items())
               .select_many(lambda item: [(item[0], login) for login in sorted(item[1])]))
print(memberships.to_list())
print(from_collection(groups.values()).select_many(F()).distinct().order_by(F()).to_list())
tr.abort()
space.close()
```

Output:

```text
[('managers', 'ann'), ('sales', 'ann'), ('sales', 'bob'), ('support', 'cy')]
['ann', 'bob', 'cy']
```

Next: [3. Indexes and the planner](03-indexes-and-the-planner.md)
