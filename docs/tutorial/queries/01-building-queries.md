# 1. Building queries

A query starts with `from_collection(source)` and is refined by operators, each of which returns a **new**
query: a `Queryable` is immutable and lazy, and nothing runs until it is listed, counted, iterated or asked for
its first element. This chapter covers the conditions (`where` and the `F` builder), the projections
(`select`), the order and the window (`order_by`, `then_by`, `skip`, `take`) and the ways to run a query.

## The data

The ERP of the [complete tutorial](../README.md): 1,000 opportunities with `number`, `title`, `stage`,
`amount`, an `owner` (a user's cell handle) and a `contact` (a person's number), and 200 persons.

```python
from proto_db import F, MISSING, MemoryStorage, ObjectSpace, ProtoUserError, from_collection

import erp_app
from erp_app import plan_of

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
erp_app.seed(db)
tr = db.new_transaction()
deals = tr.get_root_object("opportunities")
print(len(deals), "opportunities; one of them:", deals.get_at(7).title, deals.get_at(7).stage, deals.get_at(7).amount)
```

Output:

```text
1000 opportunities; one of them: opportunity 7 qualified 1259
```

## Conditions

`F.name` names an attribute (`F.a.b` a nested path, `F()` the element itself). A comparison with a value
builds a predicate; `&`, `|` and `~` combine predicates:

```python
def numbers(query, limit=6):
    return [d.number for d in query.order_by(F.number).take(limit).to_list()]

queries = {
    "stage == 'won' (none yet)":         from_collection(deals).where(F.stage == "won"),
    "amount > 9990":                     from_collection(deals).where(F.amount > 9990),
    "stage in (lead, proposal)":         from_collection(deals).where(F.stage.in_(["lead", "proposal"])),
    "amount between 5000 and 5010":      from_collection(deals).where(F.amount.between(5000, 5010)),
    "lead and amount < 1100":            from_collection(deals).where((F.stage == "lead") & (F.amount < 1100)),
    "amount < 1010 or amount > 9990":    from_collection(deals).where((F.amount < 1010) | (F.amount > 9990)),
    "not a lead, amount < 1100":         from_collection(deals).where(~(F.stage == "lead") & (F.amount < 1100)),
}
for label, query in queries.items():
    print(f"{label:34} {query.count():4}  first: {numbers(query)}")
```

Output:

```text
stage == 'won' (none yet)             0  first: []
amount > 9990                         1  first: [243]
stage in (lead, proposal)           667  first: [0, 2, 3, 5, 6, 8]
amount between 5000 and 5010          1  first: [838]
lead and amount < 1100                4  first: [0, 489, 732, 975]
amount < 1010 or amount > 9990        3  first: [0, 243, 973]
not a lead, amount < 1100            10  first: [1, 2, 244, 245, 487, 488]
```

The other operators: `!=`, `<=`, `>=`, the `between` family (`between_open`, `between_left_open`,
`between_right_open`, or `range(low, high, "[)")`), `contains` (a substring of a string, or an item of a
list), `is_none` / `not_none` and `is_true` / `not_true`. In a query, an attribute that a record does not
have is `MISSING`, not `None` (reading it as `record.fax` still gives `None`, as chapter 2 of the complete
tutorial shows); the presence tests and the comparisons treat it differently:

```python
print("previous is None:", from_collection(deals).where(F.previous.is_none()).count(),
      "| fax is None:", from_collection(deals).where(F.fax.is_none()).count(),
      "| fax > 0:", from_collection(deals).where(F.fax > 0).count())
```

Output:

```text
previous is None: 1000 | fax is None: 1000 | fax > 0: 0
```

Every opportunity holds `previous=None`, and none has a `fax`. `is_none()` matches both an explicit `None`
and an absent attribute; a comparison is never true for an absent one. To tell the two apart on one record,
use `has_defined_attr`.

Two Python habits do not work, and both fail loudly instead of answering wrongly:

```python
try:
    (F.stage == "lead") and (F.amount > 5000)           # Python would silently keep only one side
except ProtoUserError as error:
    print("and:", type(error).__name__)
try:
    F.stage in ["lead", "won"]                          # compares each item and asks for a truth value
except ProtoUserError as error:
    print("in: ", type(error).__name__, "- write F.stage.in_([...])")
```

Output:

```text
and: ProtoUserError
in:  ProtoUserError - write F.stage.in_([...])
```

## Conditions the planner cannot read

`startswith`, `endswith`, the computed values (`lower()`, `upper()`, `length()`, `abs()`, arithmetic),
comparisons between two fields, and any Python callable still work. They run element by element in Python,
and the plan says so with a `PythonFilter` node:

```python
for label, query in [
    ("== a value (index)", from_collection(deals).where(F.amount == 9991)),
    ("startswith", from_collection(deals).where(F.title.startswith("opportunity 99"))),
    ("a lambda", from_collection(deals).where(lambda d: d.amount % 1000 == 0)),
    ("amount * 2 > 19900", from_collection(deals).where(F.amount * 2 > 19900)),
]:
    print(f"{label:20} {plan_of(query):32} {query.count()} rows")
```

Output:

```text
== a value (index)   IndexLookup                      1 rows
startswith           PythonFilter <- CollectionScan   11 rows
a lambda             PythonFilter <- CollectionScan   1 rows
amount * 2 > 19900   PythonFilter <- CollectionScan   5 rows
```

A `PythonFilter` cannot use an index and cannot be described by a pager token. Prefer `F` comparisons with a
value where the question allows it.

## Projections

`select` shapes each row: one path gives its value, several paths or a mapping give a dictionary, a function
gives its result.

```python
top = from_collection(deals).where(F.amount > 9950).order_by(F.number)
print(top.select(F.number).to_list())
print(top.select({"n": F.number, "stage": F.stage}).take(2).to_list())
print(top.select(lambda d: f"#{d.number}: {d.amount}").take(2).to_list())
print(from_collection(deals).where(F.number == 3).select(F.fax).to_list(), MISSING is not None)
```

Output:

```text
[242, 243, 486, 729, 972]
[{'n': 242, 'stage': 'proposal'}, {'n': 243, 'stage': 'lead'}]
['#242: 9954', '#243: 9991']
[MISSING] True
```

A path that a record does not have projects to `MISSING` (falsy, and distinct from `None`); in a mapping,
absent values are left out.

## Order and window

`order_by` sorts stably by one total order across types (`MISSING` first, then `None`, then the value ranks of
the [concepts](../../source/concepts.rst)); `then_by` breaks ties and must follow `order_by`; `skip` and
`take` cut a window:

```python
ranked = (from_collection(deals).where(F.stage == "proposal")
          .order_by(F.amount, descending=True).then_by(F.number))
print([(d.number, d.amount) for d in ranked.take(3).to_list()])
print([(d.number, d.amount) for d in ranked.skip(3).take(3).to_list()])
try:
    from_collection(deals).where(F.amount > 0).then_by(F.number)
except ProtoUserError as error:
    print(error)
```

Output:

```text
[(242, 9954), (485, 9945), (728, 9936)]
[(971, 9927), (239, 9843), (482, 9834)]
then_by must directly follow order_by or then_by
```

## Running a query

`to_list()`, iteration, `count()`, `first()` and `first_or_none()` run it; each run executes the plan again on
the same source, so a query object can be kept and rerun:

```python
proposals = from_collection(deals).where(F.stage == "proposal").order_by(F.amount)
print(proposals.count(), proposals.first().number, proposals.first_or_none(lambda d: d.amount > 99_999))
print(sum(1 for _ in proposals), proposals.count(lambda d: d.amount >= 9000))
print(proposals)
```

Output:

```text
333 974 None
333 36
Queryable(Dictionary, ['where', 'order'])
```

## Plain Python data

`from_collection` takes any iterable as well, copied once into a scan (never indexed). A dictionary is
refused: iterate `.items()` or `.values()` explicitly.

```python
rows = [{"login": "ann", "deals": 334}, {"login": "bob", "deals": 333}, {"login": "cy", "deals": 333}]
print(from_collection(rows).where(F.deals > 333).select(F.login).to_list())
tr.abort()
space.close()
```

Output:

```text
['ann']
```

Next: [2. Aggregates, joins and relations](02-aggregates-and-joins.md)
