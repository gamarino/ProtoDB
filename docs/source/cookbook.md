# ProtoDB Cookbook

Practical, copy-paste-ready recipes for common tasks.

## 1. Basic CRUD with Root Objects

Create a list, store it as a root, commit, then read it back.

```python
from proto_db.db_access import ObjectSpace, Database
from proto_db.memory_storage import MemoryStorage  # or StandaloneFileStorage

# Set up DB
storage = MemoryStorage()
os = ObjectSpace(storage)
db = Database(os, "example")

# Write
tr1 = db.new_transaction()
my_list = tr1.new_list()           # proto_db.lists.List
my_list = my_list.add(1).add(2)
tr1.set_root_object("numbers", my_list)
tr1.commit()

# Read in a new transaction
tr2 = db.new_transaction()
loaded = tr2.get_root_object("numbers")
assert list(loaded.as_iterable()) == [1, 2]
```

## 2. Working with Secondary Indexes

Add a secondary index to a collection and use it in a query plan.

```python
from proto_db.queries import WherePlan, Expression

tr = db.new_transaction()
people = tr.new_list()
people = people.add({"id": 1, "city": "NY"})
people = people.add({"id": 2, "city": "SF"})

# Add index on the 'city' field (applies to Lists and Dictionaries that support indexing)
people = people.add_index("city")

plan = WherePlan(base=people.as_query_plan(), expr=Expression(field="city", op="==", value="NY"))
print(plan.explain())  # Expect to see that an IndexedSearchPlan is used
result = list(plan.execute().as_iterable())
```

## 3. The Right Way to Use Sets and Hashing

### Recipe: Hashing Non-Atom Objects Safely

If you need to store non-Atom Python objects in a persistent Set, give them a stable, content-based `__hash__`.

```python
class UserKey:
    def __init__(self, username: str, tenant: str):
        self.username = username
        self.tenant = tenant

    def __eq__(self, other):
        return isinstance(other, UserKey) and (self.username, self.tenant) == (other.username, other.tenant)

    def __hash__(self):
        # Content-based hash is stable across sessions
        return hash((self.username, self.tenant))
```

- This avoids relying on `id()`-based identity.
- For Atoms, the Set will use their stable AtomPointer-derived hash automatically once persisted.

### Recipe: Working with Temporary Sets

Use Sets for intermediate work without persisting them.

```python
tr = db.new_transaction()
s1 = tr.new_hash_set().add(1).add(2)
s2 = tr.new_hash_set().add(2).add(3)

# Intermediate calculation
union_set = s1.union(s2)
assert set(union_set.as_iterable()) == {1, 2, 3}

# Discard: do not attach union_set to any root or persistent structure
tr.abort()  # No writes performed for the temporary Set
```

## 4. Querying: From Simple Filters to Aggregations

```python
from proto_db.queries import WherePlan, Expression, AndMerge, GroupByPlan

tr = db.new_transaction()
orders = tr.new_list()
orders = orders.add({"id": 1, "user": "alice", "amount": 30})
orders = orders.add({"id": 2, "user": "bob", "amount": 50})
orders = orders.add({"id": 3, "user": "alice", "amount": 20})

# Add indexes
orders = orders.add_index("user").add_index("amount")

# Simple filter
p1 = WherePlan(base=orders.as_query_plan(), expr=Expression(field="user", op="==", value="alice"))
print(p1.explain())
rows = list(p1.execute().as_iterable())

# AndMerge: combine two indexed filters (user == 'alice' AND amount == 20)
p2 = AndMerge(
    WherePlan(base=orders.as_query_plan(), expr=Expression(field="user", op="==", value="alice")),
    WherePlan(base=orders.as_query_plan(), expr=Expression(field="amount", op="==", value=20)),
)
print(p2.explain())
rows2 = list(p2.execute().as_iterable())

# GroupBy: aggregate total amount by user
group_plan = GroupByPlan(
    base=orders.as_query_plan(),
    key_field="user",
    agg={"total": ("amount", "sum")}
)
summary = list(group_plan.execute().as_iterable())
```
