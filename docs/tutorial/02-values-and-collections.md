# 2. Objects and immutable collections

Everything `proto_db` stores is immutable. A change makes a new version that shares what did not change with
the old one. This chapter shows the records and the collections the ERP uses, what "a change returns a
new collection" means in practice, what it costs on disk, and why the database is built this way.

## Records

A record is a `DBObject`: named attributes, immutable. It holds the canonical values (`None`, `bool`, `int`,
`float`, `Decimal`, `str`, `bytes`, dates and times, `UUID`, `tuple`, `list`, `dict`), other records and
collections. Anything else is refused when the record is built, not when it is committed.

```python
from decimal import Decimal

from proto_db import MemoryStorage, ObjectSpace, ProtoUserError, ProtoValidationError

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")

with db.new_transaction() as tr:
    pat = tr.new_object(name="Pat Doe", company="Globex", phones=["555-0100"], credit=Decimal("2500.00"))
    print(pat.name, pat.company, pat.phones, pat.credit)
    try:
        pat.company = "Initech"
    except ProtoUserError as error:
        print(error)
    moved = pat.with_(company="Initech")
    print(pat.company, "->", moved.company)
    try:
        pat.phones.append("555-0199")             # a stored list is frozen too
    except ProtoValidationError:
        print("the phone list is frozen")
    try:
        tr.new_object(tags={"vip"})               # a Python set is not a value; use tr.new_set()
    except ProtoValidationError as error:
        print("refused:", error)
```

Output:

```text
Pat Doe Globex ['555-0100'] 2500.00
DBObject is immutable; use with_(company=...) to create a new version
Globex -> Initech
the phone list is frozen
refused: Field 'tags' of proto_db.core.dbobject.DBObject: Unsupported value type: set
```

`with_` returns a new record with the changes and leaves the original as it was. A missing attribute reads as
`None` (`pat.fax` is `None`); `has_defined_attr("fax")` tells "absent" from "set to `None`".

## The collections

| Collection | In the ERP | What it gives |
|---|---|---|
| `Dictionary` | persons by number, opportunities by number, groups by name | a map ordered by key; ordered iteration and key ranges |
| `Set` | the logins of a group | membership, `union`, `intersection`, `difference` |
| `List` | an activity log | positions, `append_last`, `insert_at`, `slice` |
| `HashDictionary` | permissions by `(login, group)` | a map whose keys may be any value, tuples included |
| `CountedSet` | how many opportunities reached each stage | a multiset: `count_of`, `total_count` |
| `RepeatedKeysDictionary` | several records under one key | a multimap |

```python
with db.new_transaction() as tr:
    persons = tr.new_dictionary()
    for number, (name, company) in enumerate([("Pat Doe", "Globex"), ("Lou Ray", "Initech"), ("Kim Wu", "Globex")]):
        persons = persons.set_at(number, tr.new_object(name=name, company=company))
    groups = (tr.new_dictionary()
              .set_at("sales", tr.new_set().add("ann").add("bob"))
              .set_at("support", tr.new_set().add("cy")))
    log = tr.new_list().append_last("import").append_last("first review")
    grants = tr.new_hash_dictionary().set_at(("ann", "sales"), "manager").set_at(("bob", "sales"), "member")
    reached = tr.new_counted_set().add("lead").add("lead").add("qualified")
    for name, value in (("persons", persons), ("groups", groups), ("log", log), ("grants", grants),
                        ("reached", reached)):
        tr.set_root_object(name, value)

tr = db.new_transaction()
persons, groups = tr.get_root_object("persons"), tr.get_root_object("groups")
print([p.name for p in persons.values()])
print(sorted(groups.get_at("sales")), "ann" in groups.get_at("sales"), "cy" in groups.get_at("sales"))
print(list(tr.get_root_object("log")), tr.get_root_object("grants").get_at(("ann", "sales")))
reached = tr.get_root_object("reached")
print(reached.count_of("lead"), reached.count_of("qualified"), reached.total_count)
tr.abort()
```

Output:

```text
['Pat Doe', 'Lou Ray', 'Kim Wu']
['ann', 'bob'] True False
['import', 'first review'] manager
2 1 3
```

## A change returns a new collection

Every mutator (`set_at`, `remove_at`, `add`, `append_last`, ...) returns a new collection and leaves its
receiver untouched. Whoever still holds the old one, a variable, another record, an open transaction, keeps
seeing exactly what it saw:

```python
reader = db.new_transaction()                         # opened before the change

with db.new_transaction() as tr:
    groups = tr.get_root_object("groups")
    sales = groups.get_at("sales")
    bigger = sales.add("dee")
    print(sorted(sales), sorted(bigger))
    tr.set_root_object("groups", groups.set_at("sales", bigger))

print("the reader still sees", sorted(reader.get_root_object("groups").get_at("sales")))
reader.abort()
tr = db.new_transaction()
print("a new transaction sees", sorted(tr.get_root_object("groups").get_at("sales")))
tr.abort()
```

Output:

```text
['ann', 'bob'] ['ann', 'bob', 'dee']
the reader still sees ['ann', 'bob']
a new transaction sees ['ann', 'bob', 'dee']
```

The group changed by **assigning** a new version: the new set into the dictionary, the new dictionary into
the root. A version you derive and never assign publishes nothing.

## Structural sharing: what a new version costs

A new version is not a copy. The collections are balanced trees, and a change copies only the path from the
changed leaf to the root (path copying); every other subtree is shared, by pointer, between the old version
and the new one. On disk, an edit therefore writes a few nodes, not the collection. The block measures it on
a space of 10,000 persons:

```python
import os
import tempfile

from proto_db import FileStorage

os.chdir(tempfile.mkdtemp(prefix="proto_db-tutorial-"))   # a fresh directory: files stay out of yours

def directory_bytes(path):
    return sum(os.path.getsize(os.path.join(path, name)) for name in os.listdir(path))

disk = ObjectSpace(FileStorage("persons-space"))
people_db = disk.new_database("erp")
with people_db.new_transaction() as tr:
    persons = tr.new_dictionary()
    for number in range(10_000):
        persons = persons.set_at(number, tr.new_object(name=f"person {number}", company=f"company {number % 50}"))
    tr.set_root_object("persons", persons)
loaded = directory_bytes("persons-space")

with people_db.new_transaction() as tr:
    persons = tr.get_root_object("persons")
    tr.set_root_object("persons", persons.set_at(4_242, persons.get_at(4_242).with_(company="Globex")))
edit = directory_bytes("persons-space") - loaded
print(f"10,000 persons: {loaded:,} bytes; one edit: {edit:,} bytes ({100 * edit / loaded:.2f} %)")
assert edit < loaded / 50                             # the edit wrote a path, not the collection
disk.close()
```

Sample output (one run on the machine that built this page, an AMD Ryzen 5 5500U, CPython 3.14.6 free-threaded;
the byte counts vary slightly from run to run, because each version stores its commit time):

```text
10,000 persons: 2,257,332 bytes; one edit: 3,249 bytes (0.14 %)
```

The edit cost about the height of the tree, not the size of the collection. It is also
permanent: nothing is ever deleted, so a space grows with every edit it keeps.

## Why values are immutable

Immutability is the first design principle of `proto_db`, and most of what the following chapters show
follows from it:

* **Snapshots are free.** A transaction's base is a pointer to one version; nothing can change under it, so
  readers never block writers and never see half of a commit (chapter 5).
* **History is free.** Old versions are not overwritten, so "as of last Tuesday" is a read, and the chain
  of versions of a record is its audit trail (chapter 6).
* **Caches never go stale.** A pointer names one content forever, so a decoded object can be shared by every
  transaction of the process, in every thread, with no invalidation.
* **Indexes cannot drift.** An index is part of its collection, rebuilt along the same path as the change, so
  no version of a collection has an index that disagrees with it (chapter 4).

The price is that a change is a new version, with the write cost measured above, and that a space keeps what
it wrote.

```python
space.close()
```

Depth: [modelling data](../source/tutorials/modelling-data.rst) (records, collections, values and entities),
[immutability in the design guide](../source/design-guide/immutability.rst).

Next: [3. Modelling](03-modelling.md)
