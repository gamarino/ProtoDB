# 7. History

Every commit is a version of the whole database, and old versions stay readable (chapter 6 of the
[complete tutorial](../06-time.md)). For a semantic database, a version holds more than data: it holds the
catalog and the module list of its moment. A semantic session over an old version reads that version's
entities **through that version's model**, so an as-of report is never old data read through today's rules.

## A history with a model change in it

```python
import base64
from decimal import Decimal

from proto_db import MemoryStorage, ObjectSpace, ProtoUserError
from proto_db.semantic import semantic

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
db.run(lambda tr: tr.install_module("erp_model"))

def load(tr):
    s = semantic(tr)
    ann = s.create("erp.User", login="ann", name="Ann Lee")
    pat = s.create("erp.Person", name="Pat Doe", company="Globex")
    deal = s.create("erp.Opportunity", number=1, title="ERP licences", amount=Decimal("12000"), owner=ann, contact=pat)
    return ann, deal

ann, deal = db.run(load)
db.run(lambda tr: semantic(tr).invoke(deal, "move_to", stage="qualified"))
db.run(lambda tr: tr.install_module("erp_labels"))                  # behaviour arrives
db.run(lambda tr: semantic(tr).invoke(deal, "move_to", stage="proposal"))
db.run(lambda tr: tr.install_module("erp_crm"))                     # and is refined
db.run(lambda tr: semantic(tr).update(deal, probability=Decimal("0.5")))

for entry in reversed(db.history()):                                # oldest first
    if entry.version < 2:                                           # the empty database, the bare install
        continue
    s = semantic(db.snapshot_at(entry))
    print(entry.version, s.get(deal).stage, s.catalog()["module_set"])
```

Output:

```text
2 lead ['base', 'erp_model']
3 qualified ['base', 'erp_model']
4 qualified ['base', 'erp_labels', 'erp_model']
5 proposal ['base', 'erp_labels', 'erp_model']
6 proposal ['base', 'erp_crm', 'erp_labels', 'erp_model']
7 proposal ['base', 'erp_crm', 'erp_labels', 'erp_model']
```

Each version records its module set (the catalog's `module_set`, sorted): `erp_labels` arrived at version 4,
`erp_crm` at version 6, and each version's entity reads as it was then.

## The model of the time

```python
def as_of(version):
    return db.snapshot_at(next(entry for entry in db.history() if entry.version == version))

for version in (5, 7):
    s = semantic(as_of(version))
    print(version, s.get(deal).stage, "|", s.get(deal).label())
tr = db.new_transaction()
print("now", semantic(tr).get(deal).label())
tr.abort()
```

Output:

```text
5 proposal | #1 ERP licences (proposal)
7 proposal | #1 ERP licences (proposal), expected 6000.0 USD
now #1 ERP licences (proposal), expected 6000.0 USD
```

At version 5 only `erp_labels` was installed, so the label is its plain form; from version 7 `erp_crm`'s
refinement applies, with that version's probability (0.5). The session over a snapshot resolves the methods with
the registry of the snapshot's version, as it resolves the fields with its catalog.

A record read from a snapshot without the semantic session resolves with the database's **current** modules,
unless the read happens inside `with snapshot:`. The block is explicit so that one as-of report never switches
the rest of the process to old behaviour:

```python
old = as_of(5)
record = old.get_mutable(deal.name)
print("today's modules: ", record.label())
with old:
    print("the modules of then:", old.get_mutable(deal.name).label())
```

Output:

```text
today's modules:  #1 ERP licences (proposal), expected 1200.0 USD
the modules of then: #1 ERP licences (proposal)
```

## The catalog of the time

`describe` over a snapshot describes that version's model. A report generator that asks the catalog which
fields exist gets the fields that existed then:

```python
db.run(lambda tr: tr.install_module("erp_forecast"))              # version 8: a new field
for version in (7, 8):
    attributes = [a["name"] for a in semantic(as_of(version)).describe("erp.Opportunity")["attributes"]]
    print(version, attributes[-3:])
print("close_date at version 7:", semantic(as_of(7)).get(deal).close_date)
```

Output:

```text
7 ['owner', 'contact', 'quotes']
8 ['contact', 'quotes', 'close_date']
close_date at version 7: None
```

`erp_forecast` (chapter 5) added `close_date` at version 8; version 7 does not describe it. One caution, shown
on the last line: a version reads an attribute its model does not declare as `None`, not as an error, so ask
`describe` before relying on an attribute in an as-of report.

## Reading a version again later

`version()` names the version a session read, as a number and a reference that `Database.snapshot_at` accepts
once decoded. Stored with a report, it lets anyone reread exactly what the report read. A session over a
snapshot is read-only:

```python
tr = db.new_transaction()
read = semantic(tr).version()
tr.abort()
db.run(lambda tr: semantic(tr).update(deal, probability=Decimal("0.9")))

again = semantic(db.snapshot_at(base64.b64decode(read["ref"])))
print("version", read["number"], "probability then:", again.get(deal).probability)
try:
    again.update(deal, probability=Decimal("0.1"))
except ProtoUserError:
    print("writing through a snapshot: ProtoUserError")
space.close()
```

Output:

```text
version 8 probability then: 0.5
writing through a snapshot: ProtoUserError
```

## History in the data, and history of the data

Two different things answer "what was it then?":

* **As-of reads** (this chapter) answer what the database said at a moment: any entity, any version, the model
  of that moment. They need nothing in the model.
* **Version binding** (chapter 2) records, *in* the data, which version of an entity a fact referred to: a
  quote keeps the contact as it was when it was sent, and a query of today reads it. Use it when the
  reference to the past is part of the meaning of the fact.

The topic tutorials go further: [extensible models](../../source/tutorials/extensible-models.rst), section 9,
on as-of reads with the registry; [modelling data](../../source/tutorials/modelling-data.rst) on versioned
values; and [the history-links page](../../source/design-guide/history-links.rst) of the design guide on why a
long history does not slow the current queries down.

Back to [the semantic model tutorial](README.md).
