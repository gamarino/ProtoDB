# 8. Extensible models and the runtime registry

An ERP grows by modules: a CRM module adds behaviour to the opportunities that a sales module declared, and a
regional module changes a default that both rely on. `proto_db` lets a module change a model **without
editing the module that defined it, and without rewriting stored records**. Two mechanisms do it: a record's
ordered **parents**, and a per-database list of installed **modules** whose extensions form the
**runtime registry**. One rule finds an attribute, whether it is data or behaviour: the record itself, then
its parents in C3 order, then the registry.

## Parents between records

A record may list other records as its parents. An attribute it does not hold itself is looked up there;
what it holds itself is its own:

```python
from proto_db import MemoryStorage, ObjectSpace

storage = MemoryStorage()
space = ObjectSpace(storage)
db = space.new_database("erp")

with db.new_transaction() as tr:
    globex = tr.new_object(company="Globex", city="Rosario", currency="ARS")
    pat = tr.new_object(parents=(globex,), name="Pat Doe", city="Funes")
    print(pat.name, pat.city, pat.company, pat.currency)         # own, own (shadows), inherited, inherited
    print(pat.has_defined_attr("city"), pat.has_defined_attr("company"), "company" in pat)
```

Output:

```text
Pat Doe Funes Globex ARS
True False True
```

Copying the company's fields into each person would work until the company is corrected: the copy cannot tell
its own fields from the copied ones. A parent keeps them apart. A miss returns `None`, as for any record; `in`
searches the parents, and `has_defined_attr` sees only the record's own attributes (which is also all that an
index or a query reads).

## A module adds behaviour

Behaviour lives in **registry modules**: ordinary Python modules that declare themselves with
`proto_db.registry.module` and **extend registry names**. The semantic model of chapter 3 declares the name
`erp.Opportunity`; the module `examples/tutorial/erp_labels.py` extends it:

```text
from proto_db.registry import module

labels = module("erp_labels", depends=("erp_model",))

@labels.extends("erp.Opportunity")
class Opportunity:
    currency = "ARS"                                  # a data default

    def label(self):                                  # a method
        return f"#{self.number} {self.title} ({self.stage})"
```

The class is a container for definitions; it is never instantiated. Opportunities created *before* the module
is installed get its behaviour as soon as it is:

```python
from decimal import Decimal

from proto_db.semantic import semantic

db.run(lambda tr: tr.install_module("erp_model"))

def load(tr):
    s = semantic(tr)
    ann = s.create("erp.User", login="ann", name="Ann Lee")
    pat = s.create("erp.Person", name="Pat Doe", company="Globex")
    return s.create("erp.Opportunity", number=1, title="ERP licences", amount=Decimal("12000"),
                    probability=Decimal("0.5"), owner=ann, contact=pat)

deal = db.run(load)
with db.new_transaction() as tr:
    print(tr.installed_modules(), "| has a label:", semantic(tr).get(deal).label is not None)

db.run(lambda tr: tr.install_module("erp_labels"))
with db.new_transaction() as tr:
    opportunity = semantic(tr).get(deal)
    print(tr.installed_modules())
    print(opportunity.label(), "|", opportunity.currency)
```

Output:

```text
('base', 'erp_model') | has a label: False
('base', 'erp_model', 'erp_labels')
#1 ERP licences (lead) | ARS
```

No record was rewritten. The database stores the module **names** (`installed_modules`) and the data; the
method exists only in the processes that import the module. A database used from another language maps each
module name to that language's own code.

## A later module refines an earlier one

`erp_crm` depends on `erp_labels`, overrides `label` and calls the definition it overrides with
`self._super()`; it also replaces the currency default:

```text
@crm.extends("erp.Opportunity")
class Opportunity:
    currency = "USD"

    def label(self):
        return f"{self._super().label()}, expected {self.expected} {self.currency}"
```

```python
other = ObjectSpace(storage)                          # another process, opened before erp_crm is installed
other_db = other.open_database("erp")
with other_db.new_transaction() as tr:
    print("other process:", semantic(tr).get(deal).label())

with db.new_transaction() as tr:
    before_crm = tr.snapshot_ref                      # remembered for the as-of read below

db.run(lambda tr: tr.install_module("erp_crm"))
with db.new_transaction() as tr:
    print("this process: ", semantic(tr).get(deal).label())
    print([extension.module for extension in tr.registry.chain("erp.Opportunity")])
```

Output:

```text
other process: #1 ERP licences (lead)
this process:  #1 ERP licences (lead), expected 6000.0 USD
['erp_crm', 'erp_labels', 'erp_model']
```

`registry.chain(name)` lists the extensions of a name, the module loaded last first. Loading follows the
declared dependencies, not the order of installation, so two databases that installed the same modules in
another order behave the same. `self._super().label()` reached `erp_labels`' definition with the same `self`,
and `self.currency`, looked up from the start, found the newest default. When several registry names are
parents of one another, the order is the C3 linearization, the one Python uses for classes; a hierarchy that
has none is refused, never resolved in some arbitrary order.

## Another process, and an explicit reload

The second `ObjectSpace` over the same storage stands for another process. It bound its registry to the
module list it found, before `erp_crm`. Now that the database lists one more module, it refuses to resolve
with a registry that no longer matches, and rebuilds only when asked:

```python
from proto_db import ProtoValidationError

with other_db.new_transaction() as tr:
    record = tr.get_mutable(deal.name)                # the stored record of the entity's cell
    print("own data needs no registry:", record.title)
    try:
        semantic(tr).get(deal).label()
    except ProtoValidationError as error:
        print("ProtoValidationError:", str(error).split(";")[0])

other.reload_registry("erp")
with other_db.new_transaction() as tr:
    print("after the reload:", semantic(tr).get(deal).label())
```

Output:

```text
own data needs no registry: ERP licences
ProtoValidationError: Database 'erp': registry built for modules ['base', 'erp_model', 'erp_labels'] but the database lists ['base', 'erp_model', 'erp_labels', 'erp_crm']
after the reload: #1 ERP licences (lead), expected 6000.0 USD
```

Transactions already running keep the registry they started with, and a rebuild never happens between two
lines of a running program: a mismatch is an error, and `ObjectSpace.reload_registry(database)` rebuilds when
the process is ready (after deploying the new module's code). A process that cannot import a listed module
still reads data, and fails at the first lookup that needs the module, naming it.

## As-of reads use the modules of their time

A version of the database records the modules installed then. Reading an old version through today's modules
would mix old data with new rules. A record read from a snapshot resolves with the database's current modules,
unless it is read inside `with snapshot:`, where the snapshot's objects resolve with the registry of their
own version; a semantic session over a snapshot always does:

```python
old = db.snapshot_at(before_crm)
record = old.get_mutable(deal.name)                   # the opportunity as stored in that version
print("old data, today's modules:  ", record.label())
with old:
    print("old data, the modules of then:", old.get_mutable(deal.name).label())
print("a semantic session over the snapshot:", semantic(old).get(deal).label())
space.close()
other.close()
```

Output:

```text
old data, today's modules:   #1 ERP licences (lead), expected 6000.0 USD
old data, the modules of then: #1 ERP licences (lead)
a semantic session over the snapshot: #1 ERP licences (lead)
```

The block is explicit on purpose: an as-of read for one report never switches the rest of the process to old
behaviour.

Depth: [extensible models](../source/tutorials/extensible-models.rst) (catalog objects, data defaults
overridden by the catalog and by instances, C3 shown on a diamond, every error the registry raises, what is
never persisted), [the registry in the design guide](../source/design-guide/registry.rst), and
[modules and composition in the semantic model tutorial](semantic-model/05-modules-and-composition.md).

Next: [9. Vector search](09-vector-search.md)
