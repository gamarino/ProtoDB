Extensible models: parents, modules and the runtime registry
============================================================

An application that is extended by modules needs models that a module can change without editing the
module that defined them: a CRM module adds behaviour to the partners that a contacts module defined, and a
pricing module changes a default that both rely on. ``proto_db`` gives every ``DBObject`` ordered **parents**,
and a per-database list of installed **modules** whose extensions form an in-memory **registry**. One rule
finds an attribute, whether it is data or behaviour: the object, then its parents in C3 order, then the
registry. Every code block on this page runs in the test suite, and every ``Output`` was printed by the block
above it.

Each section follows the same order: what the obvious solution would be, why it fails (shown by running it),
and what ``proto_db`` does instead, with its cost. The design is explained in
:doc:`../design-guide/registry`; the decisions are D62 in ``docs/decisions.md``.

1. Plain data objects are unchanged
-----------------------------------

Nothing in this tutorial is needed by an application that does not use it. A ``DBObject`` without parents, in
a database that never installed a module, has the API, the bytes on disk and the behaviour it had before the
registry existed. A read of an attribute that was never set returns ``None``, as it always did.

.. code-block:: python

   from proto_db import MemoryStorage, ObjectSpace

   storage = MemoryStorage()
   space = ObjectSpace(storage)
   db = space.new_database("erp")

   with db.new_transaction() as tr:
       ana = tr.new_object(name="ana", email="ana@example.com")
       tr.set_root_object("ana", ana)
       print(ana.name, ana.email, ana.phone)
       print(ana.parents, tr.installed_modules())

.. code-block:: text
   :caption: Output

   ana ana@example.com None
   () ()

``ana.parents`` is empty and the database lists no modules. Such a record is written in format version 4,
byte for byte as before (:doc:`../format-spec`, "Format 5"): a reader that predates the registry still reads
it.

2. Parents between objects
--------------------------

The obvious way to share data between records is to copy it: the fields of a shared record (a company's name
and city) are copied into each record that needs them, and the record then sets the ones it overrides. It
works until the shared record is corrected. The copy cannot tell which of its fields were copied and which
were its own, so refreshing it overwrites its own choices:

.. code-block:: python

   with db.new_transaction() as tr:
       company = tr.new_object(company="ACME", city="Rosario")
       carl = tr.new_object(name="carl", company=company.company, city=company.city).with_(city="Funes")
       corrected = company.with_(company="ACME Ltd")
       refreshed = carl.with_(company=corrected.company, city=corrected.city)   # re-copy the shared fields
       print(refreshed.company, refreshed.city)

.. code-block:: text
   :caption: Output

   ACME Ltd Rosario

``carl`` lived in Funes, and the refresh moved him back to Rosario. The alternative is to *refer* to the
shared record and to look a missing attribute up there. A ``DBObject`` may list other objects as its
parents; an attribute it does not hold itself is looked up along them, and what it holds itself is its own.

.. code-block:: python

   with db.new_transaction() as tr:
       company = tr.new_object(company="ACME", city="Rosario", fax=None)
       bob = tr.new_object(parents=(company,), name="bob", city="Funes")
       print(bob.name, bob.city, bob.company)          # own, own (shadows the parent's), inherited
       print(bob["company"])                           # the item form reads the same thing
       field = "ci" + "ty"
       print(bob[field])                               # ... so the name may be computed at run time
       tr.set_root_object("bob", bob)

.. code-block:: text
   :caption: Output

   bob Funes ACME
   ACME
   Funes

``bob.x`` and ``bob["x"]`` run the same lookup. The item form exists for names computed at run time (a form
field, a report column, a name read from a configuration).

A parent is a reference to one *version* of a record, like any reference between immutable records: a
corrected company is a new version, and ``bob`` keeps reading the version he lists until he is pointed at
the new one. Pointing him there is safe, because his own attributes are kept apart from what he inherits:

.. code-block:: python

   with db.new_transaction() as tr:
       bob = tr.get_root_object("bob")
       corrected = bob.parents[0].with_(company="ACME Ltd")
       print(bob.company, "->", bob.with_parents(corrected).company, bob.with_parents(corrected).city)

.. code-block:: text
   :caption: Output

   ACME -> ACME Ltd Funes

A value that every record must see the moment it changes belongs in a ``MutableObject``, a cell whose value
changes by commit (section 10 and :doc:`modelling-data`).

A miss returns ``None``, in both forms, with or without parents. That is a deliberate choice for models that
grow over time: an object written before a module added a field is read by the module's code without an
error, and the code decides what an absent field means. The price is that ``None`` does not tell an absent
attribute from one explicitly set to ``None``. Two tests do:

.. code-block:: python

   with db.new_transaction() as tr:
       bob = tr.get_root_object("bob")
       print(bob.fax, bob.pager)                       # both None: one inherited, one absent
       print("fax" in bob, "pager" in bob)             # `in` searches the whole linearization
       print(bob.has_defined_attr("fax"), bob.has_defined_attr("name"))   # own attributes only
       print([n for n in ("name", "city", "company", "fax") if bob.has_defined_attr(n)])   # own ones

.. code-block:: text
   :caption: Output

   None None
   True False
   False True
   ['name', 'city']

``"fax" in bob`` is true because a parent defines ``fax`` (as ``None``); ``"pager" in bob`` is false because
nothing does. The obvious alternative, raising on a miss as Python's attribute access does, would make every
object written before a field existed an error for the code that reads the field.
``has_defined_attr`` keeps its earlier meaning, the object's *own* attributes, which is what
index keys and query paths read too: an index never sees an inherited value.

Writes never climb the parents. ``with_`` sets an attribute on the new version of the object itself, and the
parent is untouched:

.. code-block:: python

   with db.new_transaction() as tr:
       bob = tr.get_root_object("bob")
       moved = bob.with_(company="ACME South")
       print(moved.company, bob.company, bob.parents[0].company)
       try:
           bob["company"] = "other"
       except Exception as error:
           print(type(error).__name__, error)

.. code-block:: text
   :caption: Output

   ACME South ACME ACME
   ProtoUserError DBObject is immutable; use with_(company=...) to create a new version

The names of the ``DBObject`` API are not attribute names. ``o.oid`` is the atom's identity, and ``o["oid"]``
is refused rather than answered with something that is not an attribute:

.. code-block:: python

   with db.new_transaction() as tr:
       bob = tr.get_root_object("bob")
       for name in ("oid", "with_", "_private"):
           try:
               bob[name]
           except Exception as error:
               print(type(error).__name__, error)
       print(bob["parents"], len(bob.parents))

.. code-block:: text
   :caption: Output

   ProtoAttributeNameError 'oid' is part of the DBObject API, not an attribute name: read it as o.oid
   ProtoAttributeNameError 'with_' is part of the DBObject API, not an attribute name: read it as o.with_
   ProtoAttributeNameError '_private' is not an attribute name
   None 1

``ProtoAttributeNameError`` is a ``ProtoValidationError`` and also a ``KeyError``, so code that tries
``obj[name]`` first and falls back to ``getattr`` on a ``KeyError`` (a Django template's ``{{ obj.oid }}``)
still reads ``o.oid``.

``bob["parents"]`` is ``None``, not the parents: ``parents`` and ``with_parents`` were ordinary field names
before parents existed, and a record written then that holds such a field still loads, with the field
readable through ``o["parents"]`` and carried over by ``with_``. A new record cannot create one: code
written before parents existed that passed ``parents=[mom, dad]`` to store a field now sets resolution
parents, and the record inherits their attributes. Rename such a field before upgrading (the CHANGELOG has
the migration note). On a ``DBObject``, ``o.parents`` is always the API member (a subclass that defines its own ``parents`` member wins,
since Python members come first).

``DBObject`` now defines ``__getitem__``, ``__contains__``, ``__setitem__`` and ``__delitem__`` (the last
two raise), and sets ``__iter__ = None``, so a record is still not iterable. Two consequences for existing
code: a mixin listed after ``DBObject`` in a subclass's bases no longer provides any of these, and
%-formatting with a record as the mapping formats instead of raising ``TypeError``:

.. code-block:: python

   with db.new_transaction() as tr:
       print("%(name)s works at %(company)s in %(city)s" % tr.get_root_object("bob"))

.. code-block:: text
   :caption: Output

   bob works at ACME in Funes

3. The first registry module
----------------------------

Parents between objects share *data*. Behaviour is the harder half. The obvious way to give records methods
in Python is a subclass of ``DBObject``. It works for a fixed model, and it fails for an extensible one: a
record is persisted under its class's type name, and another module cannot add behaviour to records that are
already stored without changing that class.

.. code-block:: python

   from proto_db import DBObject

   class PlainPartner(DBObject):
       __type_name__ = "tutorial.PlainPartner"

       def greeting(self):
           return f"Dear {self.name}"

   class CrmPartner(PlainPartner):                     # "a CRM module extends partners"
       __type_name__ = "tutorial.CrmPartner"

       def greeting(self):
           return super().greeting() + ", our customer"

   with db.new_transaction() as tr:
       tr.set_root_object("old", PlainPartner(name="carla", context=tr))

   with db.new_transaction() as tr:
       old = tr.get_root_object("old")
       print(type(old).__name__, "|", old.greeting())

.. code-block:: text
   :caption: Output

   PlainPartner | Dear carla

The record saved before the extension loads as ``PlainPartner``, so the CRM behaviour never applies to it. To
fix it, the CRM module would have to rewrite every stored partner, or patch ``PlainPartner`` in place, which
only one module can do safely. The stored type name is also a Python class name, which a Smalltalk process
reading the same database cannot use.

The registry turns this around. A record never names its code: it names a **registry name**, a plain string
such as ``"tutorial.Partner"``, among its parents. Modules **extend** registry names, and the registry of a
process chains the extensions of each name in module load order. A module is an ordinary Python module that
declares itself with :func:`proto_db.registry.module`; there is no manifest file.

The tutorial writes its modules into its working directory and imports them from there. Module names start
with ``tut_`` because a registry module name is process-wide: one name maps to one Python module in a process.

.. code-block:: python

   import importlib, os, sys, textwrap

   sys.path.insert(0, os.getcwd())

   def write_module(name, source):
       """Write the Python module ``name`` into the working directory."""
       with open(f"{name}.py", "w", encoding="utf-8") as f:
           f.write(textwrap.dedent(source))
       importlib.invalidate_caches()

   write_module("tut_contacts", '''
       from proto_db.registry import module

       contacts = module("tut_contacts")

       @contacts.extends("tutorial.Partner")
       class Partner:
           currency = "ARS"                          # a data default

           def display_name(self):                   # a method
               return self.name.title()

           def greeting(self):
               return f"Dear {self.display_name()}"
   ''')

   import tut_contacts
   print(tut_contacts.contacts)

.. code-block:: text
   :caption: Output

   <registry module 'tut_contacts' of 'tut_contacts'>

The class ``Partner`` is only a container for the definitions: it is never instantiated, it has no base
class, and its Python MRO is not used. A plain function in it is a method; any other attribute is a data
default. Importing the module declares the extension in the process. Nothing is written to any database yet.

4. Installing a module, and the catalog object
-----------------------------------------------

A database lists the modules it uses. ``tr.install_module(name)`` adds the module, and the dependencies its
code declares, at commit. The module ``base``, which holds the default mechanisms, is added first, implicitly:
a database that never installs a module never has it and pays nothing for it. A list written by another
language that does not start with ``base`` is read as if ``base`` came first, and the next install writes it
that way.

.. code-block:: python

   with db.new_transaction() as tr:
       tr.install_module("tut_contacts")
       print(tr.installed_modules())

   with db.new_transaction() as tr:
       print(tr.installed_modules())
       partner_class = tr.catalog_object("tutorial.Partner")
       print(partner_class.parents)

.. code-block:: text
   :caption: Output

   ('base', 'tut_contacts')
   ('base', 'tut_contacts')
   ('tutorial.Partner',)

Installing also created a **catalog object** for each registry name the module introduces. It is a plain
``DBObject`` stored in the database's catalog (the root ``__catalog``), whose only parent is the registry
name. An instance lists the catalog object as its parent. Lookup then goes from the instance to the catalog
object's data, and from there into the registry chain of the name, which is searched last.

.. code-block:: python

   from proto_db.resolution import linearize

   with db.new_transaction() as tr:
       partner_class = tr.catalog_object("tutorial.Partner")
       dora = tr.new_object(parents=(partner_class,), name="dora perez", company="Globex")
       tr.set_root_object("dora", dora)
       print(dora.display_name(), "|", dora.greeting(), "|", dora.currency)
       print([type(node).__name__ if not isinstance(node, str) else node for node in linearize(dora)])

.. code-block:: text
   :caption: Output

   Dora Perez | Dear Dora Perez | ARS
   ['DBObject', 'DBObject', 'tutorial.Partner', 'base']

``dora.display_name()`` is found in the registry and bound to ``dora``: inside the method ``self is dora``.
The method is not stored anywhere in the database. The record holds its attributes and its parents, and the
code lives only in the imported Python module.

How the list of modules is kept matters as soon as two processes install modules at the same time. The
obvious solution is a root that holds a list, read and written like any other root. Two installs then
conflict, and a careless merge would let the last writer win and lose an install:

.. code-block:: python

   from proto_db import ProtoConflictError

   with db.new_transaction() as tr:
       tr.set_root_object("my_modules", ("base",))

   first, second = db.new_transaction(), db.new_transaction()
   first.set_root_object("my_modules", first.get_root_object("my_modules") + ("contacts",))
   second.set_root_object("my_modules", second.get_root_object("my_modules") + ("notes",))
   first.commit()
   try:
       second.commit()
   except ProtoConflictError as error:
       print("second install:", type(error).__name__)

.. code-block:: text
   :caption: Output

   second install: ProtoConflictError

The read-modify-write is correct (the second install conflicts instead of overwriting the first), but every
install now retries against every other. ``install_module`` instead merges *add-if-absent* at commit, against
the list current at that moment. Installing is additive and permanent (there is no uninstall), so the union is
always the right answer, and two installs never conflict with each other:

.. code-block:: python

   write_module("tut_notes", '''
       from proto_db.registry import module

       notes = module("tut_notes")

       @notes.extends("tutorial.Note")
       class Note:
           pinned = False
   ''')

   first, second = db.new_transaction(), db.new_transaction()
   second.install_module("tut_notes")
   first.install_module("tut_contacts")                # already installed: adds nothing
   second.commit()
   first.commit()
   with db.new_transaction() as tr:
       print(tr.installed_modules())

.. code-block:: text
   :caption: Output

   ('base', 'tut_contacts', 'tut_notes')

The order of the list is the commit order. Reading the list for resolution never enters a transaction's read
set either, so an install never makes ordinary work conflict. On a storage whose root lock is not global
(``CloudStorage`` shared by several processes) a commit that installs can still lose the root's
compare-and-swap, like any commit; it then raises ``ProtoConflictError`` and ``Database.run`` retries it,
merging onto the winner. No module is ever imported while the root lock is held: a module's import-time code
may itself commit. It must not resolve registry objects of a database that lists the module being imported:
that lookup raises ``ModulesBeingImported`` (a ``ProtoUserError``) and nothing is cached, so the first lookup
after the import sees the whole module.

5. A second module extends the first, with ``_super``
-----------------------------------------------------

The CRM module adds the company to the displayed name of every partner, and introduces leads, which are
partners. It does not touch ``tut_contacts``; it declares that it depends on it and extends the same registry
name. Before installing it, the tutorial remembers the current version of the database for section 9.

.. code-block:: python

   write_module("tut_crm", '''
       from proto_db.registry import module

       crm = module("tut_crm", depends=("tut_contacts",))

       @crm.extends("tutorial.Partner")
       class Partner:
           def display_name(self):
               name = self._super().display_name()     # the next definition: tut_contacts'
               return f"{name} ({self.company})" if self.company else name

       @crm.extends("tutorial.Lead", parents=("tutorial.Partner",))
       class Lead:
           stage = "new"

           def win(self):
               return self.with_(stage="won")
   ''')

   with db.new_transaction() as tr:
       before_crm = tr.snapshot_ref                    # for section 9

   with db.new_transaction() as tr:
       tr.install_module("tut_crm")

   with db.new_transaction() as tr:
       dora = tr.get_root_object("dora")
       print(dora.display_name(), "|", dora.greeting())
       print([extension.module for extension in tr.registry.chain("tutorial.Partner")])

.. code-block:: text
   :caption: Output

   Dora Perez (Globex) | Dear Dora Perez (Globex)
   ['tut_crm', 'tut_contacts']

``tr.registry.chain(name)`` lists the extensions of a name, newest first: the module loaded last is found
first. ``self._super().display_name()`` reaches the next definition after the running one, with the same
``self``. ``dora.greeting()``, which ``tut_contacts`` defined and nobody overrode, calls
``self.display_name()``: that lookup starts from the beginning again, so the CRM override is found. This is
late binding: a method sees every later override, however far down the chain it was defined.

Why not Python's ``super()``? It needs ``self`` to be an instance of the class whose method is running, and
the container class of an extension has no instances: the receiver is a ``DBObject``.

.. code-block:: python

   class Partner:                                      # a container class, as in a module
       def display_name(self):
           return super().display_name()

   with db.new_transaction() as tr:
       try:
           Partner.display_name(tr.get_root_object("dora"))
       except TypeError as error:
           print("TypeError:", error)

.. code-block:: text
   :caption: Output

   TypeError: super(type, obj): obj (instance of DBObject) is not an instance or subtype of type (Partner).

``_super()`` instead reads the code object of the method that calls it, which the registry indexed to its
extension when the module was loaded, and continues from that extension's position in the receiver's
linearization. ``self`` stays the receiver (``self is dora``), so a method can return ``self.with_(...)`` and
compare ``self`` with other objects as usual.

A lead is a partner. Its catalog object was created by this install, because ``tut_crm`` is the first module
that extends ``tutorial.Lead``:

.. code-block:: python

   with db.new_transaction() as tr:
       lead_class = tr.catalog_object("tutorial.Lead")
       eva = tr.new_object(parents=(lead_class,), name="eva diaz", company="Initech")
       won = eva.win()
       print(eva.display_name(), "|", eva.stage, "->", won.stage, "|", won.parents == eva.parents)
       tr.set_root_object("eva", eva)

.. code-block:: text
   :caption: Output

   Eva Diaz (Initech) | new -> won | True

``win`` returns a new version (``self.with_``), as every change of an immutable object does, and the new
version keeps its parents.

The end of a chain is a miss like any other: ``self._super().f`` is ``None`` when nothing after the running
method defines ``f``. A method that may or may not have a next definition tests it first, with the idiom
``if self._super().f: ...``. Section 7 uses it.

6. Data defaults: overridden by a module, by the catalog, by an instance
------------------------------------------------------------------------

Data and methods are found by the same rule, so data defaults extend exactly as methods do. A pricing module
changes the currency default of every partner; one partner sets its own:

.. code-block:: python

   write_module("tut_pricing", '''
       from proto_db.registry import module

       pricing = module("tut_pricing", depends=("tut_contacts",))

       @pricing.extends("tutorial.Partner")
       class Partner:
           currency = "USD"
           price_lists = ["retail"]                   # stored as an immutable value
   ''')

   with db.new_transaction() as tr:
       tr.install_module("tut_pricing")

   with db.new_transaction() as tr:
       partner_class = tr.catalog_object("tutorial.Partner")
       dora, eva = tr.get_root_object("dora"), tr.get_root_object("eva")
       frank = tr.new_object(parents=(partner_class,), name="frank", currency="EUR")
       print(dora.currency, eva.currency, frank.currency)
       print(type(dora.price_lists).__name__, list(dora.price_lists))
       try:
           dora.price_lists.append("wholesale")
       except Exception as error:
           print(type(error).__name__)

.. code-block:: text
   :caption: Output

   USD USD EUR
   FrozenList ['retail']
   ProtoValidationError

The instance's own ``currency`` comes first, then the newest module's default. The obvious way to write a
shared default in Python, a class attribute, is a trap when the default is mutable: every instance shares the
one list, so a change made through one instance reaches all of them.

.. code-block:: python

   class PythonPartner:
       price_lists = ["retail"]

   first, second = PythonPartner(), PythonPartner()
   first.price_lists.append("wholesale")               # meant for `first` only
   print(second.price_lists)

.. code-block:: text
   :caption: Output

   ['retail', 'wholesale']

A registry default passes through the value model when the module is loaded, so it is an immutable value
(``FrozenList`` above), and nothing can change it behind the objects that inherit it: an object that needs
another list sets its own with ``with_``.

Between the instance and the registry sits the catalog object. Its data is ordinary application data: a
transaction replaces it with ``set_catalog_object``, and the new version is found before the registry's
defaults.

.. code-block:: python

   with db.new_transaction() as tr:
       lead_class = tr.catalog_object("tutorial.Lead")
       tr.set_catalog_object("tutorial.Lead", lead_class.with_(currency="BRL"))

   with db.new_transaction() as tr:
       new_lead = tr.new_object(parents=(tr.catalog_object("tutorial.Lead"),), name="gus")
       print(new_lead.currency, tr.get_root_object("eva").currency)

.. code-block:: text
   :caption: Output

   BRL USD

``gus``, created after the update, sees ``BRL``. ``eva`` still sees ``USD``: she lists, as her parent, the
catalog object's version that existed when she was created, and an immutable record never changes. This is
the current semantics, stated plainly: **objects created before a catalog update keep the version they
reference; only objects created afterwards see the update.** A value that must change for every existing
object belongs in a module's default (a new module version) or in a ``MutableObject`` that the objects
reference, which is a stable identity whose value changes by commit.

Two transactions that update the same catalog entry concurrently follow the normal conflict rules: the first
updater wins and the second commit raises ``ProtoConflictError`` (``Database.run`` retries it on the new
version). An install that adds other names never conflicts with an update.

.. code-block:: python

   first, second = db.new_transaction(), db.new_transaction()
   for tr, currency in ((first, "CLP"), (second, "PEN")):
       tr.set_catalog_object("tutorial.Lead", tr.catalog_object("tutorial.Lead").with_(currency=currency))
   first.commit()
   try:
       second.commit()
   except ProtoConflictError as error:
       print("second update:", type(error).__name__)
   with db.new_transaction() as tr:
       print(tr.catalog_object("tutorial.Lead").currency)

.. code-block:: text
   :caption: Output

   second update: ProtoConflictError
   CLP

7. A diamond of registry names, and C3
--------------------------------------

An activity module lets leads be mailed and scheduled. Both capabilities are threads of messages, so both
registry names have ``tutorial.Thread`` as their parent, and a lead reaches ``tutorial.Thread`` through two
paths:

.. code-block:: text

                      tutorial.Lead
                     /      |      \
    tutorial.Partner  tutorial.Mailable  tutorial.Schedulable
            |                 \              /
            |                  tutorial.Thread
             \                       |
              `-----------------  base

Each ``trail`` method appends its own name and passes on to the next definition, if there is one:

.. code-block:: python

   write_module("tut_activity", '''
       from proto_db.registry import module

       activity = module("tut_activity", depends=("tut_crm",))

       @activity.extends("tutorial.Thread")
       class Thread:
           def trail(self):
               rest = self._super().trail() if self._super().trail else []
               return ["Thread"] + rest

       @activity.extends("tutorial.Mailable", parents=("tutorial.Thread",))
       class Mailable:
           def trail(self):
               return ["Mailable"] + self._super().trail()

       @activity.extends("tutorial.Schedulable", parents=("tutorial.Thread",))
       class Schedulable:
           def trail(self):
               return ["Schedulable"] + self._super().trail()

       @activity.extends("tutorial.Lead", parents=("tutorial.Mailable", "tutorial.Schedulable"))
       class Lead:
           def trail(self):
               return ["Lead"] + self._super().trail()
   ''')

   with db.new_transaction() as tr:
       tr.install_module("tut_activity")

   with db.new_transaction() as tr:
       eva = tr.get_root_object("eva")
       print(eva.trail())
       print([node for node in linearize(eva) if isinstance(node, str)])

.. code-block:: text
   :caption: Output

   ['Lead', 'Mailable', 'Schedulable', 'Thread']
   ['tutorial.Lead', 'tutorial.Partner', 'tutorial.Mailable', 'tutorial.Schedulable', 'tutorial.Thread', 'base']

A registry name's parents are the union of the ``parents=`` its extensions declare, in load order:
``tutorial.Lead`` has ``tutorial.Partner`` (from ``tut_crm``), then ``tutorial.Mailable`` and
``tutorial.Schedulable`` (from ``tut_activity``). ``base`` is the implicit last parent of every registry
name. ``tutorial.Thread`` is visited once, after both of its children, and ``base`` last.

The obvious order is a depth-first walk of the parents. It is easy to write, and wrong:

.. code-block:: python

   def depth_first(registry, name):
       order = [name]
       for parent in registry.parents_of(name):
           order += depth_first(registry, parent)
       return order

   with db.new_transaction() as tr:
       print(depth_first(tr.registry, "tutorial.Lead"))

.. code-block:: text
   :caption: Output

   ['tutorial.Lead', 'tutorial.Partner', 'base', 'tutorial.Mailable', 'tutorial.Thread', 'base', 'base', 'tutorial.Schedulable', 'tutorial.Thread', 'base', 'base', 'base']

It visits ``tutorial.Thread`` twice and ``base`` six times, and it reaches ``base`` before
``tutorial.Mailable``: a definition in ``base`` would hide an override in ``tutorial.Mailable``, and
``Thread.trail`` would run twice. Removing the duplicates after the walk (keeping the first of each) still
leaves ``base`` third, before the names that override it. The C3 linearization, which Python
uses for its own classes, keeps every node after all of its children and keeps each node's own order of
parents. When no such order exists the hierarchy is refused, never resolved in some arbitrary order:

.. code-block:: python

   with db.new_transaction() as tr:
       odd = tr.new_object(parents=("tutorial.Thread", "tutorial.Mailable"))
       try:
           odd.trail()
       except Exception as error:
           print(type(error).__name__, error)

.. code-block:: text
   :caption: Output

   ProtoValidationError Inconsistent parent hierarchy: no C3 linearization can order registry name 'tutorial.Thread', registry name 'tutorial.Mailable'

The object asks for ``tutorial.Thread`` before ``tutorial.Mailable``, and ``tutorial.Mailable`` must come
before its own parent ``tutorial.Thread``: no order satisfies both.

8. The errors
-------------

A miss is not an error. Everything else that goes wrong is, and it is raised where it happens, naming what is
wrong: an error that is swallowed, or a lookup that silently resolves from the wrong place, is the worst
outcome for an extensible model, because the wrong behaviour runs with no trace.

``_super`` outside a method
~~~~~~~~~~~~~~~~~~~~~~~~~~~

``_super()`` finds its position from the code that calls it. Code that is not a registry method has no
position, so it is refused, never resolved from the start:

.. code-block:: python

   with db.new_transaction() as tr:
       eva = tr.get_root_object("eva")
       try:
           eva._super()
       except Exception as error:
           # The message goes on to name the calling code and its file and line; only its start is printed.
           print(type(error).__name__, str(error).split(";")[0])

.. code-block:: text
   :caption: Output

   ProtoUserError _super() must be called directly in the body of a registry method

The same refusal covers a closure or a helper function defined inside a method: its code is not a registry
method's, so it has no position. Generators are a separate case. A generator method's code *is* indexed to its
extension, so its position could be found exactly as in any method; ``_super()`` is nevertheless refused in
generator functions, async generators and generator expressions, **by design**, and allowed in coroutine
methods (``async def``). In both cases the position comes from the running code, not from when it runs (a
coroutine's body also runs after the call returned). The refusal of generators is a choice that may be
relaxed later; until then, call ``_super()`` in the method and pass the result to the generator.

A method may be decorated when the decorator uses ``functools.wraps``: the registry indexes the method by the
code of the function the decorator wrapped (``inspect.unwrap``), so ``_super()`` inside its body works, and
one decorator may wrap any number of methods. A decorator without ``functools.wraps`` hides the method's own
code: wrapping two methods with it is refused at install, and ``_super()`` inside the one it wraps is
refused; both errors mention ``functools.wraps``.

A refused definition
~~~~~~~~~~~~~~~~~~~~

An extension defines methods (plain functions) and data defaults, nothing else. A ``property`` would be a
value computed on every read, a second mechanism beside methods with its own rules for ``_super`` and caching;
a value that must be computed is a method. So the declaration is refused when the module is imported:

.. code-block:: python

   write_module("tut_bad", '''
       from proto_db.registry import module

       bad = module("tut_bad")

       @bad.extends("tutorial.Partner")
       class Partner:
           @property
           def initials(self):
               return self.name[:1]
   ''')

   try:
       import tut_bad
   except Exception as error:
       print(type(error).__name__, error)

.. code-block:: text
   :caption: Output

   ProtoValidationError Module 'tut_bad', extension of 'tutorial.Partner': definition 'initials': a property is refused; define a plain function

``staticmethod``, ``classmethod``, a callable instance, a lambda assigned as a default and a nested class are
refused the same way. So is a data default that holds a database object anywhere inside it: a default is
shared by every object, in every database the process opens, and an object belongs to one database.

A mismatch after another process installs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A second ``ObjectSpace`` over the same storage stands for another process. It opens the database and binds it
to the module list it finds:

.. code-block:: python

   other = ObjectSpace(storage)
   other_db = other.open_database("erp")
   with other_db.new_transaction() as tr:
       print(tr.get_root_object("dora").display_name())

   write_module("tut_audit", '''
       from proto_db.registry import module

       audit = module("tut_audit", depends=("tut_contacts",))

       @audit.extends("tutorial.Partner")
       class Partner:
           audited = True
   ''')

   with db.new_transaction() as tr:                    # this process installs another module
       tr.install_module("tut_audit")

   with other_db.new_transaction() as tr:
       dora = tr.get_root_object("dora")
       print(dora.name)                                # own attributes need no registry
       try:
           dora.display_name()
       except Exception as error:
           print(type(error).__name__, error)

.. code-block:: text
   :caption: Output

   Dora Perez (Globex)
   dora perez
   ProtoValidationError Database 'erp': registry built for modules ['base', 'tut_contacts', 'tut_notes', 'tut_crm', 'tut_pricing', 'tut_activity'] but the database lists ['base', 'tut_contacts', 'tut_notes', 'tut_crm', 'tut_pricing', 'tut_activity', 'tut_audit']; there is no automatic rebuild (spec Q2): call ObjectSpace.reload_registry('erp') to rebuild it

The other space's registry was built for the list it bound; the database now lists one more module. The
obvious reaction, rebuilding the registry automatically, would change the behaviour of a running process
between two lines of its code, with transactions on the old and on the new behaviour interleaved. A mismatch
is therefore an error, and the rebuild is an explicit call, made when the process is ready (after deploying
the new module's code, for example):

.. code-block:: python

   other.reload_registry("erp")
   with other_db.new_transaction() as tr:
       dora = tr.get_root_object("dora")
       print(dora.display_name(), dora.audited)

.. code-block:: text
   :caption: Output

   Dora Perez (Globex) True

Transactions that were already running keep the registry they started with. Everything that resolves for
that database afterwards in that space, later snapshots and installing transactions included, uses registries
built after the reload, so a development workflow that reloads edited code never mixes the old and the new.
There is no automatic rebuild.

A module that cannot be imported
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A process whose deployment lacks a listed module still opens the database and reads data; the error comes at
the first lookup that needs the registry. A runtime's ``modules_mapping`` maps a module name to the Python
import path that implements it; here it points ``tut_audit`` at a path that does not exist, as a deployment
that forgot the module would:

.. code-block:: python

   lacking = ObjectSpace(storage, modules_mapping={"tut_audit": "tut_audit_not_deployed"})
   lacking_db = lacking.open_database("erp")
   with lacking_db.new_transaction() as tr:
       dora = tr.get_root_object("dora")
       print(dora.name)
       try:
           dora.display_name()
       except Exception as error:
           print(type(error).__name__, error)
   try:
       lacking.reload_registry("erp")
   except Exception as error:
       print("reload:", type(error).__name__)

.. code-block:: text
   :caption: Output

   dora perez
   ProtoNotSupportedError Database 'erp' lists modules this process cannot load: Module 'tut_audit' cannot be imported from 'tut_audit_not_deployed': No module named 'tut_audit_not_deployed'
   reload: ProtoNotSupportedError

The failure is remembered for that module list until ``reload_registry``, which tries the import again:
Python does not cache a failed import, and every commit would otherwise retry it; retrying only on request is
the same rule as for a mismatch. A failed reload is not remembered itself: after it, the next lookup tries the
import once more, and remembers that failure.

And one thing that is not an error
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A registry name that no module extends is an empty node: it contributes nothing, and lookups go on past it.
That keeps a model extensible (a name may be listed before the module that extends it is written), and it
means a misspelt name is silent. ``Registry.unknown_names`` finds such names, for a test or a tool to flag:

.. code-block:: python

   with db.new_transaction() as tr:
       typo = tr.new_object(parents=("tutorial.Laed",), name="hal")
       print(typo.stage, typo.name)
       print(tr.registry.unknown_names(("tutorial.Lead", "tutorial.Laed")))

.. code-block:: text
   :caption: Output

   None hal
   ('tutorial.Laed',)

9. As-of reads use the modules of their time
--------------------------------------------

Every committed version of a database stays readable. Section 5 remembered the version just before
``tut_crm`` was installed. The obvious way to rebuild a report of that time is to read the old version's
data and run it through the code the process has now:

.. code-block:: python

   old = db.snapshot_at(before_crm)
   print(old.get_root_object("dora").display_name())   # old data, today's modules

.. code-block:: text
   :caption: Output

   Dora Perez (Globex)

The data is the old version's, but the behaviour is today's: the display name shows the company, and showing
it is what ``tut_crm`` added, after that version. A report of last quarter would mix last quarter's records with this
quarter's rules. The module list is part of each version, so an as-of read can resolve with the modules that
were installed then. Inside ``with old:`` the snapshot is the reader of the block, and the objects read from
it resolve with its registry:

.. code-block:: python

   print(old.registry.key)
   with old:
       print(old.get_root_object("dora").display_name())
   with db.new_transaction() as tr:
       print(tr.get_root_object("dora").display_name())

.. code-block:: text
   :caption: Output

   ('base', 'tut_contacts', 'tut_notes')
   Dora Perez
   Dora Perez (Globex)

Outside the block, as in the first attempt, an object resolves with the database's current binding, the
snapshot's objects included. The block is explicit on purpose: an as-of read never changes how other code
resolves, so reading an old version for one report cannot switch the rest of the process to old behaviour.

Which registry an object uses is decided by the code doing the resolving, never by the order in which records
were loaded:

* an object created (or derived with ``with_``) in a transaction resolves through that transaction's registry,
  for its whole life, even after the transaction ended;
* a loaded object resolves through the current **reader**: the transaction of a ``with tr:`` block or of
  ``Database.run``, or the snapshot of a ``with snapshot:`` block;
* outside any reader, only if every database of the space that lists modules lists the same ones; otherwise
  the lookup raises ``ProtoUserError`` and asks for a block of the intended database;
* a transaction or reader of a database that lists no modules decides too: it has no registry, so a lookup
  that reaches a registry name raises ``ProtoNotSupportedError`` ("needs a registry"), whatever the other
  databases of the space list. Own attributes and object parents resolve as always.

10. The versioned entity
------------------------

A lead changes stage over time, other records refer to it, and the sales history must stay available. The
obvious solution, a mutable record updated in place, loses the history; the relational answer, a table of
versions with a validity column, makes every query remember a "current versions only" condition
(:doc:`modelling-data` shows that cost). The ``proto_db`` pattern combines two things:

* a ``MutableObject``, a named cell whose value changes by commit, gives the lead a **stable identity** that
  other records hold;
* each value of the cell is a version that points at the one it replaced (``previous``), and keeps its parents.

The collection of current leads, and its index, hold only the current version of each lead:

.. code-block:: python

   from proto_db import EqualityIndex, F, MutableObject, from_collection

   with db.new_transaction() as tr:
       lead_class = tr.catalog_object("tutorial.Lead")
       leads = tr.new_dictionary().add_index(EqualityIndex("by_stage", "stage"))
       for name in ("ivan", "julia", "kim"):
           version = tr.new_object(parents=(lead_class,), name=name, stage="new", previous=None)
           tr.new_mutable(f"lead:{name}", version)
           leads = leads.set_at(name, version)
       tr.set_root_object("leads", leads)
       tr.set_root_object("call", tr.new_object(subject="first call", lead=MutableObject("lead:ivan")))

   def advance(name, stage):
       """Commit the next version of a lead: a new record that remembers the one before it."""
       with db.new_transaction() as tr:
           current = tr.get_mutable(f"lead:{name}")
           version = current.with_(stage=stage, previous=current)
           tr.set_mutable(f"lead:{name}", version)
           tr.set_root_object("leads", tr.get_root_object("leads").set_at(name, version))

   for stage in ("qualified", "proposal", "won"):
       advance("ivan", stage)
   advance("julia", "qualified")

   with db.new_transaction() as tr:
       call = tr.get_root_object("call")
       ivan = call.lead.get(tr)                        # the handle always reads the current version
       chain = []
       while ivan is not None:
           chain.append(ivan.stage)
           ivan = ivan.previous
       print(call.subject, "->", list(reversed(chain)))
       leads = tr.get_root_object("leads")
       print("new:", sorted(o.name for o in from_collection(leads).where(F.stage == "new").to_list()))
       print("won:", sorted(o.name for o in from_collection(leads).where(F.stage == "won").to_list()))
       print(len(leads), "leads,", leads.get_index("by_stage").explain()["entries"], "index entries")
       print(call.lead.get(tr).display_name(), call.lead.get(tr).trail())

.. code-block:: text
   :caption: Output

   first call -> ['new', 'qualified', 'proposal', 'won']
   new: ['kim']
   won: ['ivan']
   3 leads, 3 index entries
   Ivan ['Lead', 'Mailable', 'Schedulable', 'Thread']

``ivan`` went through four stages, but only his current version is in the collection, so the index holds one
entry per lead and the query for ``"new"`` needs no "is current" condition: superseded versions are not in
the collection, so no query can meet them. The call record holds the identity (the handle), so it always reads
the current lead. Every version keeps the parents ``with_`` carried over, so every version resolves the same
methods. The history is reachable from the current version, through ``previous``, and costs nothing to the
queries that ignore it.

The index reads the object's **own** attributes, never inherited ones: an index of an inherited default would
have to change whenever a module or a catalog object changed it, for every object that inherits it. That is
why the pattern above sets ``stage`` on every version instead of relying on the ``"new"`` default of
``tut_crm``. The query path reads own attributes in the same way:

.. code-block:: python

   with db.new_transaction() as tr:
       relying = tr.new_object(parents=(tr.catalog_object("tutorial.Lead"),), name="lou")
       some = tr.new_list().append_last(relying)
       print(relying.stage, from_collection(some).where(F.stage == "new").count())

.. code-block:: text
   :caption: Output

   new 0

``lou.stage`` reads ``"new"`` through resolution, and a query for ``stage == "new"`` does not find him: set an
attribute that is queried or indexed on the object itself.

11. What is never persisted, and the multi-language rule
--------------------------------------------------------

The obvious way to make a stored object come back with its behaviour is to store, with the data, where its
code lives: Python's ``pickle`` does exactly that.

.. code-block:: python

   import pickle

   write_module("tut_invoice", '''
       class Invoice:
           def __init__(self, number):
               self.number = number

           def label(self):
               return f"INV-{self.number}"
   ''')
   import tut_invoice

   data = pickle.dumps(tut_invoice.Invoice(7))
   print([name for name in (b"tut_invoice", b"Invoice", b"number") if name in data])

.. code-block:: text
   :caption: Output

   [b'tut_invoice', b'Invoice', b'number']

The bytes name the Python module and the class. Only a Python process that can import ``tut_invoice`` can
read them back; a Smalltalk process cannot, and renaming or moving the module makes the stored data
unreadable. The data would be tied to one language and to one layout of its code. ``proto_db`` stores data
and the names of the installed modules, nothing language-specific: no Python code, no Python class of a
module or an extension, no import path. A record lists its parents as other records and registry names; the
module list holds module names and the dependencies each declared when it was installed.

.. code-block:: python

   from pprint import pprint
   from proto_db.tools.notation import describe_object

   with db.new_transaction() as tr:
       pprint(describe_object(tr.get_root_object("eva")), width=100)
       pprint(tr.get_root_object("__currentModules").entries(), width=100)

.. code-block:: text
   :caption: Output

   {'attrs': [['company', {'str': 'Initech'}], ['name', {'str': 'eva diaz'}]],
    'object': 'proto_db.core.dbobject.DBObject',
    'parents': [{'attrs': [],
                 'object': 'proto_db.core.dbobject.DBObject',
                 'parents': [{'str': 'tutorial.Lead'}]}]}
   (('base', ()),
    ('tut_contacts', ()),
    ('tut_notes', ()),
    ('tut_crm', ('tut_contacts',)),
    ('tut_pricing', ('tut_contacts',)),
    ('tut_activity', ('tut_crm',)),
    ('tut_audit', ('tut_contacts',)))

``describe_object`` is the language-neutral description of a stored record that the conformance vectors use
(:doc:`../format-spec`). ``eva`` is a plain ``DBObject`` (``proto_db.core.dbobject.DBObject`` is the format
name of the built-in record type, written on disk as a small integer, the same for every implementation) with
two own attributes and one parent: the catalog object's version she was created with, whose only parent is the
string ``"tutorial.Lead"``. ``stage``, ``currency`` and the methods appear nowhere: they are found at run time
in the modules that this process imported.

The methods exist only in the processes that imported the modules. That has two consequences.

* **Every language has its own version of each module.** One module is not written in several languages, and
  code of one language is not run from another. A database used from Python and from Smalltalk lists module
  names; each runtime maps a name to its own code (in Python, ``modules_mapping`` maps a name to an import
  path, and by default the name is the import path, except ``base``, which maps to ``proto_db.modules.base``),
  and resolves by its own rules over the same parents. A
  process that cannot load a listed module raises, as section 8 showed: it never runs with part of the model
  missing.
* **Methods are never stale and never travel.** Changing a method is deploying new code and calling
  ``reload_registry`` (or restarting), not rewriting data. A record cannot carry code into a process that did
  not choose to run it.

What the format stores (the ``"p"`` key of a record, the roots ``__currentModules`` and ``__catalog``) is
specified in :doc:`../format-spec`, so that every implementation reads the same bytes.

.. code-block:: python

   for opened in (other, lacking, space):
       opened.close()
   sys.path.remove(os.getcwd())
