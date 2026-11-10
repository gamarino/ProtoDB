Writing a custom index
======================

ProtoDB has three index kinds (equality, range, vector). New kinds are added by subclassing ``Index`` and
implementing a small set of hooks; the planner, the collections and the persistence layer need no change.
This tutorial builds a **substring index**: it answers ``F.field.contains("abc")`` without reading every
element, and shows how to prove that it is correct. Every code block runs in the test suite.

What problem it solves
----------------------

Without an index, a substring condition reads every element:

.. code-block:: python

   from proto_db import F, MemoryStorage, ObjectSpace, from_collection

   space = ObjectSpace(MemoryStorage())
   db = space.new_database("catalogue")
   WORDS = ["alphabet", "cabinet", "dinosaur", "carbon", "orbit", "rabbit", "abbot", "habit", "ab", "grab it"]

   with db.new_transaction() as tr:
       items = tr.new_list()
       for i in range(3000):
           items = items.append_last(tr.new_object(name=f"{WORDS[i % 10]}-{i}"))
       items = items.append_last(tr.new_object(name=["abbot", "x"]))     # a list: "contains" means an item
       items = items.append_last(tr.new_object(price=5))                  # no name at all
       tr.set_root_object("items", items)

   def plan_of(query):
       names, node = [], query.explain()
       while node:
           names.append(node["plan"])
           node = node["children"][0] if node.get("children") else None
       return " <- ".join(names)

   tr = db.new_transaction()
   items = tr.get_root_object("items")
   query = from_collection(items).where(F.name.contains("bbo"))
   print(plan_of(query), "->", query.count(), "rows out of", len(items))
   tr.abort()

.. code-block:: text
   :caption: Output

   Filter <- CollectionScan -> 300 rows out of 3002

Notice the two odd elements. In the term vocabulary, ``contains`` on a string key means "substring", but on
a list key it means "one of the items equals the operand", and on a missing key it matches nothing. An index
must give the same answer as a scan for *every* element, not only the tidy ones. That is the main design
constraint of this tutorial.

The idea
--------

A *trigram* is a run of three characters. Every string containing ``"bbo"`` contains the trigram ``"bbo"``,
so the elements that contain an operand are among those that contain all the operand's trigrams. The index
files each element under every trigram of its key; a query takes the smallest of the operand's trigram
lists as candidates and checks each candidate against the real condition. The index only *prunes*; the final
check is the scan's own rule (``term.matches``), so the answer cannot differ from a scan's.

Two kinds of elements cannot be pruned by trigrams, and neither may be forgotten:

* elements whose key is not a string (a list, a number, ``None``, a missing key): they go into a *side
  group* that every query also checks;
* strings shorter than three characters are never matches for an operand of three or more characters, so
  they can be left out. For operands shorter than three characters the index says "I do not support this
  term" and the planner scans.

The class
---------

.. code-block:: python

   from proto_db import Index, KeySpec, ProtoValidationError
   from proto_db.collections import avl
   from proto_db.indexing.keyed import EMPTY_GROUP, group_add, group_iter, group_remove
   from proto_db.query.plans import Estimate, IndexPlan
   from proto_db.query.terms import CONTAINS

   def trigrams(text):
       """The distinct three-character substrings of ``text``."""
       return {text[i:i + 3] for i in range(len(text) - 2)}

   class TrigramIndex(Index):
       __type_name__ = "acme.search.TrigramIndex"          # a stable, dotted name: it is stored in the database

       def __init__(self, name, key_spec, *, context=None):
           if isinstance(key_spec, str):
               key_spec = KeySpec.path(key_spec)
           super().__init__(name, key_spec, context=context)
           # Public attributes are the persisted state. Indexes are immutable: set them with object.__setattr__.
           object.__setattr__(self, "postings", None)      # keyed tree: trigram -> group of elements
           object.__setattr__(self, "side", EMPTY_GROUP)   # elements whose key is not a string
           object.__setattr__(self, "total_count", 0)

       def _text(self, element):
           key = self.key_spec.extract(element)
           return key if isinstance(key, str) else None

       def with_added(self, element):
           text = self._text(element)
           if text is None:
               return self._with(side=group_add(self.side, element), total_count=self.total_count + 1)
           postings = self.postings
           for gram in trigrams(text):
               group = avl.key_get(postings, gram) or EMPTY_GROUP
               postings = avl.key_put(postings, gram, group_add(group, element))[0]
           return self._with(postings=postings, total_count=self.total_count + 1)

       def with_removed(self, element):
           text = self._text(element)
           if text is None:
               side = group_remove(self.side, element)
               if side is None:
                   raise ProtoValidationError(f"{self.name!r} does not hold {element!r}")
               return self._with(side=side, total_count=self.total_count - 1)
           postings = self.postings
           for gram in trigrams(text):
               group = group_remove(avl.key_get(postings, gram) or EMPTY_GROUP, element)
               if group is None:
                   raise ProtoValidationError(f"{self.name!r} does not hold {element!r}")
               postings = (avl.key_put(postings, gram, group)[0] if group[1]
                           else avl.key_remove(postings, gram)[0])
           return self._with(postings=postings, total_count=self.total_count - 1)

       def supports(self, term):
           return (self._on_key(term) and term.op == CONTAINS
                   and isinstance(term.value, str) and len(term.value) >= 3)

       def _candidates(self, term):
           groups = [avl.key_get(self.postings, gram) or EMPTY_GROUP for gram in trigrams(term.value)]
           return min(groups, key=lambda group: group[1])

       def estimate(self, term):
           self._check_supported(term)
           rows = self._candidates(term)[1] + self.side[1]
           return Estimate(rows=rows, cost=2 * rows + len(trigrams(term.value)))

       def plan(self, term):
           self._check_supported(term)
           candidates, side = self._candidates(term), self.side     # immutable state only

           def produce():
               for element in list(group_iter(candidates)) + list(group_iter(side)):
                   if term.matches(element):                         # the scan's own rule decides
                       yield element
           return IndexPlan(self, term, produce, kind="TrigramLookup")

Each part is one hook of the framework. The full list is in :doc:`../indexing-framework`; these are the
ones this index needs.

``__init__``
   Creates an **empty** index. The framework calls ``empty()`` (by default,
   ``TrigramIndex(name, key_spec)`` again) to rebuild the definition, so the constructor must take the
   definition fields and nothing else.

``with_added`` and ``with_removed``
   Return a **new** index: the receiver is never modified. ``self._with(field=...)`` copies the index with
   some fields replaced. The AVL functions are pure too (path copying), so an update shares almost
   everything with the version before it, like every ProtoDB structure. An element can occur several times
   in a ``List``, and ``group_add`` / ``group_remove`` keep that count. Removing an element the index does
   not hold is an error, never a silent no-op: it would mean the collection and its index have diverged.

``supports``
   Answers "can this index answer this term *exactly*?". It starts with ``self._on_key(term)``: an index on
   ``name`` never answers a term on ``price``. When it says no, the planner takes another path.

``estimate`` and ``plan``
   The planner calls ``estimate`` to compare paths and ``plan`` to build the executable. The plan's
   ``produce`` function must capture immutable state only, because plans run more than once and from
   several threads.

Using it
--------

.. code-block:: python

   with db.new_transaction() as tr:
       items = tr.get_root_object("items")
       tr.set_root_object("items", items.add_index(TrigramIndex("by_name", "name")))

   tr = db.new_transaction()
   items = tr.get_root_object("items")
   index = items.get_index("by_name")
   term_query = from_collection(items).where(F.name.contains("bbo"))
   print(plan_of(term_query), "->", term_query.count(), "rows")
   print("candidates the index examines:", int(index.estimate(term_query.plan().term).rows), "of", len(items))

   short = from_collection(items).where(F.name.contains("ab"))
   print("a 2-character operand:", plan_of(short), "->", short.count(), "rows")

   listed = from_collection(items).where(F.name.contains("abbot"))
   scan = from_collection(items.remove_index("by_name")).where(F.name.contains("abbot"))
   print("'abbot' (strings and a list item):", listed.count(), "with the index,", scan.count(), "by a scan")
   tr.abort()

.. code-block:: text
   :caption: Output

   TrigramLookup -> 300 rows
   candidates the index examines: 302 of 3002
   a 2-character operand: Filter <- CollectionScan -> 2100 rows
   'abbot' (strings and a list item): 301 with the index, 301 by a scan

The index returned 300 rows after examining 302 candidates instead of 3,002 elements. The operand ``"ab"``
is too short, so the planner scanned. For ``"abbot"`` the list ``["abbot", "x"]`` is found through the side
group, together with the 300 strings, and agrees with a scan.

Proving it
----------

A custom index is code you wrote, so its correctness is yours to show. The repository ships the *index
contract* (``tests/contracts/index_contract.py``): a reusable test suite that takes an index kind, a way to
draw random elements and the terms to ask, and checks every hook. A kind that passes implements the framework
correctly. It checks that every plan equals the scan on random data, through random sequences of adds, removes
and replacements; that the index survives a save and a load; that concurrent readers agree; that unsupported
terms are refused; that the index works attached to every collection type; and that no hook modifies the
receiver.

.. code-block:: python

   from proto_db import DBObject
   from proto_db.query.terms import Term
   from tests.contracts.index_contract import IndexContract

   def draw(rng):
       """Random elements, including the awkward ones: lists, None, numbers, short strings."""
       pick = rng.randrange(7)
       if pick == 0:
           return ["abbot", "x"]
       if pick == 1:
           return None
       if pick == 2:
           return 7
       if pick == 3:
           return "ab"
       return rng.choice(["alphabet", "cabinet", "rabbit", "habit", "abbot", "grab it"]) + "-" + str(rng.randrange(3))

   def terms(rng, elements, key_spec):
       return [Term(key_spec.term_path, CONTAINS, operand) for operand in ("abb", "bit", "-1", "abbot", "ab", "zzz")]

   contract = IndexContract("TrigramIndex", TrigramIndex, draw, term_strategy=terms,
                            key_specs=(KeySpec.element(),), collection_key_spec=KeySpec.element())
   contract.run_all(seeds=range(6))
   print("the contract passes: every hook agrees with a scan")

.. code-block:: text
   :caption: Output

   the contract passes: every hook agrees with a scan

A contract that cannot fail proves nothing, so here is a defective version. It forgets the side group, which
is exactly the mistake the list element in the first section was there to catch:

.. code-block:: python

   class ForgetfulTrigramIndex(TrigramIndex):
       __type_name__ = "acme.search.ForgetfulTrigramIndex"

       def plan(self, term):
           self._check_supported(term)
           candidates = self._candidates(term)
           return IndexPlan(self, term, lambda: (e for e in group_iter(candidates) if term.matches(e)),
                            kind="TrigramLookup")

   broken = IndexContract("ForgetfulTrigramIndex", ForgetfulTrigramIndex, draw, term_strategy=terms,
                          key_specs=(KeySpec.element(),), collection_key_spec=KeySpec.element())
   try:
       broken.run_all(seeds=range(6))
   except AssertionError:
       print("the contract rejects the index that forgets the side group")

.. code-block:: text
   :caption: Output

   the contract rejects the index that forgets the side group

Registering the contract for your kind in ``tests/contracts/index_contract.py`` (see the ``REGISTRATIONS``
section there) puts it in the project's test run.

Persistence and distribution
----------------------------

An index is an atom like any other: it is saved with the collection, children first, and loaded lazily.
What is stored is the class's ``__type_name__`` and the public attributes. That has consequences:

* The class must be importable (so that it registers itself) in every process that opens the database. A
  program that never imported ``TrigramIndex`` and opens a database that holds one gets
  ``ProtoCorruptionError: Unknown atom type 'acme.search.TrigramIndex'`` when it first touches the index,
  never a wrong answer.
* A change to what the index stores (a new field, a different key) is a *new version of the class*:
  give it a new ``__type_name__`` or a ``__type_aliases__`` entry and rebuild the index with
  ``add_index`` in a transaction. Do not change meaning under an existing name.
* Functions are never stored. If your index needs one (a tokeniser, say), register it by name, as
  ``register_extractor`` does for keys, or make it a parameter that is a plain value.

.. code-block:: python

   from proto_db import FileStorage, Term

   disk = ObjectSpace(FileStorage("catalogue-space"))
   catalogue = disk.new_database("catalogue")
   with catalogue.new_transaction() as tr:
       tr.set_root_object("words", tr.new_list().add_index(TrigramIndex("by_text", KeySpec.element()))
                          .append_last("rabbit hole").append_last("cabinet").append_last("a rabbit"))
   disk.close()

   again = ObjectSpace(FileStorage("catalogue-space"))
   tr = again.open_database("catalogue").new_transaction()
   words = tr.get_root_object("words")
   index = words.get_index("by_text")
   found = index.plan(Term(None, CONTAINS, "abbit")).execute()
   print(type(index).__name__, sorted(found))
   tr.abort()
   again.close()
   space.close()

.. code-block:: text
   :caption: Output

   TrigramIndex ['a rabbit', 'rabbit hole']

The two extension points besides ``plan`` are worth knowing. An index that can yield its elements in key
order overrides ``supports_order`` and ``ordered_plan`` (``RangeIndex`` does, and ``order_by`` uses it). An
index whose answer depends on the whole collection, as "k nearest" does, overrides ``supports_filtered`` and
``filtered_plan`` so that ``near(k) AND condition`` means "the k nearest among those satisfying the
condition" (``VectorIndex`` does). Both are documented on :class:`proto_db.indexing.base.Index`.

When not to write an index
--------------------------

* If an existing index answers the question with a computed key (``register_extractor`` and
  ``KeySpec.extractor``), use that. A lower-cased or bucketed key needs no new class.
* If the question is full-text search with ranking, stemming and language support, a search engine is the
  better tool. This trigram index finds substrings, nothing more.
* Every index costs a new version of its structure per write. Measure the write cost of your index before
  attaching it to a collection that changes often.
