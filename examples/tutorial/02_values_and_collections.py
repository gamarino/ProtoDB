#!/usr/bin/env python3
"""2. Objects and immutable collections

Generated from docs/tutorial/02-values-and-collections.md by ``python tests/docs/test_markdown_tutorials.py --write-scripts``:
the code below is the chapter's code blocks, verbatim and in order. After the blocks of each output the chapter
shows, ``_expect`` compares what they printed with it, so the script checks every figure the chapter shows,
exits with status 1 on the first difference, and prints "02_values_and_collections: OK" at the end. Edit the chapter, not this file.

Run it from anywhere: python examples/tutorial/02_values_and_collections.py
"""
import os as _os
import sys as _sys

_HERE = _os.path.dirname(_os.path.abspath(__file__))
_REPO = _os.path.normpath(_os.path.join(_HERE, *[_os.pardir] * 2))
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


# -- 02-values-and-collections.md, line 13 ----------------------------------------
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
_expect("""
Pat Doe Globex ['555-0100'] 2500.00
DBObject is immutable; use with_(company=...) to create a new version
Globex -> Initech
the phone list is frozen
refused: Field 'tags' of proto_db.core.dbobject.DBObject: Unsupported value type: set
""")

# -- 02-values-and-collections.md, line 64 ----------------------------------------
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
_expect("""
['Pat Doe', 'Lou Ray', 'Kim Wu']
['ann', 'bob'] True False
['import', 'first review'] manager
2 1 3
""")

# -- 02-values-and-collections.md, line 104 ----------------------------------------
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
_expect("""
['ann', 'bob'] ['ann', 'bob', 'dee']
the reader still sees ['ann', 'bob']
a new transaction sees ['ann', 'bob', 'dee']
""")

# -- 02-values-and-collections.md, line 139 ----------------------------------------
import os
import tempfile

from proto_db import FileStorage

workdir = tempfile.TemporaryDirectory(prefix="proto_db-tutorial-")   # this chapter's own directory
home = os.getcwd()
os.chdir(workdir.name)                                # its files stay out of yours

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
_sample()

# -- 02-values-and-collections.md, line 197 ----------------------------------------
space.close()

os.chdir(home)
workdir.cleanup()                                     # removes only the directory this chapter created

print("02_values_and_collections: OK")
