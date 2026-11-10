#!/usr/bin/env python3
"""Getting started with ProtoDB: a space, a database, transactions and persistent collections.

Run from the repository root::

    python examples/simple_example.py

The script checks every value it reads back and exits with an error if one is wrong.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proto_db import MemoryStorage, ObjectSpace  # noqa: E402


def main() -> None:
    """Write a few roots, read them back in a later transaction and show the version history."""
    space = ObjectSpace(MemoryStorage())
    try:
        db = space.new_database("example")

        # A transaction is a context manager: it commits on success and aborts on an exception.
        with db.new_transaction() as tr:
            numbers = tr.new_list().append_last(1).append_last(2).append_last(3)
            ada = tr.new_object(name="Ada Lovelace", born=1815)
            people = tr.new_dictionary().set_at("ada", ada)
            tr.set_root_object("numbers", numbers)
            tr.set_root_object("people", people)

        # Collections are immutable: every change returns a new version, the old one is untouched.
        with db.new_transaction() as tr:
            numbers = tr.get_root_object("numbers")
            longer = numbers.append_last(4)
            assert list(numbers) == [1, 2, 3] and list(longer) == [1, 2, 3, 4]
            tr.set_root_object("numbers", longer)

            people = tr.get_root_object("people")
            ada = people.get_at("ada")
            # DBObjects are immutable records too: with_() returns a new version.
            people = people.set_at("ada", ada.with_(field="mathematics"))
            people = people.set_at("alan", tr.new_object(name="Alan Turing", born=1912, field="computing"))
            tr.set_root_object("people", people)

        tr = db.new_transaction()
        numbers = list(tr.get_root_object("numbers"))
        people = tr.get_root_object("people")
        names = sorted(p.name for p in people.values())
        print("numbers:", numbers)
        print("people:", names)
        print("ada works in", people.get_at("ada").field)
        tr.abort()
        assert numbers == [1, 2, 3, 4]
        assert names == ["Ada Lovelace", "Alan Turing"]

        versions = [entry.version for entry in db.history()]
        print("versions, newest first:", versions)
        assert versions == [2, 1, 0]
    finally:
        space.close()
    print("simple_example: OK")


if __name__ == "__main__":
    main()
