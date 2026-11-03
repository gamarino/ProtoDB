#!/usr/bin/env python3
"""Persistent storage: a FileStorage space survives a close and reopen, with its history and indexes.

Run from the repository root::

    python examples/file_storage.py

The space lives in a temporary directory that is removed at the end.  The first part writes a catalogue of books with an index; the second reopens the
space, as a new process would, queries it and checks every result.

``FileStorage(path, durability=...)`` takes ``"fsync"`` (the default: a commit is on
disk when it returns), ``"flush"`` (written to the OS, survives a process crash but not
a power loss) or ``"none"`` (for tests and benchmarks).
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proto_db import F, FileStorage, ObjectSpace, RangeIndex, from_collection  # noqa: E402

BOOKS = [("Dune", "Herbert", 1965), ("Neuromancer", "Gibson", 1984), ("Foundation", "Asimov", 1951),
         ("Hyperion", "Simmons", 1989), ("Solaris", "Lem", 1961)]


def write(path: str) -> None:
    """Create the space and commit the catalogue in two transactions."""
    space = ObjectSpace(FileStorage(path))
    try:
        db = space.new_database("library")
        with db.new_transaction() as tr:
            books = tr.new_dictionary().add_index(RangeIndex("by_year", "year"))
            for title, author, year in BOOKS[:3]:
                books = books.set_at(title, tr.new_object(title=title, author=author, year=year))
            tr.set_root_object("books", books)
        with db.new_transaction() as tr:
            books = tr.get_root_object("books")
            for title, author, year in BOOKS[3:]:
                books = books.set_at(title, tr.new_object(title=title, author=author, year=year))
            tr.set_root_object("books", books)
    finally:
        space.close()


def read(path: str) -> None:
    """Reopen the space and query it."""
    space = ObjectSpace(FileStorage(path))
    try:
        print("databases:", space.list_databases())
        db = space.open_database("library")
        print("versions:", [entry.version for entry in db.history()])
        tr = db.new_transaction()
        books = tr.get_root_object("books")
        sixties = from_collection(books).where(F.year.between(1960, 1969)).order_by(F.year)
        print("plan:", sixties.explain()["plan"])
        titles = [b.title for b in sixties.to_list()]
        print("books from the sixties:", titles)
        tr.abort()
        assert space.list_databases() == ["library"]
        assert [entry.version for entry in db.history()] == [2, 1, 0]
        assert len(books) == len(BOOKS)
        assert titles == ["Solaris", "Dune"]
    finally:
        space.close()


def main() -> None:
    """Write, close, reopen and read the space."""
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "space")
        write(path)
        read(path)
    print("file_storage: OK")


if __name__ == "__main__":
    main()
