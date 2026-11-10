"""Verify a file-storage space, typically one another implementation wrote (Task 10.P, A2).

Usage::

    python -m proto_db.tools.verify <directory> [--out FILE]

The verifier opens the space read-only (it never publishes a root), then:

1. reads and checks the root record;
2. decodes every atom reachable from the space root, depth first: the space root, every
   database root and every older version it links to, every collection with its tree nodes and
   indexes, every record. Each atom is decoded once (by pointer). An atom of a type this process
   has no class for is read as an opaque atom (D59) and listed under ``opaque_types``;
3. validates every collection reached (:meth:`IndexedCollection.validate`: the structure, then
   every index against the elements);
4. prints the current content of every database in the notation of the conformance manifest
   (:mod:`proto_db.tools.notation`) as JSON, to standard output or to ``--out``.

Every failure is reported on standard error as ``FAILED <where>: <exception class>: <message>``;
the verifier goes on with the next atom, so one run lists every failure it can reach.

Exit status: 0 when everything decoded and validated; 1 when anything failed (the dump is still
written, for the parts that could be read); 2 when the directory holds no readable space.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from ..collections.base import IndexedCollection
from ..core.atom import Atom, OpaqueAtom
from ..storage import format as fmt
from ..storage.file import FileStorage
from ..transactions import ObjectSpace, _SpaceContext
from ..values import FrozenDict
from . import notation

__all__ = ["main", "verify"]


def _children(value, out: list) -> None:
    """Append every atom held by the canonical value ``value`` (at any depth) to ``out``."""
    if isinstance(value, Atom):
        out.append(value)
    elif isinstance(value, (tuple, list)):
        for item in value:
            _children(item, out)
    elif isinstance(value, (dict, FrozenDict)):
        for key, item in value.items():
            _children(key, out)
            _children(item, out)


def _where(atom) -> str:
    return f"{type(atom).__type_name__} at {atom.atom_pointer}"


def verify(directory: str) -> tuple[dict, list[str]]:
    """Check the space in ``directory``; return ``(report, failures)`` (see the module docstring).
    ``FileNotFoundError`` when the directory holds no ``space.root``."""
    root_file = Path(directory) / "space.root"
    if not root_file.is_file():
        raise FileNotFoundError(f"{directory} holds no space.root: not a file-storage space")
    record = fmt.decode_root(root_file.read_bytes())
    failures: list[str] = []
    report = {"root": {"version": record.version, "segment": record.segment.hex, "offset": record.offset,
                       "epoch": record.epoch},
              "atoms": 0, "collections": 0, "opaque_types": [], "databases": {}}
    space = ObjectSpace(FileStorage(directory))
    try:
        top = space._current_space(_SpaceContext(space.storage))
        seen: set = set()
        opaque: set = set()
        stack = [top] if top is not None else []
        while stack:
            atom = stack.pop()
            ptr = atom.atom_pointer
            if ptr is None or ptr in seen:
                continue
            seen.add(ptr)
            try:
                state = atom._state()              # decodes the record
            except Exception as e:                 # noqa: BLE001 - reported, then the walk goes on
                failures.append(f"FAILED decoding {_where(atom)}: {type(e).__name__}: {e}")
                continue
            report["atoms"] += 1
            if isinstance(atom, OpaqueAtom):
                opaque.add(type(atom).__type_name__)
            if isinstance(atom, IndexedCollection):
                report["collections"] += 1
                try:
                    atom.validate()
                except Exception as e:             # noqa: BLE001
                    failures.append(f"FAILED validating {_where(atom)}{_root_label(space, atom)}: "
                                    f"{type(e).__name__}: {e}")
            children: list = []
            for value in state.values():
                _children(value, children)
            stack.extend(reversed(children))
        report["opaque_types"] = sorted(opaque)
        for name in space.list_databases():
            try:
                report["databases"][name] = space.open_database(name).run(notation.describe_database)
            except Exception as e:                 # noqa: BLE001
                failures.append(f"FAILED describing database {name!r}: {type(e).__name__}: {e}")
    finally:
        space.close()
    return report, failures


def _root_label(space, atom) -> str:
    """`` (root 'name' of database 'db')`` when ``atom`` is the current version of a root."""
    for db_name in space.list_databases():
        tx = space.open_database(db_name).new_transaction()
        try:
            for name in tx.root_names():
                if tx.get_root_object(name).atom_pointer == atom.atom_pointer:
                    return f" (root {name!r} of database {db_name!r})"
        finally:
            tx.abort()
    return ""


def main(argv=None) -> int:
    """Command-line entry point; returns the exit status."""
    parser = argparse.ArgumentParser(prog="python -m proto_db.tools.verify",
                                     description="Decode and validate every reachable atom of a file-storage space "
                                                 "and print its content in the conformance notation.")
    parser.add_argument("directory", help="the space's directory (holding space.root)")
    parser.add_argument("--out", help="write the JSON dump to this file instead of standard output")
    args = parser.parse_args(argv)
    try:
        report, failures = verify(args.directory)
    except Exception as e:                         # noqa: BLE001 - no readable space at all
        print(f"FAILED opening {args.directory}: {type(e).__name__}: {e}", file=sys.stderr)
        return 2
    text = json.dumps(report, indent=1, ensure_ascii=False)
    if args.out:
        Path(args.out).write_text(text + "\n", encoding="utf-8")
    else:
        print(text)
    for line in failures:
        print(line, file=sys.stderr)
    if failures:
        print(f"{len(failures)} failure(s); {report['atoms']} atoms decoded", file=sys.stderr)
        return 1
    print(f"OK: {report['atoms']} atoms decoded, {report['collections']} collections validated",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
