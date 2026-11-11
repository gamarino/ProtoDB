"""Verify a file-storage space, typically one another implementation wrote (Task 10.P, A2).

Usage::

    python -m proto_db.tools.verify <directory> [--out FILE]

The verifier opens the space read-only (it never publishes a root), then:

1. reads and checks the root record;
2. decodes every atom reachable from the space root, depth first: the space root, every
   database root and every older version it links to, every collection with its tree nodes and
   indexes, every record. Each atom is decoded once (by pointer). An atom of a type this process
   has no class for is read as an opaque atom (D59) and listed under ``opaque_types``;
3. checks that every record in a segment of the current write version is in its canonical
   encoding: re-encoding the decoded record (keeping the writer's attribute order, which is
   not normative) gives the stored bytes. A non-minimal integer, string, binary or container
   header, a ``float 32``, a non-canonical type tag or any other form the spec forbids writers to
   produce is a ``non-canonical`` failure. Type names are compared as stored: an application
   alias in ``t`` or in a reference (left by a rename) is valid; a built-in type must be its
   integer alias. The root record's body must be the canonical four-key map. Records in older
   segments hold legacy forms readers accept, and are not checked;
4. validates every collection reached (:meth:`IndexedCollection.validate`: the structure, then
   every index against the elements);
5. prints the current content of every database in the notation of the conformance manifest
   (:mod:`proto_db.tools.notation`) as JSON, to standard output or to ``--out``.

Every failure is reported on standard error as ``FAILED <where>: <exception class>: <message>``;
the verifier goes on with the next atom, so one run lists every failure it can reach.

Exit status: 0 when everything decoded, was canonical and validated; 1 when anything failed (the
dump is still written, for the parts that could be read), a missing or damaged segment included;
2 only when there is no readable root record (no ``space.root``, a bad CRC, an unknown version).
"""
from __future__ import annotations

import argparse
import json
import struct
import sys
from pathlib import Path

import msgpack

from ..collections.base import IndexedCollection
from ..core import codec
from ..core.atom import BUILTIN_TYPE_ALIASES, Atom, OpaqueAtom, type_name_of, type_tag
from ..storage import format as fmt
from ..storage.file import FileStorage
from ..transactions import ObjectSpace, _SpaceContext
from ..values import FrozenDict
from . import notation

__all__ = ["main", "verify"]


def _children(value, out: list) -> None:
    """Append every atom held by the canonical value ``value`` (at any depth: an explicit stack)
    to ``out``, in encoding order."""
    pending = [value]
    while pending:
        value = pending.pop()
        if isinstance(value, Atom):
            out.append(value)
        elif isinstance(value, (tuple, list)):
            pending.extend(reversed(value))
        elif isinstance(value, (dict, FrozenDict)):
            pending.extend(reversed([x for pair in value.items() for x in pair]))


def _where(atom) -> str:
    return f"{type(atom).__type_name__} at {atom.atom_pointer}"


class _NoRoot(Exception):
    """The directory holds no readable root record (exit status 2)."""


def _segment_version(directory: str, segment, cache: dict):
    """The format version in ``segment``'s header, or ``None`` when it cannot be read."""
    if segment not in cache:
        try:
            with open(Path(directory) / f"{segment.hex}.seg", "rb") as f:
                cache[segment] = struct.unpack_from("<8sH", f.read(fmt.SEGMENT_HEADER_SIZE))[1]
        except (OSError, struct.error):
            cache[segment] = None
    return cache[segment]


def _ref_bodies(data, out: list) -> list:
    """The bodies of the ``@ref`` maps in the plain data ``data``, in encoding order (at any depth:
    an explicit stack)."""
    pending = [data]
    while pending:
        data = pending.pop()
        if type(data) is dict:
            if len(data) == 1 and "@ref" in data and type(data["@ref"]) is list:
                out.append(data["@ref"])
                continue
            pending.extend(reversed([x for pair in data.items() for x in pair]))
        elif type(data) is list:
            pending.extend(reversed(data))
    return out


def _as_stored(stored_tag, written_tag):
    """The type tag to expect: the stored one when it is an application name naming the same
    type as the re-encoded tag (an alias left by a supported rename, D57), else the re-encoded
    one.  A built-in type is canonical only as its alias (an integer)."""
    if (type(stored_tag) is str and stored_tag not in BUILTIN_TYPE_ALIASES and type(written_tag) is str
            and type_name_of(stored_tag) == type_name_of(written_tag)):  # a str tag never raises
        return stored_tag
    return written_tag


def _non_canonical(storage, context, ptr) -> str:
    """Why the record at ``ptr`` is not in its canonical encoding, or ``""``.

    Type names are compared as stored: an application alias in ``t`` or in a reference is a
    valid spelling (a record written before a rename keeps it), not a non-canonical encoding
    (re-review I-R1)."""
    _, payload = storage.get_record(ptr)
    raw = msgpack.unpackb(payload, raw=False)
    state = codec.decode_state(raw["s"], context.load)
    encoded = codec.encode_state(state, lambda a: a.atom_pointer)
    stored_refs, written_refs = _ref_bodies(raw["s"], []), _ref_bodies(encoded, [])
    if len(stored_refs) == len(written_refs):
        for stored, written in zip(stored_refs, written_refs):
            if len(stored) == 3 and len(written) == 3:
                written[2] = _as_stored(stored[2], written[2])
    again = msgpack.packb({"t": _as_stored(raw["t"], type_tag(type_name_of(raw["t"]))), "oid": raw["oid"],
                           "s": encoded}, use_bin_type=True)
    if again == bytes(payload):
        return ""
    at = next((i for i, (a, b) in enumerate(zip(again, payload)) if a != b), min(len(again), len(payload)))
    return (f"the record is not in its canonical encoding (first difference at payload byte {at} of "
            f"{len(payload)}; canonical {len(again)} bytes)")


def verify(directory: str) -> tuple[dict, list[str]]:
    """Check the space in ``directory``; return ``(report, failures)`` (see the module docstring).
    :class:`_NoRoot` when the directory holds no readable root record."""
    root_file = Path(directory) / "space.root"
    if not root_file.is_file():
        raise _NoRoot(f"{directory} holds no space.root: not a file-storage space")
    data = root_file.read_bytes()
    try:
        record = fmt.decode_root(data)
    except Exception as e:                         # noqa: BLE001
        raise _NoRoot(f"space.root: {type(e).__name__}: {e}") from e
    failures: list[str] = []
    fields = msgpack.unpackb(data[4:], raw=False)
    body = msgpack.packb({"version": fields["version"], "segment": fields["segment"], "offset": fields["offset"],
                          "epoch": fields["epoch"]}, use_bin_type=True)
    if body != data[4:]:
        failures.append("FAILED non-canonical space.root: its body is not the canonical msgpack map "
                        "{version, segment, offset, epoch} (minimal encodings, that key order, no other key)")
    report = {"root": {"version": record.version, "segment": record.segment.hex, "offset": record.offset,
                       "epoch": record.epoch},
              "atoms": 0, "collections": 0, "opaque_types": [], "databases": {}}
    try:
        space = ObjectSpace(FileStorage(directory))
    except Exception as e:                         # noqa: BLE001 - a segment the storage cannot open
        failures.append(f"FAILED opening the storage: {type(e).__name__}: {e}")
        return report, failures
    versions: dict = {}
    try:
        try:
            top = space._current_space(_SpaceContext(space.storage))
        except Exception as e:                     # noqa: BLE001
            failures.append(f"FAILED reading the space root: {type(e).__name__}: {e}")
            top = None
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
            if _segment_version(directory, ptr.segment, versions) == fmt.WRITE_FORMAT_VERSION:
                try:
                    why = _non_canonical(space.storage, _SpaceContext(space.storage), ptr)
                except Exception as e:             # noqa: BLE001
                    why = f"{type(e).__name__}: {e}"
                if why:
                    failures.append(f"FAILED non-canonical {_where(atom)}: {why}")
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
        try:
            names = space.list_databases() if top is not None else []
        except Exception as e:                     # noqa: BLE001 - its nodes are reported above
            failures.append(f"FAILED listing the databases: {type(e).__name__}: {e}")
            names = []
        for name in names:
            try:
                report["databases"][name] = space.open_database(name).run(notation.describe_database)
            except Exception as e:                 # noqa: BLE001
                failures.append(f"FAILED describing database {name!r}: {type(e).__name__}: {e}")
    finally:
        space.close()
    return report, failures


def _root_label(space, atom) -> str:
    """`` (root 'name' of database 'db')`` when ``atom`` is the current version of a root, ``""``
    when it is not.  When the label cannot be told, the reason is stated in the label: the failure
    being labelled is still reported as itself, and the labelling failure is not hidden."""
    try:
        return _find_root_label(space, atom)
    except Exception as e:                         # noqa: BLE001 - stated in the reported line
        return f" (root label not found: {type(e).__name__}: {e})"


def _find_root_label(space, atom) -> str:
    for db_name in space.list_databases():
        tx = space.open_database(db_name).new_transaction()
        try:
            for name in tx.root_names():
                if tx.get_root_object(name).atom_pointer == atom.atom_pointer:
                    return f" (root {name!r} of database {db_name!r})"
        finally:
            tx.abort()
    return ""


def dump_text(value) -> str:
    """The JSON text of ``value``: ``json.dumps(value, indent=1, ensure_ascii=False)`` for the
    first :data:`INDENTED_LEVELS` levels of nesting; deeper content is written on one line, with
    ``json``'s default separators.  The JSON value is the same either way: the layout only keeps
    the text linear in the size of a deep description (a record chain nests as deep as the chain,
    R128; one indented line per level would make it quadratic).  Written with an explicit stack,
    since ``json``'s encoder recurses; scalars and keys are written by ``json`` itself."""
    out: list = []
    stack: list = []            # [iterator, closing bracket, depth, first item?, is a dict?]

    def open_value(v, depth: int) -> None:
        if isinstance(v, dict):
            if not v:
                out.append("{}")
            else:
                out.append("{")
                stack.append([iter(v.items()), "}", depth + 1, True, True])
        elif isinstance(v, (list, tuple)):
            if not v:
                out.append("[]")
            else:
                out.append("[")
                stack.append([iter(v), "]", depth + 1, True, False])
        else:
            out.append(json.dumps(v, ensure_ascii=False))

    open_value(value, 0)
    while stack:
        frame = stack[-1]
        item = next(frame[0], _END)
        depth = frame[2]
        indented = depth <= INDENTED_LEVELS
        if item is _END:
            stack.pop()
            out.append("\n" + " " * (depth - 1) + frame[1] if indented else frame[1])
            continue
        if indented:
            out.append(("\n" if frame[3] else ",\n") + " " * depth)
        elif not frame[3]:
            out.append(", ")
        frame[3] = False
        if frame[4]:
            key, item = item
            # A key is a str, or a scalar json writes as one (dict keys of the report are str).
            out.append(json.dumps({key: 0}, ensure_ascii=False)[1:-4] + ": ")
        open_value(item, depth)
    return "".join(out)


#: Nesting levels :func:`dump_text` indents; the conformance manifest's descriptions are far
#: shallower, so their dumps are exactly ``json.dumps(..., indent=1)``.
INDENTED_LEVELS = 64
_END = object()


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
    except _NoRoot as e:
        print(f"FAILED opening {args.directory}: {e}", file=sys.stderr)
        return 2
    text = dump_text(report)
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
