"""The step-6 expected output of the protoBaseST handoff (Task 11; preflight F33).

``python -m proto_db.agent.step6 --path P --database D --out FILE [--canonical FILE] [--mapping M=I ...]``
opens the database read-only in effect (it only reads) and writes, as JSON with sorted keys and two-space
indentation (the agent interface's ``to_json`` form), ``{"describe": {type name: description}, "queries":
{name: {"query", "rows", "version"}}}``: ``describe`` of every entity the catalog lists, and the answers
(the query, its rows and the version read; no ``explain``, no ``terms``) of the named queries of
``docs/kit/semantic/queries.py`` (``STEP6``), their starting cells found through ``list_entities``.  With
``--canonical`` it also writes the canonical form of the catalog (format spec, "Composition and the canonical
form").  Two runs over one database give identical bytes: protoBaseST's output for the same database must be
equal, which ``tests/conformance/test_catalog_cross_language.py`` checks with :func:`differences` and
:func:`canonical_differences`.

The kit's modules import as ``docs.kit.semantic.*`` (preflight F41); this command puts the repository root
on ``sys.path`` and uses the kit's module mapping unless ``--mapping`` is given.
"""
import argparse
import json
import os
import sys

import msgpack

from ..core import codec
from ..errors import ProtoCorruptionError
from ..transactions import ObjectSpace
from ..storage.file import FileStorage
from .operations import describe, from_json, list_entities, query

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _kit():
    """The kit's query module (the repository root on ``sys.path``: ``docs.kit`` is a namespace package)."""
    if REPO not in sys.path:
        sys.path.insert(0, REPO)
    from docs.kit.semantic import queries
    return queries


def _open(path: str, mapping: dict | None) -> ObjectSpace:
    return ObjectSpace(FileStorage(path), modules_mapping=_kit().MAPPING if mapping is None else mapping)


def expected(db) -> dict:
    """The expected output for the open database ``db``."""
    kit = _kit()
    entities = list_entities(db)["entities"]
    out = {"describe": {entity["type_name"]: describe(db, entity["type_name"]) for entity in entities},
           "queries": {}}
    cells = {"company": kit.company_cell(list_entities(db, kit.COMPANY))}
    for name, tree in kit.STEP6.items():
        answer = query(db, from_json(tree(cells)))
        # Only what the format specification defines is compared: the query, its rows and the version read.
        # ``explain`` (the planner's plan and estimates) and ``terms`` belong to the implementation.
        out["queries"][name] = {key: answer[key] for key in ("query", "rows", "version")}
        if name == "widget":
            cells["widget"] = kit.widget_cell(answer)
    return out


def canonical(db) -> bytes:
    """The canonical form of the catalog of the current version of ``db``."""
    from ..catalog.model import canonical_form
    from ..semantic import semantic
    tr = db.new_transaction()
    try:
        return canonical_form(semantic(tr).catalog())
    finally:
        tr.abort()


def text(output: dict) -> str:
    """``output`` as the file's text: sorted keys, two-space indentation, a final newline."""
    return json.dumps(output, sort_keys=True, ensure_ascii=False, indent=2) + "\n"


def write_expected(path: str, database: str, out: str, *, canonical_out: str | None = None, mapping=None) -> dict:
    """Write the expected output of ``database`` in the space at ``path`` to ``out`` (and its canonical form
    to ``canonical_out``); return the output."""
    space = _open(path, mapping)
    try:
        db = space.open_database(database)
        output, form = to_plain(expected(db)), canonical(db)
    finally:
        space.close()
    with open(out, "w", encoding="utf-8", newline="\n") as f:
        f.write(text(output))
    if canonical_out is not None:
        with open(canonical_out, "wb") as f:
            f.write(form)
    return output


def canonical_bytes(path: str, database: str, mapping=None) -> bytes:
    """The canonical form of the catalog of ``database`` in the space at ``path``."""
    space = _open(path, mapping)
    try:
        return canonical(space.open_database(database))
    finally:
        space.close()


def to_plain(output: dict) -> dict:
    """``output`` as the JSON file holds it (what a reader of the file gets)."""
    return json.loads(text(output))


# -- the comparator ------------------------------------------------------------------------------------------

def differences(expected_value, actual_value, path: str = "") -> list:
    """The differences between two decoded JSON values, one line each (``path: what differs``), empty when
    they are equal.  Equal means the same type (``1`` is not ``1.0`` nor ``True``), the same keys and the same
    items in the same order."""
    here = path or "<root>"
    if isinstance(expected_value, dict) and isinstance(actual_value, dict):
        found = []
        for key in expected_value.keys() | actual_value.keys():
            sub = f"{path}.{key}" if path else str(key)
            if key not in actual_value:
                found.append(f"{sub}: missing in the actual output")
            elif key not in expected_value:
                found.append(f"{sub}: not in the expected output")
            else:
                found.extend(differences(expected_value[key], actual_value[key], sub))
        return sorted(found)
    if isinstance(expected_value, list) and isinstance(actual_value, list):
        if len(expected_value) != len(actual_value):
            return [f"{here}: length {len(expected_value)} expected, {len(actual_value)} actual"]
        return [d for i, (e, a) in enumerate(zip(expected_value, actual_value))
                for d in differences(e, a, f"{path}[{i}]")]
    if type(expected_value) is not type(actual_value) or expected_value != actual_value:
        return [f"{here}: expected {expected_value!r}, actual {actual_value!r}"]
    return []


def _no_reference(pointer, type_name):
    raise ProtoCorruptionError("A canonical form holds no references")


def _plain(value):
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


def decode_canonical(data: bytes):
    """One canonical form as plain values (a tuple as a list): what two implementations compare."""
    return _plain(codec.decode_value(msgpack.unpackb(data, raw=False), _no_reference))


def file_differences(expected_value, actual_path: str) -> list:
    """:func:`differences` between ``expected_value`` and the JSON file at ``actual_path``: the comparison is of
    the parsed data, never of the file's formatting (indentation, key order, escaping, number spelling)."""
    with open(actual_path, encoding="utf-8") as f:
        return differences(expected_value, json.load(f))


def canonical_differences(expected_bytes: bytes, actual_bytes: bytes) -> list:
    """The differences between two canonical forms: none when the bytes are equal; else the differences of
    the decoded values (:func:`differences`), or one line when the bytes alone differ or one is not msgpack."""
    if expected_bytes == actual_bytes:
        return []
    try:
        expected_value, actual_value = decode_canonical(expected_bytes), decode_canonical(actual_bytes)
    except (msgpack.UnpackException, ValueError, ProtoCorruptionError) as e:
        return [f"<bytes>: not one msgpack value ({type(e).__name__})"]
    found = differences(expected_value, actual_value)
    return found or ["<bytes>: the decoded values are equal but the encodings differ (not canonical)"]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m proto_db.agent.step6", description=__doc__.splitlines()[0])
    parser.add_argument("--path", required=True, help="the directory of the FileStorage space")
    parser.add_argument("--database", required=True, help="the database to read")
    parser.add_argument("--out", required=True, help="the file to write the expected output to")
    parser.add_argument("--canonical", help="also write the catalog's canonical form to this file")
    parser.add_argument("--mapping", action="append", metavar="MODULE=IMPORT.PATH",
                        help="where the code of a registry module is imported from (default: the kit's)")
    args = parser.parse_args(argv)
    from .__main__ import parse_mapping
    if REPO not in sys.path:
        sys.path.insert(0, REPO)
    write_expected(args.path, args.database, args.out, canonical_out=args.canonical,
                   mapping=parse_mapping(args.mapping) if args.mapping else None)
    return 0


if __name__ == "__main__":
    sys.exit(main())
