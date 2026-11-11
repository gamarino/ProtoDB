"""Value nesting depth: ProtoBase reads back every value it writes, at any depth up to the limit (B12, D60).

The format allows a value to nest at most ``MAX_VALUE_DEPTH`` (1022) MessagePack containers (format
spec, "Value nesting depth").  Catches: a codec walk that recurses once per level (a bare
``RecursionError`` at about 330 levels when reading and 497 when writing, depending on the Python
stack in use, so ProtoBase wrote values it could not read back); a writer and a reader that refuse
at different depths, or with other error classes; an iterative walk that writes other bytes, meets
atoms in another order or decodes other values than the recursive walk it replaces; a helper on
the same paths (canonical checks, map keys, order keys, hashes, equality, the verifier) still
recursing.
"""
from __future__ import annotations

import json
import os
import random
import subprocess
import sys
import threading
import uuid
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import msgpack
import pytest

from proto_db.collections.base import same_value
from proto_db.core import codec
from proto_db.core.atom import Atom, AtomPointer
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoCorruptionError, ProtoValidationError
from proto_db.indexing.base import element_identity
from proto_db.indexing.keyed import surrogate
from proto_db.storage.file import FileStorage
from proto_db.tools import notation, verify
from proto_db.transactions import ObjectSpace
from proto_db import values
from proto_db.values import (MAX_VALUE_DEPTH, MISSING, FrozenDict, FrozenList, MutableObject, canonical,
                             check_map_keys, hash_key, is_orderable, map_key_ids, order_key)

ROOT = Path(__file__).resolve().parents[2]
DEPTHS = [1, 331, 497, 498, MAX_VALUE_DEPTH]


# -- helpers -----------------------------------------------------------------------------------

def nest(n: int, leaf=1, kind: str = "list"):
    """``leaf`` inside ``n`` containers of ``kind``, built without recursion."""
    value = leaf
    for _ in range(n):
        value = [value] if kind == "list" else (value,) if kind == "tuple" else {"k": value}
    return value


def wire_nest(n: int, leaf_wire=1, kind: str = "list"):
    """The wire data of :func:`nest`, around the wire form ``leaf_wire``: what a raw packer writes."""
    data = leaf_wire
    for _ in range(n):
        data = [data] if kind == "list" else {"@tup": [data]} if kind == "tuple" else {"@map": [["k", data]]}
    return data


def wire_depth(data) -> int:
    """The MessagePack containers the plain msgpack data ``data`` nests (an explicit stack)."""
    deepest, pending = 0, [(data, 0)]
    while pending:
        item, depth = pending.pop()
        if type(item) is list:
            pending.extend((x, depth + 1) for x in item)
            depth += 1
        elif type(item) is dict:
            pending.extend((x, depth + 1) for pair in item.items() for x in pair)
            depth += 1
        deepest = max(deepest, depth)
    return deepest


def bottom(value):
    """The innermost item of a value of single-item containers (a list, tuple or one-pair map)."""
    while isinstance(value, (list, tuple, dict)):
        value = next(iter(value.values())) if isinstance(value, dict) else value[0]
    return value


def on_deep_stack(fn, frames: int = 900):
    """``fn()`` run in a new thread with ``frames`` Python frames already on its stack, under the
    default recursion limit: whatever ``fn`` does, it has only the rest of the stack."""
    out = {}

    def dive(n):
        if n > 0:
            return dive(n - 1)
        depth, frame = 0, sys._getframe()
        while frame is not None:
            depth, frame = depth + 1, frame.f_back
        out["depth"] = depth
        return fn()

    def run():
        here, frame = 0, sys._getframe()
        while frame is not None:
            here, frame = here + 1, frame.f_back
        try:
            out["value"] = dive(frames - here - 1)
        except BaseException as e:          # noqa: BLE001 - re-raised in the caller's thread
            out["error"] = e

    assert sys.getrecursionlimit() <= 1000
    worker = threading.Thread(target=run)
    worker.start()
    worker.join()
    if "error" in out:
        raise out["error"]
    assert out["depth"] >= frames - 1
    return out["value"]


class _NoContext:
    """The context of the stubs that stand for referenced atoms: never used to load."""


SEGMENT = uuid.UUID("0123456789abcdef0123456789abcdef")


def stub(ptr: AtomPointer, type_name: str = "proto_db.core.dbobject.DBObject") -> Atom:
    return Atom._stub(_NoContext(), ptr, type_name)


def pointer_of(atom: Atom) -> AtomPointer:
    return atom.atom_pointer


def saved_atom(offset: int) -> DBObject:
    """A record that stands for one saved at ``offset`` (its pointer set; nothing is stored)."""
    atom = DBObject(x=1)
    object.__setattr__(atom, "atom_pointer", AtomPointer(SEGMENT, offset))
    return atom


def round_trip(value) -> bytes:
    """Encode ``value``, pack, unpack, decode, encode again: the bytes, checked to be stable."""
    data = msgpack.packb(codec.encode_value(value, pointer_of), use_bin_type=True)
    decoded = codec.decode_value(msgpack.unpackb(data, raw=False), stub)
    assert msgpack.packb(codec.encode_value(decoded, pointer_of), use_bin_type=True) == data
    assert msgpack.packb(codec.encode_value(codec.redecode_value(value, stub), pointer_of),
                         use_bin_type=True) == data
    return data


# -- round trips at the reported depths ---------------------------------------------------------

@pytest.mark.parametrize("stack", ["default", "deep"])
@pytest.mark.parametrize("n", DEPTHS)
def test_nested_lists_round_trip_at_every_reported_depth(n, stack):
    def check():
        value = canonical(nest(n))
        data = round_trip(value)
        assert wire_depth(msgpack.unpackb(data, raw=False)) == n
        decoded = codec.decode_value(msgpack.unpackb(data, raw=False), stub)
        assert bottom(decoded) == 1 and type(decoded) is FrozenList
        return True
    assert (on_deep_stack(check) if stack == "deep" else check())


def _shapes():
    """(name, value at exactly MAX_VALUE_DEPTH, its wire form one container deeper, by a raw packer)."""
    aware = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    out = []
    for name, kind, leaf, leaf_wire, leaf_cost in [
            ("lists-int", "list", 1, 1, 0),
            ("lists-aware-datetime", "list", aware, codec.encode_value(aware, None), 3),
            ("lists-decimal", "list", Decimal("1.20"), {"@dec": "1.20"}, 1),
            ("lists-ref", "list", saved_atom(26), {"@ref": [SEGMENT.bytes, 26, 19]}, 2),
            ("lists-empty-map", "list", {}, {"@map": []}, 2),
            ("tuples", "tuple", 1, 1, 0),
            ("maps", "map", 1, 1, 0)]:
        per = {"list": 1, "tuple": 2, "map": 3}[kind]
        levels, rest = divmod(MAX_VALUE_DEPTH - leaf_cost, per)
        value = nest(rest, nest(levels, leaf, kind))            # rest lists outside: exactly the limit
        over = [wire_nest(rest, wire_nest(levels, leaf_wire, kind))]
        out.append((name, value, over))
    # Only the leaf crosses the limit: the time-zone array of an aware @dt is container 1023.
    out.append(("leaf-crosses", nest(MAX_VALUE_DEPTH - 3, aware),
                wire_nest(MAX_VALUE_DEPTH - 2, codec.encode_value(aware, None))))
    mixed = nest(200, nest(100, nest(319, aware), "map"), "tuple")
    out.append(("mixed", mixed, [wire_nest(200, wire_nest(100, wire_nest(319, codec.encode_value(aware, None)),
                                                          "map"), "tuple")]))
    return out


SHAPES = _shapes()


@pytest.mark.parametrize("stack", ["default", "deep"])
@pytest.mark.parametrize("name, value, over", SHAPES, ids=[s[0] for s in SHAPES])
def test_writer_and_reader_agree_on_the_limit_for_every_kind(name, value, over, stack):
    def check():
        data = round_trip(canonical(value))
        assert wire_depth(msgpack.unpackb(data, raw=False)) == MAX_VALUE_DEPTH
        # One container more: the writer refuses the value (canonical and the encoder), the reader
        # its bytes, both at the same count.
        raw = msgpack.packb(over, use_bin_type=True)
        assert wire_depth(msgpack.unpackb(raw, raw=False)) == MAX_VALUE_DEPTH + 1
        deeper = [value]
        with pytest.raises(ProtoValidationError, match="MAX_VALUE_DEPTH = 1022"):
            canonical(deeper)
        with pytest.raises(ProtoValidationError, match="MAX_VALUE_DEPTH = 1022"):
            codec.encode_value(deeper, pointer_of)
        with pytest.raises(ProtoCorruptionError, match="MAX_VALUE_DEPTH = 1022"):
            codec.decode_value(msgpack.unpackb(raw, raw=False), stub)
        return True
    assert (on_deep_stack(check) if stack == "deep" else check())


def test_the_writer_names_the_field():
    too_deep = nest(MAX_VALUE_DEPTH + 1)
    with pytest.raises(ProtoValidationError, match=r"Field 'v' of .*DBObject: .*MAX_VALUE_DEPTH"):
        DBObject(v=too_deep)
    with pytest.raises(ProtoValidationError, match=r"Attribute 'v': .*MAX_VALUE_DEPTH"):
        codec.encode_state({"v": too_deep}, pointer_of)
    with pytest.raises(ProtoCorruptionError, match="MAX_VALUE_DEPTH"):
        codec.decode_state({"v": wire_nest(MAX_VALUE_DEPTH + 1)}, stub)
    space = ObjectSpace(_memory())
    tr = space.new_database("d").new_transaction()
    with pytest.raises(ProtoValidationError, match="MAX_VALUE_DEPTH"):
        tr.set_root_object("r", too_deep)
    tr.abort()
    space.close()


def _memory():
    from proto_db.storage.memory import MemoryStorage
    return MemoryStorage()


def test_the_messagepack_layer_holds_a_record_at_the_limit_and_no_deeper():
    """The justification of 1022 (format spec): an atom payload adds two containers, and
    msgpack-python (>= 1.2.0, the declared floor) packs and unpacks 1024 and refuses 1025."""
    assert msgpack.version >= (1, 2, 0)
    envelope = {"t": 19, "oid": bytes(16), "s": {"v": nest(MAX_VALUE_DEPTH)}}
    data = msgpack.packb(envelope, use_bin_type=True)
    assert wire_depth(msgpack.unpackb(data, raw=False)) == MAX_VALUE_DEPTH + 2 == 1024
    with pytest.raises(ValueError):
        msgpack.packb({"t": 19, "oid": bytes(16), "s": {"v": nest(MAX_VALUE_DEPTH + 1)}}, use_bin_type=True)
    deeper = data.replace(b"\x91" * MAX_VALUE_DEPTH, b"\x91" * (MAX_VALUE_DEPTH + 1))
    with pytest.raises(msgpack.exceptions.StackError):
        msgpack.unpackb(deeper, raw=False)


# -- through an ObjectSpace, a fresh process and the verifier -----------------------------------

def _python():
    flags = ["-X", f"gil={int(sys._is_gil_enabled())}"] if hasattr(sys, "_is_gil_enabled") else []
    return [sys.executable, *flags]


_READER = r"""
import sys
from proto_db.storage.file import FileStorage
from proto_db.transactions import ObjectSpace
space = ObjectSpace(FileStorage(sys.argv[1]))
with space.open_database("d").new_transaction() as tr:
    record = tr.get_root_object("r")
    for name in ("lists", "mixed"):
        value, depth = getattr(record, name), 0
        while isinstance(value, (list, tuple, dict)):
            value = next(iter(value.values())) if isinstance(value, dict) else value[0]
            depth += 1
        print(name, depth, repr(value))
space.close()
"""


def test_a_record_at_the_limit_is_read_back_by_a_fresh_process_and_verifies(tmp_path):
    path = str(tmp_path / "space")
    aware = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    space = ObjectSpace(FileStorage(path))
    with space.new_database("d").new_transaction() as tr:
        tr.set_root_object("r", DBObject(lists=nest(MAX_VALUE_DEPTH),
                                         mixed=nest(200, nest(100, nest(319, aware), "map"), "tuple")))
    space.close()
    env = {**os.environ, "PYTHONPATH": str(ROOT)}
    result = subprocess.run([*_python(), "-c", _READER, path], cwd=ROOT, capture_output=True, text=True, env=env)
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "lists 1022 1", f"mixed 619 {aware!r}"]
    result = subprocess.run([*_python(), "-m", "proto_db.tools.verify", path, "--out", str(tmp_path / "dump.json")],
                            cwd=ROOT, capture_output=True, text=True, env=env)
    assert result.returncode == 0, result.stderr
    assert "OK:" in result.stderr


#: A placeholder value whose packed bytes are replaced by deeper data than any writer produces.
_PLACEHOLDER = b"\x00over-limit placeholder\x00"


def _space_with_an_over_limit_record(path: str) -> None:
    """A space whose root record holds a value one container deeper than the limit: written as a
    placeholder, whose packed bytes are replaced by 1023 arrays before the record is framed (no
    conforming writer can produce it, and msgpack's packer refuses it)."""
    storage = FileStorage(path)
    real_append = storage._append
    marker = msgpack.packb(_PLACEHOLDER, use_bin_type=True)

    def append(kind, payload):
        if marker in payload:
            payload = payload.replace(marker, b"\x91" * (MAX_VALUE_DEPTH + 1) + b"\x01")
        return real_append(kind, payload)
    storage._append = append
    space = ObjectSpace(storage)
    with space.new_database("d").new_transaction() as tr:
        tr.set_root_object("r", DBObject(v=_PLACEHOLDER))
    space.close()


def test_the_verifier_names_a_record_deeper_than_the_limit(tmp_path, capsys):
    path = str(tmp_path / "space")
    _space_with_an_over_limit_record(path)
    code = verify.main([path, "--out", str(tmp_path / "dump.json")])
    err = capsys.readouterr().err
    assert code == 1
    failures = [line for line in err.splitlines() if line.startswith("FAILED")]
    assert any(line.startswith("FAILED decoding proto_db.core.dbobject.DBObject at ") and "ProtoCorruptionError" in line
               and "MAX_VALUE_DEPTH = 1022" in line for line in failures), err
    space = ObjectSpace(FileStorage(path))
    with space.open_database("d").new_transaction() as tr:
        with pytest.raises(ProtoCorruptionError, match="MAX_VALUE_DEPTH"):
            tr.get_root_object("r").v
    space.close()


def test_the_over_limit_conformance_vectors_are_named_failures():
    manifest = json.loads((ROOT / "tests" / "conformance" / "manifest.json").read_text())
    ids = {v["id"] for v in manifest["invalid"] if v["id"].startswith("value-depth-over-")}
    assert ids == {"value-depth-over-list", "value-depth-over-mixed", "value-depth-over-leaf",
                   "value-depth-over-empty-list", "value-depth-over-empty-tuple", "value-depth-over-empty-map",
                   "value-depth-over-map-key"}
    for vid in sorted(ids):
        data = (ROOT / "tests" / "conformance" / "vectors" / "invalid" / f"{vid}.bin").read_bytes()
        with pytest.raises(ProtoCorruptionError, match="MAX_VALUE_DEPTH = 1022"):
            on_deep_stack(lambda: codec.decode_value(msgpack.unpackb(data, raw=False), stub))


# -- the helpers on the same paths, at depth ------------------------------------------------------

def test_keys_hashes_equality_and_the_verifier_walks_work_at_depth():
    def check():
        deep_key = nest(500, 7, "tuple")                       # 1000 containers; 1003 as a map key
        other_key = nest(500, 8, "tuple")
        assert is_orderable(deep_key) and not is_orderable(nest(500, [1], "tuple"))
        assert order_key(deep_key) < order_key(other_key)
        assert hash_key(deep_key) == hash_key(nest(500, 7.0, "tuple")) != hash_key(other_key)
        assert map_key_ids(deep_key) == map_key_ids(nest(500, 7, "tuple"))
        check_map_keys([deep_key, other_key])
        with pytest.raises(ProtoValidationError, match="conflict"):
            check_map_keys([deep_key, nest(500, Decimal(7), "tuple")])
        keyed = canonical({deep_key: "a", other_key: "b"})
        data = round_trip(keyed)
        assert wire_depth(msgpack.unpackb(data, raw=False)) == 1003
        deep = canonical(nest(MAX_VALUE_DEPTH - 1, [1, 2]))
        assert same_value(deep, canonical(nest(MAX_VALUE_DEPTH - 1, [1, 2])))
        assert not same_value(deep, canonical(nest(MAX_VALUE_DEPTH - 1, [1, 3])))
        assert element_identity(deep) == element_identity(canonical(nest(MAX_VALUE_DEPTH - 1, [1.0, 2])))
        assert surrogate(deep) == surrogate(canonical(nest(MAX_VALUE_DEPTH - 1, [1.0, 2])))
        described = notation.describe(deep)
        depth = 0
        while "list" in described:
            described, depth = described["list"][0], depth + 1
        assert depth == MAX_VALUE_DEPTH and described == {"int": "1"}
        atom = saved_atom(26)
        found = []
        verify._children(canonical(nest(MAX_VALUE_DEPTH - 2, atom)), found)
        assert found == [atom]
        wire = codec.encode_value(canonical(nest(MAX_VALUE_DEPTH - 2, atom)), pointer_of)
        assert verify._ref_bodies(wire, []) == [[SEGMENT.bytes, 26, 19]]
        return True
    assert on_deep_stack(check)


# -- byte identity: the iterative walks against the recursive code they replace -----------------

def _ref_canonical(value):
    """``canonical()`` as it was before B12 (recursive), the reference for the depths it handles."""
    cls = type(value)
    if cls is str:
        return values.check_text(value)
    if cls in values._EXACT_SCALARS or value is MISSING:
        return value
    plain = values.plain_scalar(value)
    if plain is not None:
        return values.check_text(plain) if type(plain) is str else plain
    if isinstance(value, tuple):
        items = tuple(_ref_canonical(v) for v in value)
        return value if type(value) is tuple and all(a is b for a, b in zip(items, value)) else items
    if isinstance(value, list):
        items = [_ref_canonical(v) for v in value]
        if type(value) is FrozenList and all(a is b for a, b in zip(items, value)):
            return value
        return FrozenList(items)
    if isinstance(value, dict):
        pairs = [(_ref_canonical(k), _ref_canonical(v)) for k, v in value.items()]
        if type(value) is FrozenDict and all(
                ka is kb and va is vb for (ka, va), (kb, vb) in zip(pairs, value.items())):
            return value
        if not all(type(k) in values._PLAIN_KEY_TYPES for k, _ in pairs):
            check_map_keys(k for k, _ in pairs)
        out = FrozenDict(pairs)
        if len(out) != len(pairs):
            raise ProtoValidationError("conflict")
        return out
    if values.is_atom(value) or isinstance(value, MutableObject):
        return value
    raise ProtoValidationError(f"Unsupported value type: {type(value).__name__}")


def _ref_encode(value, save_child):
    """``encode_value`` as it was before B12 (recursive); scalars by the unchanged scalar code."""
    if isinstance(value, list):
        return [_ref_encode(v, save_child) for v in value]
    if isinstance(value, tuple):
        return {"@tup": [_ref_encode(v, save_child) for v in value]}
    if isinstance(value, dict):
        if type(value) is not FrozenDict:
            value = _ref_canonical(value)
        elif not all(type(k) in codec._PLAIN_KEY_TYPES for k in value):
            check_map_keys(value)
        return {"@map": [[_ref_encode(k, save_child), _ref_encode(v, save_child)] for k, v in value.items()]}
    return codec._encode_leaf(value, save_child)


def _ref_decode(data, load_ref):
    """``decode_value`` as it was before B12 (recursive); tagged scalars by the unchanged decoders."""
    if data is None or type(data) in (bool, int, float, str, bytes):
        return data
    if type(data) is list:
        return FrozenList(_ref_decode(v, load_ref) for v in data)
    (tag, body), = data.items()
    if tag == "@tup":
        return tuple(_ref_decode(v, load_ref) for v in body)
    if tag == "@map":
        pairs = [(_ref_decode(k, load_ref), _ref_decode(v, load_ref)) for k, v in body]
        if not all(type(k) is str for k, _ in pairs):
            check_map_keys((k for k, _ in pairs), [codec._holds_zone_form(k) for k, _ in body])
        return FrozenDict(pairs)
    return codec._DECODERS[tag](body, load_ref)


def _ref_redecode(value, load_ref, inline):
    t = type(value)
    if t is str or t is bytes:
        inline[0] += len(value)
        return value
    if t is int or t is float or value is None or t is bool:
        return value
    if t is tuple:
        return tuple([_ref_redecode(v, load_ref, inline) for v in value])
    if t in codec._ATOM_CLASSES:
        return load_ref(value.atom_pointer, type(value).__type_name__)
    if t is list or t is FrozenList:
        return FrozenList([_ref_redecode(v, load_ref, inline) for v in value])
    if t is dict or t is FrozenDict:
        out = {}
        for k, v in value.items():
            out[_ref_redecode(k, load_ref, inline)] = _ref_redecode(v, load_ref, inline)
        return FrozenDict(out)
    return codec.decode_value(codec.encode_value(value, pointer_of), load_ref)


_ZONE = ZoneInfo("America/Argentina/Buenos_Aires")


def _scalar(rng: random.Random):
    return rng.choice([
        lambda: None, lambda: rng.random() < 0.5, lambda: rng.randint(-1000, 1000), lambda: 2**70 + rng.randint(0, 9),
        lambda: rng.random(), lambda: float("nan"), lambda: rng.choice(["a", "bc", "año", ""]),
        lambda: bytes([rng.randint(0, 255)] * rng.randint(0, 3)), lambda: Decimal(rng.choice(["1.20", "-0", "1E+2"])),
        lambda: date(2026, 1, rng.randint(1, 28)), lambda: time(1, 2, rng.randint(0, 59)),
        lambda: time(1, 2, 3, tzinfo=timezone(timedelta(hours=-3))),
        lambda: datetime(2026, 1, 2, 3, 4, rng.randint(0, 59)),
        lambda: datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        lambda: datetime(2026, 1, 2, 3, 4, 5, tzinfo=_ZONE),
        lambda: timedelta(days=rng.randint(-5, 5), seconds=7), lambda: uuid.UUID(int=rng.getrandbits(128)),
        lambda: MISSING, lambda: MutableObject("cell"),
        lambda: saved_atom(rng.randint(0, 10**6))])()


def _random_value(rng: random.Random, depth: int):
    """A random value nested at most ``depth`` levels, every kind of container and scalar included."""
    if depth == 0 or rng.random() < 0.2:
        return _scalar(rng)
    kind = rng.choice(["list", "tuple", "dict", "spine"])
    if kind == "spine":                         # a long single-item chain: crosses the recursion budget
        return [_random_value(rng, depth - 1)]
    items = [_random_value(rng, depth - 1) for _ in range(rng.randint(0, 3))]
    if kind == "list":
        return items
    if kind == "tuple":
        return tuple(items)
    keys = [f"k{i}" for i in range(len(items))]
    if items and rng.random() < 0.4:
        keys[0] = rng.choice([7, b"b", (1, "x"), (2, (3, "y"))])
    return dict(zip(keys, items))


@pytest.mark.parametrize("seed", range(40))
def test_iterative_walks_write_and_read_what_the_recursive_walks_did(seed):
    rng = random.Random(seed)
    for _ in range(10):
        raw = _random_value(rng, rng.choice([3, 8, 20, 60]))
        value = canonical(raw)
        assert notation.describe(value) == notation.describe(_ref_canonical(raw))
        assert canonical(value) is value
        assert values._canonical_tree(value, 0) is value if isinstance(value, (list, tuple, dict)) else True

        met, ref_met = [], []

        def saving(atom, met=met):
            met.append(atom)
            return atom.atom_pointer

        def ref_saving(atom):
            ref_met.append(atom)
            return atom.atom_pointer
        data = msgpack.packb(codec.encode_value(value, saving), use_bin_type=True)
        assert data == msgpack.packb(_ref_encode(value, ref_saving), use_bin_type=True)
        assert met == ref_met                   # atoms met in encoding order
        if isinstance(value, (list, tuple, dict)):
            forced = []
            assert msgpack.packb(codec._encode_tree(value, lambda a: forced.append(a) or a.atom_pointer, 0),
                                 use_bin_type=True) == data
            assert forced == met
        assert values.exact_token(value) == msgpack.packb(
            _ref_encode(value, values.TokenAtoms()), use_bin_type=True)

        wire = msgpack.unpackb(data, raw=False)
        loads, ref_loads = [], []
        decoded = codec.decode_value(wire, lambda p, t: loads.append(p) or stub(p, t))
        ref_decoded = _ref_decode(wire, lambda p, t: ref_loads.append(p) or stub(p, t))
        assert notation.describe(decoded) == notation.describe(ref_decoded)
        assert loads == ref_loads
        if isinstance(wire, (list, dict)):
            forced_loads = []
            again = codec._decode_tree(wire, lambda p, t: forced_loads.append(p) or stub(p, t), 0)
            assert notation.describe(again) == notation.describe(decoded) and forced_loads == loads

        inline, ref_inline = [0], [0]
        redecoded = codec.redecode_value(value, stub, inline)
        assert notation.describe(redecoded) == notation.describe(_ref_redecode(value, stub, ref_inline))
        assert notation.describe(redecoded) == notation.describe(decoded)
        assert inline == ref_inline
        if type(value) in (list, tuple, dict, FrozenList, FrozenDict):
            forced_inline = [0]
            forced = codec._redecode_tree(value, stub, forced_inline)
            assert notation.describe(forced) == notation.describe(decoded) and forced_inline == inline


@pytest.mark.parametrize("seed", range(6))
def test_random_values_near_the_limit_round_trip_on_a_deep_stack(seed):
    rng = random.Random(1000 + seed)

    def check():
        for _ in range(3):
            core = _random_value(rng, 6)
            value = canonical(core)
            spine = MAX_VALUE_DEPTH - wire_depth(codec.encode_value(value, pointer_of))
            deep = canonical(nest(spine, core))
            data = round_trip(deep)
            assert wire_depth(msgpack.unpackb(data, raw=False)) == MAX_VALUE_DEPTH
            with pytest.raises(ProtoValidationError, match="MAX_VALUE_DEPTH"):
                canonical([deep])
        return True
    assert on_deep_stack(check)


# -- other walks over stored values: the query result cache, explain() and to_arrow -------------
#
# Off the storage paths, but they walk values the product now writes at any depth up to the
# limit (review M1): each must work on a deep stack and give what its recursive form gave.

def _ref_freeze(value, size, refs):
    """The query cache's recursive ``_freeze`` before B12 (reference)."""
    from proto_db.query import cache
    if value is None or value is MISSING:
        return value
    if isinstance(value, Atom):
        size[0] += cache._REF_SIZE
        refs[0] += 1
        return cache._AtomRef(value.atom_pointer, type(value).__type_name__)
    if isinstance(value, dict):
        size[0] += sys.getsizeof(value)
        return FrozenDict((_ref_freeze(k, size, refs), _ref_freeze(v, size, refs)) for k, v in value.items())
    if isinstance(value, list):
        size[0] += sys.getsizeof(value)
        return FrozenList(_ref_freeze(v, size, refs) for v in value)
    if isinstance(value, tuple):
        size[0] += sys.getsizeof(value)
        return tuple(_ref_freeze(v, size, refs) for v in value)
    size[0] += sys.getsizeof(value)
    return value


def _ref_display(value):
    """``display_value`` before B12 (reference)."""
    import math
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    if isinstance(value, (list, tuple)):
        return [_ref_display(v) for v in value]
    if isinstance(value, dict):
        return {"@dict": [[_ref_display(k), _ref_display(v)] for k, v in value.items()]}
    return repr(value)


def _cache_shape(value):
    """A comparable rendering of a frozen value: atom references by pointer, containers by type."""
    from proto_db.query import cache
    if type(value) is cache._AtomRef:
        return ("ref", value.ptr, value.type_name)
    if isinstance(value, dict):
        return (type(value).__name__, [(_cache_shape(k), _cache_shape(v)) for k, v in value.items()])
    if isinstance(value, (list, tuple)):
        return (type(value).__name__, [_cache_shape(v) for v in value])
    return notation.describe(value)


@pytest.mark.parametrize("seed", range(10))
def test_the_cache_and_explain_walks_give_what_the_recursive_walks_gave(seed):
    from proto_db.query import cache
    from proto_db.query.terms import display_value
    rng = random.Random(500 + seed)
    for _ in range(10):
        value = canonical(_random_value(rng, rng.choice([3, 8, 20])))
        size, refs, ref_size, ref_refs = [0], [0], [0], [0]
        frozen = cache._freeze(value, size, refs)
        assert _cache_shape(frozen) == _cache_shape(_ref_freeze(value, ref_size, ref_refs))
        assert (size, refs) == (ref_size, ref_refs)
        loads = []
        thawed = cache._thaw(frozen, lambda p, t: loads.append(p) or stub(p, t))
        assert notation.describe(thawed) == notation.describe(value)
        assert loads == [a.atom_pointer for a in _atoms_in_order(value)]
        rendered = display_value(value)
        assert rendered == _ref_display(value) or (rendered != rendered)   # NaN renders as its repr
        assert json.dumps(rendered) == json.dumps(_ref_display(value))


def _atoms_in_order(value):
    """The atoms of ``value`` in the order a pre-order walk meets them."""
    out, pending = [], [value]
    while pending:
        item = pending.pop()
        if isinstance(item, Atom):
            out.append(item)
        elif isinstance(item, dict):
            pending.extend(reversed([x for pair in item.items() for x in pair]))
        elif isinstance(item, (list, tuple)):
            pending.extend(reversed(item))
    return out


def test_the_query_result_cache_holds_values_at_the_limit_on_a_deep_stack():
    """The reviewer's case: a cached result row nested 400 lists deep raised RecursionError."""
    from proto_db.query.linq import from_collection
    space = ObjectSpace(_memory())
    db = space.new_database("d")
    deep = [nest(MAX_VALUE_DEPTH), nest((MAX_VALUE_DEPTH - 2) // 2, saved_atom(7), "tuple"),
            nest((MAX_VALUE_DEPTH - 2) // 3, 5, "map")]

    def write(tr):
        tr.set_root_object("items", tr.new_list().extend(deep))
        tr.set_root_object("refs", tr.new_list().extend([nest(MAX_VALUE_DEPTH - 2, DBObject(x=1))]))
    db.run(write)

    def check():
        with db.new_transaction() as tr:
            for name, expected in (("items", deep), ("refs", None)):
                query = from_collection(tr.get_root_object(name))
                first = list(space.query_cache.result(query))
                again = list(space.query_cache.result(query))  # from the cache: thawed again
                assert [notation.describe(v) for v in again] == [notation.describe(v) for v in first]
                if expected is not None:
                    assert [notation.describe(v) for v in first] == [notation.describe(canonical(v))
                                                                      for v in expected]
                else:
                    assert bottom(again[0]).x == 1
        return True
    try:
        assert on_deep_stack(check)
    finally:
        space.close()


def test_explain_renders_operands_at_the_limit_on_a_deep_stack():
    from proto_db.query.terms import display_value

    def check():
        rendered = display_value(nest(MAX_VALUE_DEPTH))
        assert wire_depth(rendered) == MAX_VALUE_DEPTH and bottom(rendered) == 1
        maps = display_value(nest(300, "x", "map"))
        assert wire_depth(maps) == 300 * 3                     # {"@dict": [[k, v]]}: 3 per map
        return True
    assert on_deep_stack(check)


def test_to_arrow_converts_values_at_the_limit_on_a_deep_stack():
    """``_plain`` (pure Python: pyarrow is not needed to call it) walks lists, maps and DBObjects."""
    from proto_db.interop import arrow

    def check():
        assert bottom(arrow._plain(nest(MAX_VALUE_DEPTH), "c")) == 1
        maps = arrow._plain(nest(340, 5, "map"), "c")
        assert wire_depth(maps) == 340 and bottom(maps) == 5
        chain = DBObject(a=1)
        for _ in range(600):
            chain = DBObject(a=chain)
        plain = arrow._plain(chain, "c")
        assert wire_depth(plain) == 601 and bottom(plain) == 1
        with pytest.raises(ProtoValidationError, match=r"column 'c'(\.k)+ holds the non-finite Decimal"):
            arrow._plain(nest(400, Decimal("NaN"), "map"), "c")
        with pytest.raises(ProtoValidationError, match=r"column 'c'(\[\])+ mixes naive and aware"):
            arrow._plain(nest(500, [datetime(2026, 1, 1), datetime(2026, 1, 1, tzinfo=timezone.utc)]), "c")
        return True
    assert on_deep_stack(check)
