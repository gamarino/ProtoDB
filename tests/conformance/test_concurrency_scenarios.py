"""The concurrency conformance scenarios (spec R60, proto_db side; R62: protoBaseST waits for its port).

``concurrency_scenarios.json`` holds the scenarios as data: a base state, the operations of two
transactions A and B (both opened on the base), the commit order, and the expected outcome -- the
final state, or the unit the second commit conflicts on.  This runner executes every scenario
through proto_db's public API in every commit mode (standalone, group commit, cluster:
:mod:`tests.support.modes`), and checks that every row of the tables of spec sections 5 and 6
has at least one scenario.  Another implementation runs the same file with its own runner; the
notation is defined in the file's ``notation`` entry.

Catches: an implementation whose outcome for a listed operation differs from the spec's table
(a missed conflict, a spurious one, a unit named wrongly, or a published state that is not the
serial one), in any commit mode.
"""
import json
from pathlib import Path

import pytest

from proto_db.collections.dictionaries import Dictionary, RepeatedKeysDictionary
from proto_db.collections.hash_dictionaries import HashDictionary
from proto_db.collections.lists import List
from proto_db.collections.sets import CountedSet, Set
from proto_db.errors import ProtoConflictError
from proto_db.indexing.equality import EqualityIndex
from proto_db.indexing.keyspec import KeySpec
from proto_db.indexing.range import RangeIndex
from proto_db.query.planner import plan_query
from proto_db.query.terms import EQ, GT, Term
from tests.support.modes import MODES, space_in_mode
from tests.support.outcomes import new_database_once

HERE = Path(__file__).resolve().parent
SCENARIOS = json.loads((HERE / "concurrency_scenarios.json").read_text())


# -- the notation (the file's "notation" entry) -------------------------------------------------

def _index(kind: str):
    return {"equality": EqualityIndex("e", KeySpec.element()), "range": RangeIndex("r", KeySpec.element())}[kind]


def build(tr, desc: dict):
    """The value a root notation ``desc`` describes, built in transaction ``tr``."""
    if "value" in desc:
        return desc["value"]
    kinds = [k for k in ("dictionary", "hash_dictionary", "dictionary_items", "hash_dictionary_items") if k in desc]
    if kinds:
        kind = kinds[0]
        value = tr.new_hash_dictionary() if kind.startswith("hash") else tr.new_dictionary()
        pairs = desc[kind].items() if not kind.endswith("_items") else desc[kind]
        for k, v in pairs:
            value = value.set_at(k, v)
    elif "set" in desc:
        value = tr.new_set()
        for e in desc["set"]:
            value = value.add(e)
    elif "counted_set" in desc:
        value = tr.new_counted_set()
        for e, n in desc["counted_set"]:
            for _ in range(n):
                value = value.add(e)
    elif "rkd" in desc:
        value = tr.new_repeated_keys_dictionary()
        for k, records in desc["rkd"]:
            for r in records:
                value = value.set_at(k, r)
    elif "list" in desc:
        value = tr.new_list().extend(desc["list"])
    else:
        raise ValueError(f"Unknown root notation {desc!r}")
    for kind in desc.get("indexes", ()):
        value = value.add_index(_index(kind))
    return value


_INDEX_KINDS = {"e": "equality", "r": "range"}


def _pairs(value, name: str) -> dict:
    """A dictionary's notation: a JSON object when every key is a string, else the pairs sorted
    by the exact JSON text of the key (``10`` and ``10.0`` are one key of two forms)."""
    items = list(value.items())
    if all(type(k) is str for k, _ in items):
        return {name: dict(items)}
    return {f"{name}_items": sorted(([k, v] for k, v in items), key=lambda p: _text(p[0]))}


def _text(value) -> str:
    """The exact JSON text of ``value``: it tells ``10`` from ``10.0`` where ``==`` does not."""
    return json.dumps(value, sort_keys=True)


def describe(value) -> dict:
    """The root notation of a committed value (inverse of :func:`build`)."""
    kind = type(value)
    if kind is Dictionary:
        desc = _pairs(value, "dictionary")
    elif kind is HashDictionary:
        desc = _pairs(value, "hash_dictionary")
    elif kind is Set:
        desc = {"set": sorted(value.elements())}
    elif kind is CountedSet:
        desc = {"counted_set": sorted([e, n] for e, n in value.items())}
    elif kind is RepeatedKeysDictionary:
        desc = {"rkd": sorted([k, sorted(r for r, n in cs.items() for _ in range(n))] for k, cs in value.items())}
    elif kind is List:
        desc = {"list": list(value)}
    else:
        return {"value": value}
    names = value.index_names()
    if names:
        desc["indexes"] = sorted(_INDEX_KINDS[n] for n in names)
    return desc


def seed(db, base: dict) -> None:
    def fill(tr):
        for name, desc in base.get("roots", {}).items():
            tr.set_root_object(name, build(tr, desc))
        for name, value in base.get("mutables", {}).items():
            tr.new_mutable(name, value)
    db.run(fill)


def state(db) -> dict:
    def read(tr):
        return {"roots": {n: describe(tr.get_root_object(n)) for n in sorted(tr.root_names())},
                "mutables": {n: tr.get_mutable(n) for n in sorted(tr.mutable_names())}}
    return db.run(read)


def _plan(c, term=None):
    return sorted(plan_query(c, term).execute())


#: A collection write: the version a method returns is assigned back to the root.
WRITES = {
    "set_at": lambda c, k, v: c.set_at(k, v),
    "remove_at": lambda c, k: c.remove_at(k),
    "add": lambda c, e: c.add(e),
    "remove": lambda c, e: c.remove(e),
    "union": lambda c, es: c.union(es),
    "difference": lambda c, es: c.difference(es),
    "intersection": lambda c, es: c.intersection(es),
    "remove_record": lambda c, k, r: c.remove_record(k, r),
    "remove_key": lambda c, k: c.remove_key(k),
    "append_last": lambda c, v: c.append_last(v),
    "append_first": lambda c, v: c.append_first(v),
    "extend": lambda c, vs: c.extend(vs),
    "insert_at": lambda c, i, v: c.insert_at(i, v),
    "slice": lambda c, start, stop: c.slice(start, stop),
    "add_index": lambda c, kind: c.add_index(_index(kind)),
    "remove_index": lambda c, kind: c.remove_index({"equality": "e", "range": "r"}[kind]),
}

#: A collection read: its result is discarded; what it records is the point.
READS = {
    "get_at": lambda c, k: c.get_at(k),
    "get": lambda c, k: c.get(k),
    "has": lambda c, k: c.has(k),
    "count_of": lambda c, e: c.count_of(e),
    "len": lambda c: len(c),
    "count": lambda c: c.count,
    "items": lambda c: list(c.items()),
    "elements": lambda c: list(c.elements()),
    "keys": lambda c: list(c.keys()),
    "key_at_position": lambda c, i: c.key_at_position(i),
    "position_of": lambda c, k: c.position_of(k),
    "index_names": lambda c: c.index_names(),
    "get_index": lambda c, kind: c.get_index({"equality": "e", "range": "r"}[kind]),
    "lookup": lambda c, v: _plan(c, Term(None, EQ, v)),
    "greater_than": lambda c, v: _plan(c, Term(None, GT, v)),
    "scan": lambda c: _plan(c),
}


def run_op(tr, op: list) -> None:
    kind, args = op[0], op[1:]
    if kind == "set_root":
        tr.set_root_object(args[0], build(tr, args[1]))
    elif kind == "remove_root":
        tr.remove_root_object(args[0])
    elif kind == "get_root":
        tr.get_root_object(args[0])
    elif kind == "root_names":
        tr.root_names()
    elif kind == "new_mutable":
        tr.new_mutable(args[0], args[1])
    elif kind == "set_mutable":
        tr.set_mutable(args[0], args[1])
    elif kind == "remove_mutable":
        tr.remove_mutable(args[0])
    elif kind == "get_mutable":
        tr.get_mutable(args[0])
    elif kind == "mutable_names":
        tr.mutable_names()
    elif kind == "write":
        root, method = args[0], args[1]
        tr.set_root_object(root, WRITES[method](tr.get_root_object(root), *args[2:]))
    elif kind == "copy":
        value = tr.get_root_object(args[0])
        if len(args) > 2:
            value = WRITES[args[2]](value, *args[3:])
        tr.set_root_object(args[1], value)
    elif kind == "nest":
        tr.set_root_object(args[1], tr.new_object(**{args[2]: tr.get_root_object(args[0])}))
    elif kind == "copy_to_mutable":
        tr.new_mutable(args[1], tr.get_root_object(args[0]))
    elif kind == "wrap":
        tr.set_root_object(args[1], (tr.get_root_object(args[0]),))
    elif kind == "wrap_to_mutable":
        tr.new_mutable(args[1], (tr.get_root_object(args[0]),))
    elif kind == "read":
        root, method = args[0], args[1]
        READS[method](tr.get_root_object(root), *args[2:])
    else:
        raise ValueError(f"Unknown scenario operation {op!r}")


def _unit(unit) -> dict:
    """The notation of a conflict unit; a range's key (the range itself) is not part of it."""
    out = {"kind": unit.kind.value, "root": unit.root}
    if unit.kind.value != "range":
        out["key"] = unit.key
    if unit.index is not None:
        out["index"] = unit.index
    return out


def _normal(value):
    return json.loads(json.dumps(value))


# -- the scenarios ----------------------------------------------------------------------------

def test_every_table_row_has_a_scenario():
    """R60 (Task 14 ruling 4): at least one scenario per row of the spec's section 5 and 6 tables."""
    rows = set(SCENARIOS["rows"])
    covered = {s["row"] for s in SCENARIOS["scenarios"]}
    assert covered <= rows, sorted(covered - rows)
    assert rows <= covered, sorted(rows - covered)
    names = [s["name"] for s in SCENARIOS["scenarios"]]
    assert len(names) == len(set(names))


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("scenario", SCENARIOS["scenarios"], ids=[s["name"] for s in SCENARIOS["scenarios"]])
def test_concurrency_scenarios(scenario, mode, tmp_path):
    with space_in_mode(mode, tmp_path) as space:
        # Setup, not under test: in cluster mode the first commit of an empty cluster runs a
        # recovery round, which waits for a majority on the real clock (request_timeout); a round
        # that loses that race applied nothing and is tried again (the house helper).
        db = new_database_once(space)
        seed(db, scenario["base"])
        coordinator = space.coordinator
        recoveries = coordinator.stats()["recoveries"] if coordinator is not None else 0
        txs = {"a": db.new_transaction(), "b": db.new_transaction()}
        for name in ("a", "b"):
            for op in scenario[name]:
                run_op(txs[name], op)
        first, second = scenario["order"]
        txs[first].commit()
        expect = scenario["expect"]
        if "conflict" in expect:
            after_first = _text(state(db))
            with pytest.raises(ProtoConflictError) as e:
                txs[second].commit()
            got = _normal(_unit(e.value.unit))
            wanted = expect["conflict"]
            assert {k: got.get(k) for k in wanted} == wanted, (got, wanted)
            assert e.value.database == "db"
            # The failed commit published nothing: the state is the first commit's.
            assert _text(state(db)) == after_first
        else:
            txs[second].commit()
        if "state" in expect:
            # Compared as exact JSON text: a published 10 where the serial order leaves 10.0 fails.
            assert _text(state(db)) == _text(expect["state"])
        if coordinator is not None:
            # The commits under test ran on certain leases (the cluster mode's clock is frozen, so
            # no grant ends and no term becomes suspect): no recovery round, whose real-time wait
            # could turn into a conflict the scenario does not describe.
            assert coordinator.stats()["recoveries"] == recoveries, "a scenario step ran a recovery round"
