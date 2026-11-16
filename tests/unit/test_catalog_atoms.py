"""Format 6: the feature ``catalog``, the semantic catalog's atom types and the neutral model (Task 2).

Catches: catalog types that are not aliases 24 to 34 in the chapter's order, or whose records do not
declare the feature ``catalog``; a catalog record that lands in a segment below version 6; a space that
uses no catalog whose bytes change; a reader of format 5 that misreads a format-6 segment instead of
refusing it; a model that does not survive its records; a field written at its default; an ``ext`` that
is not kept as written; a canonical form that depends on oids, record placement or the order of the
model's maps; a ``__`` root of the wrong type read as catalog or intent-log state; an invalid model
accepted, or refused without naming where.
"""
from __future__ import annotations

import copy
import datetime
import hashlib
import json
import uuid
from decimal import Decimal
from pathlib import Path

import msgpack
import pytest

from proto_db.catalog import atoms as catalog_atoms
from proto_db.catalog.atoms import (CATALOG_RECORD_TYPES, INTENT_LOG_ROOT, SEMANTIC_CATALOG_ROOT, Attribute,
                                    Constraint, Entity, Intent, IntentRecord, Role, SemanticCatalog,
                                    intent_log_root, intent_records, semantic_catalog_root)
from proto_db.catalog.model import canonical_form, check_model, from_atoms, to_atoms
from proto_db.collections.lists import List
from proto_db.core import codec
from proto_db.core.atom import BUILTIN_ATOM_MODULES, BUILTIN_TYPE_ALIASES, class_defines, type_tag
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoCorruptionError, ProtoNotSupportedError, ProtoValidationError
from proto_db.storage import format as fmt
from proto_db.storage.file import FileStorage
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace

pytestmark = [pytest.mark.timeout(120)]

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests" / "conformance" / "catalog_model.json"
CATALOG_NAMES = ["SemanticCatalog", "Module", "Extension", "Entity", "Relationship", "Attribute", "Role",
                 "Intent", "Constraint", "Expression", "IntentRecord"]


def _fixture() -> dict:
    return json.loads(FIXTURE.read_text())


def _rich() -> dict:
    """A model that states every field of every catalog type at least once (no field at its default)."""
    expr = {"ast": ["<=", ["attr", "a"], ["const", Decimal("2.50")]], "ext": {"source": "a <= 2.50"}}
    attribute = {"name": "a", "inherited_from": "t.x.Base", "kind": "computed_stored", "value_type": "decimal",
                 "values": ["x", "y"], "required": True, "default": None, "unit": "currency:t", "indexed": True,
                 "expression": expr, "languages": ["python"], "meaning": "A", "label": {"und": "A", "es": "A"},
                 "help": {"und": "Help"}, "group": "g", "order": 0, "synonyms": ["aa"],
                 "examples": [Decimal("1.5"), (1, "a"), [1, 2], {"k": b"\x00"}, datetime.date(2026, 11, 15)],
                 "caveats": "c", "sensitivity": "secret", "certification": "certified", "deprecated": True,
                 "provenance": ["m1", "m2"], "ext": {"z": 1, "a": [1, (2,)]}}
    role = {"name": "r", "inherited_from": "t.x.Base", "target": "t.x.T", "inverse": "back",
            "cardinality": "0..1", "inverse_cardinality": "1..*", "ordered": True, "index_by": ["d.e"],
            "binding": "version", "on_delete": "cascade", "required": True, "indexed": True,
            "languages": ["python"], "meaning": "R", "label": {"und": "R"}, "help": {"und": "H"}, "group": "g",
            "order": 3, "synonyms": ["rr"], "examples": ["e"], "caveats": "c", "sensitivity": "public",
            "certification": "proposed", "deprecated": True, "provenance": ["m1"], "ext": {"k": "v"}}
    intent = {"name": "i", "meaning": "I", "parameters": [["p", "int"], ["q", "t.x.T"], ["u", None]],
              "requires": {"ast": ["not", ["attr", "closed"]]}, "commutes_with": ["i"], "effects": ["writes a"],
              "languages": ["python"], "provenance": ["m1"], "ext": {"e": 1}}
    constraint = {"name": "c", "scope": "entity", "expression": copy.deepcopy(expr), "enforced": False,
                  "meaning": "C", "message": "{entity} breaks c", "provenance": ["m1"], "ext": {"e": 2}}
    entity = {"type_name": "t.x.T", "supertypes": ["t.x.Base"], "meaning": "T", "label": {"und": "T"},
              "help": {"und": "T help"}, "synonyms": ["tee"], "key": ["a"], "attributes": [attribute, role],
              "intents": [intent], "constraints": [constraint], "certification": "reviewed",
              "provenance": ["m1", "m2"], "ext": {"e": 3}}
    relationship = {"type_name": "t.x.L", "meaning": "L",
                    "roles": [dict(role, name="left", participant=True),
                              dict(role, name="right", target="t.x.Base", participant=True)],
                    "attributes": [dict(attribute, name="w")], "provenance": ["m1"]}
    extension = {"module": "m1", "extends": "t.x.T",
                 "entity": {"meaning": "T", "label": {"und": "T"}, "help": {"und": "h"}, "synonyms": ["tee"],
                            "key": ["a"], "supertypes": ["t.x.Base"], "certification": "reviewed"},
                 "elements": [attribute, role, intent, constraint], "ext": {"e": 4}}
    return {"module_set": ["base", "m1", "m2"],
            "modules": {"base": {"name": "base", "version": "1"},
                        "m1": {"name": "m1", "version": "1", "extensions": [extension], "ext": {"e": 5}},
                        "m2": {"name": "m2", "version": "2", "depends": ["m1"]}},
            "entities": {"t.x.T": entity, "t.x.L": relationship},
            "ext": {"catalog": True}}


def _space(path=None):
    return ObjectSpace(FileStorage(str(path)) if path is not None else MemoryStorage())


def _write_catalog(tr, model):
    """Store ``model`` as ``__semanticCatalog`` through the transaction's internal write set (the
    semantic layer's own write path; applications cannot name a ``__`` root)."""
    tr._writes[SEMANTIC_CATALOG_ROOT] = to_atoms(model, tr)


def _read_catalog(db) -> dict:
    tr = db.new_transaction()
    try:
        return from_atoms(semantic_catalog_root(tr._objects))
    finally:
        tr.abort()


def _segment_versions(path: Path) -> list:
    return sorted(int.from_bytes(f.read_bytes()[8:10], "little") for f in path.glob("*.seg"))


# -- the types and the feature --------------------------------------------------------------------

def test_catalog_types_are_appended_aliases_24_to_34():
    expected = tuple(f"proto_db.catalog.{name}" for name in CATALOG_NAMES)
    assert BUILTIN_TYPE_ALIASES[24:] == expected and len(BUILTIN_TYPE_ALIASES) == 35
    assert tuple(cls.__type_name__ for cls in CATALOG_RECORD_TYPES) == expected
    assert fmt.CATALOG_TYPES == {name: 24 + i for i, name in enumerate(expected)}
    assert fmt.FEATURE_VERSIONS["catalog"] == 6 and fmt.write_version({"catalog"}) == 6
    for i, cls in enumerate(CATALOG_RECORD_TYPES):
        name = cls.__type_name__
        assert type_tag(name) == 24 + i
        assert BUILTIN_ATOM_MODULES[name] == "proto_db.catalog.atoms"
        assert cls._format_features == {"catalog"} and not cls._parented
        for tag in (24 + i, name):
            assert fmt.payload_features({"t": tag, "oid": b"\0" * 16, "s": {}}) == {"catalog"}
            assert fmt.payload_features({"t": tag, "oid": b"\0" * 16, "s": {}, "p": []}) == {"catalog", "parents"}
        assert not [field for field in cls._fields if class_defines(cls, field)], name
    # The registry's and the plain types are unchanged.
    assert fmt.payload_features({"t": 22, "oid": b"\0" * 16, "s": {}}) == {"registry"}
    assert fmt.payload_features({"t": 19, "oid": b"\0" * 16, "s": {}}) == frozenset()
    assert fmt.payload_features({"t": 35, "oid": b"\0" * 16, "s": {}}) == frozenset()
    assert fmt.payload_features({"t": True, "oid": b"\0" * 16, "s": {}}) == frozenset()


def test_role_fields_are_the_attribute_fields_minus_the_value_ones_plus_its_own():
    removed = {"kind", "value_type", "values", "default", "unit", "expression"}
    own = {"target", "inverse", "cardinality", "inverse_cardinality", "participant", "ordered", "index_by",
           "binding", "on_delete"}
    assert set(Role._fields) == (set(Attribute._fields) - removed) | own


def test_a_catalog_record_lands_in_a_v6_segment(tmp_path):
    model = _fixture()
    space = _space(tmp_path / "s")
    db = space.new_database("db")
    db.run(lambda tr: tr.set_root_object("plain", DBObject(n=1)))
    assert space.storage.read_root().version == 4
    db.run(lambda tr: _write_catalog(tr, model))
    assert space.storage.read_root().version == 6
    space.close()
    assert _segment_versions(tmp_path / "s") == [4, 6]
    space = _space(tmp_path / "s")
    try:
        assert _read_catalog(space.open_database("db")) == model
    finally:
        space.close()


def test_a_space_without_catalog_keeps_v4_and_v5_bytes(tmp_path, monkeypatch):
    """The parents and registry vectors (formats 4 and 5) regenerate byte for byte with format 6 in
    the code: data that uses no catalog keeps its version and its bytes."""
    import tests.conformance.generate as g
    pinned = dict(line.split("  ")[::-1] for line in g.PINS.read_text().splitlines())
    monkeypatch.setattr(g, "VECTORS", tmp_path / "vectors")
    manifest = {}
    g.gen_parents_database(manifest)
    g.gen_registry_database(manifest)
    written = sorted(f for f in (tmp_path / "vectors").rglob("*") if f.is_file())
    assert {f.relative_to(tmp_path / "vectors").parts[0] for f in written} == {"parents", "registry"}
    for f in written:
        rel = f.relative_to(tmp_path / "vectors").as_posix()
        assert hashlib.sha256(f.read_bytes()).hexdigest() == pinned[rel], rel
    committed = json.loads((ROOT / "tests" / "conformance" / "manifest.json").read_text())
    assert manifest["parents_database"] == committed["parents_database"]
    assert manifest["registry_database"] == committed["registry_database"]


def test_a_v5_reader_refuses_a_v6_segment(tmp_path, monkeypatch):
    space = _space(tmp_path / "s")
    db = space.new_database("db")
    db.run(lambda tr: tr.set_root_object("plain", DBObject(n=1)))
    db.run(lambda tr: _write_catalog(tr, _fixture()))
    space.close()
    v6 = [f for f in (tmp_path / "s").glob("*.seg") if int.from_bytes(f.read_bytes()[8:10], "little") == 6]
    assert len(v6) == 1
    monkeypatch.setattr(fmt, "READABLE_FORMAT_VERSIONS", frozenset({2, 3, 4, 5}))
    with pytest.raises(ProtoNotSupportedError):
        fmt.decode_segment_header(v6[0].read_bytes())
    with pytest.raises(ProtoNotSupportedError):
        fmt.decode_root((tmp_path / "s" / "space.root").read_bytes())
    with pytest.raises(ProtoNotSupportedError):
        _space(tmp_path / "s").open_database("db")


# -- the model and its records --------------------------------------------------------------------

@pytest.mark.parametrize("make", [_fixture, _rich], ids=["fixture", "every_field"])
def test_model_round_trips_through_atoms(make, tmp_path):
    model = make()
    check_model(model)
    space = _space()
    assert from_atoms(to_atoms(model, None)) == model                 # unsaved records
    db = space.new_database("db")
    db.run(lambda tr: _write_catalog(tr, model))
    assert _read_catalog(db) == model                                 # saved and loaded
    space.close()


def test_defaults_are_not_written():
    model = _fixture()
    order = model["entities"]["conformance.sales.Order"]
    stated = copy.deepcopy(model)
    s_order = stated["entities"]["conformance.sales.Order"]
    s_order.update(supertypes=[], help={})
    s_order["attributes"][0].update(indexed=False, deprecated=False, sensitivity="internal", synonyms=[], ext={})
    s_order["attributes"][3].update(binding="identity", ordered=False, required=False, index_by=[])
    s_order["intents"][1].update(parameters=[], commutes_with=[], languages=[])
    stated["entities"]["conformance.sales.Customer"]["constraints"][0].update(enforced=True)
    stated["modules"]["base"].update(depends=[], extensions=[])
    check_model(stated)
    assert canonical_form(stated) == canonical_form(model)
    catalog = to_atoms(stated, None)
    assert from_atoms(catalog) == model
    states = {}
    for name, record in catalog.entities.items():
        states[name] = record
    number = states["conformance.sales.Order"].attributes[0]._state()
    assert not {"indexed", "deprecated", "sensitivity", "synonyms", "ext"} & set(number)
    customer = states["conformance.sales.Order"].attributes[3]._state()
    assert not {"binding", "ordered", "required", "index_by"} & set(customer)
    assert "enforced" not in states["conformance.sales.Customer"].constraints[0]._state()
    assert set(catalog.modules.get_at("base")._state()) == {"name", "version"}
    assert set(states["conformance.sales.Order"]._state()) == {
        "type_name", "meaning", "label", "synonyms", "key", "attributes", "intents", "provenance"}
    # A declared default (``default``, even nil) and the declared values of other fields are written.
    assert order["attributes"][1]["default"] == "open"
    assert states["conformance.sales.Order"].attributes[1]._state()["default"] == "open"
    nil_default = to_atoms(_rich(), None).entities.get_at("t.x.T").attributes[0]._state()
    assert "default" in nil_default and nil_default["default"] is None and nil_default["order"] == 0
    # A required field is written even when it is empty.
    empty = {"module_set": [], "modules": {}, "entities": {}}
    assert from_atoms(to_atoms(empty, None)) == empty
    assert set(to_atoms(empty, None)._state()) == {"modules", "module_set", "entities"}
    record = IntentRecord(intent=("t.x.T", "i"), target="t.x.T#" + "0" * 32, arguments={}, transaction=1)
    assert record._state()["arguments"] == {}


def test_ext_is_kept_byte_for_byte():
    ext = {"z": [1, (2, "b")], "a": {"n": Decimal("1.10"), 3: b"\x00\xff"}, "m": datetime.date(2026, 1, 2),
           "u": uuid.UUID(int=7), "x": None}
    model = _fixture()
    model["entities"]["conformance.sales.Order"]["attributes"][0]["ext"] = ext
    model["ext"] = {"tool": "t"}
    storage = MemoryStorage()
    space = ObjectSpace(storage)
    db = space.new_database("db")
    db.run(lambda tr: _write_catalog(tr, model))
    tr = db.new_transaction()
    stored = semantic_catalog_root(tr._objects)
    record = stored.entities.get_at("conformance.sales.Order").attributes[0]
    raw = storage.get_atom(record.atom_pointer)                       # the stored envelope, as unpacked
    read = from_atoms(stored)
    tr.abort()
    got = read["entities"]["conformance.sales.Order"]["attributes"][0]["ext"]
    assert got == ext and list(got) == list(ext)                       # values, kinds and key order
    assert type(got["z"][1]) is tuple and type(got["a"]["n"]) is Decimal
    assert read["ext"] == {"tool": "t"}
    again = to_atoms(read, None).entities.get_at("conformance.sales.Order").attributes[0]._state()["ext"]

    def no_atom(atom):
        raise AssertionError("no atoms in ext")
    written = msgpack.packb(codec.encode_value(again, no_atom), use_bin_type=True)
    assert written == msgpack.packb(raw["s"]["ext"], use_bin_type=True)     # re-written byte for byte
    space.close()


def test_canonical_form_ignores_oids_and_placement(tmp_path):
    model = _fixture()
    expected = canonical_form(model)
    first = _space(tmp_path / "a")
    db = first.new_database("db")
    db.run(lambda tr: _write_catalog(tr, model))
    second = _space(tmp_path / "b")
    other = second.new_database("other")
    other.run(lambda tr: [tr.set_root_object(f"r{i}", DBObject(i=i)) for i in range(20)])
    db2 = second.new_database("db")
    db2.run(lambda tr: _write_catalog(tr, model))
    a, b = _read_catalog(db), _read_catalog(db2)
    ta, tb = db.new_transaction(), db2.new_transaction()
    pa, pb = semantic_catalog_root(ta._objects).atom_pointer, semantic_catalog_root(tb._objects).atom_pointer
    oa, ob = semantic_catalog_root(ta._objects).oid, semantic_catalog_root(tb._objects).oid
    ta.abort()
    tb.abort()
    assert pa != pb and oa != ob
    assert canonical_form(a) == canonical_form(b) == expected
    # The order of the model's maps does not matter either.
    reordered = json.loads(json.dumps(model), object_pairs_hook=lambda pairs: dict(reversed(pairs)))
    assert canonical_form(reordered) == expected
    decoded = codec.decode_value(msgpack.unpackb(expected, raw=False), lambda *_: None)
    assert list(decoded) == ["entities", "module_set", "modules"]
    first.close()
    second.close()


# -- the roots ------------------------------------------------------------------------------------

def test_wrong_typed_semantic_root_is_corrupt():
    space = _space()
    db = space.new_database("db")
    tr = db.new_transaction()
    tr._writes[SEMANTIC_CATALOG_ROOT] = DBObject(kind="not a catalog")   # data written before the names were reserved
    tr._writes[INTENT_LOG_ROOT] = 5
    tr.commit()
    tr = db.new_transaction()
    with pytest.raises(ProtoCorruptionError, match="__semanticCatalog"):
        semantic_catalog_root(tr._objects)
    with pytest.raises(ProtoCorruptionError, match="__intentLog"):
        intent_log_root(tr._objects)
    tr.abort()
    record = IntentRecord(intent=("t.x.T", "i"), target="t.x.T#" + "ab" * 16, arguments={"n": 1}, transaction=3)
    tr = db.new_transaction()
    tr._writes[INTENT_LOG_ROOT] = List.from_iterable([record, DBObject(kind="not a record")])
    tr._writes[SEMANTIC_CATALOG_ROOT] = Entity(type_name="t.x.T")
    tr.commit()
    tr = db.new_transaction()
    log = intent_log_root(tr._objects)
    assert isinstance(log, List)
    with pytest.raises(ProtoCorruptionError, match="element 1"):
        intent_records(log)
    with pytest.raises(ProtoCorruptionError, match="__semanticCatalog"):
        semantic_catalog_root(tr._objects)
    tr.abort()
    tr = db.new_transaction()
    tr._writes[INTENT_LOG_ROOT] = List.from_iterable([record])
    tr.commit()
    tr = db.new_transaction()
    (read,) = intent_records(intent_log_root(tr._objects))
    assert (read.intent, read.target, dict(read.arguments), read.transaction) == (
        ("t.x.T", "i"), "t.x.T#" + "ab" * 16, {"n": 1}, 3)
    tr.abort()
    space.close()


def test_a_malformed_stored_catalog_is_corrupt():
    catalog = to_atoms(_fixture(), None)
    with pytest.raises(ProtoCorruptionError):
        from_atoms(Entity(type_name="t.x.T"))
    order = catalog.entities.get_at("conformance.sales.Order")
    object.__setattr__(order, "synonyms", ["a list, not a tuple"])
    with pytest.raises(ProtoCorruptionError, match="synonyms"):
        from_atoms(catalog)
    catalog = to_atoms(_fixture(), None)
    object.__setattr__(catalog.entities.get_at("conformance.sales.Order").attributes[3], "cardinality", "few")
    with pytest.raises(ProtoCorruptionError, match="cardinality"):
        from_atoms(catalog)
    catalog = to_atoms(_fixture(), None)
    object.__setattr__(catalog.entities.get_at("conformance.sales.Order"), "intents", (Constraint(
        name="c", scope="entity", expression=catalog_atoms.Expression(ast=["const", True])),))
    with pytest.raises(ProtoCorruptionError, match="intents"):
        from_atoms(catalog)


def test_applications_cannot_write_semantic_roots():
    """A regression guard (it passes on 2.0.0rc4: every ``__`` root is reserved), not a failing-first test."""
    space = _space()
    db = space.new_database("db")
    tr = db.new_transaction()
    for name in (SEMANTIC_CATALOG_ROOT, INTENT_LOG_ROOT):
        with pytest.raises(ProtoValidationError, match="reserved"):
            tr.set_root_object(name, 1)
        with pytest.raises(ProtoValidationError, match="reserved"):
            tr.set_root_object(name, 1, replace=True)
        with pytest.raises(ProtoValidationError, match="reserved"):
            tr.remove_root_object(name)
    tr.abort()
    space.close()


# -- refusals of the model ------------------------------------------------------------------------

def _broken(edit):
    model = _fixture()
    edit(model)
    return model


@pytest.mark.parametrize("edit, where", [
    (lambda m: m["entities"]["conformance.sales.Order"]["attributes"][3].update(cardinality="few"),
     "entities['conformance.sales.Order'].attributes[3].cardinality"),
    (lambda m: m["entities"]["conformance.sales.Order"]["attributes"][0].update(colour="red"),
     "entities['conformance.sales.Order'].attributes[0].colour"),
    (lambda m: m["entities"]["conformance.sales.Order"]["attributes"][0].pop("kind"),
     "entities['conformance.sales.Order'].attributes[0]"),
    (lambda m: m["entities"]["conformance.sales.Order"]["attributes"][0].update(value_type="integer"),
     "entities['conformance.sales.Order'].attributes[0].value_type"),
    (lambda m: m["entities"]["conformance.sales.Order"]["attributes"][0].update(required="yes"),
     "entities['conformance.sales.Order'].attributes[0].required"),
    (lambda m: m["entities"]["conformance.sales.Order"]["attributes"][0].update(label="Number"),
     "entities['conformance.sales.Order'].attributes[0].label"),
    (lambda m: m["entities"]["conformance.sales.Order"]["intents"][0]["parameters"].append(["x", "integer"]),
     "entities['conformance.sales.Order'].intents[0].parameters[3]"),
    (lambda m: m["entities"]["conformance.sales.Order"]["intents"][1]["requires"].update(ast="state == 'open'"),
     "entities['conformance.sales.Order'].intents[1].requires.ast"),
    (lambda m: m["entities"]["conformance.sales.OrderLine"]["roles"].pop(),
     "entities['conformance.sales.OrderLine'].roles"),
    (lambda m: m["entities"]["conformance.sales.Order"]["attributes"].append({"name": "x"}),
     "entities['conformance.sales.Order'].attributes[6]"),
    (lambda m: m["entities"]["conformance.sales.Order"]["attributes"][0].update(default=DBObject()),
     "entities['conformance.sales.Order'].attributes[0].default"),
    (lambda m: m["entities"]["conformance.sales.Order"].update(type_name="conformance.sales.Other"),
     "entities['conformance.sales.Order']"),
    (lambda m: m["modules"]["conformance.catalog_sales"]["extensions"][0]["entity"].update(attributes=[]),
     "modules['conformance.catalog_sales'].extensions[0].entity"),
    (lambda m: m.update(module_set=["conformance.catalog_sales", "base", "conformance.catalog_base"]),
     "module_set"),
    (lambda m: m["modules"].pop("base"), "modules"),
    (lambda m: m.update(extra=1), "the model"),
])
def test_an_invalid_model_is_refused_naming_where(edit, where):
    with pytest.raises(ProtoValidationError) as info:
        check_model(_broken(edit))
    assert f"at {where}:" in str(info.value)


def test_intent_records_are_checked_when_built():
    good = dict(intent=("t.x.T", "i"), target="t.x.T#" + "0" * 32, arguments={}, transaction=0)
    IntentRecord(**good)
    for bad in (dict(good, intent=("t.x.T",)), dict(good, target=""), dict(good, arguments={1: 2}),
                dict(good, transaction=-1), dict(good, transaction=True)):
        with pytest.raises(ProtoValidationError):
            IntentRecord(**bad)
    with pytest.raises(ProtoValidationError, match="required"):
        IntentRecord(intent=("t.x.T", "i"), target="t.x.T#" + "0" * 32, transaction=0)
    with pytest.raises(ProtoValidationError, match="no fields"):
        Intent(name="i", kind="field")
    with pytest.raises(ProtoValidationError, match="required"):
        SemanticCatalog(module_set=())


# -- fix round 1: Omission inside Extension.entity, inherited_from, contributions ------------------

@pytest.mark.parametrize("entity", [{"synonyms": []}, {"label": {}}, {"kind": "entity"},
                                    {"synonyms": [], "key": [], "help": {}, "kind": "entity"}],
                         ids=["empty-names", "empty-label", "default-kind", "all-defaults"])
def test_an_entity_contribution_holding_only_defaults_is_omitted(entity):
    """Review I2: a field whose inner values are all at their default is at its default (Omission)."""
    model = _fixture()
    original = canonical_form(model)
    extension = model["modules"]["conformance.catalog_sales"]["extensions"][-1]
    assert "entity" not in extension
    extension["entity"] = entity
    catalog = to_atoms(model, None)
    stored = catalog.modules.get_at("conformance.catalog_sales").extensions[-1]._state()
    assert "entity" not in stored
    assert canonical_form(model) == canonical_form(from_atoms(catalog)) == original
    assert from_atoms(catalog) == _fixture()


def test_defaults_inside_an_entity_contribution_are_dropped_and_the_rest_kept():
    model = _fixture()
    extension = model["modules"]["conformance.catalog_base"]["extensions"][0]
    extension["entity"].update(synonyms=[], kind="entity", help={})
    catalog = to_atoms(model, None)
    stored = catalog.modules.get_at("conformance.catalog_base").extensions[0]._state()["entity"]
    assert set(stored) == {"meaning", "label", "key"}
    assert canonical_form(model) == canonical_form(from_atoms(catalog)) == canonical_form(_fixture())


def test_inherited_intents_and_constraints_carry_inherited_from():
    """Review I3, ruling: R281 covers every inherited element."""
    assert list(Intent._fields)[:2] == ["name", "inherited_from"]
    assert list(Constraint._fields)[:2] == ["name", "inherited_from"]
    rush = _fixture()["entities"]["conformance.sales.RushOrder"]
    assert [i.get("inherited_from") for i in rush["intents"]] == ["conformance.sales.Order"] * 2
    model = _rich()
    model["entities"]["t.x.T"]["intents"][0]["inherited_from"] = "t.x.Base"
    model["entities"]["t.x.T"]["constraints"][0]["inherited_from"] = "t.x.Base"
    check_model(model)
    assert from_atoms(to_atoms(model, None)) == model


def test_a_subtype_inherits_the_key_but_not_the_presentation():
    """Ruling M1: key is inherited (and not redefined); label, synonyms and meaning are the subtype's own."""
    entities = _fixture()["entities"]
    order, rush = entities["conformance.sales.Order"], entities["conformance.sales.RushOrder"]
    assert rush["key"] == order["key"]
    assert "synonyms" not in rush and rush["label"] != order["label"] and rush["meaning"] != order["meaning"]


def _compose_single(extension: dict) -> tuple:
    """The composed record of a type declared by one Extension only (no supertype, no other module):
    what a reader composing from contributions builds (format spec, "Composition and the canonical form")."""
    module = extension["module"]
    entity = dict(extension.get("entity", {}))
    kind = entity.pop("kind", "entity")
    out = {"type_name": extension["extends"], **entity}
    for element in extension.get("elements", []):
        element = dict(element, provenance=[module])
        if "kind" in element or ("target" in element and not element.get("participant")):
            out.setdefault("attributes", []).append(element)
        elif "target" in element:
            out.setdefault("roles", []).append(element)
        elif "scope" in element:
            out.setdefault("constraints", []).append(element)
        else:
            out.setdefault("intents", []).append(element)
    out["provenance"] = [module]
    return kind, out


def test_composition_from_contributions_reproduces_the_stored_records():
    """Review I4, ruling: an Extension states its entity's kind and a relationship's roles are marked,
    so the stored Relationship of OrderLine (and every single-contribution type) is derived from its
    Extension exactly."""
    model = _fixture()
    single = {"conformance.sales.Order", "conformance.sales.OrderLine", "conformance.sales.Product"}
    extensions = {e["extends"]: e for m in model["modules"].values() for e in m.get("extensions", [])
                  if e["extends"] in single}
    assert set(extensions) == single
    for type_name, extension in extensions.items():
        kind, composed = _compose_single(extension)
        assert composed == model["entities"][type_name], type_name
        assert (kind == "relationship") == ("roles" in model["entities"][type_name])
    catalog = to_atoms(model, None)
    assert type(catalog.entities.get_at("conformance.sales.OrderLine")).__name__ == "Relationship"
    assert all(r.participant is True for r in catalog.entities.get_at("conformance.sales.OrderLine").roles)


@pytest.mark.parametrize("edit, where", [
    (lambda m: m["modules"]["conformance.catalog_sales"]["extensions"][1]["entity"].pop("kind"),
     "modules['conformance.catalog_sales'].extensions[1].elements[0]"),
    (lambda m: m["modules"]["conformance.catalog_sales"]["extensions"][1]["entity"].update(kind="link"),
     "modules['conformance.catalog_sales'].extensions[1].entity['kind']"),
    (lambda m: m["entities"]["conformance.sales.OrderLine"]["roles"][0].pop("participant"),
     "entities['conformance.sales.OrderLine'].roles[0]"),
    (lambda m: m["entities"]["conformance.sales.Order"]["attributes"][3].update(participant=True),
     "entities['conformance.sales.Order'].attributes[3]"),
])
def test_relationship_markers_are_checked(edit, where):
    with pytest.raises(ProtoValidationError) as info:
        check_model(_broken(edit))
    assert f"at {where}:" in str(info.value)
