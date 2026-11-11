"""Strings holding surrogate code points are refused when the value is validated (R128).

A Python ``str`` may hold lone surrogates (U+D800-U+DFFF, from ``surrogateescape`` decoding or
from ``"\\ud83d\\ude00"`` written as two escapes).  They have no UTF-8 encoding, so the format
(D58) cannot store them.  Catches: such a string accepted by a constructor or a collection and
failing only when the transaction commits (or, with a store that does not encode, never); an
error that does not say which field holds it; valid non-ASCII text refused.
"""
from __future__ import annotations

import enum

import pytest

from proto_db.core import codec
from proto_db.core.atom import Atom
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoValidationError
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from proto_db.indexing import EqualityIndex
from proto_db.indexing.keyspec import KeySpec
from proto_db.values import MutableObject, canonical

LONE = "ab\ud800cd"
PAIR_AS_TWO = "\ud83d" + "\ude00"     # two surrogate code points, not the emoji U+1F600


class _Text(str):
    pass


@pytest.mark.parametrize("text", [LONE, "\udfff", PAIR_AS_TWO, "ñ\udc80"])
def test_canonical_refuses_surrogates(text):
    with pytest.raises(ProtoValidationError, match="surrogate"):
        canonical(text)


def test_a_str_subclass_is_checked_too():
    with pytest.raises(ProtoValidationError, match="surrogate"):
        canonical(_Text(LONE))


def test_valid_text_is_accepted():
    for text in ("plain", "ñandú", "\U0001F600", "日本語", ""):
        assert canonical(text) is text


@pytest.mark.parametrize("value", [LONE, [1, LONE], (LONE,), {"k": LONE}, {LONE: 1}, [{"x": (LONE,)}]])
def test_a_field_is_refused_at_construction_naming_the_field(value):
    with pytest.raises(ProtoValidationError, match="'notes'.*surrogate"):
        DBObject(title="ok", notes=value)


def test_with_names_the_field():
    obj = DBObject(title="ok")
    with pytest.raises(ProtoValidationError, match="'title'.*surrogate"):
        obj.with_(title=LONE)


def test_a_field_name_with_a_surrogate_is_refused():
    with pytest.raises(ProtoValidationError, match="surrogate"):
        DBObject(**{"na\udc00me": 1})


def test_collections_refuse_at_the_call():
    space = ObjectSpace(MemoryStorage())
    with space.new_database("d").new_transaction() as tr:
        with pytest.raises(ProtoValidationError, match="surrogate"):
            tr.new_dictionary().set_at(LONE, 1)
        with pytest.raises(ProtoValidationError, match="surrogate"):
            tr.new_dictionary().set_at("k", LONE)
        with pytest.raises(ProtoValidationError, match="surrogate"):
            tr.new_list().append_last(LONE)
        with pytest.raises(ProtoValidationError, match="surrogate"):
            tr.new_set().add(LONE)
        tr.set_root_object("ok", DBObject(text="ñandú \U0001F600"))
    with space.open_database("d").new_transaction() as tr:
        assert tr.get_root_object("ok").text == "ñandú \U0001F600"
    space.close()


def test_names_with_surrogates_are_refused():
    space = ObjectSpace(MemoryStorage())
    with pytest.raises(ProtoValidationError, match="surrogate"):
        space.new_database(LONE)
    db = space.new_database("d")
    with db.new_transaction() as tr:
        with pytest.raises(ProtoValidationError, match="surrogate"):
            tr.set_root_object(LONE, DBObject(n=1))
        with pytest.raises(ProtoValidationError, match="surrogate"):
            tr.new_mutable(LONE, 1)
    space.close()


class _RawRecord(Atom):
    """An Atom subclass that sets its state without canonical(): the codec is the last check."""
    __type_name__ = "tests.surrogates.RawRecord"

    def __init__(self, **state):
        super().__init__()
        for name, value in state.items():
            object.__setattr__(self, name, value)


def test_the_codec_refuses_naming_the_field_for_a_state_that_skipped_validation():
    with pytest.raises(ProtoValidationError, match="'body'.*surrogate"):
        codec.encode_state({"body": ["x", LONE]}, lambda atom: None)
    with pytest.raises(ProtoValidationError, match="surrogate"):
        codec.encode_state({"na\ud800me": 1}, lambda atom: None)
    space = ObjectSpace(MemoryStorage())
    with space.new_database("d").new_transaction() as tr:
        tr.set_root_object("raw", _RawRecord(body=LONE))
        with pytest.raises(ProtoValidationError, match="'body'.*surrogate"):
            tr.commit()
    space.close()


# -- review of R128: I3 (mutable handles), m2 (index definitions) ------------------------------

def test_a_mutable_handle_name_is_checked():
    with pytest.raises(ProtoValidationError, match="surrogate"):
        MutableObject(LONE)
    forged = object.__new__(MutableObject)          # a handle that skipped its constructor
    object.__setattr__(forged, "_name", LONE)
    with pytest.raises(ProtoValidationError, match="'cell'.*surrogate"):
        codec.encode_state({"cell": forged}, lambda atom: None)


def test_index_definitions_are_checked_when_built():
    with pytest.raises(ProtoValidationError, match="surrogate"):
        EqualityIndex(LONE, KeySpec.element())
    with pytest.raises(ProtoValidationError, match="surrogate"):
        KeySpec.path("a." + LONE)
    with pytest.raises(ProtoValidationError, match="surrogate"):
        KeySpec.extractor("x" + LONE)
