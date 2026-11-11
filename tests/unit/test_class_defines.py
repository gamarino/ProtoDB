"""``class_defines``: which names of a class hide a persisted field (pre-tag review m1, R137a).

Catches: a fast path that answers differently from the MRO walk it replaces -- a metaclass
attribute counted as defined, a descriptor that raises when read on the class missed, an
attribute added to a class after a first query missed -- and a fast path that still walks
the MRO for every field name (the 0eed600 cost on the save and load paths).
"""
from __future__ import annotations

import pytest

from proto_db.core import atom as atom_module
from proto_db.core.atom import class_defines
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoValidationError


class _RaisesAttributeError:
    """A descriptor that raises ``AttributeError`` when read on the class (``hasattr`` misses it)."""

    def __get__(self, obj, owner=None):
        raise AttributeError("not readable on the class")


class _RaisesOther:
    def __get__(self, obj, owner=None):
        raise RuntimeError("not readable at all")


class _Base(DBObject):
    __type_name__ = "tests.class_defines.Base"
    inherited = 1

    def method(self):
        return 1


class _Child(_Base):
    __type_name__ = "tests.class_defines.Child"
    hidden = _RaisesAttributeError()
    broken = _RaisesOther()

    @property
    def prop(self):
        return 1


def _walk(cls, name):
    """The rule itself: some class of the MRO has ``name`` in its ``__dict__``."""
    return any(name in klass.__dict__ for klass in cls.__mro__)


@pytest.mark.parametrize("name", ["inherited", "method", "prop", "hidden", "broken", "with_",
                                  "mro", "register", "zz_absent", "__init__", "__class__"])
def test_the_answer_is_the_mro_walk(name):
    for _ in range(3):              # the first query fills the cache; the next ones read it
        assert class_defines(_Child, name) is _walk(_Child, name)


def test_metaclass_attributes_do_not_count():
    assert not class_defines(_Child, "mro")
    assert not class_defines(_Child, "register")


def test_a_descriptor_that_raises_attribute_error_on_the_class_counts():
    assert not hasattr(_Child, "hidden")
    assert class_defines(_Child, "hidden")
    assert class_defines(_Child, "hidden")
    with pytest.raises(ProtoValidationError, match="hidden"):
        _Child(hidden=1)


def test_a_descriptor_that_raises_another_error_on_the_class_counts():
    assert class_defines(_Child, "broken")
    assert class_defines(_Child, "broken")


def test_an_attribute_added_after_a_first_query_counts():
    class Late(DBObject):
        __type_name__ = "tests.class_defines.Late"
    assert not class_defines(Late, "added_later")
    Late.added_later = 5
    assert class_defines(Late, "added_later")
    del Late.added_later
    assert not class_defines(Late, "added_later")


def test_a_name_absent_from_the_class_does_not_walk_the_mro_twice(monkeypatch):
    # The 0eed600 cost: every field of every saved or loaded atom walked the whole MRO.
    class Fresh(DBObject):
        __type_name__ = "tests.class_defines.Fresh"
    walks = []
    real = atom_module._walk_mro
    monkeypatch.setattr(atom_module, "_walk_mro", lambda cls, name: walks.append(name) or real(cls, name))
    for _ in range(5):
        assert not class_defines(Fresh, "field_a")
        assert class_defines(Fresh, "with_")
    assert walks.count("field_a") == 1


def test_records_of_a_class_round_trip_with_the_cache():
    obj = _Child(value=3, other="x")
    assert obj.value == 3 and obj.other == "x"
    with pytest.raises(ProtoValidationError, match="inherited"):
        _Child(inherited=2)
