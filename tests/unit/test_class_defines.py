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


def test_a_name_absent_from_the_class_never_walks_the_mro(monkeypatch):
    # The 0eed600 cost: every field of every saved or loaded atom walked the whole MRO.
    class Fresh(DBObject):
        __type_name__ = "tests.class_defines.Fresh"
    walks = []
    real = atom_module._walk_mro
    monkeypatch.setattr(atom_module, "_walk_mro", lambda cls, name: walks.append(name) or real(cls, name))
    for _ in range(5):
        assert not class_defines(Fresh, "field_a")
        assert class_defines(Fresh, "with_")
    assert walks.count("field_a") == 0


def test_records_of_a_class_round_trip_with_the_cache():
    obj = _Child(value=3, other="x")
    assert obj.value == 3 and obj.other == "x"
    with pytest.raises(ProtoValidationError, match="inherited"):
        _Child(inherited=2)


def test_the_cache_stays_bounded_by_the_class_whatever_names_are_asked():
    # Review of R128, I2: schemaless records bring any field names; the cache holds only what the
    # class defines, never the names asked about.
    class Open(DBObject):
        __type_name__ = "tests.class_defines.Open"
        hidden = _RaisesAttributeError()
    for i in range(100_000):
        assert not class_defines(Open, f"field_{i}")
    assert class_defines(Open, "hidden")
    cached = atom_module._unreadable_names[Open]
    assert len(cached) <= sum(len(k.__dict__) for k in Open.__mro__)
    assert "field_7" not in cached and "hidden" in cached


def test_listing_a_class_survives_its_dict_changing_meanwhile():
    # Review of R128 (9cadc3a, re-review N1): a class dict may change while threads list the class
    # for the first time -- another thread's first class read (Python 3.14 caches annotations in
    # class dicts) or application code setting a class attribute.  A listing that iterated the live
    # dict raised "dictionary changed size during iteration".  A plain record class with a large
    # dict, eight threads released together, and a thread writing into the dict meanwhile.
    import threading
    for round_ in range(30):
        atom_module._unreadable_names.clear()
        namespace = {f"m{j}": j for j in range(2000)}
        cls = type(f"Wide{round_}", (DBObject,), dict(namespace, __type_name__=f"tests.class_defines.Wide{round_}"))
        barrier = threading.Barrier(9)
        errors = []
        stop = threading.Event()

        def ask():
            barrier.wait()
            try:
                class_defines(cls, "field")
            except Exception as e:         # reported below
                errors.append(e)

        def write():
            barrier.wait()
            n = 0
            while not stop.is_set():
                setattr(cls, f"_scratch{n % 64}", n)
                delattr(cls, f"_scratch{n % 64}")
                n += 1
        threads = [threading.Thread(target=ask) for _ in range(8)]
        writer = threading.Thread(target=write)
        for t in threads + [writer]:
            t.start()
        for t in threads:
            t.join()
        stop.set()
        writer.join()
        assert errors == [], errors[:1]


def test_the_first_listing_of_a_class_writes_nothing_into_its_dicts():
    # Re-review N2: a class read of __annotations__ (or __annotate__) on a class without its own
    # annotations makes Python 3.14 store __annotations_cache__ and __annotate_func__ in its dict, a
    # hidden write into application classes at their first save or load.  The listing skips them.
    cls = type("Plain", (DBObject,), {"__type_name__": "tests.class_defines.Plain", "m": 1})
    before = {k: set(k.__dict__) for k in cls.__mro__}
    assert not class_defines(cls, "field")
    assert {k: set(k.__dict__) for k in cls.__mro__} == before
