"""List under the precondition model (spec sections 5 and 6, R9, R14-R16, R19, R48; review
correction of R15).

One pair per row of the List tables: the operation against a concurrent change of the same unit
(conflict, naming the unit, or composition) and of another one (both published).  Interleavings
are explicit: open A, open B, commit B, commit A.
"""
import pytest

from proto_db.collections.linking import fine_grained, link_of
from proto_db.collections.lists import List
from proto_db.errors import ProtoConflictError, ProtoValidationError
from proto_db.preconditions import Unit, UnitKind, token
from tests.support.modes import db_any_mode, mode, space_any_mode  # noqa: F401 -- fixtures


@pytest.fixture
def db(db_any_mode):
    """The database ``db`` in each commit mode: standalone, group commit, cluster (spec section 12,
    Task 14; :mod:`tests.support.modes`)."""
    return db_any_mode


def position(i, root="l"):
    return Unit(UnitKind.POSITION, root, key=i)


def whole(root="l"):
    return Unit(UnitKind.WHOLE, root)


def seed_list(db, *items):
    db.run(lambda tr: tr.set_root_object("l", tr.new_list().extend(items)))


def write(tr, fn, root="l"):
    tr.set_root_object(root, fn(tr.get_root_object(root)))


def content(db, root="l"):
    return list(db.run(lambda tr: list(tr.get_root_object(root))))


def conflict(tr):
    with pytest.raises(ProtoConflictError) as e:
        tr.commit()
    assert e.value.database == "db"
    return e.value.unit


def preconditions(tr):
    return tr._read_log.preconditions()


def fine_units(tr, root="l"):
    return {u for u in preconditions(tr) if u.root == root and u.kind is not UnitKind.ROOT_PRESENCE}


# -- classification (R1, R48, N5) ----------------------------------------------------------------

def test_list_is_fine_grained_and_linked(db):
    seed_list(db, 1, 2, 3)
    assert fine_grained(List)
    tr = db.new_transaction()
    lst = tr.get_root_object("l")
    assert link_of(lst) is not None
    # Reading the root records presence and class only (section 6): no whole-root precondition.
    assert fine_units(tr) == set()
    tr.abort()


def test_subclass_overriding_a_listed_operation_is_an_object_root():
    class Stamped(List):
        __type_name__ = "tests.unit.precondition_list.Stamped"

        def set_at(self, index, value):
            return super().set_at(index, value)

    assert not fine_grained(Stamped)

    class Plain(List):
        __type_name__ = "tests.unit.precondition_list.Plain"

    assert fine_grained(Plain)


class Rotating(List):
    """A List subclass whose method changes content through ``_with`` without a record of
    intentions (module level: a type name is registered once, whatever the modes)."""
    __type_name__ = "tests.unit.precondition_list.Rotating"

    def emptied(self):
        return self._with(root=None, count=0)


def test_unlogged_content_change_marks_the_version_whole(db):
    """A subclass method that changes content through ``_with`` without a record of intentions
    cannot be reapplied: the version is whole (P-B), published under a WHOLE precondition."""
    db.run(lambda tr: tr.set_root_object("l", Rotating(context=tr).extend([1, 2])))
    a, b = db.new_transaction(), db.new_transaction()
    emptied = a.get_root_object("l").emptied()
    assert emptied._whole
    a.set_root_object("l", emptied)
    assert whole() in a._read_log
    write(b, lambda lst: lst.append_last(3))
    b.commit()
    assert conflict(a) == whole()


# -- append_last / extend: no precondition, appended in commit order (section 5) --------------------

def test_append_last_concurrent_both_kept_commit_order(db):
    seed_list(db, 1, 2, 3)
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda lst: lst.append_last("a"))
    write(b, lambda lst: lst.append_last("b"))
    assert fine_units(a) == set() and fine_units(b) == set()
    b.commit()
    a.commit()
    assert content(db) == [1, 2, 3, "b", "a"]


def test_extend_concurrent_both_kept(db):
    seed_list(db, 1, 2, 3)
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda lst: lst.extend(["a1", "a2"]))
    write(b, lambda lst: lst.extend(["b1", "b2"]).append_last("b3"))
    b.commit()
    a.commit()
    assert content(db) == [1, 2, 3, "b1", "b2", "b3", "a1", "a2"]


def test_appends_compose_with_a_concurrent_set_at(db):
    seed_list(db, 1, 2, 3)
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda lst: lst.append_last(4))
    write(b, lambda lst: lst.set_at(0, 10))
    b.commit()
    a.commit()
    assert content(db) == [10, 2, 3, 4]


def test_empty_extend_records_nothing_and_publishes_the_base(db):
    seed_list(db, 1)
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda lst: lst.extend([]))
    write(b, lambda lst: lst.append_last(2))
    b.commit()
    a.commit()
    assert content(db) == [1, 2]


# -- set_at(i, v): position i exists and equals the base (section 5, R15) --------------------------

def test_set_at_other_position_no_conflict(db):
    seed_list(db, 1, 2, 3)
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda lst: lst.set_at(0, "a"))
    write(b, lambda lst: lst.set_at(1, "b"))
    assert preconditions(a)[position(0)] == token(1)
    b.commit()
    a.commit()
    assert content(db) == ["a", "b", 3]


def test_set_at_same_position_conflicts(db):
    seed_list(db, 1, 2, 3)
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda lst: lst.set_at(2, "a"))
    write(b, lambda lst: lst.set_at(2, "b"))
    b.commit()
    with pytest.raises(ProtoConflictError, match="Position 2 of root 'l'") as e:
        a.commit()
    assert e.value.unit == Unit(UnitKind.POSITION, "l", key=2)
    assert content(db) == [1, 2, "b"]


def test_set_at_position_removed_concurrently_conflicts(db):
    """'i exists in the current state': a concurrent removal that leaves position i absent conflicts."""
    seed_list(db, 1, 2, 3)
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda lst: lst.set_at(2, "a"))
    write(b, lambda lst: lst.remove_at(0).remove_at(0))
    b.commit()
    assert conflict(a) == position(2)


def test_set_at_aba_by_value_holds(db):
    """R7: position 1 changed and came back to the base value: the precondition holds."""
    seed_list(db, 1, 2, 3)
    a = db.new_transaction()
    write(a, lambda lst: lst.set_at(1, "a"))
    db.run(lambda tr: write(tr, lambda lst: lst.set_at(1, "x")))
    db.run(lambda tr: write(tr, lambda lst: lst.set_at(1, 2)))
    a.commit()
    assert content(db) == [1, "a", 3]


def test_set_at_exact_token_tells_one_from_one_point_zero(db):
    seed_list(db, 1, 2, 3)
    a = db.new_transaction()
    write(a, lambda lst: lst.set_at(0, "a"))
    db.run(lambda tr: write(tr, lambda lst: lst.set_at(0, 1.0)))
    assert conflict(a) == position(0)


def test_set_at_twice_records_once_and_last_value_wins(db):
    seed_list(db, 1, 2, 3)
    a, b = db.new_transaction(), db.new_transaction()
    lst = a.get_root_object("l").set_at(1, "x")
    before = dict(preconditions(a))
    lst = lst.set_at(1, "y")
    assert lst.get_at(1) == "y"
    assert preconditions(a) == before                                  # R9
    a.set_root_object("l", lst)
    write(b, lambda l: l.append_last(4))
    b.commit()
    a.commit()
    assert content(db) == [1, "y", 3, 4]


def test_set_at_on_own_append_follows_it(db):
    """Brief test inverted by controller ruling (P-A): serializability prevails over folding by R9.
    An index past the base length names "my append" only for that length: after a concurrent
    append, set_at(len_base) would give [..., z, y], which neither serial order gives
    ([..., y, x] or [..., y, z]).  The set_at records the whole list (it depends on the length):
    the concurrent append conflicts; without one, the transaction's own list is published."""
    seed_list(db, 1, 2, 3)
    len_base = 3
    a, b = db.new_transaction(), db.new_transaction()
    lst = a.get_root_object("l").append_last("x").set_at(len_base, "y")
    assert lst.get_at(len_base) == "y"
    assert fine_units(a) == {whole()}
    a.set_root_object("l", lst)
    write(b, lambda l: l.append_last("z"))
    b.commit()
    assert conflict(a) == whole()
    assert content(db) == [1, 2, 3, "z"]
    db.run(lambda tr: write(tr, lambda l: l.append_last("x").set_at(4, "y")))
    assert content(db) == [1, 2, 3, "z", "y"]


def test_set_at_on_one_of_several_own_appends(db):
    """Controller ruling (P-A): a set_at on any own-appended position records the whole list."""
    seed_list(db, 1)
    a, b = db.new_transaction(), db.new_transaction()
    lst = a.get_root_object("l").extend(["p", "q"]).append_last("r").set_at(2, "Q").set_at(0, 10)
    assert lst.get_at(2) == "Q" and whole() in preconditions(a)
    a.set_root_object("l", lst)
    write(b, lambda l: l.extend(["z1", "z2"]))
    b.commit()
    assert conflict(a) == whole()
    db.run(lambda tr: write(tr, lambda l: l.extend(["p", "q"]).append_last("r").set_at(4, "Q").set_at(0, 10)))
    assert content(db) == [10, "z1", "z2", "p", "Q", "r"]


# -- get_at(i): position i exists and equals the base (section 6) ---------------------------------

def test_get_at_pair(db):
    """get_at(i) records Unit(POSITION, i): a concurrent change of i conflicts, of j does not."""
    seed_list(db, 1, 2, 3)
    a, b = db.new_transaction(), db.new_transaction()
    lst = a.get_root_object("l")
    assert lst.get_at(1) == 2
    assert preconditions(a)[position(1)] == token(2)
    a.set_root_object("l", lst.append_last(4))
    write(b, lambda l: l.set_at(2, "b"))
    b.commit()
    a.commit()
    assert content(db) == [1, 2, "b", 4]

    a, b = db.new_transaction(), db.new_transaction()
    lst = a.get_root_object("l")
    assert lst.get_at(1) == 2
    a.set_root_object("l", lst.append_last(5))
    write(b, lambda l: l.set_at(1, "b"))
    b.commit()
    assert conflict(a) == position(1)


def test_get_at_after_own_set_at_records_nothing(db):
    seed_list(db, 1, 2, 3)
    a = db.new_transaction()
    lst = a.get_root_object("l").set_at(1, "x")
    before = dict(preconditions(a))
    assert lst.get_at(1) == "x"
    assert preconditions(a) == before                                  # R9
    a.abort()


def test_get_at_on_own_append_records_whole(db):
    """Controller ruling (P-A): reading an own-appended position by index depends on the base
    length, so it records the whole list; a concurrent append conflicts."""
    seed_list(db, 1, 2, 3)
    a, b = db.new_transaction(), db.new_transaction()
    lst = a.get_root_object("l").append_last("x")
    assert lst.get_at(3) == "x"
    assert fine_units(a) == {whole()}
    a.set_root_object("l", lst)
    write(b, lambda l: l.append_last("z"))
    b.commit()
    assert conflict(a) == whole()


def test_negative_index_records_whole(db):
    """Review correction of R15: a negative index is normalized against the transaction's own view
    (a read of the length) and records the whole list."""
    seed_list(db, 1, 2, 3)
    a, b = db.new_transaction(), db.new_transaction()
    lst = a.get_root_object("l").append_last(4)
    assert lst.get_at(-1) == 4                                         # the transaction's own view
    assert whole() in preconditions(a)
    a.set_root_object("l", lst)
    write(b, lambda l: l.append_last("b"))
    b.commit()
    assert conflict(a) == whole()


def test_negative_set_at_records_whole(db):
    seed_list(db, 1, 2, 3)
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda lst: lst.set_at(-1, "a"))
    assert whole() in preconditions(a)
    write(b, lambda lst: lst.append_last("b"))
    b.commit()
    assert conflict(a) == whole()
    # Without a concurrent change the transaction's value is published as is.
    db.run(lambda tr: write(tr, lambda lst: lst.set_at(-1, "c")))
    assert content(db) == [1, 2, 3, "c"]


@pytest.mark.parametrize("bad", ["1", 1.0, True, None])
def test_invalid_index_raises_before_recording(db, bad):
    """Ruling: the argument is validated before any unit is recorded; the error type stays."""
    seed_list(db, 1, 2, 3)
    a = db.new_transaction()
    lst = a.get_root_object("l")
    with pytest.raises(ProtoValidationError):
        lst.get_at(bad)
    with pytest.raises(ProtoValidationError):
        lst.set_at(bad, 1)
    assert fine_units(a) == set()
    a.abort()


def test_set_at_invalid_value_raises_before_recording(db):
    seed_list(db, 1, 2, 3)
    a = db.new_transaction()
    with pytest.raises(ProtoValidationError):
        a.get_root_object("l").set_at(0, object())
    assert fine_units(a) == set()
    a.abort()


@pytest.mark.parametrize("op", ["get", "set"])
def test_out_of_range_raises_and_records_the_length(db, op):
    """A position outside the transaction's view raises ProtoValidationError as before.  The error
    reveals the length, so it records the whole list (P-A): a concurrent append that would have
    made the position exist conflicts."""
    seed_list(db, 1, 2, 3)
    a, b = db.new_transaction(), db.new_transaction()
    lst = a.get_root_object("l")
    with pytest.raises(ProtoValidationError, match="out of range"):
        lst.get_at(3) if op == "get" else lst.set_at(3, "x")
    assert whole() in preconditions(a)
    a.set_root_object("l", lst.append_last("a"))
    write(b, lambda l: l.append_last("b"))
    b.commit()
    assert conflict(a) == whole()


# -- structural operations: the whole list (R14, R16, R19) ------------------------------------------

def test_insert_at_conflicts_with_concurrent_append(db):
    seed_list(db, 1, 2, 3)
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda lst: lst.insert_at(1, "a"))
    assert whole() in preconditions(a)
    write(b, lambda lst: lst.append_last("b"))
    b.commit()
    assert conflict(a) == whole()
    assert content(db) == [1, 2, 3, "b"]


@pytest.mark.parametrize("structural", [lambda lst: lst.remove_at(0), lambda lst: lst.append_first("a"),
                                        lambda lst: lst.slice(0, 2), lambda lst: lst.head(1),
                                        lambda lst: lst.tail(1)],
                         ids=["remove_at", "append_first", "slice", "head", "tail"])
def test_structural_operation_conflicts_with_concurrent_set_at(db, structural):
    seed_list(db, 1, 2, 3)
    a, b = db.new_transaction(), db.new_transaction()
    write(a, structural)
    assert whole() in preconditions(a)
    write(b, lambda lst: lst.set_at(2, "b"))
    b.commit()
    assert conflict(a) == whole()


def test_structural_operation_published_as_is_without_concurrent_change(db):
    seed_list(db, 1, 2, 3)
    with db.new_transaction() as tr:
        write(tr, lambda lst: lst.append_last(4).insert_at(0, 0).set_at(2, "x").remove_at(1))
    assert content(db) == [0, "x", 3, 4]


def test_reads_after_a_structural_operation_add_nothing(db):
    """R19: by R14 the list is already the whole unit; later reads add nothing."""
    seed_list(db, 1, 2, 3)
    a = db.new_transaction()
    lst = a.get_root_object("l").insert_at(0, 0)
    before = dict(preconditions(a))
    assert lst.get_at(1) == 1 and lst.get_at(-1) == 3
    lst = lst.set_at(2, "x")
    assert len(lst) == 4
    assert preconditions(a) == before
    assert fine_units(a) == {whole()}
    a.abort()


@pytest.mark.parametrize("read", [len, list, lambda lst: lst.count, lambda lst: 2 in lst,
                                  lambda lst: lst.content_equals(List.from_iterable([1, 2, 3]))],
                         ids=["len", "iteration", "count", "contains", "content_equals"])
def test_whole_reads_conflict_with_any_change(db, read):
    seed_list(db, 1, 2, 3)
    a, b = db.new_transaction(), db.new_transaction()
    lst = a.get_root_object("l")
    read(lst)
    assert whole() in preconditions(a)
    a.set_root_object("l", lst.append_last("a"))
    write(b, lambda l: l.set_at(0, "b"))
    b.commit()
    assert conflict(a) == whole()


def test_repr_records_nothing(db):
    seed_list(db, 1, 2, 3)
    a = db.new_transaction()
    assert repr(a.get_root_object("l")) == "<List count=3>"
    assert fine_units(a) == set()
    a.abort()


# -- records per version and per transaction (R43-R45) ---------------------------------------------

def test_unassigned_branch_publishes_nothing_but_its_reads_stay(db):
    seed_list(db, 1, 2, 3)
    a, b = db.new_transaction(), db.new_transaction()
    base = a.get_root_object("l")
    base.set_at(0, "dropped")                                          # derived, never assigned
    a.set_root_object("l", base.append_last("kept"))
    write(b, lambda l: l.set_at(1, "b"))
    b.commit()
    a.commit()
    assert content(db) == [1, "b", 3, "kept"]

    a, b = db.new_transaction(), db.new_transaction()
    base = a.get_root_object("l")
    base.set_at(0, "dropped")
    a.set_root_object("l", base.append_last("kept"))
    write(b, lambda l: l.set_at(0, "b"))
    b.commit()
    assert conflict(a) == position(0)                                  # the branch's precondition stays


def test_list_committed_with_appends_links_with_a_fresh_record(db):
    """A version committed with its own appends may be the cached atom a later transaction links:
    the linked copy starts with no record of its own (R41), so the base length is the list's."""
    db.run(lambda tr: tr.set_root_object("l", tr.new_list().extend([1, 2, 3]).append_last(4)))
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda lst: lst.set_at(0, "a"))
    assert preconditions(a)[position(0)] == token(1)
    write(b, lambda lst: lst.set_at(0, "b"))
    b.commit()
    assert conflict(a) == position(0)


def test_set_root_object_of_another_root_replaces(db):
    """R11: a linked list assigned to another root is an object write of that root."""
    seed_list(db, 1, 2)
    a = db.new_transaction()
    a.set_root_object("m", a.get_root_object("l").append_last(3))
    assert Unit(UnitKind.ROOT, "m") in a._read_log
    a.commit()
    assert content(db, "m") == [1, 2, 3]


def test_reapplication_keeps_indexes_in_step(db):
    """R34: the intentions are reapplied through the same tree operations, indexes included."""
    from proto_db.indexing.equality import EqualityIndex
    db.run(lambda tr: tr.set_root_object(
        "l", tr.new_list().add_index(EqualityIndex("by_v", "v")).extend(
            [tr.new_object(v=1), tr.new_object(v=2)])))
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda lst: lst.append_last(a.new_object(v=3)).set_at(0, a.new_object(v=9)))
    write(b, lambda lst: lst.append_last(b.new_object(v=4)))
    b.commit()
    a.commit()
    lst = db.run(lambda tr: tr.get_root_object("l"))
    assert [o.v for o in lst] == [9, 2, 4, 3]
    lst.get_index("by_v").validate(list(lst.elements()))
