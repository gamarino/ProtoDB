"""End-to-end: ObjectSpace over FileStorage (Task 5.1).

Catches: commits acknowledged but lost on a crash (the F0 WAL durability bug), data
committed after a read becoming unreadable, root published before its data is durable,
durability modes ignored, and indexes attached to a root collection lost or stale after
reopen.
"""
import os
import subprocess
import sys
import textwrap

import pytest

from proto_db.indexing import EqualityIndex, RangeIndex
from proto_db.query.terms import EQ, LT, Term
from proto_db.storage.file import FileStorage
from proto_db.transactions import ObjectSpace
from tests.support.faulty_fs import FaultPlan, FaultyFS

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Commits two transactions, then dies without closing the storage, as a crash would.
WRITER = textwrap.dedent("""
    import os, sys
    from proto_db.storage.file import FileStorage
    from proto_db.transactions import ObjectSpace

    space = ObjectSpace(FileStorage(sys.argv[1]))
    db = space.new_database("CrashDB")
    tr = db.new_transaction()
    tr.set_root_object("first", tr.new_list().append_last(1).append_last(2))
    tr.commit()
    tr = db.new_transaction()
    tr.set_root_object("second", tr.new_dictionary().set_at("k", "v"))
    tr.commit()
    os._exit(0)
""")


def open_space(path, **kwargs):
    return ObjectSpace(FileStorage(path, **kwargs))


# -- F0 WAL durability tests, ported ---------------------------------------------------------

def test_committed_data_survives_a_crash(tmp_path):
    path = str(tmp_path / "space")
    env = dict(os.environ, PYTHONPATH=REPO)
    subprocess.run([sys.executable, "-c", WRITER, path], check=True, env=env, timeout=120)
    space = open_space(path)
    try:
        tr = space.open_database("CrashDB").new_transaction()
        assert list(tr.get_root_object("first")) == [1, 2]
        assert tr.get_root_object("second").get_at("k") == "v"
        tr.abort()
    finally:
        space.close()


def test_data_committed_after_a_read_is_readable(tmp_path):
    space = open_space(str(tmp_path / "space"))
    try:
        db = space.new_database("ReadDB")
        tr = db.new_transaction()
        tr.set_root_object("first", tr.new_list().append_last(1))
        tr.commit()
        tr = db.new_transaction()
        assert list(tr.get_root_object("first")) == [1]
        tr.abort()
        tr = db.new_transaction()
        tr.set_root_object("second", tr.new_list().append_last(2).append_last(3))
        tr.commit()
        tr = db.new_transaction()
        assert list(tr.get_root_object("second")) == [2, 3]
        tr.abort()
    finally:
        space.close()


def test_page_read_at_end_of_segment_sees_later_commits(tmp_path):
    """Port of the F0 page-cache test: a cached page at the end of a segment must not hide
    records appended after it was read."""
    path = str(tmp_path / "space")
    space = open_space(path, page_size=64 * 1024)
    try:
        db = space.new_database("db")
        for i in range(5):
            with db.new_transaction() as tr:
                tr.set_root_object(f"r{i}", tr.new_list().append_last(i))
            tr = db.new_transaction()
            for j in range(i + 1):
                assert list(tr.get_root_object(f"r{j}")) == [j]
            tr.abort()
    finally:
        space.close()


# -- durability ------------------------------------------------------------------------------

class _RecordingFS(FaultyFS):
    """Records the order of fsync and rename calls."""

    def __init__(self):
        super().__init__(FaultPlan(0, "write"))
        self.events = []

    def fsync(self, fd):
        path = self._paths[fd]
        super().fsync(fd)
        self.events.append(("fsync", os.path.basename(path)))

    def replace(self, src, dst):
        super().replace(src, dst)
        self.events.append(("rename", os.path.basename(dst)))


@pytest.mark.parametrize("durability", ["fsync", "flush", "none"])
def test_commit_honours_the_durability_mode(tmp_path, durability):
    fs = _RecordingFS()
    space = ObjectSpace(FileStorage(str(tmp_path / "space"), durability=durability, _fs=fs))
    try:
        db = space.new_database("db")
        fs.events.clear()
        with db.new_transaction() as tr:
            tr.set_root_object("x", tr.new_list().append_last(1))
        renames = [i for i, e in enumerate(fs.events) if e == ("rename", "space.root")]
        assert len(renames) == 1
        seg_syncs = [i for i, e in enumerate(fs.events) if e[0] == "fsync" and e[1].endswith(".seg")]
        if durability == "fsync":
            assert seg_syncs and max(seg_syncs) < renames[0], fs.events
        else:
            assert not seg_syncs
    finally:
        space.close()


def test_default_durability_survives_power_loss(tmp_path):
    fs = FaultyFS(FaultPlan(0, "write"))
    path = str(tmp_path / "space")
    space = ObjectSpace(FileStorage(path, _fs=fs))
    db = space.new_database("db")
    with db.new_transaction() as tr:
        tr.set_root_object("d", tr.new_dictionary().set_at("k", "v"))
    fs.power_loss()  # storage abandoned, as on a power cut
    reopened = open_space(path)
    try:
        tr = reopened.open_database("db").new_transaction()
        assert tr.get_root_object("d").get_at("k") == "v"
        tr.abort()
    finally:
        reopened.close()


# -- reopen with indexes ---------------------------------------------------------------------

def test_reopen_keeps_roots_history_and_indexes(tmp_path):
    path = str(tmp_path / "space")
    space = open_space(path)
    db = space.new_database("people")
    with db.new_transaction() as tr:
        people = tr.new_dictionary().add_index(EqualityIndex("by_city", "city")) \
            .add_index(RangeIndex("by_age", "age"))
        for key, name, city, age in [("a", "ann", "Paris", 25), ("b", "bob", "Rome", 40),
                                     ("c", "cid", "Paris", 33)]:
            people = people.set_at(key, tr.new_object(name=name, city=city, age=age))
        tr.set_root_object("people", people)
    # A blind merge maintains the indexes of the committed version.
    with db.new_transaction() as tr:
        tr.set_root_object("people", tr.new_dictionary().set_at(
            "d", tr.new_object(name="dan", city="Paris", age=19)).remove_at("b"))
    space.close()

    space = open_space(path)
    try:
        db = space.open_database("people")
        assert space.list_databases() == ["people"]
        assert [h.version for h in db.history()] == [2, 1, 0]
        tr = db.new_transaction()
        people = tr.get_root_object("people")
        assert sorted(people.keys()) == ["a", "c", "d"]
        assert people.index_names() == ["by_age", "by_city"]
        paris = people.get_index("by_city").plan(Term("city", EQ, "Paris")).execute()
        assert sorted(p.name for p in paris) == ["ann", "cid", "dan"]
        young = people.get_index("by_age").plan(Term("age", LT, 30)).execute()
        assert sorted(p.name for p in young) == ["ann", "dan"]
        # Keep working after reopen.
        tr.set_root_object("people", people.set_at("e", tr.new_object(name="eve", city="Rome", age=50)))
        tr.commit()
        tr = db.new_transaction()
        rome = tr.get_root_object("people").get_index("by_city").plan(Term("city", EQ, "Rome")).execute()
        assert [p.name for p in rome] == ["eve"]
        tr.abort()
    finally:
        space.close()


def test_fresh_interpreter_opens_a_space(tmp_path):
    """Root atoms resolve without importing proto_db.transactions' dependencies first."""
    path = str(tmp_path / "space")
    space = open_space(path)
    db = space.new_database("db")
    with db.new_transaction() as tr:
        tr.set_root_object("l", tr.new_list().append_last("x"))
    space.close()
    child = textwrap.dedent("""
        import sys
        from proto_db.storage.file import FileStorage
        from proto_db.transactions import ObjectSpace
        space = ObjectSpace(FileStorage(sys.argv[1]))
        tr = space.open_database("db").new_transaction()
        print(list(tr.get_root_object("l")))
        space.close()
    """)
    env = dict(os.environ, PYTHONPATH=REPO)
    out = subprocess.run([sys.executable, "-c", child, path], check=True, env=env, timeout=120,
                         capture_output=True, text=True).stdout
    assert out.strip() == "['x']"


def test_two_processes_conflict_on_the_same_root(tmp_path):
    """Process exclusion: a second process's commit to a root this one read is a conflict."""
    path = str(tmp_path / "space")
    space = open_space(path)
    db = space.new_database("db")
    with db.new_transaction() as tr:
        tr.set_root_object("n", 0)
    tr = db.new_transaction()
    n = tr.get_root_object("n")
    child = textwrap.dedent("""
        import sys
        from proto_db.storage.file import FileStorage
        from proto_db.transactions import ObjectSpace
        space = ObjectSpace(FileStorage(sys.argv[1]))
        space.open_database("db").run(lambda tr: tr.set_root_object("n", tr.get_root_object("n") + 10))
        space.close()
    """)
    env = dict(os.environ, PYTHONPATH=REPO)
    subprocess.run([sys.executable, "-c", child, path], check=True, env=env, timeout=120)
    tr.set_root_object("n", n + 1)
    from proto_db.errors import ProtoConflictError
    with pytest.raises(ProtoConflictError):
        tr.commit()
    assert db.run(lambda tr: tr.set_root_object("n", tr.get_root_object("n") + 1) or 0) == 0
    tr = db.new_transaction()
    assert tr.get_root_object("n") == 11
    tr.abort()
    space.close()
