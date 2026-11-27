#!/usr/bin/env python3
"""5. Concurrency: preconditions and intentions

Generated from docs/tutorial/05-concurrency.md by ``python tests/docs/test_markdown_tutorials.py --write-scripts``:
the code below is the chapter's code blocks, verbatim and in order. After the blocks of each output the chapter
shows, ``_expect`` compares what they printed with it, so the script checks every figure the chapter shows,
exits with status 1 on the first difference, and prints "05_concurrency: OK" at the end. Edit the chapter, not this file.

Run it from anywhere: python examples/tutorial/05_concurrency.py
"""
import os as _os
import sys as _sys

_HERE = _os.path.dirname(_os.path.abspath(__file__))
_REPO = _os.path.normpath(_os.path.join(_HERE, *[_os.pardir] * 2))
_sys.path[:0] = [_REPO, _os.path.join(_REPO, "examples", "tutorial")]


class _Tee:
    """Standard output that also keeps what was printed since the last check."""

    def __init__(self, stream):
        self.stream, self.kept = stream, []

    def write(self, text):
        self.kept.append(text)
        return self.stream.write(text)

    def flush(self):
        self.stream.flush()


_sys.stdout = _Tee(_sys.stdout)


def _expect(shown):
    """Exit with status 1 unless the code since the last check printed ``shown`` (the chapter's Output)."""
    printed = "".join(_sys.stdout.kept)
    _sys.stdout.kept.clear()
    def normal(text):
        return "\n".join(line.rstrip() for line in text.strip().splitlines())
    if normal(printed) != normal(shown):
        _sys.stderr.write(f"--- the chapter shows ---\n{normal(shown)}\n--- the code printed ---\n{normal(printed)}\n")
        _sys.exit(1)


def _sample():
    """The chapter shows what the code since the last check printed as a sample of one run: not compared."""
    _sys.stdout.kept.clear()


# -- 05-concurrency.md, line 18 ----------------------------------------
import threading

from proto_db import MemoryStorage, ObjectSpace, ProtoConflictError, ProtoValidationError

import erp_app

space = ObjectSpace(MemoryStorage())
db = space.new_database("erp")
erp_app.seed(db)

report = db.new_transaction()                         # a long report starts
erp_app.advance(db, 3, "won", "bob")                  # Bob closes deal 3 meanwhile
print("the report sees deal 3 as", report.get_root_object("opportunities").get_at(3).stage)
report.abort()
print("a new transaction sees it as", db.new_transaction().get_root_object("opportunities").get_at(3).stage)
_expect("""
the report sees deal 3 as lead
a new transaction sees it as won
""")

# -- 05-concurrency.md, line 50 ----------------------------------------
def race(first, second):
    """Run first(tr) and second(tr) on one snapshot; commit first, then second; say what happened."""
    a, b = db.new_transaction(), db.new_transaction()
    first(a)
    second(b)
    a.commit()
    try:
        b.commit()
        return "both committed"
    except ProtoConflictError as error:
        unit = error.unit
        name = unit.root if unit.kind.name == "ROOT" else unit.key
        key = "" if name is None or unit.kind.name == "RANGE" else f" {name!r}"
        index = f" of index {unit.index!r}" if unit.index else ""
        return f"conflict on {unit.kind.name}{key}{index}"

def move(number, stage):
    """A transaction function: the next version of opportunity ``number``, at ``stage``."""
    def work(tr):
        deals = tr.get_root_object("opportunities")
        current = deals.get_at(number)
        tr.set_root_object("opportunities", deals.set_at(number, current.with_(previous=current, stage=stage)))
    return work

print("Ann and Bob move the same deal:", race(move(5, "qualified"), move(5, "lost")))
_expect("""
Ann and Bob move the same deal: conflict on KEY 5
""")

# -- 05-concurrency.md, line 93 ----------------------------------------
with db.new_transaction() as tr:
    tr.new_mutable("version:opportunities", 0)

def whole_root(work):
    """The obvious approach: the transaction depends on the whole root, through its version number."""
    def guarded(tr):
        version = tr.get_mutable("version:opportunities")
        work(tr)
        tr.set_mutable("version:opportunities", version + 1)
    return guarded

print("whole root, different deals:", race(whole_root(move(6, "won")), whole_root(move(7, "won"))))
_expect("""
whole root, different deals: conflict on CELL 'version:opportunities'
""")

# -- 05-concurrency.md, line 119 ----------------------------------------
def concurrent_moves(wrap, first_deal):
    """Four threads, 25 moves each, of deals no other thread touches; return the attempts run needed."""
    attempts = []

    def worker(t):
        for i in range(25):
            def work(tr, number=first_deal + 100 * t + i):
                attempts.append(1)                        # counts every attempt, retries included
                move(number, "won")(tr)
            db.run(wrap(work), retries=10_000)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return len(attempts)

whole = concurrent_moves(whole_root, first_deal=100)
fine = concurrent_moves(lambda work: work, first_deal=500)
deals = db.new_transaction().get_root_object("opportunities")
assert all(deals.get_at(100 + 100 * t + i).stage == "won" and deals.get_at(500 + 100 * t + i).stage == "won"
           for t in range(4) for i in range(25))
assert fine == 100                                     # deals no other thread touches never conflict
print(f"100 commits: {whole} attempts with a version per root, {fine} with the precondition model")
_sample()

# -- 05-concurrency.md, line 184 ----------------------------------------
print("different deals:", race(move(8, "won"), move(9, "lost")))
deals = db.new_transaction().get_root_object("opportunities")
print("deal 8:", deals.get_at(8).stage, "| deal 9:", deals.get_at(9).stage)
_expect("""
different deals: both committed
deal 8: won | deal 9: lost
""")

# -- 05-concurrency.md, line 255 ----------------------------------------
from proto_db import F, from_collection

with db.new_transaction() as tr:
    tr.set_root_object("company", tr.new_object(name="ACME", currency="ARS", fiscal_year=2026))
    tr.set_root_object("group:sales", tr.new_set().add("ann").add("bob"))
    tr.set_root_object("reached", tr.new_counted_set())
    tr.set_root_object("activity", tr.new_list().append_last("import"))

def on_root(name, change):
    return lambda tr: tr.set_root_object(name, change(tr.get_root_object(name)))

def on_company(**changes):
    return on_root("company", lambda company: company.with_(**changes))

def rename_user(login, name):
    return lambda tr: tr.set_mutable(f"user:{login}", tr.get_mutable(f"user:{login}").with_(name=name))

def rewrite(number, **changes):
    """A transaction function: a new version of deal ``number`` with ``changes`` (its stage kept)."""
    def work(tr):
        deals = tr.get_root_object("opportunities")
        tr.set_root_object("opportunities", deals.set_at(number, deals.get_at(number).with_(**changes)))
    return work

def after_query(query, number, stage):
    """Read with ``query``, then write deal ``number``: the commit depends on what the query read."""
    def work(tr):
        query(tr.get_root_object("opportunities")).count()
        move(number, stage)(tr)
    return work

leads = lambda deals: from_collection(deals).where(F.stage == "lead")
largest = lambda deals: from_collection(deals).where(F.amount >= 9900)
by_title = lambda deals: from_collection(deals).where(F.title == "opportunity 7")      # no index: a scan

cases = [
    ("record root, two attributes", on_company(currency="USD"), on_company(fiscal_year=2027)),
    ("cell, two corrections", rename_user("cy", "Cy Park Jr."), rename_user("cy", "C. Park")),
    ("set, two adds", on_root("group:sales", lambda s: s.add("dee")), on_root("group:sales", lambda s: s.add("eve"))),
    ("set, two removes", on_root("group:sales", lambda s: s.remove("bob")), on_root("group:sales", lambda s: s.remove("bob"))),
    ("counted set, two adds", on_root("reached", lambda c: c.add("won")), on_root("reached", lambda c: c.add("won"))),
    ("list, two appends", on_root("activity", lambda l: l.append_last("a")), on_root("activity", lambda l: l.append_last("b"))),
    ("list, append and insert", on_root("activity", lambda l: l.append_last("c")), on_root("activity", lambda l: l.insert_at(0, "x"))),
    ("lead lookup; a proposal is lost", move(11, "lost"), after_query(leads, 20, "qualified")),
    ("lead lookup; a lead is qualified", move(12, "qualified"), after_query(leads, 21, "qualified")),
    ("lead lookup; a lead's amount changes", rewrite(15, amount=4321), after_query(leads, 25, "qualified")),
    ("amount >= 9900; a small deal changes", move(13, "won"), after_query(largest, 22, "won")),
    ("amount >= 9900; a large deal changes", move(242, "won"), after_query(largest, 23, "won")),
    ("title scan; any deal changes", move(14, "won"), after_query(by_title, 24, "won")),
]
for label, first, second in cases:
    print(f"{label:38} {race(first, second)}")
_expect("""
record root, two attributes            conflict on ROOT 'company'
cell, two corrections                  conflict on CELL 'user:cy'
set, two adds                          both committed
set, two removes                       conflict on ELEMENT 'bob'
counted set, two adds                  both committed
list, two appends                      both committed
list, append and insert                conflict on WHOLE
lead lookup; a proposal is lost        both committed
lead lookup; a lead is qualified       conflict on INDEX_GROUP 'lead' of index 'by_stage'
lead lookup; a lead's amount changes   conflict on INDEX_GROUP 'lead' of index 'by_stage'
amount >= 9900; a small deal changes   both committed
amount >= 9900; a large deal changes   conflict on RANGE of index 'by_amount'
title scan; any deal changes           conflict on WHOLE
""")

# -- 05-concurrency.md, line 350 ----------------------------------------
def add_member(login):
    def work(tr):
        groups = tr.get_root_object("groups")
        tr.set_root_object("groups", groups.set_at("sales", groups.get_at("sales").add(login)))
    return work

print("nested set, two adds:", race(add_member("dee"), add_member("eve")))
print("set root, two adds:  ", race(on_root("group:sales", lambda s: s.add("fay")),
                                     on_root("group:sales", lambda s: s.add("gus"))))
_expect("""
nested set, two adds: conflict on KEY 'sales'
set root, two adds:   both committed
""")

# -- 05-concurrency.md, line 376 ----------------------------------------
keep = lambda tr: tr.set_root_object("report", tr.new_object(deals=tr.get_root_object("opportunities")))
print("a record holding the deals:", race(move(16, "won"), keep))
_expect("""
a record holding the deals: conflict on WHOLE
""")

# -- 05-concurrency.md, line 396 ----------------------------------------
first, second = db.new_transaction(), db.new_transaction()
first.set_root_object("company", first.new_object(name="ACME", currency="USD"))    # neither read the root
second.set_root_object("company", second.new_object(name="ACME", currency="EUR"))
first.commit()
try:
    second.commit()
except ProtoConflictError as error:
    print("blind write refused:", error)
print("currency:", db.new_transaction().get_root_object("company").currency)
_expect("""
blind write refused: Root 'company' of database 'erp' was changed by a concurrent commit (unit ROOT)
currency: USD
""")

# -- 05-concurrency.md, line 426 ----------------------------------------
def retire(login):
    return lambda tr: tr.remove_mutable(f"user:{login}")

def assign(number, login, check):
    def work(tr):
        if check and tr.get_mutable(f"user:{login}") is None:
            raise ProtoValidationError(f"no user {login!r}")
        deals = tr.get_root_object("opportunities")
        tr.set_root_object("opportunities", deals.set_at(number, deals.get_at(number).with_(owner=erp_app.user(login))))
    return work

print("unchecked:", race(retire("cy"), assign(30, "cy", check=False)))
with db.new_transaction() as tr:
    tr.new_mutable("user:cy", tr.new_object(login="cy", name="Cy Park"))
print("checked:  ", race(retire("cy"), assign(31, "cy", check=True)))
_expect("""
unchecked: both committed
checked:   conflict on CELL 'user:cy'
""")

# -- 05-concurrency.md, line 460 ----------------------------------------
from proto_db import EqualityIndex

with db.new_transaction() as tr:
    tr.set_root_object("logins", tr.new_dictionary())                                   # keyed by login
    tr.set_root_object("staff", tr.new_dictionary().add_index(EqualityIndex("by_login", "login")))
    tr.set_root_object("accounts", tr.new_dictionary().add_index(EqualityIndex("by_login", "login", unique=True)))
    tr.set_root_object("people", tr.new_dictionary())                                   # no index
    for root in ("staff", "people"):                  # 50 existing members: enough for the planner to use an index
        members = tr.get_root_object(root)
        for seat in range(100, 150):
            members = members.set_at(seat, tr.new_object(login=f"user{seat}"))
        tr.set_root_object(root, members)

def by_key(n):
    def work(tr):
        logins = tr.get_root_object("logins")
        if logins.has("dee"):
            raise ProtoValidationError("dee exists")
        tr.set_root_object("logins", logins.set_at("dee", tr.new_object(login="dee", seat=n)))
    return work

def by_lookup(n):
    def work(tr):
        staff = tr.get_root_object("staff")
        if from_collection(staff).where(F.login == "dee").count():
            raise ProtoValidationError("dee exists")
        tr.set_root_object("staff", staff.set_at(n, tr.new_object(login="dee")))
    return work

def unique_no_read(n):
    def work(tr):
        accounts = tr.get_root_object("accounts")
        tr.set_root_object("accounts", accounts.set_at(n, tr.new_object(login="dee")))
    return work

def by_scan(n):
    def work(tr):
        people = tr.get_root_object("people")
        if any(person.login == "dee" for person in people.values()):
            raise ProtoValidationError("dee exists")
        tr.set_root_object("people", people.set_at(n, tr.new_object(login="dee")))
    return work

for label, make in (("by key, has(k)", by_key), ("by an equality lookup", by_lookup),
                    ("unique index, no read", unique_no_read), ("by a scan", by_scan)):
    print(f"{label:24} {race(make(1), make(2))}")
try:
    db.run(unique_no_read(3))
except ProtoValidationError:
    print("the retry of a duplicate: ProtoValidationError")
_expect("""
by key, has(k)           conflict on KEY 'dee'
by an equality lookup    conflict on INDEX_GROUP 'dee' of index 'by_login'
unique index, no read    conflict on INDEX_GROUP 'dee' of index 'by_login'
by a scan                conflict on WHOLE
the retry of a duplicate: ProtoValidationError
""")

# -- 05-concurrency.md, line 543 ----------------------------------------
attempts, notices = [], []

def bob_closes(tr):
    attempts.append(len(attempts) + 1)
    if len(attempts) == 1:                            # Ann commits while Bob's first attempt runs
        erp_app.advance(db, 40, "proposal", "ann")
    deals = tr.get_root_object("opportunities")
    current = deals.get_at(40)
    tr.set_root_object("opportunities", deals.set_at(40, current.with_(previous=current, stage="won",
                                                                       changed_by="bob")))
    return f"deal 40 won after {current.stage}"

notices.append(db.run(bob_closes))                    # the external effect: after run returned, once
deal = db.new_transaction().get_root_object("opportunities").get_at(40)
chain = []
while deal is not None:
    chain.append(f"{deal.stage} ({deal.changed_by})")
    deal = deal.previous
print("attempts:", attempts, "| notices:", notices)
print("history of deal 40:", " -> ".join(reversed(chain)))
_expect("""
attempts: [1, 2] | notices: ['deal 40 won after proposal']
history of deal 40: qualified (import) -> proposal (ann) -> won (bob)
""")

# -- 05-concurrency.md, line 596 ----------------------------------------
space.close()

print("05_concurrency: OK")
