# 5. Concurrency: preconditions and intentions

Several users work the ERP at once: Ann qualifies a lead while Bob closes a deal and Cy corrects a contact.
Each of them runs a transaction on a snapshot, and the commits arrive in some order. This chapter answers the
question every concurrent database has to answer: **which of those commits may all succeed, and which must
be refused?** It shows the obvious answer, where it fails, and the model `proto_db` 2.0.0rc5 uses instead,
the **precondition model**.

The rules behind every result here are stated in [the concurrency model](../source/concurrency-model.rst);
the [concurrency tutorial](../source/tutorials/concurrency.rst) has more patterns.

## What a transaction sees

A transaction reads one snapshot, its *base*, taken when it starts. A commit by someone else never changes
what an open transaction reads, and nothing a transaction writes is visible to anyone else before it commits.
Readers never block writers, and writers never block readers.

```python
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
```

Output:

```text
the report sees deal 3 as lead
a new transaction sees it as won
```

A snapshot alone is not enough. If Ann and Bob both read deal 5 and each writes a new version of it, both
worked on a consistent snapshot, and yet publishing both would lose one of the two changes. What the commit
must check is the subject of the rest of the chapter.

To look at one decision at a time, the helper below runs two transactions on the **same** snapshot, commits
the first, then tries the second, and reports what happened to it:

```python
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
```

Output:

```text
Ann and Bob move the same deal: conflict on KEY 5
```

The second commit was refused, and nothing of it was published. Its retry (below) sees Ann's version first.

## The obvious approach: one version number per root

The classic answer is optimistic locking on what a transaction reads: remember the version of every root you
read, and refuse the commit if any of them changed. It is simple, it is correct, and it is what `proto_db` did
until 2.0.0rc4 (the unit of conflict was the named root). Written by hand, it is a version number per root
that every writer reads and increments:

```python
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
```

Output:

```text
whole root, different deals: conflict on CELL 'version:opportunities'
```

Ann and Bob changed **different** opportunities, and one of them was refused anyway. That is the first
failure: **a collection conflicts as a whole**. The opportunities are one root, so every writer of any
opportunity conflicts with every other, and a busy collection serializes all of its writers. Retrying makes
it correct, not cheap. Four users, each moving 25 deals of their own, in four threads:

```python
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
```

Sample output (the first count varies from run to run, the second is always 100):

```text
100 commits: 128 attempts with a version per root, 100 with the precondition model
```

Every attempt beyond 100 is work redone because of a change to a deal it never looked at. The second figure
is the model this chapter builds up to: the same 100 moves, each committed at its first attempt.

The other two failures of the whole-root approach showed up in practice, and are why 2.0.0rc5 replaced it:

* **A merge was bolted on.** To let writers of different keys coexist, rc4 merged a collection written
  *without being read* by replaying its operations, but only under a condition the caller could not see. The
  same call merged or replaced depending on how the value had been built.
* **Blind writes lost updates.** A root or a cell written without being read first was compared with nothing:
  the last writer won, silently.

## The model: preconditions and intentions

Every operation of a transaction records two things, as data:

* a **precondition**: what it saw of its base, which must still hold when it commits ("deal 5 is still the
  version I read");
* an **intention**: the change it wants to make ("deal 5 becomes this new version").

At commit, every precondition is checked against the **current** state, the newest committed version. If
they all hold, the intentions are **reapplied on the current state**, and that is what is published. If any
fails, the whole transaction fails with `ProtoConflictError`, which names the precondition that failed, and
nothing is published.

That is why the two writers of different deals both commit:

```python
print("different deals:", race(move(8, "won"), move(9, "lost")))
deals = db.new_transaction().get_root_object("opportunities")
print("deal 8:", deals.get_at(8).stage, "| deal 9:", deals.get_at(9).stage)
```

Output:

```text
different deals: both committed
deal 8: won | deal 9: lost
```

The second transaction's base did not hold deal 8 as won. It did not have to: its precondition was about key
9 only ("key 9 still holds the version I read"), which held, and its intention ("key 9 becomes this version")
was applied to the dictionary as the first commit left it. The published dictionary has both changes. Nothing
was merged by guessing: an intention says exactly which key gets which value.

The guarantee is that committed transactions are **serializable in commit order**: each one read what a
serial execution of all of them, in that order, would have shown it. The test suite checks this with threads
of randomly generated transactions, re-run serially and compared
(`tests/concurrency/test_serializability.py`).

## The conflict units

A precondition protects a **unit**: the smallest part of the state the operation depends on. Which unit an
operation takes depends on the operation and on the value of the root:

| You do | The unit | Two transactions conflict when |
|---|---|---|
| read or write a record, number or string root | the root (`ROOT`) | either changed it |
| read or write a mutable cell | the cell (`CELL`) | either changed it, read or not |
| `get_at(k)`, `has(k)`, `set_at(k)`, `remove_at(k)` on a dictionary root | the key (`KEY`) | both touch key `k` and one changes it |
| `add(e)` to a set | none | never: every order leaves `e` present |
| `remove(e)` from a set | the element (`ELEMENT`) | `e` changed concurrently |
| `add` to a counted set | none | never: counts compose |
| `append_last` to a list | none | never: appends land in commit order |
| `set_at(i)` of an existing list position | the position (`POSITION`) | both change position `i` |
| `insert_at`, `remove_at`, `append_first` on a list | the whole list (`WHOLE`) | anything changed the list |
| a query answered by an equality index | the group of the key (`INDEX_GROUP`) | an element entered or left that group |
| a query answered by a range index | the key range (`RANGE`) | a key inside the range changed |
| `len`, iteration, `keys()`, `values()`, a scan | the whole collection (`WHOLE`) | anything in it changed |

Two rules shape the table. **Reading is declaring**: whatever a transaction read is taken as necessary for
its logic, and must be unchanged at commit, so do not read what the logic does not need. And **coarser when
in doubt**: an operation whose fine unit is not proven safe takes a larger one, in the end the whole
collection. An extra conflict costs a retry; a result that no serial order produces is never acceptable.

The races below run each line of the table on the ERP:

```python
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
    ("amount >= 9900; a small deal changes", move(13, "won"), after_query(largest, 22, "won")),
    ("amount >= 9900; a large deal changes", move(242, "won"), after_query(largest, 23, "won")),
    ("title scan; any deal changes", move(14, "won"), after_query(by_title, 24, "won")),
]
for label, first, second in cases:
    print(f"{label:38} {race(first, second)}")
```

Output:

```text
record root, two attributes            conflict on ROOT 'company'
cell, two corrections                  conflict on CELL 'user:cy'
set, two adds                          both committed
set, two removes                       conflict on ELEMENT 'bob'
counted set, two adds                  both committed
list, two appends                      both committed
list, append and insert                conflict on WHOLE
lead lookup; a proposal is lost        both committed
lead lookup; a lead is qualified       conflict on INDEX_GROUP 'lead' of index 'by_stage'
amount >= 9900; a small deal changes   both committed
amount >= 9900; a large deal changes   conflict on RANGE of index 'by_amount'
title scan; any deal changes           conflict on WHOLE
```

Reading the results:

* A **record** is one unit: two transactions that change different attributes of the company conflict.
  There is no per-attribute merge; if two attributes change independently, they belong in two roots or cells.
* A **set** and a **counted set** let concurrent adders all commit, because every serial order gives the same
  result. Two removes of the same member conflict: each declared "bob is still here".
* A **list** appends in commit order; an insertion shifts every later position, so it takes the whole list.
* A **query** declares what its plan read. The equality lookup of `"lead"` conflicts only when a deal enters
  or leaves the `lead` group (deal 12 left it); the range query only when a key inside its range changes (deal
  242 is one of the largest); a scan conflicts with anything. Indexes narrow conflicts as well as reads,
  and `explain()` tells you which plan, and so which unit, a query will use.

### One level only

Fine units apply to the collection that is the **direct value of a root**. A collection inside another
collection, or inside a record, has no units of its own: changing it is writing the unit that contains it.
`erp_app` keeps the groups as sets inside one dictionary, so two adds to the same group are two writes of
the same key:

```python
def add_member(login):
    def work(tr):
        groups = tr.get_root_object("groups")
        tr.set_root_object("groups", groups.set_at("sales", groups.get_at("sales").add(login)))
    return work

print("nested set, two adds:", race(add_member("dee"), add_member("eve")))
print("set root, two adds:  ", race(on_root("group:sales", lambda s: s.add("fay")),
                                     on_root("group:sales", lambda s: s.add("gus"))))
```

Output:

```text
nested set, two adds: conflict on KEY 'sales'
set root, two adds:   both committed
```

When concurrent writers are expected inside one part of the model, give that part a root of its own.

## Writes you did not read are checked too

A write is a precondition as well. Writing a root or a cell without reading it first still compares it with
the value it had in the transaction's base. The third failure of the whole-root approach, the silent last
writer, does not exist:

```python
first, second = db.new_transaction(), db.new_transaction()
first.set_root_object("company", first.new_object(name="ACME", currency="USD"))    # neither read the root
second.set_root_object("company", second.new_object(name="ACME", currency="EUR"))
first.commit()
try:
    second.commit()
except ProtoConflictError as error:
    print("blind write refused:", error)
print("currency:", db.new_transaction().get_root_object("company").currency)
```

Output:

```text
blind write refused: Root 'company' of database 'erp' was changed by a concurrent commit (unit ROOT)
currency: USD
```

A collection built separately (`tr.new_dictionary()`) and assigned to an existing root is such a blind write
too: it **replaces** the root, compared against the base value. Fine-grained intentions are recorded only on
the collection that `get_root_object` returned (or a version derived from it), assigned back to the same root
of the same transaction.

## Conflicts that protect you: checking a reference

Some conflicts are the point. Assigning an opportunity to Cy is valid only while Cy's user exists. If the
assigning transaction reads Cy's cell, that read is a precondition, and a concurrent removal of the user
becomes a conflict instead of a dangling owner:

```python
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
```

Output:

```text
unchecked: both committed
checked:   conflict on CELL 'user:cy'
```

The unchecked assignment committed, and deal 30 now names a user that no longer exists. The checked one
conflicted on Cy's cell, and its retry would find no user and refuse.

## `Database.run` retries

A conflict is not an error in your program. It says "someone else changed what you relied on; redo the work
on the new state". `Database.run(fn)` does that: it calls `fn(tr)` in a fresh transaction, commits, and on a
conflict calls `fn` again on a new snapshot, with a bounded exponential back-off with jitter, up to `retries`
times (10 by default). Here Bob's move of deal 40 conflicts with Ann's, which commits while Bob's first
attempt runs:

```python
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
```

Output:

```text
attempts: [1, 2] | notices: ['deal 40 won after proposal']
history of deal 40: qualified (import) -> proposal (ann) -> won (bob)
```

Bob's second attempt read Ann's version, so his new version points at it: no update was lost, and the history
holds both. Three rules for the function given to `run`:

* It may run **several times**, so external effects (an e-mail, an API call, a message) go **after** `run`
  returns, from what it returned.
* It does its reading **inside**, through the `tr` it receives. A value read before `run` belongs to an
  older snapshot, and nothing checks it at commit.
* `run` re-raises the conflict after `retries` attempts. A unit that many writers hit needs a design where
  they touch different units (a key each, a set, a counted set), not more retries.

`ProtoOutcomeUnknownError` (a commit that may or may not have been applied, on cloud and cluster storage) is
never retried: a retry could apply the work twice (chapter 10).

## The two deliberate gaps

The model checks what a transaction read and wrote, and nothing else. Two things follow:

* **Reading from a `Snapshot` records nothing.** A summary computed from `db.snapshot_at(...)` is not refused
  when its source changes; that is the deliberate way out, the analogue of `READ COMMITTED`.
* **A read-modify-write split across two transactions is not seen.** If one transaction reads and a later one
  writes a value computed from that read, the second commit has nothing to compare with. Read and write in the
  same transaction, which is what `run` makes natural.

```python
space.close()
```

## Limits

These follow from the model; none of them can produce a result that no serial order produces:

* **Hot units.** Of N concurrent transactions on the same unit, one commits per attempt and the others redo
  their work. Commutative operations (`Set.add`, counted sets, `List.append_last`) do not have this limit.
* **Traversals.** A transaction that iterates a collection and then writes conflicts with any concurrent change
  of it; a range query, with any change inside its range.
* **No fairness.** A long transaction over a heavily written unit can exhaust its retries.
* **One ordering point per space.** Validation and publication of one space run one at a time; the commit
  rate does not grow with cores. Building and saving the work does run in parallel.

Depth: [the concurrency tutorial](../source/tutorials/concurrency.rst) ("check and add", unique indexes,
identifiers without a shared counter, lock cells), [the concurrency model](../source/concurrency-model.rst)
(every rule, the commit protocol, the known limits), and
[queries under concurrency](queries/06-queries-under-concurrency.md).

Next: [6. Time](06-time.md)
