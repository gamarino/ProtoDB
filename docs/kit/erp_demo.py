#!/usr/bin/env python3
"""A 10-minute ERP demo of ProtoDB, in three acts (recruiting kit, R109).

Run from the repository root::

    python docs/kit/erp_demo.py              # runs straight through in a few seconds
    python docs/kit/erp_demo.py --pause      # waits for Enter between acts, for a live talk

Act 1: "as of" and audit questions, with no audit table.
Act 2: a stable paginated report while another user keeps editing.
Act 3: a cluster of three nodes has one node cut off by a network partition (as a crashed machine looks
to its peers), and no acknowledged commit is lost.

The model is the one of the ERP tutorial (docs/source/tutorials/erp.rst): users, persons and sales
opportunities, where an opportunity is a value and each edit is a new version that points at the
previous one.  The cluster act uses the setup of the cluster tutorial (docs/source/tutorials/
cluster.rst): three nodes in one process on a simulated network, sharing an in-memory object store.

The demo checks what it shows.  Each "[check]" line is an assertion that held; a failed check
stops the demo with exit status 1.  It ends with "every check passed" only when all of them held.
The lines that start with "Why (design, not checked here" explain a result with the design's own
statement and name the document that holds it; the demo does not test those statements.
"""
import argparse
import datetime as dt
import os
import shutil
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from proto_db import (Agg, EqualityIndex, F, FileStorage, MutableObject, ObjectSpace,  # noqa: E402
                      RangeIndex, from_collection)
from proto_db.errors import ProtoDBError, ProtoOutcomeUnknownError  # noqa: E402

STAGES = ["lead", "qualified", "proposal", "won", "lost"]
CHECKS = []


def check(condition: bool, what: str) -> None:
    """Assert ``condition`` and print it as a check that held."""
    if not condition:
        raise SystemExit(f"[FAILED] {what}")
    CHECKS.append(what)
    print(f"   [check] {what}")


def say(text: str = "") -> None:
    print(text, flush=True)


def title(text: str, pause: bool) -> None:
    if pause:
        input("\n(press Enter) ")
    say()
    say(text)
    say("=" * len(text))


# ------------------------------------------------------------------------------------------------
# The model
# ------------------------------------------------------------------------------------------------

def build_erp(path: str):
    """A space with users (entities), persons and 600 opportunities (values)."""
    space = ObjectSpace(FileStorage(path))
    db = space.new_database("erp")
    with db.new_transaction() as tr:
        logins = {"ann": "Ann Lee", "bob": "Bob Ruiz", "cy": "Cy Park"}
        users = {login: tr.new_mutable(f"user:{login}", tr.new_object(login=login, name=name))
                 for login, name in logins.items()}
        persons = tr.new_dictionary().add_index(EqualityIndex("by_company", "company"))
        for i in range(100):
            persons = persons.set_at(i, tr.new_object(name=f"person {i}", company=f"company {i % 10}"))
        opportunities = (tr.new_dictionary()
                         .add_index(EqualityIndex("by_stage", "stage"))
                         .add_index(RangeIndex("by_amount", "amount"))
                         .add_index(EqualityIndex("by_owner", "owner")))
        for n in range(600):
            opportunities = opportunities.set_at(n, tr.new_object(
                number=n, title=f"opportunity {n}", stage=STAGES[n % 3], amount=1000 + (n * 37) % 9000,
                owner=users[list(logins)[n % 3]], contact=n % 100, changed_by="import", previous=None))
        tr.set_root_object("persons", persons)
        tr.set_root_object("opportunities", opportunities)
    return space, db


def pipeline(source):
    """Opportunities and total amount per stage, in one query."""
    rows = (from_collection(source)
            .group_by(F.stage, {"stage": Agg.key(), "n": Agg.count(), "amount": Agg.sum(F.amount)})
            .order_by(F.stage).to_list())
    return [(r["stage"], r["n"], r["amount"]) for r in rows]


def advance(db, number, stage, who, **changes):
    """A user moves an opportunity: a new version that remembers the old one and who made it."""
    def work(tr):
        opportunities = tr.get_root_object("opportunities")
        current = opportunities.get_at(number)
        revised = current.with_(previous=current, stage=stage, changed_by=who, **changes)
        tr.set_root_object("opportunities", opportunities.set_at(number, revised))
    db.run(work)


def audit(opportunity):
    """Every version of an opportunity, oldest first: (stage, amount, changed_by)."""
    trail = []
    while opportunity is not None:
        trail.append((opportunity.stage, opportunity.amount, opportunity.changed_by))
        opportunity = opportunity.previous
    return list(reversed(trail))


def as_of(db, moment):
    """The database as it was at ``moment``: the newest committed version not later than it."""
    for entry in db.history():                     # newest first
        if entry.committed_at <= moment:
            return db.snapshot_at(entry)
    raise LookupError("the database did not exist yet")


def now_pipeline(db):
    tr = db.new_transaction()
    try:
        return pipeline(tr.get_root_object("opportunities"))
    finally:
        tr.abort()


# ------------------------------------------------------------------------------------------------
# Act 1: "as of" and audit, with no audit table
# ------------------------------------------------------------------------------------------------

def act_1(db) -> None:
    say("The sales team works the pipeline during the week. Nobody wrote an audit table or a trigger.")
    say()
    printed_monday = now_pipeline(db)              # the report someone printed on Monday morning
    monday = dt.datetime.now(dt.timezone.utc)
    time.sleep(0.02)
    edits = [(3, "qualified", "ann", {}), (3, "proposal", "ann", {"amount": 7500}), (3, "won", "bob", {}),
             (4, "lost", "cy", {})]
    for number, stage, who, changes in edits:
        advance(db, number, stage, who, **changes)
        time.sleep(0.01)                           # distinct commit timestamps on a fast machine
    printed_wednesday = now_pipeline(db)
    wednesday = dt.datetime.now(dt.timezone.utc)
    time.sleep(0.02)
    advance(db, 6, "qualified", "ann")
    with db.new_transaction() as tr:               # a correction to a user entity, committed once
        ann = MutableObject("user:ann")
        ann.set(tr, ann.get(tr).with_(name="Ann Lee-Smith"))

    say("Q: what did the pipeline look like on Monday, and on Wednesday?")
    on_monday = pipeline(as_of(db, monday).get_root_object("opportunities"))
    on_wednesday = pipeline(as_of(db, wednesday).get_root_object("opportunities"))
    say(f"   now:       {now_pipeline(db)}")
    say(f"   Wednesday: {on_wednesday}")
    say(f"   Monday:    {on_monday}")
    check(on_monday == printed_monday, "the 'as of Monday' answer equals the report printed on Monday")
    check(on_wednesday == printed_wednesday, "the 'as of Wednesday' answer equals the report printed on Wednesday")
    check(now_pipeline(db) != on_wednesday, "the data changed after Wednesday, and the old answer did not")
    say("   Same query function, three versions of the data, found by time. No history table was read.")
    say()

    say("Q: who changed opportunity 3, and what did it look like before?")
    tr = db.new_transaction()
    try:
        trail = audit(tr.get_root_object("opportunities").get_at(3))
        for row in trail:
            say(f"   {row}")
        expected = [("lead", 1111, "import"), ("qualified", 1111, "ann"), ("proposal", 7500, "ann"),
                    ("won", 7500, "bob")]
        check(trail == expected, "the version chain is exactly the four edits made, in order, with their authors")
        owner = tr.get_root_object("opportunities").get_at(3).owner.get(tr).name
        then = as_of(db, monday).get_mutable("user:ann").name
        say(f"   owner today: {owner}; the same user as of Monday: {then}")
        check(owner == "Ann Lee-Smith" and then == "Ann Lee",
              "a user entity shows its current name now and its old name 'as of' Monday")
    finally:
        tr.abort()
    say("   Why (design, not checked here; docs/source/design-principles.rst, P1): a commit never overwrites,")
    say("   so every past version is a complete database, and every version of an opportunity points at the")
    say("   one it replaced.")


# ------------------------------------------------------------------------------------------------
# Act 2: a stable paginated report while another user edits
# ------------------------------------------------------------------------------------------------

def act_2(db) -> None:
    say("A report of the open opportunities, most valuable first, 100 rows per page.")
    say("Another user, Cy, works in another thread: before each page is read, Cy wins a deal on that very")
    say("page. A pager reading live data would drop those rows; this one must still show them as open.")
    say()
    tr = db.new_transaction()
    try:
        rows_at_start = [o.number for o in from_collection(tr.get_root_object("opportunities"))
                         .where(F.stage != "won").order_by(F.amount, descending=True).then_by(F.number).to_list()]
        page_size = 100
        pager = tr.pager("opportunities", where=[["stage", "!=", "won"]], order_by=[("amount", True), "number"],
                         page_size=page_size)
        token = pager.token()                      # a portable reference to this snapshot and query
        pages = pager.pages
    finally:
        tr.abort()

    edits_done = []
    want_edit = threading.Semaphore(0)
    stop = threading.Event()

    def cy_works():
        """Cy wins a deal in the middle of the page the report reads next, before it is read."""
        while True:
            want_edit.acquire()
            if stop.is_set():
                return
            k = len(edits_done)
            target = rows_at_start[min(k * page_size + page_size // 2, len(rows_at_start) - 1)]
            advance(db, target, "won", "cy")
            edits_done.append(target)

    cy = threading.Thread(target=cy_works, name="cy")
    cy.start()
    report, page_rows = [], []
    try:
        resumed = db.resume_pager(token)           # as a web request on another day or process would
        for k in range(pages):
            committed = len(edits_done)
            want_edit.release()                    # Cy wins a deal on page k, and commits, first
            deadline = time.monotonic() + 30
            while len(edits_done) == committed and cy.is_alive():
                if time.monotonic() > deadline:
                    raise SystemExit("[FAILED] Cy's edit never committed")
                time.sleep(0.001)
            page_rows.append([o.number for o in resumed.page(k)])
            report += page_rows[-1]
    finally:
        stop.set()
        want_edit.release()
        cy.join()

    tr = db.new_transaction()
    try:
        opportunities = tr.get_root_object("opportunities")
        open_now = from_collection(opportunities).where(F.stage != "won").count()
        won_now = [number for number in edits_done if opportunities.get_at(number).stage == "won"]
    finally:
        tr.abort()
    say(f"   pages read: {pages}; Cy's edits, each committed just before its page was read: {edits_done}")
    say(f"   rows in the report: {len(report)}, distinct: {len(set(report))}; open at the start: "
        f"{len(rows_at_start)}; open now: {open_now}")
    check(len(edits_done) == pages and pages >= 3, "another user committed an edit before every page was read")
    check(won_now == edits_done, "every deal Cy edited is 'won' in the live data")
    check(all(number in page_rows[k] for k, number in enumerate(edits_done)),
          "every deal Cy won before its page was read is still on that page, as open at the report's start")
    check(report == rows_at_start, "the report has exactly the rows open at its start, in order")
    check(len(set(report)) == len(report), "no row repeated, none skipped")
    check(open_now == len(rows_at_start) - len(edits_done), "the edits did happen: fewer deals are open now")
    say("   Why (design, not checked here; docs/source/tutorials/erp.rst, Question 3): the pager is bound")
    say("   to one snapshot, and a snapshot never changes (P1). No cursor held open on a server, no long")
    say("   REPEATABLE READ transaction, no temporary table.")


# ------------------------------------------------------------------------------------------------
# Act 3: a node cut off, no acknowledged commit lost
# ------------------------------------------------------------------------------------------------

class MemoryObjectStore:
    """The five operations of an object store, in memory (as in the cluster tutorial)."""

    def __init__(self):
        from proto_db.storage.cloud import ObjectNotFoundError, PreconditionFailedError
        self._missing, self._precondition = ObjectNotFoundError, PreconditionFailedError
        self._lock, self._objects, self._generation = threading.Lock(), {}, 0

    def _put(self, key, data):
        self._generation += 1
        self._objects[key] = (bytes(data), str(self._generation))
        return self._objects[key][1]

    def put_if_absent(self, key, data):
        with self._lock:
            if key in self._objects:
                raise self._precondition(key)
            return self._put(key, data)

    def put_if_match(self, key, data, etag):
        with self._lock:
            current = self._objects.get(key)
            if current is None or current[1] != etag:
                raise self._precondition(key)
            return self._put(key, data)

    def get(self, key):
        with self._lock:
            if key not in self._objects:
                raise self._missing(key)
            return self._objects[key]

    def list(self, prefix):
        with self._lock:
            return sorted(k for k in self._objects if k.startswith(prefix))

    def delete(self, key):
        with self._lock:
            self._objects.pop(key, None)


def act_3(workdir: str) -> None:
    from proto_db import ClusterCoordinator, Peer, TieredStorage
    from proto_db.cluster.transport import InProcessNetwork

    say("Three peer nodes in one process, on a simulated network, sharing an object store.")
    say("A salesperson on node n0 keeps registering orders. Then n0 is cut off from the others,")
    say("as a crashed machine looks to its peers, in the middle of that work.")
    say()
    store, network = MemoryObjectStore(), InProcessNetwork(seed=1)
    ids = ["n0", "n1", "n2"]
    peers = [Peer(node_id, "127.0.0.1", 0) for node_id in ids]
    # Short timers so the demo runs fast. They are liveness settings only: no safety rule depends on them.
    fast = dict(lease_duration=0.9, request_timeout=0.1, startup_grace=0.0, backoff=(0.001, 0.02),
                persist_interval=0.02, persist_lag=0.3)
    nodes = {i: ClusterCoordinator(i, peers, transport=network.endpoint(), **fast) for i in ids}
    spaces = {}
    try:
        for node in nodes.values():
            node.start()
        for i in ids:
            spaces[i] = ObjectSpace(TieredStorage(os.path.join(workdir, "nodes", i), store, durability="none",
                                                  ship_interval=0.02), coordinator=nodes[i], lock_timeout=5.0)
        deadline = time.monotonic() + 30
        while any(node.stats()["rejoining"] for node in nodes.values()):
            if time.monotonic() > deadline:
                raise SystemExit("[FAILED] the nodes never joined")
            time.sleep(0.005)
        sales = {i: (spaces[i].new_database("sales") if i == "n0" else spaces[i].open_database("sales"))
                 for i in ids}

        def register(db, order):
            def work(tr):
                orders = tr.get_root_object("orders") or tr.new_dictionary()
                tr.set_root_object("orders", orders.set_at(order, True))
            db.run(work, retries=50)

        acknowledged, unknown, failed = [], [], []
        cut = threading.Event()

        def salesperson():
            for k in range(1000):
                order = f"n0-{k}"
                try:
                    register(sales["n0"], order)
                except ProtoDBError as e:          # outcome unknown, a conflict, or a timeout
                    (unknown if isinstance(e, ProtoOutcomeUnknownError) else failed).append(order)
                    if cut.is_set():
                        return                     # n0 is gone: it stops after its first failure
                    continue
                acknowledged.append(order)         # run() returned: the commit is acknowledged
                if len(acknowledged) == 15:
                    cut.set()

        worker = threading.Thread(target=salesperson, name="salesperson")
        worker.start()
        cut.wait(60)
        network.partition({"n1", "n2"})            # n0 is in no group: isolated from every node
        worker.join(60)
        check(not worker.is_alive(), "the salesperson on n0 stopped after n0 was cut off")
        say(f"   on n0 before it was cut off: {len(acknowledged)} orders acknowledged; "
            f"{len(unknown)} ended 'outcome unknown', {len(failed)} failed")

        for k in range(10):                        # the survivors keep working
            register(sales["n1" if k % 2 else "n2"], f"n1n2-{k}")
            acknowledged.append(f"n1n2-{k}")
        say("   n1 and n2 (a majority of three) kept committing: 10 more orders acknowledged")

        # A linearizable read on n2: a barrier commit, then a transaction (cluster tutorial).
        register(sales["n2"], "read-barrier")
        tr = sales["n2"].new_transaction()
        try:
            orders = tr.get_root_object("orders")
            present = [order for order in acknowledged if orders.has(order)]
            in_doubt = [order for order in unknown if orders.has(order)]
        finally:
            tr.abort()
        say(f"   read on n2 after n0 was cut off: {len(present)} of {len(acknowledged)} acknowledged orders present")
        if unknown:
            say(f"   the {len(unknown)} order(s) with an unknown outcome: {len(in_doubt)} present "
                "(either answer is legal; the application must check, as the outcome said)")
        check(len(acknowledged) >= 25, "orders were acknowledged both before and after n0 was cut off")
        check(present == acknowledged, "every acknowledged order survived cutting n0 off by a partition")
        say("   Why (design, not checked here; docs/source/tutorials/cluster.rst, design-principles.rst P5):")
        say("   a commit is acknowledged only once a majority of voters accepted it, and any two majorities")
        say("   share a node. The protocol never lets the next commit be built on a base that misses it,")
        say("   and no timer decides safety.")
        say("   (The exercise in docs/kit/exercise-c1 is a case where an earlier version got that wrong.)")
    finally:
        network.heal()
        for space in spaces.values():
            space.close()
        for node in nodes.values():
            node.stop()
        network.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--pause", action="store_true", help="wait for Enter between acts")
    parser.add_argument("--workdir", help="directory for the demo's files (default: a temporary directory, removed)")
    args = parser.parse_args(argv)
    own = args.workdir is None
    workdir = tempfile.mkdtemp(prefix="erp-demo-") if own else args.workdir
    os.makedirs(workdir, exist_ok=True)
    try:
        say("ProtoDB: an ERP demo in three acts")
        space, db = build_erp(os.path.join(workdir, "erp-space"))
        try:
            title("Act 1: 'as of' and audit questions, with no audit table", args.pause)
            act_1(db)
            title("Act 2: a stable paginated report while another user edits", args.pause)
            act_2(db)
        finally:
            space.close()
        title("Act 3: a node cut off by a partition, no acknowledged commit lost", args.pause)
        act_3(workdir)
    finally:
        if own:
            shutil.rmtree(workdir, ignore_errors=True)
    say()
    say(f"every check passed ({len(CHECKS)} checks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
