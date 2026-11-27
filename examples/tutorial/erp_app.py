"""The running application of the proto_db tutorials: a small modelled ERP (a model only, no ERP features).

Chapter 3 of the complete tutorial (``docs/tutorial/03-modelling.md``) builds this model step by step and
explains every choice; the later chapters, and the queries tutorial, start from :func:`seed`, which builds the
same model at a size where indexes matter. The data follows ``docs/source/tutorials/erp.rst``, so the topic
tutorial and the tutorials agree on every figure they share.

The model:

* a **user** is an entity: a ``MutableObject`` cell ``user:<login>`` whose record (``login``, ``name``) may be
  corrected, and every record that holds the cell's handle sees the correction;
* a **group** is a named set of logins: the root ``groups``, a ``Dictionary`` from group name to a ``Set``;
* a **person** (a contact at a customer) is a plain record in the root ``persons``, a ``Dictionary`` keyed by
  number, indexed by company;
* an **opportunity** is a value: each change is a new version that points at the one it replaced
  (``previous``) and says who made it (``changed_by``). The root ``opportunities`` holds the current version of
  each, keyed by number, with indexes by stage, by amount and by owner.
"""
from proto_db import Agg, EqualityIndex, F, MutableObject, RangeIndex, from_collection

USERS = {"ann": "Ann Lee", "bob": "Bob Ruiz", "cy": "Cy Park"}
GROUPS = {"sales": ("ann", "bob"), "support": ("cy",), "managers": ("ann",)}
STAGES = ("lead", "qualified", "proposal", "won", "lost")
LOGINS = tuple(USERS)


def user(login: str) -> MutableObject:
    """The handle of a user's cell: it names the entity, whatever its record holds now."""
    return MutableObject(f"user:{login}")


def opportunity_record(tr, number: int, users: dict):
    """Opportunity ``number`` as first imported (the data of the ERP topic tutorial)."""
    return tr.new_object(number=number, title=f"opportunity {number}", stage=STAGES[number % 3],
                         amount=1000 + (number * 37) % 9000, owner=users[LOGINS[number % 3]],
                         contact=number % 200, changed_by="import", previous=None)


def seed(db, *, opportunities: int = 1000, persons: int = 200, indexes: bool = True) -> None:
    """Build the ERP model in the empty database ``db``, in one commit."""
    with db.new_transaction() as tr:
        users = {login: tr.new_mutable(f"user:{login}", tr.new_object(login=login, name=name))
                 for login, name in USERS.items()}
        groups = tr.new_dictionary()
        for name, members in GROUPS.items():
            groups = groups.set_at(name, tr.new_set().union(members))
        people = tr.new_dictionary()
        deals = tr.new_dictionary()
        if indexes:
            people = people.add_index(EqualityIndex("by_company", "company"))
            deals = (deals.add_index(EqualityIndex("by_stage", "stage"))
                          .add_index(RangeIndex("by_amount", "amount"))
                          .add_index(EqualityIndex("by_owner", "owner")))
        for i in range(persons):
            people = people.set_at(i, tr.new_object(name=f"person {i}", company=f"company {i % 20}"))
        for n in range(opportunities):
            deals = deals.set_at(n, opportunity_record(tr, n, users))
        tr.set_root_object("groups", groups)
        tr.set_root_object("persons", people)
        tr.set_root_object("opportunities", deals)


def pipeline(source) -> list:
    """Opportunities and total amount per stage: ``[(stage, count, amount), ...]`` in stage-name order."""
    rows = (from_collection(source)
            .group_by(F.stage, {"stage": Agg.key(), "n": Agg.count(), "amount": Agg.sum(F.amount)})
            .order_by(F.stage).to_list())
    return [(r["stage"], r["n"], r["amount"]) for r in rows]


def advance(db, number: int, stage: str, who: str, **changes) -> None:
    """Commit the next version of opportunity ``number``: a new record that remembers the one it replaces."""
    def work(tr):
        deals = tr.get_root_object("opportunities")
        current = deals.get_at(number)
        revised = current.with_(previous=current, stage=stage, changed_by=who, **changes)
        tr.set_root_object("opportunities", deals.set_at(number, revised))
    db.run(work)
