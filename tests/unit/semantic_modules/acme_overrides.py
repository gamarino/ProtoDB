"""The registry module ``acme_overrides`` (depends on ``acme_sales``), for the N2 check that composing the
semantic catalog runs no application code (spec R37, P-C): a ``Field`` subclass, an ``Invariant`` subclass and
an ``IntentSpec`` subclass that override the methods the composition reads (``element``, ``_fields``,
``reads``) and record each call in :data:`CALLS` with whether the calling thread held the space root
(``Transaction._holding_root_lock()``).  Their overrides also change the result (``meaning`` becomes
``"OVERRIDDEN"``), so a catalog composed through them would show it."""
from proto_db.registry import module
from proto_db.semantic import Extension, Field, intent
from proto_db.semantic.descriptors import IntentSpec, Invariant

overrides = module("acme_overrides", depends=("acme_sales",))

#: ``(method, root held)`` for every call of an override.
CALLS: list = []


def _note(method: str) -> None:
    from proto_db.transactions import Transaction
    CALLS.append((method, Transaction._holding_root_lock()))


class NotedField(Field):
    def element(self, name=None):
        _note("Field.element")
        return {**super().element(name), "meaning": "OVERRIDDEN"}

    def _fields(self):
        _note("Field._fields")
        return super()._fields()


class NotedInvariant(Invariant):
    __slots__ = ()

    def element(self, scope):
        _note("Invariant.element")
        return {**super().element(scope), "meaning": "OVERRIDDEN"}

    def reads(self):
        _note("Invariant.reads")
        return super().reads()


class NotedSpec(IntentSpec):
    __slots__ = ()

    def element(self, name):
        _note("IntentSpec.element")
        return {**super().element(name), "meaning": "OVERRIDDEN"}


class OrderNotes(Extension):
    __extends__ = "acme.sales.Order"
    __constraints__ = [NotedInvariant("number >= 0", name="number_not_negative", meaning="An order number is not negative")]
    note = NotedField(str, meaning="A free note on the order")

    @intent(meaning="Mark the order as reviewed")
    def review(self):
        return None


OrderNotes.__dict__["review"].__proto_intent__.__class__ = NotedSpec
