"""The application type of the conformance vectors: an atom class with a declared, language-neutral
name (D57), written with a string type tag instead of a built-in alias.  It was persisted before
under the name ``conformance.LegacyInvoice`` (``__type_aliases__``): the legacy database vector holds
a record and a reference spelled with that name."""
from proto_db.core.dbobject import DBObject


class Invoice(DBObject):
    """An application record; its persisted type name is ``conformance.Invoice``."""

    __type_name__ = "conformance.Invoice"
    __type_aliases__ = ("conformance.LegacyInvoice",)
