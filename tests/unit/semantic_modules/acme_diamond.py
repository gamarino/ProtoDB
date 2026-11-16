"""The registry module ``acme_diamond`` (no dependencies): a diamond of semantic classes, ``D(B, C)`` with
``B(A)`` and ``C(A)`` (R280: multiple semantic bases follow C3)."""
from proto_db import DBObject
from proto_db.registry import module
from proto_db.semantic import Field

diamond = module("acme_diamond")


class A(DBObject):
    __type_name__ = "acme.diamond.A"
    __key__ = ("x",)
    x = Field(int, label="x of A")


class B(A):
    __type_name__ = "acme.diamond.B"
    x = Field(int, label="x of B")


class C(A):
    __type_name__ = "acme.diamond.C"
    x = Field(int, label="x of C")
    y = Field(str)


class D(B, C):
    __type_name__ = "acme.diamond.D"
    z = Field(bool)
