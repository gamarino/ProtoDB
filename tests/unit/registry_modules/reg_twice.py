"""Declares one registry module twice, from two lines: refused (it would drop extensions)."""
from proto_db.registry import module

first = module("reg_twice")
second = module("reg_twice")
