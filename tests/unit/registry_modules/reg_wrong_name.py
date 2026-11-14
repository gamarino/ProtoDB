"""Declares a registry module under a name that is not the one it is imported for."""
from proto_db.registry import module

module("reg_not_the_mapped_name")
