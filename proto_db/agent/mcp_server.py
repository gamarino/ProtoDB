"""The MCP server of the agent interface (spec section 11; Task 7; ruling R266).

:func:`serve` exposes the four operations of :mod:`proto_db.agent.operations` as MCP tools of the same
names, over stdio, on one database of a :class:`~proto_db.storage.file.FileStorage` space.  Each tool
answers the operation's JSON as text; an error answers the tool error with the operation's message.  With
``raw=True`` every tool runs in raw mode (the agent experiment's control group: the stored structure only).

The MCP SDK is the optional extra ``agent`` (``pip install proto_db[agent]``); it is imported when the
server is built, never when :mod:`proto_db.agent` is imported.
"""
from __future__ import annotations

import json
from collections.abc import Mapping

from . import operations

__all__ = ["serve", "build_server"]

_INSTRUCTIONS = (
    "A proto_db database that describes itself.  Start with list_entities and describe to learn its entities, "
    "their attributes and roles, what each means and which module defined it.  check_answerable says whether "
    "the catalog names every term of a question; a term it does not match cannot be answered from this "
    "database (suggestions are only suggestions).  query runs a query tree on one version and answers its "
    "rows, explain(), the version read and the catalog terms used; personal and secret attributes are never "
    "returned.  Agents never write.")

_QUERY_HELP = (
    "Run a query tree (spec section 11): {\"from\": [\"via\", [\"cell\", <cell name>], <role>, ...] or "
    "[\"root\", <name>], \"of_type\": <type name>? (subtypes included), \"where\": <expression>?, "
    "\"select\": [<attribute path>, ...]?, \"aggregate\": [\"sum\", <expression>] | [\"count\"]?, "
    "\"as_of\": <the 'ref' of a version an earlier answer gave>?}.  Expressions are lists: [\"attr\", step, ...], "
    "[\"const\", value], [\"==\" | \"!=\" | \"<\" | \"<=\" | \">\" | \">=\", a, b], [\"+\" | \"-\" | \"*\", a, b], "
    "[\"and\", ...], [\"or\", ...], [\"not\", a].  A constant that JSON cannot spell is a one-key object: "
    "{\"decimal\": \"10.50\"}, {\"date\": \"2026-06-01\"}, {\"datetime\": ...}, {\"time\": ...}, {\"cell\": name}.  "
    "Answers decimals as strings and dates in ISO 8601.  'terms' lists the catalog elements the query used, each "
    "with its own 'sensitivity' (public, internal, personal or secret); one whose values were kept from you adds "
    "\"withheld\": true and 'withheld_because': {\"reads\": {type_name, name, sensitivity}} or "
    "{\"python_only\": true}.")


def _dump(value) -> str:
    return json.dumps(value, ensure_ascii=False)


def build_server(db, *, raw: bool = False):
    """The MCP server (a ``FastMCP``) whose four tools run the operations on ``db``."""
    from mcp.server.fastmcp import FastMCP

    server = FastMCP("proto_db" + (" (raw)" if raw else ""), instructions=_INSTRUCTIONS, log_level="WARNING")

    @server.tool(name="list_entities", description="Where to start: the entity types of the database, with label "
                 "and meaning, and per type the names of the cells holding its entities (a query starts at "
                 "[\"cell\", name]).  An optional filter keeps the types whose name, label, synonyms or meaning "
                 "contain it.  At most 'limit' cell names per answer; pass the answer's 'next' as 'after' for the "
                 "next page (it reads the same version).")
    def list_entities(filter: str | None = None, limit: int = operations.LIST_LIMIT, after: str | None = None) -> str:
        return _dump(operations.list_entities(db, filter, limit=limit, after=after, raw=raw))

    @server.tool(name="describe", description="The composed entity type_name: its attributes, roles, intents, "
                 "constraints, the module that defined or modified each element (provenance), certification "
                 "and sensitivity.")
    def describe(type_name: str) -> str:
        return _dump(operations.describe(db, type_name, raw=raw))

    @server.tool(name="check_answerable", description="Whether the catalog names every term of a question: per "
                 "term its exact matches (case-folded, on name, label or synonym), the unmatched terms, and "
                 "suggestions by substring for those (only suggestions, never matches).")
    def check_answerable(terms: list[str]) -> str:
        return _dump(operations.check_answerable(db, terms, raw=raw))

    @server.tool(name="query", description=_QUERY_HELP)
    def query(tree: dict) -> str:
        return _dump(operations.query(db, operations.from_json(tree), raw=raw))

    return server


def serve(path: str, database: str, *, mapping: Mapping[str, str] | None = None, raw: bool = False) -> None:
    """Serve the database ``database`` of the file space at ``path`` over stdio until the client closes it.
    ``mapping`` is the space's ``modules_mapping`` (registry module name -> import path of its code)."""
    from ..storage.file import FileStorage
    from ..transactions import ObjectSpace

    space = ObjectSpace(FileStorage(path), modules_mapping=dict(mapping) if mapping else None)
    try:
        server = build_server(space.open_database(database), raw=raw)
        server.run("stdio")
    finally:
        space.close()
