"""The agent interface of the semantic model (spec section 11; Task 7).

- :mod:`proto_db.agent.operations` holds the neutral operations (``list_entities``, ``describe``,
  ``check_answerable``, ``query``) and their JSON form; it imports no third-party package;
- :mod:`proto_db.agent.mcp_server` serves them as MCP tools of the same names (``serve``); it needs the
  optional extra ``agent`` (the MCP SDK), imported only when the server starts;
- ``python -m proto_db.agent --path P --database D [--mapping module=import.path ...] [--raw]`` runs the
  server over stdio.

Agents never write: the interface reads one version of the database at a time.
"""
from .operations import check_answerable, describe, from_json, list_entities, query, to_json

__all__ = ["list_entities", "describe", "check_answerable", "query", "to_json", "from_json"]
