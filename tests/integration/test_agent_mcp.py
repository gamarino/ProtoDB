"""The MCP server of the agent interface over stdio (spec section 11; Task 7; preflight F30).

Catches: a server that does not expose the four operations as tools of the same names, or whose
``describe`` answer is not the operation's JSON (label, meaning, provenance).  It needs the optional
``agent`` extra (the MCP SDK); without it the test skips with that reason.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys

import pytest

mcp = pytest.importorskip("mcp", reason="the MCP SDK is the optional extra 'agent' (pip install proto_db[agent])")

from mcp import ClientSession, StdioServerParameters  # noqa: E402
from mcp.client.stdio import stdio_client  # noqa: E402

from proto_db import FileStorage, ObjectSpace  # noqa: E402
from proto_db.semantic import semantic  # noqa: E402
from tests.unit.semantic_modules import MAPPING  # noqa: E402

pytestmark = [pytest.mark.timeout(120)]

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _parameters(path: str, *extra: str) -> StdioServerParameters:
    return StdioServerParameters(
        command=sys.executable,
        args=["-m", "proto_db.agent", "--path", path, "--database", "acme",
              "--mapping", f"acme_sales={MAPPING['acme_sales']}", *extra],
        cwd=REPO, env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"))


async def _session_calls(path: str):
    async with stdio_client(_parameters(path)) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            described = await session.call_tool("describe", {"type_name": "acme.sales.Order"})
            missing = await session.call_tool("describe", {"type_name": "acme.ads.Campaign"})
            listed = await session.call_tool("list_entities", {"filter": "acme.core.Company", "limit": 5})
    async with stdio_client(_parameters(path, "--raw")) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            raw = await session.call_tool("list_entities", {})
    return tools, described, missing, listed, raw


def test_mcp_server_lists_the_four_tools_and_answers_describe(tmp_path):
    path = str(tmp_path / "space")
    space = ObjectSpace(FileStorage(path), modules_mapping=MAPPING)
    try:
        db = space.new_database("acme")
        db.run(lambda tr: tr.install_module("acme_sales"))
        company = db.run(lambda tr: semantic(tr).create("acme.core.Company", name="ACME")).name
    finally:
        space.close()
    tools, described, missing, listed, raw = asyncio.run(_session_calls(path))
    assert sorted(tool.name for tool in tools.tools) == ["check_answerable", "describe", "list_entities", "query"]
    assert not described.isError
    order = json.loads(described.content[0].text)
    assert order["type_name"] == "acme.sales.Order" and order["meaning"].startswith("A customer's order")
    number = next(e for e in order["attributes"] if e["name"] == "number")
    assert number["provenance"] == ["acme_sales"] and number["label"] == {"und": "Number"}
    assert missing.isError and "acme.ads.Campaign" in missing.content[0].text
    assert json.loads(listed.content[0].text) == {"entities": [
        {"type_name": "acme.core.Company", "label": {"und": "Company"},
         "meaning": "The company that sells: it has customers, products and orders", "cells": [company]}],
        "next": None}
    assert json.loads(raw.content[0].text) == {"roots": [], "cells": [company], "next": None}


@pytest.mark.parametrize("raw", [False, True])
def test_a_root_of_strings_reaches_the_agent_as_a_type_only(raw):
    from proto_db import MemoryStorage
    from proto_db.agent.mcp_server import build_server
    secret = "123-45-6789"
    space = ObjectSpace(MemoryStorage(), modules_mapping=MAPPING)
    try:
        db = space.new_database("acme")
        db.run(lambda tr: tr.install_module("acme_sales"))

        def build(tr):
            tr.set_root_object("ssns", tr.new_list().append_last(secret))
            tr.set_root_object("scalar", secret)
        db.run(build)
        server = build_server(db, raw=raw)
        for tree in ({"from": ["root", "ssns"]}, {"from": ["root", "ssns"], "aggregate": ["count"]},
                     {"from": ["via", ["root", "scalar"], "org"]}):
            with pytest.raises(Exception) as refused:
                asyncio.run(server.call_tool("query", {"tree": tree}))
            message = str(refused.value)
            assert secret not in message and "the query's 'from'" in message and "a str" in message, message
    finally:
        space.close()


def test_a_corrupt_store_reaches_the_agent_by_its_kind_only_over_mcp(monkeypatch):
    from proto_db.agent.mcp_server import build_server
    from tests.unit.test_agent_operations import _corrupt_store
    marker = "SNTNL-corrupt-123-45-6789"
    first, second, db = _corrupt_store(monkeypatch, marker)
    try:
        for raw in (False, True):
            with pytest.raises(Exception) as refused:
                asyncio.run(build_server(db, raw=raw).call_tool("query", {"tree": {"from": ["via", ["cell", "holder"],
                                                                                            "t"]}}))
            message = str(refused.value)
            assert marker not in message and "SNTNL" not in message and "ProtoCorruptionError" in message, message
    finally:
        second.close()
        first.close()
