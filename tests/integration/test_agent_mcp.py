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
from tests.unit.semantic_modules import MAPPING  # noqa: E402

pytestmark = [pytest.mark.timeout(120)]

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


async def _session_calls(path: str):
    parameters = StdioServerParameters(
        command=sys.executable,
        args=["-m", "proto_db.agent", "--path", path, "--database", "acme",
              "--mapping", f"acme_sales={MAPPING['acme_sales']}"],
        cwd=REPO, env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"))
    async with stdio_client(parameters) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            described = await session.call_tool("describe", {"type_name": "acme.sales.Order"})
            missing = await session.call_tool("describe", {"type_name": "acme.ads.Campaign"})
            return tools, described, missing


def test_mcp_server_lists_the_four_tools_and_answers_describe(tmp_path):
    path = str(tmp_path / "space")
    space = ObjectSpace(FileStorage(path), modules_mapping=MAPPING)
    try:
        db = space.new_database("acme")
        db.run(lambda tr: tr.install_module("acme_sales"))
    finally:
        space.close()
    tools, described, missing = asyncio.run(_session_calls(path))
    assert sorted(tool.name for tool in tools.tools) == ["check_answerable", "describe", "list_entities", "query"]
    assert not described.isError
    order = json.loads(described.content[0].text)
    assert order["type_name"] == "acme.sales.Order" and order["meaning"].startswith("A customer's order")
    number = next(e for e in order["attributes"] if e["name"] == "number")
    assert number["provenance"] == ["acme_sales"] and number["label"] == {"und": "Number"}
    assert missing.isError and "acme.ads.Campaign" in missing.content[0].text
