"""The agent experiment harness's planted agents (Task 12; preflight F36): they validate the harness, never a model.

``python tests/support/planted_agent.py --truth FILE --behaviour B --mcp-config FILE [--touch-mcp] PROMPT``

``FILE`` is the ground truth the harness wrote with ``--truth-out`` (by question text); the question is read from
the prompt's ``Question:`` line.  Behaviours:

- ``echo``: answers the ground truth (every answer correct);
- ``wrong``: answers each answerable question with a near miss: a number one unit up in its last digit
  (``1980.00`` -> ``1980.01``), a date one day later, a cell name with its last character changed; and declares
  the questions that cannot be answered so (only the answerable ones are incorrect);
- ``answerable``: answers ``0`` to each question that cannot be answered, and the ground truth to the rest (only
  the unanswerable ones are incorrect);
- ``crash``: prints the right answer, then exits 3 (never correct);
- ``malformed``: declares the questions that cannot be answered as ``NOT ANSWERABLE.`` (a malformed declaration),
  and answers the ground truth to the rest;
- ``hang``: starts a child in a new session that holds the agent's stdout until something is written to the FIFO
  ``--release`` names, then waits forever itself (no sleep): the harness's timeout and its bounded wait for the
  output after the kill are what end the run.

Every behaviour first prints its working directory and how many entries it holds (``cwd: PATH holds N``).

With ``--touch-mcp`` the agent first starts the server of the configuration (the MCP SDK's stdio client), calls
``list_entities`` and prints ``mode seen: catalog`` or ``mode seen: raw`` from what it answered, which shows that
the harness's configuration starts the server, in the mode of the run.
"""
import argparse
import asyncio
import datetime
import json
import os
import subprocess
import sys
import threading
from decimal import Decimal

NOT_ANSWERABLE = "not_answerable"


def _question(prompt: str) -> str:
    for line in prompt.splitlines():
        if line.startswith("Question: "):
            return line[len("Question: "):]
    raise SystemExit("the prompt has no Question: line")


def _near_miss(kind: str, expected: str) -> str:
    if kind == "number":
        value = Decimal(expected)
        return str(value + Decimal(1).scaleb(value.as_tuple().exponent))
    if kind == "date":
        return (datetime.date.fromisoformat(expected) + datetime.timedelta(days=1)).isoformat()
    return expected[:-1] + ("0" if expected[-1] != "0" else "1")


def _touch(config_path: str) -> str:
    """Start the configured server, call list_entities and say which mode answered."""
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
    with open(config_path, encoding="utf-8") as f:
        [server] = json.load(f)["mcpServers"].values()
    parameters = StdioServerParameters(command=server["command"], args=server["args"],
                                       env={**os.environ, **server.get("env", {})})

    async def run() -> dict:
        async with stdio_client(parameters) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool("list_entities", {"limit": 1})
                if result.isError:
                    raise SystemExit(f"list_entities failed: {result.content[0].text}")
                return json.loads(result.content[0].text)
    listed = asyncio.run(run())
    print(f"list_entities -> {json.dumps(listed)[:200]}")
    return "catalog" if "entities" in listed else "raw"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--truth", required=True)
    parser.add_argument("--behaviour", required=True,
                        choices=("echo", "wrong", "answerable", "crash", "malformed", "hang"))
    parser.add_argument("--release", help="the FIFO that ends the hang behaviour's child")
    parser.add_argument("--mcp-config", required=True)
    parser.add_argument("--touch-mcp", action="store_true")
    parser.add_argument("prompt")
    args = parser.parse_args(argv)
    print(f"cwd: {os.getcwd()} holds {len(os.listdir())}", flush=True)
    if args.behaviour == "hang":
        subprocess.Popen([sys.executable, "-c", "import sys; open(sys.argv[1]).read()", args.release],
                         start_new_session=True)
        threading.Event().wait()
    if args.touch_mcp:
        print(f"mode seen: {_touch(args.mcp_config)}")
    with open(args.truth, encoding="utf-8") as f:
        truth = json.load(f)[_question(args.prompt)]
    unanswerable = truth["expected"] == NOT_ANSWERABLE
    if args.behaviour == "wrong" and not unanswerable:
        answer = _near_miss(truth["answer"], truth["expected"])
    elif args.behaviour == "answerable" and unanswerable:
        answer = "0"
    elif args.behaviour == "malformed" and unanswerable:
        answer = "NOT ANSWERABLE."
    else:
        answer = "NOT ANSWERABLE" if unanswerable else truth["expected"]
    print(f"I looked it up.\nANSWER: {answer}")
    return 3 if args.behaviour == "crash" else 0


if __name__ == "__main__":
    sys.exit(main())
