"""``python -m proto_db.agent --path P --database D [--mapping module=import.path ...] [--raw] [--log FILE]``:
the MCP server of the agent interface over stdio (:func:`proto_db.agent.mcp_server.serve`).

The operator's log (what the agent is not told) is discarded unless ``--log FILE`` names a file to append it
to; it never goes to the server's stderr, which the MCP client collects."""
import argparse
import os
import sys


def parse_mapping(items) -> dict:
    """``{module: import path}`` from ``--mapping module=import.path`` items."""
    out = {}
    for item in items or ():
        name, sep, path = item.partition("=")
        if not sep or not name or not path:
            raise SystemExit(f"--mapping takes module=import.path, not {item!r}")
        out[name] = path
    return out


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m proto_db.agent", description=__doc__.splitlines()[0])
    parser.add_argument("--path", required=True, help="the directory of the FileStorage space")
    parser.add_argument("--database", required=True, help="the database to serve")
    parser.add_argument("--mapping", action="append", metavar="MODULE=IMPORT.PATH",
                        help="where the code of a registry module is imported from (repeatable)")
    parser.add_argument("--raw", action="store_true", help="expose only the stored structure (no catalog)")
    parser.add_argument("--log", metavar="FILE", help="append the operator's log to FILE (default: discarded; "
                                                      "never the server's stderr, which the MCP client collects)")
    args = parser.parse_args(argv)
    if os.getcwd() not in sys.path:
        sys.path.insert(0, os.getcwd())     # mapped modules import from the working directory (preflight F41)
    try:
        import mcp  # noqa: F401
    except ImportError:
        raise SystemExit("The MCP server needs the optional extra 'agent': pip install proto_db[agent]") from None
    from .mcp_server import serve
    serve(args.path, args.database, mapping=parse_mapping(args.mapping), raw=args.raw, log=args.log)
    return 0


if __name__ == "__main__":
    sys.exit(main())
