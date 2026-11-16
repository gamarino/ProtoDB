"""The agent experiment of the semantic model: correct answers of an agent with and without the catalog (spec
sections 3 and 15; Task 12; preflight F19, F33, F36).

::

    python -m benchmarks.semantic.agent_experiment --agent-cmd "<template>" --out FILE --model NAME
        [--workdir DIR] [--modes catalog,raw] [--questions q01,q02,...] [--timeout SECONDS]
    python -m benchmarks.semantic.agent_experiment --verify FILE

Each question of ``benchmarks/semantic/questions.json`` is asked once per mode of the MCP server
(``python -m proto_db.agent``) over the demo's database:

- ``catalog``: the agent interface as the demo shows it (the catalog: meanings, provenance, sensitivity);
- ``raw``: the control group (``--raw``): the stored structure only.  The control agent still sees the stored
  type names and attribute names (``acme.sales.Customer``, ``credit_limit``), but none of their meaning, labels,
  synonyms, units or computed attributes (spec section 11).

The database is built by the demo (:func:`docs.kit.semantic.semantic_demo.build_database`, steps 3 and 4) in
``--workdir`` (default ``FILE`` without its extension, plus ``.work``), or reused when it is already there.  The
ground truth is computed before any agent runs, in this process, by running each question's named query
(``docs/kit/semantic/queries.py``, ``QUESTIONS``) in catalog mode and its declared post-step (``POST``); the
space is closed before the agents start.

The agent command is the operator's choice: a template with ``{mcp_config}`` (the path of an MCP client
configuration, the ``mcpServers`` form, that starts the server in the run's mode) and ``{question}`` (the
prompt: the question and the answer format).  The template is split as a shell would split it (``shlex``), then
each placeholder is replaced inside its word, so the question is always one argument and never reaches a shell.
The agent runs in an empty directory under the workdir, with a timeout; it should be restricted to the server's
four tools.  The harness never sees the model: ``--model`` names it in the results.

Scoring never judges by similarity.  The answer is the last stdout line that starts with ``ANSWER:``:

- a number must parse as a plain decimal (digits, an optional sign and decimal point) and equal the ground truth
  after ``Decimal`` parsing (``1980`` equals ``1980.00``; ``1,980.00``, ``$1980`` and ``1980 units`` are
  unparsable);
- a date must be ``YYYY-MM-DD`` and equal; a cell name must be equal, character for character;
- a question the database cannot answer is correct only when the agent declares ``NOT ANSWERABLE``; any other
  answer to it is incorrect, and the declaration is incorrect for a question that can be answered;
- a run whose agent exited with an error or timed out is never correct, whatever it printed.

The run is self-reporting.  Every run's transcript (the command, the prompt, stdout, stderr, the exit code, the
time) is written to ``FILE`` without its extension, plus ``.transcripts/``, as it ends; the results (``FILE``)
name the model, the commit (and whether the tree was dirty), the database version read, every run's verdict and
a per-mode summary.  Before the results are written, every run is re-scored from its transcript on disk and the
summary recounted; a run the transcript contradicts, a missing or duplicated run, or a summary that does not add
up is written with the results (``"verified": false`` and ``"problems"``) and the command exits 1.  ``--verify
FILE`` (:func:`verify`) repeats that check on a written report.
"""
from __future__ import annotations

import argparse
import contextlib
import datetime as _dt
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import sysconfig
import time
from decimal import Decimal, InvalidOperation

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO not in sys.path:
    sys.path.insert(0, REPO)        # the kit's modules import as docs.kit.semantic.* (preflight F41)

QUESTIONS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "questions.json")
MODES = ("catalog", "raw")
ANSWER_KINDS = ("number", "date", "cell")
NOT_ANSWERABLE = "not_answerable"
#: What the agent answers when the database does not hold what a question needs (case does not matter).
DECLARATION = "NOT ANSWERABLE"
SERVER = "proto_db"

_NUMBER = re.compile(r"[+-]?\d+(\.\d+)?")
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


class ExperimentError(Exception):
    """The experiment cannot run as asked: a malformed question file, template or ground truth."""


# -- the questions and their ground truth ------------------------------------------------------------------------

def _queries():
    from docs.kit.semantic import queries
    return queries


def load_questions(path: str = QUESTIONS_FILE) -> list:
    """The questions of ``path``, checked: unique ids and texts; per question either ``"not_answerable"`` or an
    answer kind of :data:`ANSWER_KINDS` and a ground truth naming a query of ``queries.QUESTIONS`` and, if any,
    a post-step of ``queries.POST``."""
    with open(path, encoding="utf-8") as f:
        questions = json.load(f)["questions"]
    queries = _queries()
    ids, texts = set(), set()
    for q in questions:
        where = q.get("id")
        if not isinstance(where, str) or where in ids or not q.get("question") or q["question"] in texts:
            raise ExperimentError(f"question {where!r}: a missing or duplicated id or text")
        ids.add(where)
        texts.add(q["question"])
        truth = q.get("ground_truth")
        if truth == NOT_ANSWERABLE:
            if "answer" in q:
                raise ExperimentError(f"question {where}: a question that cannot be answered has no answer kind")
            continue
        if q.get("answer") not in ANSWER_KINDS:
            raise ExperimentError(f"question {where}: answer kind {q.get('answer')!r} is not one of {ANSWER_KINDS}")
        if not isinstance(truth, dict) or truth.get("query") not in queries.QUESTIONS:
            raise ExperimentError(f"question {where}: the ground truth names no query of queries.QUESTIONS")
        if truth.get("post") is not None and truth["post"] not in queries.POST:
            raise ExperimentError(f"question {where}: post-step {truth['post']!r} is not one of queries.POST")
    return questions


def build_database(workdir: str) -> str:
    """The path of the demo's space in ``workdir``, built by the demo (steps 3 and 4) unless it is already there.
    The build's own output goes to ``workdir/build.log``."""
    from proto_db import FileStorage, ObjectSpace
    path = os.path.join(workdir, "space")
    if os.path.isdir(path):
        space = ObjectSpace(FileStorage(path), modules_mapping=_queries().MAPPING)
        try:
            space.open_database(_queries().DATABASE)
            return path
        finally:
            space.close()
    os.makedirs(workdir, exist_ok=True)
    from docs.kit.semantic import semantic_demo
    with open(os.path.join(workdir, "build.log"), "w", encoding="utf-8") as log, contextlib.redirect_stdout(log):
        semantic_demo.build_database(workdir)
    return path


def starting_cells(db) -> dict:
    """The cells the ground-truth queries start from, found through the agent interface: the company
    (``list_entities``) and each product by name (a query from the company)."""
    from proto_db.agent import operations
    queries = _queries()
    cells = {"company": queries.company_cell(operations.list_entities(db, queries.COMPANY))}
    for name in queries.PRODUCT_NAMES:
        found = operations.query(db, operations.from_json(queries.product_named(cells["company"], name)))
        cells[name] = queries.widget_cell(found)
    return cells


def _answer_of(kind: str, value) -> str:
    """A ground-truth value in the results' JSON form, checked against its kind."""
    if kind == "number":
        number = Decimal(str(value))
        if not number.is_finite():
            raise ExperimentError(f"a ground truth of kind number is {value!r}")
        return str(number)
    if not isinstance(value, str) or (kind == "date" and not _DATE.fullmatch(value)) or \
            (kind == "cell" and "#" not in value):
        raise ExperimentError(f"a ground truth of kind {kind} is {value!r}")
    return value


def ground_truth(space_path: str, questions: list) -> dict:
    """Per question id: ``{"answer": kind, "expected": the answer in JSON form, "query": the tree run, "post"}``
    (``expected`` is ``"not_answerable"`` and the rest ``None`` for a question the database cannot answer),
    computed in catalog mode on the current version of the space at ``space_path``, which is closed after."""
    from proto_db import FileStorage, ObjectSpace
    from proto_db.agent import operations
    queries = _queries()
    space = ObjectSpace(FileStorage(space_path), modules_mapping=queries.MAPPING)
    try:
        db = space.open_database(queries.DATABASE)
        cells = starting_cells(db)
        out = {}
        for q in questions:
            truth = q["ground_truth"]
            if truth == NOT_ANSWERABLE:
                out[q["id"]] = {"answer": None, "expected": NOT_ANSWERABLE, "query": None, "post": None}
                continue
            tree = queries.QUESTIONS[truth["query"]](cells)
            rows = operations.to_json(operations.query(db, operations.from_json(tree)))["rows"]
            post = truth.get("post")
            if post is not None:
                value = queries.POST[post](rows)
            elif len(rows) == 1 and len(rows[0]) == 1 and set(rows[0]) <= {"sum", "count"}:
                [value] = rows[0].values()
            else:
                raise ExperimentError(f"question {q['id']}: a query without a post-step must be an aggregate")
            out[q["id"]] = {"answer": q["answer"], "expected": _answer_of(q["answer"], value), "query": tree,
                            "post": post}
        return out
    finally:
        space.close()


def database_version(space_path: str) -> int:
    """The number of the current version of the demo's database (the version the agents read: they never write)."""
    from proto_db import FileStorage, ObjectSpace
    from proto_db.semantic import semantic
    queries = _queries()
    space = ObjectSpace(FileStorage(space_path), modules_mapping=queries.MAPPING)
    try:
        tr = space.open_database(queries.DATABASE).new_transaction()
        try:
            return semantic(tr).version()["number"]
        finally:
            tr.abort()
    finally:
        space.close()


# -- one run of the agent ----------------------------------------------------------------------------------------

def prompt(question: str) -> str:
    """What the agent is asked: the question, the rule on sources and the answer format (the same in both modes)."""
    return (f"You are answering one question about a company's business database.  Use only the tools of the MCP "
            f"server '{SERVER}'; do not use files, the web or any other source.\n"
            f"Question: {question}\n"
            "When you are done, end your reply with exactly one line of the form\n"
            "ANSWER: <answer>\n"
            "where <answer> is a plain number (digits, an optional minus sign and decimal point; no units, currency "
            "signs or thousands separators), a date as YYYY-MM-DD, the cell name of an entity exactly as the "
            f"database gives it (when the question asks which entity), or {DECLARATION} when the database does not "
            "hold what the question needs.")


def agent_argv(template: str, mcp_config: str, question: str) -> list:
    """The agent's command line: ``template`` split as a shell would, each placeholder replaced in its word."""
    if "{mcp_config}" not in template or "{question}" not in template:
        raise ExperimentError("the agent command must contain {mcp_config} and {question}")
    return [word.replace("{mcp_config}", mcp_config).replace("{question}", question)
            for word in shlex.split(template)]


def _server_command() -> list:
    """This interpreter, in this process's GIL mode."""
    free_threaded = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    return [sys.executable, *(["-X", "gil=1"] if free_threaded and sys._is_gil_enabled() else [])]


def write_mcp_config(directory: str, space_path: str, mode: str) -> str:
    """Write the MCP client configuration of ``mode`` in ``directory`` and return its path: one server,
    ``proto_db``, which is ``python -m proto_db.agent`` over the demo's database (``--raw`` in raw mode), with
    this repository first on its ``PYTHONPATH``."""
    if mode not in MODES:
        raise ExperimentError(f"mode {mode!r} is not one of {MODES}")
    queries = _queries()
    command, *flags = _server_command()
    mapping = [arg for name, target in queries.MAPPING.items() for arg in ("--mapping", f"{name}={target}")]
    args = [*flags, "-m", "proto_db.agent", "--path", space_path, "--database", queries.DATABASE, *mapping]
    if mode == "raw":
        args.append("--raw")
    paths = [REPO, *filter(None, os.environ.get("PYTHONPATH", "").split(os.pathsep))]
    config = {"mcpServers": {SERVER: {"type": "stdio", "command": command, "args": args,
                                      "env": {"PYTHONPATH": os.pathsep.join(paths)}}}}
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, f"mcp-{mode}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)
    return path


def run_agent(argv: list, cwd: str, timeout: float) -> dict:
    """Run the agent; on a timeout its whole process group is killed (an agent starts the server as a child)."""
    started = time.monotonic()
    group = {"start_new_session": True} if hasattr(os, "killpg") else {}
    try:
        process = subprocess.Popen(argv, cwd=cwd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True, encoding="utf-8", errors="replace", **group)
    except OSError as error:
        return {"stdout": "", "stderr": f"the agent could not start: {error}", "exit_code": None,
                "timed_out": False, "seconds": round(time.monotonic() - started, 3)}
    try:
        stdout, stderr = process.communicate(timeout=timeout)
        timed_out = False
    except subprocess.TimeoutExpired:
        if group:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
        stdout, stderr = process.communicate()
        timed_out = True
    return {"stdout": stdout, "stderr": stderr, "exit_code": None if timed_out else process.returncode,
            "timed_out": timed_out, "seconds": round(time.monotonic() - started, 3)}


# -- scoring -----------------------------------------------------------------------------------------------------

def parse_answer(stdout: str):
    """The text after the last line of ``stdout`` that starts with ``ANSWER:``, stripped; ``None`` without one."""
    found = None
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("ANSWER:"):
            found = line[len("ANSWER:"):].strip()
    return found


def score(kind, expected: str, answer, *, exit_code, timed_out: bool) -> tuple:
    """``(correct, verdict)`` of one answer.  ``kind`` is ``None`` for a question that cannot be answered
    (``expected`` is then ``"not_answerable"``)."""
    if timed_out:
        return False, "timed out"
    if exit_code != 0:
        return False, f"agent failed (exit {exit_code})"
    if answer is None:
        return False, "no answer line"
    declared = answer.upper() == DECLARATION
    if expected == NOT_ANSWERABLE:
        return (True, "correct") if declared else (False, "answered a question the database cannot answer")
    if declared:
        return False, "declared not answerable"
    if kind == "number":
        if not _NUMBER.fullmatch(answer):
            return False, "unparsable"
        try:
            equal = Decimal(answer) == Decimal(expected)
        except InvalidOperation:
            return False, "unparsable"
    elif kind == "date":
        if not _DATE.fullmatch(answer):
            return False, "unparsable"
        try:
            equal = _dt.date.fromisoformat(answer) == _dt.date.fromisoformat(expected)
        except ValueError:
            return False, "unparsable"
    else:
        equal = answer == expected
    return (True, "correct") if equal else (False, "wrong")


def summarize(runs: list, modes) -> dict:
    """Per mode: runs, correct answers, failed runs (an error exit or a timeout), and the questions the database
    cannot answer with how many of them the agent declared so."""
    out = {}
    for mode in modes:
        mine = [run for run in runs if run["mode"] == mode]
        unanswerable = [run for run in mine if run["expected"] == NOT_ANSWERABLE]
        out[mode] = {"runs": len(mine), "correct": sum(run["correct"] for run in mine),
                     "failed_runs": sum(run["timed_out"] or run["exit_code"] != 0 for run in mine),
                     "not_answerable": len(unanswerable),
                     "not_answerable_detected": sum(run["correct"] for run in unanswerable)}
    return out


# -- the experiment ----------------------------------------------------------------------------------------------

def _commit() -> dict:
    def git(*args):
        done = subprocess.run(["git", "-C", REPO, *args], capture_output=True, text=True)
        return done.stdout.strip() if done.returncode == 0 else None
    try:
        sha = git("rev-parse", "HEAD")
        dirty = git("status", "--porcelain", "--untracked-files=no")
    except OSError:
        sha, dirty = None, None
    return {"sha": sha or "unknown", "dirty": None if dirty is None else dirty != ""}


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


def _base(out: str) -> str:
    return os.path.splitext(os.path.abspath(out))[0]


def run_experiment(template: str, out: str, *, model: str, workdir: str | None = None, modes=MODES,
                   ids=None, timeout: float = 600, truth_out: str | None = None) -> dict:
    """Run every question (or those of ``ids``) in every mode of ``modes``, check the results against the
    transcripts, write them to ``out`` (``"verified"`` says whether they passed) and return them; a run that does
    not verify raises :class:`ExperimentError` after it is written.  ``truth_out`` writes the ground truth, by
    question text, before any agent runs: only for the harness's own planted agents, never for a real one."""
    questions = load_questions()
    if ids is not None:
        unknown = set(ids) - {q["id"] for q in questions}
        if unknown:
            raise ExperimentError(f"unknown question ids: {sorted(unknown)}")
        questions = [q for q in questions if q["id"] in set(ids)]
    agent_argv(template, "", "")                        # the template is checked before anything runs
    workdir = os.path.abspath(workdir or _base(out) + ".work")
    started = _now()
    space = build_database(workdir)
    truth = ground_truth(space, questions)
    version = database_version(space)
    if truth_out is not None:
        with open(truth_out, "w", encoding="utf-8") as f:
            json.dump({q["question"]: {"id": q["id"], **truth[q["id"]]} for q in questions}, f, indent=2)
    transcripts = _base(out) + ".transcripts"
    cwd = os.path.join(workdir, "agent-cwd")
    os.makedirs(transcripts, exist_ok=True)
    os.makedirs(cwd, exist_ok=True)
    runs = []
    for mode in modes:
        config = write_mcp_config(workdir, space, mode)
        for q in questions:
            text = prompt(q["question"])
            argv = agent_argv(template, config, text)
            done = run_agent(argv, cwd, timeout)
            name = os.path.join(os.path.basename(transcripts), f"{mode}-{q['id']}.json")
            with open(os.path.join(os.path.dirname(transcripts), name), "w", encoding="utf-8") as f:
                json.dump({"id": q["id"], "mode": mode, "argv": argv, "prompt": text, **done}, f, indent=2)
            expected = truth[q["id"]]
            answer = parse_answer(done["stdout"])
            correct, verdict = score(expected["answer"], expected["expected"], answer,
                                     exit_code=done["exit_code"], timed_out=done["timed_out"])
            runs.append({"id": q["id"], "mode": mode, "question": q["question"], "answer_kind": expected["answer"],
                         "expected": expected["expected"], "answer": answer, "correct": correct,
                         "verdict": verdict, "exit_code": done["exit_code"], "timed_out": done["timed_out"],
                         "seconds": done["seconds"], "transcript": name})
            print(f"{mode:7} {q['id']}: {verdict} (answer {answer!r}, expected {expected['expected']!r}, "
                  f"{done['seconds']:.1f} s)", flush=True)
    report = {"model": model, "commit": _commit(), "agent_command": template, "python": sys.version,
              "started": started, "finished": _now(), "database": {"workdir": workdir, "version": version},
              "modes": list(modes), "question_ids": [q["id"] for q in questions],
              "ground_truth": {q["id"]: {"query": truth[q["id"]]["query"], "post": truth[q["id"]]["post"]}
                               for q in questions},
              "runs": runs, "summary": summarize(runs, modes)}
    problems = _check(report, os.path.dirname(os.path.abspath(out)))
    report["verified"] = not problems
    if problems:
        report["problems"] = problems
    with open(out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    if problems:
        raise ExperimentError(f"the run does not verify ({out}): " + "; ".join(problems))
    return report


def _check(report: dict, base: str) -> list:
    problems = []
    expected_runs = {(qid, mode) for qid in report["question_ids"] for mode in report["modes"]}
    seen = [(run["id"], run["mode"]) for run in report["runs"]]
    if len(seen) != len(set(seen)):
        problems.append("a run is recorded twice")
    for qid, mode in sorted(expected_runs - set(seen)):
        problems.append(f"run {qid} ({mode}) is missing")
    for run in report["runs"]:
        where = f"run {run['id']} ({run['mode']})"
        try:
            with open(os.path.join(base, run["transcript"]), encoding="utf-8") as f:
                recorded = json.load(f)
        except (OSError, ValueError) as error:
            problems.append(f"{where}: its transcript cannot be read ({error})")
            continue
        answer = parse_answer(recorded["stdout"])
        verdict = score(run["answer_kind"], run["expected"], answer, exit_code=recorded["exit_code"],
                        timed_out=recorded["timed_out"])
        if (recorded["id"], recorded["mode"]) != (run["id"], run["mode"]) or answer != run["answer"] \
                or verdict != (run["correct"], run["verdict"]):
            problems.append(f"{where}: the transcript gives answer {answer!r}, {verdict}; the report says "
                            f"{run['answer']!r}, {(run['correct'], run['verdict'])}")
        if run["correct"] and (recorded["exit_code"] != 0 or recorded["timed_out"]):
            problems.append(f"{where}: counted correct although the agent failed")
    if summarize(report["runs"], report["modes"]) != report["summary"]:
        problems.append("the summary does not add up to the runs")
    return problems


def verify(out: str) -> list:
    """The problems of the written report ``out`` against its transcripts (an empty list when it verifies)."""
    with open(out, encoding="utf-8") as f:
        report = json.load(f)
    return _check(report, os.path.dirname(os.path.abspath(out)))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m benchmarks.semantic.agent_experiment",
                                     description=__doc__.splitlines()[0])
    parser.add_argument("--agent-cmd", help="the agent command template, with {mcp_config} and {question}")
    parser.add_argument("--out", help="the results file (JSON)")
    parser.add_argument("--model", help="the model the agent runs (named in the results)")
    parser.add_argument("--workdir", help="where the demo's database is built or reused")
    parser.add_argument("--modes", default=",".join(MODES), help="catalog, raw or both (default)")
    parser.add_argument("--questions", help="comma-separated question ids (default: all)")
    parser.add_argument("--timeout", type=float, default=600, help="seconds per agent run (default 600)")
    parser.add_argument("--truth-out", help="write the ground truth here first: for the planted agents only")
    parser.add_argument("--verify", metavar="FILE", help="only verify a written report against its transcripts")
    args = parser.parse_args(argv)
    if args.verify:
        problems = verify(args.verify)
        for problem in problems:
            print(f"[FAILED] {problem}")
        print("the report verifies" if not problems else f"{len(problems)} problems")
        return 1 if problems else 0
    if not (args.agent_cmd and args.out and args.model):
        parser.error("--agent-cmd, --out and --model are required")
    try:
        import mcp  # noqa: F401
    except ImportError:
        raise SystemExit("the MCP server needs the optional extra 'agent': pip install proto_db[agent]") from None
    modes = tuple(mode.strip() for mode in args.modes.split(","))
    if not modes or set(modes) - set(MODES):
        parser.error(f"--modes takes {', '.join(MODES)}")
    ids = [i.strip() for i in args.questions.split(",")] if args.questions else None
    try:
        report = run_experiment(args.agent_cmd, args.out, model=args.model, workdir=args.workdir, modes=modes,
                                ids=ids, timeout=args.timeout, truth_out=args.truth_out)
    except ExperimentError as error:
        print(f"[FAILED] {error}")
        return 1
    print(f"model {report['model']}, commit {report['commit']['sha']}"
          + (" (dirty tree)" if report["commit"]["dirty"] else "")
          + f", database version {report['database']['version']}")
    for mode, counts in report["summary"].items():
        print(f"{mode}: {counts['correct']} of {counts['runs']} correct; {counts['failed_runs']} failed runs; "
              f"not answerable detected {counts['not_answerable_detected']} of {counts['not_answerable']}")
    print(f"verified against the transcripts; results in {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
