"""The agent experiment's questions and harness (spec section 15; Task 12; preflight F19, F33, F36).

Catches: a question set that drifts from the spec (fewer than 24 questions, fewer than four that cannot be
answered, the advertising question missing, a ground truth naming a query or a post-step that does not exist); a
ground truth that does not ask what its question says (every answerable ground truth is recomputed here from the
demo's own data, independently of the query engine); a scorer that judges by similarity (a value one unit off in
its last digit, a cell name one character off, a number followed by a unit, an "answerable" answer to a question
the database cannot answer, and an answer of a crashed or timed-out agent are all incorrect); an agent command
whose question is split by the shell; and a verification that accepts a report its transcripts contradict.

The harness itself runs the planted agents of ``tests/support/planted_agent.py``: one that echoes the ground
truth (every answer correct), one that answers each answerable question with a near miss and one that answers a
number to each question the database cannot answer (both scored incorrect exactly there, preflight F36), and one
that prints the right answer and then exits with an error (never correct).  Without the MCP SDK they do not touch
the server; with it (``test_the_echo_agent_through_the_mcp_server_in_both_modes``) the echo agent first calls
``list_entities`` through the configuration the harness wrote, so each mode's server is shown to start and to be
the mode it claims.
"""
from __future__ import annotations

import json
import os
import re
import shlex
import sys
from datetime import date
from decimal import Decimal

import pytest

from benchmarks.semantic import agent_experiment as experiment
from docs.kit.semantic import queries
from docs.kit.semantic import semantic_demo as demo

pytestmark = [pytest.mark.timeout(300)]

PLANTED = os.path.join(experiment.REPO, "tests", "support", "planted_agent.py")


def _template(behaviour: str, truth: str, *, touch: bool = False) -> str:
    """A planted agent's command template (the question and the configuration are the harness's)."""
    words = [sys.executable, PLANTED, "--truth", truth, "--behaviour", behaviour, "--mcp-config", "{mcp_config}"]
    if touch:
        words.append("--touch-mcp")
    return " ".join(shlex.quote(word) for word in words) + " {question}"


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    """The demo's database, built once (steps 3 and 4 of the demo, no agent steps), and its ground truth."""
    workdir = str(tmp_path_factory.mktemp("agent-experiment"))
    space = experiment.build_database(workdir)
    questions = experiment.load_questions()
    truth = experiment.ground_truth(space, questions)
    return {"workdir": workdir, "space": space, "questions": questions, "truth": truth, "refused": _refused(space),
            "agents": str(tmp_path_factory.mktemp("agents"))}


def _refused(space_path: str) -> set:
    """The demo's step-4 orders that are not in the database: the one the credit rule refused.  Which of
    Fabrikam's two orders it is depends on the replay order, so the recomputation reads it."""
    from proto_db import FileStorage, ObjectSpace
    from proto_db.agent import operations
    space = ObjectSpace(FileStorage(space_path), modules_mapping=queries.MAPPING)
    try:
        db = space.open_database(queries.DATABASE)
        company = queries.company_cell(operations.list_entities(db, queries.COMPANY))
        numbers = {row["values"]["number"] for row in operations.query(
            db, {"from": ["via", ["cell", company], "orders"], "select": ["number"]})["rows"]}
    finally:
        space.close()
    assert {n for n, *_ in demo.ORDERS} <= numbers
    return {n for n, *_ in demo.CONCURRENT} - numbers


# -- the questions -------------------------------------------------------------------------------------------------

def test_the_question_set_follows_the_spec():
    questions = experiment.load_questions()
    assert len(questions) == 24
    assert len({q["id"] for q in questions}) == 24 and len({q["question"] for q in questions}) == 24
    unanswerable = [q for q in questions if q["ground_truth"] == "not_answerable"]
    assert len(unanswerable) >= 4
    assert any("advertising" in q["question"] for q in unanswerable)
    for q in questions:
        if q["ground_truth"] == "not_answerable":
            assert "answer" not in q
            continue
        assert q["answer"] in experiment.ANSWER_KINDS
        assert q["ground_truth"]["query"] in queries.QUESTIONS
        assert q["ground_truth"].get("post") is None or q["ground_truth"]["post"] in queries.POST


def test_the_published_page_lists_every_question_with_its_ground_truth():
    with open(os.path.join(experiment.REPO, "docs", "kit", "semantic", "agent-experiment.md"), encoding="utf-8") as f:
        page = f.read()
    for q in experiment.load_questions():
        truth = q["ground_truth"]
        if truth == "not_answerable":
            cell = "not answerable"
        else:
            cell = f"`{truth['query']}`" + (f", `{truth['post']}`" if truth.get("post") else "")
        assert f"| {q['id']} | {q['question']} | {q.get('answer', '—')} | {cell} |" in page, q["id"]


def test_load_questions_refuses_a_ground_truth_it_cannot_compute(tmp_path):
    with open(experiment.QUESTIONS_FILE, encoding="utf-8") as f:
        good = json.load(f)
    cases = {
        "unknown query": lambda d: d["questions"][0]["ground_truth"].update(query="no_such_query"),
        "unknown post-step": lambda d: d["questions"][0]["ground_truth"].update(post="median(price)"),
        "unknown answer kind": lambda d: d["questions"][0].update(answer="text"),
        "duplicate id": lambda d: d["questions"][1].update(id=d["questions"][0]["id"]),
    }
    for what, plant in cases.items():
        data = json.loads(json.dumps(good))
        plant(data)
        path = tmp_path / f"{what.replace(' ', '-')}.json"
        path.write_text(json.dumps(data), encoding="utf-8")
        with pytest.raises(experiment.ExperimentError):
            experiment.load_questions(str(path))


def _independent_answers(refused: set) -> tuple:
    """Every answerable question's answer recomputed from the demo's own data (its ORDERS, CONCURRENT orders,
    customers and products), never through the query engine, given the step-4 orders ``refused``.  A cell is
    named by what identifies it."""
    lines = [(number, day, demo.PRODUCTS[p][0], Decimal(q), Decimal(price))
             for number, _, day, order_lines in demo.ORDERS for p, q, price in order_lines]
    lines += [(number, date(2026, 7, 1), "widget", Decimal(1), Decimal(price))
              for number, _, price in demo.CONCURRENT if number not in refused]
    totals = {}
    for number, day, _, q, price in lines:
        totals[number] = (day, totals.get(number, (day, Decimal(0)))[1] + q * price)
    limits = {name: Decimal("100000") for name in ("Northwind", "Contoso")}
    limits.update((name, Decimal(limit)) for name, limit in demo.NEW_CUSTOMERS)
    committed = dict.fromkeys(limits, Decimal(0))
    for number, customer, _, order_lines in demo.ORDERS:
        committed[("Northwind", "Contoso")[customer]] += sum(Decimal(q) * Decimal(p) for _, q, p in order_lines)
    for number, customer, price in demo.CONCURRENT:
        if number not in refused:
            committed[customer] += Decimal(price)
    left = {name: limits[name] - committed[name] for name in limits}

    def of(product, first=date.min, last=date.max):
        return [(q, price) for _, day, name, q, price in lines if name == product and first <= day <= last]
    return {
        "q01": sum(q for q, _ in of("widget", date(2026, 6, 1), date(2026, 6, 30))),
        "q02": ("customer", min(left, key=left.get)),
        "q03": max(left.values()),
        "q04": Decimal(len(totals)),
        "q05": sum(total for _, total in totals.values()),
        "q06": Decimal(len(limits)),
        "q07": sum(limits.values()),
        "q08": Decimal("12.00"),                    # demo step 3 changes the widget's list price to 12.00
        "q09": max(price for _, price in of("widget")),
        "q10": sum(q for q, _ in of("gizmo")),
        "q11": sum(q * p for q, p in of("gadget", date(2026, 1, 1), date(2026, 3, 31))),
        "q12": sum(q * p for q, p in of("widget")),
        "q13": Decimal(len(of("gizmo"))),
        "q14": Decimal(sum(1 for day, _ in totals.values() if date(2026, 7, 1) <= day <= date(2026, 12, 31))),
        "q15": totals[7][1],
        "q16": next(day for number, _, day, _ in demo.ORDERS if number == 15),
        "q17": Decimal(len(lines)),
        "q18": Decimal(sum(1 for name in limits if committed[name] * 2 > limits[name])),
        "q19": max(day for day, _ in totals.values()),
        "q20": sum(total for day, total in totals.values() if date(2026, 7, 1) <= day <= date(2026, 12, 31)),
    }, committed


def test_the_demo_refused_exactly_one_of_fabrikams_orders(world):
    """The recomputation's one input read from the database: one of Fabrikam's two orders was refused."""
    fabrikam = {number for number, customer, _ in demo.CONCURRENT if customer == "Fabrikam"}
    assert len(world["refused"]) == 1 and world["refused"] <= fabrikam


def test_every_ground_truth_answers_its_question(world):
    """The ground truth (catalog-mode query plus its declared post-step) equals the answer recomputed from
    the demo's data: the trees ask what the questions say."""
    expected, _ = _independent_answers(world["refused"])
    truth = world["truth"]
    answerable = [q["id"] for q in world["questions"] if q["ground_truth"] != "not_answerable"]
    assert sorted(answerable) == sorted(expected)
    for qid in answerable:
        found = truth[qid]
        if found["answer"] == "number":
            assert Decimal(found["expected"]) == expected[qid], qid
        elif found["answer"] == "date":
            assert date.fromisoformat(found["expected"]) == expected[qid], qid
        else:
            assert found["answer"] == "cell" and found["expected"].startswith("acme.sales.Customer#"), qid
    for q in world["questions"]:
        if q["ground_truth"] == "not_answerable":
            assert truth[q["id"]] == {"answer": None, "expected": experiment.NOT_ANSWERABLE, "query": None,
                                      "post": None}


@pytest.mark.parametrize("refused", [{101}, {102}])
def test_no_ground_truth_is_zero_or_shared_with_another_question(refused):
    """Review M7: a degenerate or confused answer must not score.  Whichever of Fabrikam's orders the demo
    refuses, every answerable question's truth is distinct from every other's, and no number is zero."""
    expected, _ = _independent_answers(refused)
    numbers = [value for value in expected.values() if isinstance(value, Decimal)]
    assert len(set(numbers)) == len(numbers) and 0 not in numbers, sorted(numbers)


def test_the_built_database_s_truths_are_distinct_and_non_zero(world):
    numbers = [Decimal(t["expected"]) for t in world["truth"].values() if t["answer"] == "number"]
    assert len(numbers) == 17 and len(set(numbers)) == len(numbers) and 0 not in numbers, sorted(numbers)


def test_the_customer_closest_to_its_limit_is_named_by_its_cell(world):
    """q02's cell is the customer the independent recomputation names (read back by its key, the name)."""
    from proto_db import FileStorage, MutableObject, ObjectSpace
    from proto_db.semantic import semantic
    expected, _ = _independent_answers(world["refused"])
    _, name = expected["q02"]
    space = ObjectSpace(FileStorage(world["space"]), modules_mapping=queries.MAPPING)
    try:
        tr = space.open_database(queries.DATABASE).new_transaction()
        try:
            assert semantic(tr).get(MutableObject(world["truth"]["q02"]["expected"])).name == name
        finally:
            tr.abort()
    finally:
        space.close()


def test_a_post_step_refuses_a_tie():
    rows = [{"entity": {"cell": f"c{i}"}, "values": {"credit_limit": "10", "committed": "5"}} for i in range(2)]
    with pytest.raises(ValueError):
        queries.POST["argmin(credit_limit - committed)"](rows)
    with pytest.raises(ValueError):
        queries.POST["only(total)"]([{"values": {"total": "1"}}, {"values": {"total": "2"}}])


# -- scoring -------------------------------------------------------------------------------------------------------

def test_the_answer_is_the_last_answer_line():
    assert experiment.parse_answer("thinking\nANSWER: 3\nmore\n  ANSWER:  12.50  \n") == "12.50"
    assert experiment.parse_answer("The answer is 12.50") is None
    assert experiment.parse_answer("") is None


@pytest.mark.parametrize("kind, expected, answer, correct, verdict", [
    ("number", "1980.00", "1980.00", True, "correct"),
    ("number", "1980.00", "1980", True, "correct"),                     # equal after Decimal parsing
    ("number", "1980.00", "1980.01", False, "wrong"),
    ("number", "8", "8 units", False, "unparsable"),
    ("number", "8", "$8", False, "unparsable"),
    ("number", "1980.00", "1,980.00", False, "unparsable"),
    ("number", "8", "NaN", False, "unparsable"),
    ("number", "8", "NOT ANSWERABLE", False, "declared not answerable"),
    ("cell", "acme.sales.Customer#ab12", "acme.sales.Customer#ab12", True, "correct"),
    ("cell", "acme.sales.Customer#ab12", "acme.sales.Customer#ab13", False, "wrong"),
    ("cell", "acme.sales.Customer#ab12", "acme.sales.customer#ab12", False, "wrong"),
    ("date", "2026-03-20", "2026-03-20", True, "correct"),
    ("date", "2026-03-20", "2026-03-21", False, "wrong"),
    ("date", "2026-03-20", "March 20, 2026", False, "unparsable"),
    (None, "not_answerable", "NOT ANSWERABLE", True, "correct"),
    (None, "not_answerable", "not answerable", True, "correct"),
    (None, "not_answerable", "0", False, "answered a question the database cannot answer"),
    ("number", "8", None, False, "no answer line"),
    (None, "not_answerable", "NOT ANSWERABLE.", False, "malformed declaration"),
    (None, "not_answerable", "**Not answerable**", False, "malformed declaration"),
    ("number", "8", "Not answerable: no stock data", False, "malformed declaration"),
    (None, "not_answerable", "N/A", False, "answered a question the database cannot answer"),
])
def test_score_never_judges_by_similarity(kind, expected, answer, correct, verdict):
    assert experiment.score(kind, expected, answer, exit_code=0, timed_out=False) == (correct, verdict)


def test_a_failed_or_timed_out_agent_is_never_correct():
    assert experiment.score("number", "8", "8", exit_code=3, timed_out=False) == (False, "agent failed (exit 3)")
    assert experiment.score("number", "8", "8", exit_code=None, timed_out=True) == (False, "timed out")
    assert experiment.score(None, "not_answerable", "NOT ANSWERABLE", exit_code=1, timed_out=False)[0] is False


def test_the_agent_command_keeps_the_question_one_argument():
    argv = experiment.agent_argv("agent --mcp-config={mcp_config} -p {question}", "/x/mcp.json",
                                 "What's the \"total\"; rm -rf ~ $(x)?")
    assert argv == ["agent", "--mcp-config=/x/mcp.json", "-p", "What's the \"total\"; rm -rf ~ $(x)?"]
    with pytest.raises(experiment.ExperimentError):
        experiment.agent_argv("agent {question}", "/x/mcp.json", "q")
    with pytest.raises(experiment.ExperimentError):
        experiment.agent_argv("agent --mcp-config {mcp_config}", "/x/mcp.json", "q")


def test_the_mcp_configuration_serves_the_database_in_its_mode(tmp_path):
    for mode in experiment.MODES:
        path = experiment.write_mcp_config(str(tmp_path), "/data/space", mode)
        with open(path, encoding="utf-8") as f:
            config = json.load(f)
        assert "raw" not in path and "catalog" not in path      # the path names no arm (review M3)
        [(name, server)] = config["mcpServers"].items()
        assert name == "proto_db" and server["command"] == sys.executable
        args = server["args"]
        assert args[args.index("-m") + 1] == "proto_db.agent" and args[args.index("--path") + 1] == "/data/space"
        assert args[args.index("--database") + 1] == queries.DATABASE
        assert ("--raw" in args) == (mode == "raw")
        assert server["env"]["PYTHONPATH"].split(os.pathsep)[0] == experiment.REPO


def test_the_prompt_asks_for_one_answer_line_and_allows_not_answerable():
    text = experiment.prompt("How many orders?")
    assert "Question: How many orders?\n" in text and "ANSWER:" in text and experiment.DECLARATION in text


# -- the harness, end to end with the planted agents (no MCP: the server is not started) ----------------------

def _run(world, tmp_path, behaviour, *, touch=False, modes=experiment.MODES, ids=None):
    truth = str(tmp_path / "truth.json")
    out = str(tmp_path / f"{behaviour}.json")
    report = experiment.run_experiment(_template(behaviour, truth, touch=touch), out, model=f"planted-{behaviour}",
                                       workdir=world["workdir"], modes=modes, ids=ids, timeout=120,
                                       truth_out=truth, agent_dir=world["agents"])
    assert experiment.verify(out) == []
    with open(out, encoding="utf-8") as f:
        assert json.load(f) == report
    return report


def _incorrect(report) -> set:
    return {(run["id"], run["mode"]) for run in report["runs"] if not run["correct"]}


def test_the_echo_agent_is_scored_all_correct(world, tmp_path):
    report = _run(world, tmp_path, "echo")
    assert report["model"] == "planted-echo" and report["commit"]["sha"]
    assert len(report["runs"]) == 48 and _incorrect(report) == set()
    for mode in experiment.MODES:
        assert report["summary"][mode] == {"runs": 24, "correct": 24, "failed_runs": 0, "not_answerable": 4,
                                           "not_answerable_detected": 4, "verdicts": {"correct": 24}}
    assert report["verified"] is True
    seen = set()
    for run in report["runs"]:
        with open(os.path.join(str(tmp_path), run["transcript"]), encoding="utf-8") as f:
            recorded = json.load(f)
        # Review M2: every run starts in its own fresh, empty directory, outside the work and the results trees.
        cwd, entries = re.search(r"^cwd: (.*) holds (\d+)$", recorded["stdout"], re.M).groups()
        assert entries == "0" and cwd not in seen and os.path.dirname(cwd) == world["agents"], cwd
        seen.add(cwd)
        # Review M3: nothing on the agent's command line names the arm; only the configuration's content does.
        assert not re.search(r"\braw\b|catalog", " ".join(recorded["argv"])), recorded["argv"]


def test_a_planted_near_miss_is_scored_incorrect(world, tmp_path):
    report = _run(world, tmp_path, "wrong", modes=("catalog",))
    answerable = {(q["id"], "catalog") for q in world["questions"] if q["ground_truth"] != "not_answerable"}
    assert _incorrect(report) == answerable
    assert {run["verdict"] for run in report["runs"] if not run["correct"]} == {"wrong"}


def test_a_planted_answer_to_an_unanswerable_question_is_scored_incorrect(world, tmp_path):
    report = _run(world, tmp_path, "answerable", modes=("raw",))
    unanswerable = {(q["id"], "raw") for q in world["questions"] if q["ground_truth"] == "not_answerable"}
    assert _incorrect(report) == unanswerable
    assert {run["verdict"] for run in report["runs"] if not run["correct"]} == \
        {"answered a question the database cannot answer"}
    assert report["summary"]["raw"]["not_answerable_detected"] == 0


def test_a_crashed_agent_never_counts_as_correct(world, tmp_path):
    """The crash agent prints the right answer, then exits 3: every run is incorrect and counted as failed."""
    report = _run(world, tmp_path, "crash", modes=("catalog",))
    assert len(_incorrect(report)) == 24
    assert report["summary"]["catalog"]["failed_runs"] == 24 and report["summary"]["catalog"]["correct"] == 0


def test_a_malformed_declaration_has_its_own_verdict(world, tmp_path):
    report = _run(world, tmp_path, "malformed", modes=("raw",))
    unanswerable = {(q["id"], "raw") for q in world["questions"] if q["ground_truth"] == "not_answerable"}
    assert _incorrect(report) == unanswerable
    assert report["summary"]["raw"]["verdicts"] == {"correct": 20, "malformed declaration": 4}


def test_the_agents_directory_must_lie_outside_what_the_harness_writes(world, tmp_path):
    truth = str(tmp_path / "truth.json")
    for inside in (os.path.join(world["workdir"], "agents"), str(tmp_path / "agents")):
        with pytest.raises(experiment.ExperimentError, match="lies inside"):
            experiment.run_experiment(_template("echo", truth), str(tmp_path / "out.json"), model="m",
                                      workdir=world["workdir"], modes=("catalog",), ids=["q01"], agent_dir=inside)


def test_a_timed_out_agent_is_killed_and_its_output_awaited_for_a_bounded_time(tmp_path):
    """The hang agent waits forever, and its child, in a session of its own, holds the agent's stdout open
    until the test releases it: only the harness's timeout and its bounded wait after the kill end the run."""
    if not hasattr(os, "mkfifo"):
        pytest.skip("the hang agent's release is a FIFO")
    release = str(tmp_path / "release")
    os.mkfifo(release)
    argv = [sys.executable, PLANTED, "--truth", "-", "--behaviour", "hang", "--release", release,
            "--mcp-config", "-", "-"]
    try:
        done = experiment.run_agent(argv, str(tmp_path), timeout=1, grace=1)
    finally:
        with open(release, "w", encoding="utf-8") as f:       # the child reads it and exits
            f.write("go")
    assert done["timed_out"] and done["exit_code"] is None and "not collected" in done["stderr"]
    assert experiment.score("number", "8", experiment.parse_answer(done["stdout"]), exit_code=done["exit_code"],
                            timed_out=done["timed_out"]) == (False, "timed out")


def test_verify_reports_a_malformed_transcript_as_a_problem(world, tmp_path):
    _run(world, tmp_path, "echo", modes=("catalog",), ids=["q01", "q02"])
    out = str(tmp_path / "echo.json")
    with open(out, encoding="utf-8") as f:
        report = json.load(f)
    first, second = (os.path.join(str(tmp_path), run["transcript"]) for run in report["runs"])
    with open(first, "w", encoding="utf-8") as f:
        json.dump({"id": "q01", "mode": "catalog"}, f)          # no stdout, no exit code
    with open(second, "w", encoding="utf-8") as f:
        json.dump(["not", "an", "object"], f)
    problems = experiment.verify(out)
    assert len(problems) == 2 and "malformed" in problems[0] and "cannot be read" in problems[1], problems
    assert experiment.main(["--verify", out]) == 1


def test_verify_finds_a_report_its_transcripts_contradict(world, tmp_path):
    _run(world, tmp_path, "echo", modes=("catalog",))
    out = str(tmp_path / "echo.json")
    with open(out, encoding="utf-8") as f:
        report = json.load(f)
    transcript = os.path.join(os.path.dirname(out), report["runs"][0]["transcript"])
    with open(transcript, encoding="utf-8") as f:
        recorded = json.load(f)
    recorded["exit_code"] = 2                          # the agent crashed, yet the report says correct
    with open(transcript, "w", encoding="utf-8") as f:
        json.dump(recorded, f)
    problems = experiment.verify(out)
    assert problems and report["runs"][0]["id"] in problems[0]

    report["runs"].pop()                               # a run missing from the report
    report["summary"]["catalog"]["correct"] += 5       # and a summary that does not add up
    with open(out, "w", encoding="utf-8") as f:
        json.dump(report, f)
    problems = experiment.verify(out)
    assert any("missing" in p for p in problems) and any("summary" in p for p in problems)


# -- through the MCP server (the optional extra 'agent') -----------------------------------------------------------

def test_the_echo_agent_through_the_mcp_server_in_both_modes(world, tmp_path):
    pytest.importorskip("mcp", reason="the server needs the optional extra 'agent' (the MCP SDK), absent from "
                                      "this interpreter (preflight F30)")
    # The server does not depend on the question: one of each answer kind and one that cannot be answered.
    ids = ["q01", "q02", "q16", "q21"]
    report = _run(world, tmp_path, "echo", touch=True, ids=ids)
    assert _incorrect(report) == set() and len(report["runs"]) == 2 * len(ids)
    for run in report["runs"]:
        with open(os.path.join(str(tmp_path), run["transcript"]), encoding="utf-8") as f:
            stdout = json.load(f)["stdout"]
        assert f"mode seen: {run['mode']}" in stdout, (run["id"], run["mode"], stdout)


def test_the_command_line_runs_and_names_the_model_and_commit(world, tmp_path):
    pytest.importorskip("mcp", reason="the command line checks for the optional extra 'agent' (the MCP SDK), "
                                      "absent from this interpreter (preflight F30)")
    truth, out = str(tmp_path / "truth.json"), str(tmp_path / "cli.json")
    code = experiment.main(["--agent-cmd", _template("echo", truth), "--out", out, "--model", "planted-echo",
                            "--workdir", world["workdir"], "--modes", "catalog", "--questions", "q01,q02,q21",
                            "--truth-out", truth, "--agent-dir", world["agents"]])
    assert code == 0
    with open(out, encoding="utf-8") as f:
        report = json.load(f)
    assert report["model"] == "planted-echo" and len(report["commit"]["sha"]) == 40
    assert [run["id"] for run in report["runs"]] == ["q01", "q02", "q21"]
    assert all(run["correct"] for run in report["runs"]) and report["verified"] is True
    assert experiment.main(["--verify", out]) == 0
