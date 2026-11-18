# The agent experiment: answers with and without the catalog

Spec section 15 asks for "correct answers of an agent with and without the catalog": the same questions, asked
of the same database by the same agent, once with the semantic catalog and once with the stored structure only.
This page holds the questions, the method and, once the run is made, both modes' answers, their correctness, the
model and the commit.

## Method

- **Database.** The demo's database (`docs/kit/semantic/semantic_demo.py`, steps 3 and 4: one company, six
  customers, three products, 30 orders). The harness builds it, or reuses it in its work directory, and computes
  the ground truth before any agent runs. Which of Fabrikam's two concurrent orders the credit rule refuses
  depends on the replay order, so a few answers (the orders' total, for one) differ between builds; the ground
  truth is always computed on the database the agents query.
- **Two modes of the same MCP server** (`python -m proto_db.agent`), with the same four tools, the same prompt
  and the same agent:
  - *catalog*: the agent interface as the demo shows it: meanings, labels, synonyms, units, provenance,
    computed attributes, sensitivity;
  - *raw* (`--raw`, the control group): the stored structure only. The control agent still sees the stored type
    names and attribute names (`acme.sales.Customer`, `credit_limit`, `price`), but not what they mean: it is
    not told that a line's `price` is the price agreed at the sale, it has no `amount` (a computed attribute,
    never stored: a term naming it is refused, never summed as 0), and its `check_answerable` matches stored
    names only. Both arms meet the same server: its name, instructions and tool descriptions do not change.
- **The agent** is the operator's: any headless agent that takes an MCP configuration. It should be restricted
  to the server's four tools (no files, shell or web). Each run starts in a fresh empty directory outside the
  results and the work directory (`--agent-dir`), and nothing on its command line names the arm (the
  configuration's path is `server-<n>/mcp.json`). Only synthetic demo data reaches it. The harness refuses an
  agent directory that lies inside the results or the work directory, but not the reverse: with the default
  (the system's temporary directory) and `--out` also under it, a run's parent directory holds the results as
  siblings. Pass an `--agent-dir` of its own, outside `--out` and the work directory; isolation rests on the
  agent's tool restrictions above.
- **Ground truth.** Each answerable question names a query of `docs/kit/semantic/queries.py` (`QUESTIONS`), run
  in catalog mode, and possibly a declared client-side post-step (`POST`) over its rows, for what the MVP's query
  tree cannot say (it has no ordering, minimum or maximum). An argmin or argmax over a tie is refused, so every
  question has one answer.
- **Scoring** never judges by similarity. The agent ends with one line `ANSWER: <answer>`. A number must be a
  plain decimal equal to the ground truth after `Decimal` parsing (`1980` equals `1980.00`; `$1980`, `1,980.00`
  and `1980 units` are unparsable, hence incorrect); a date must be `YYYY-MM-DD` and equal; a cell name must be
  equal character for character. A question the database cannot answer is correct only when the agent declares
  `NOT ANSWERABLE`, and that declaration is incorrect for any other question; one not written exactly
  (`NOT ANSWERABLE.`) is a malformed declaration, incorrect everywhere. A run whose agent exits with an error or
  times out is never correct. The summary counts the runs per verdict (correct, wrong, unparsable, no answer
  line, malformed declaration, agent failed, timed out...).
- **Self-check.** Every run's transcript is kept; the harness re-scores every run from its transcript and
  recounts the summary before it writes the results, and `--verify FILE` repeats it. The harness is validated
  by planted agents (`tests/support/planted_agent.py`, `tests/unit/test_semantic_agent_experiment.py`): one that
  echoes the ground truth is scored all correct in both modes; one whose answers are near misses (one unit off in
  the last digit, a date a day later, a cell name one character off) and one that answers a number to the
  questions the database cannot answer are scored incorrect exactly there; one that prints the right answer and
  then exits with an error is never correct. The near-miss checks rely on the questions' truths being distinct
  from each other (a near miss of one is no other's answer); the two date truths are distinct today, and no test
  enforces it.

## The questions

| Id | Question | Answer | Ground truth (query, post-step) |
|---|---|---|---|
| q01 | How many units of the widget did we sell in June 2026? | number | `widget_units_in_june` |
| q02 | Which customer is closest to its credit limit? | cell | `customer_limits`, `argmin(credit_limit - committed)` |
| q03 | What is the largest amount of credit that any customer still has available? | number | `customer_limits`, `max(credit_limit - committed)` |
| q04 | How many orders has the company received? | number | `order_count` |
| q05 | What is the total amount of all the company's orders? | number | `orders_total` |
| q06 | How many customers does the company have? | number | `customer_count` |
| q07 | What is the sum of the credit limits of all customers? | number | `credit_limits_total` |
| q08 | What is the current list price of the widget? | number | `widget_list_price`, `only(list_price)` |
| q09 | What is the highest unit price ever agreed for the widget on an order? | number | `widget_agreed_prices`, `max(price)` |
| q10 | How many units of the gizmo have we sold in total? | number | `gizmo_units` |
| q11 | What is the value of the gadget sold in the first quarter of 2026, at the prices agreed on the orders? | number | `gadget_value_first_quarter` |
| q12 | How much have the widget's sales amounted to, at the prices agreed on the orders? | number | `widget_value` |
| q13 | How many order lines include the gizmo? | number | `gizmo_line_count` |
| q14 | How many orders were placed from July to December 2026? | number | `orders_second_half_count` |
| q15 | What is the total of order number 7? | number | `order_7_total`, `only(total)` |
| q16 | On what date was order number 15 placed? | date | `order_15_date`, `only(date)` |
| q17 | How many order lines are there in all the company's orders? | number | `order_line_count` |
| q18 | How many customers have committed more than half of their credit limit? | number | `customers_over_half_their_limit` |
| q19 | What is the date of the most recently dated order? | date | `order_dates`, `max(date)` |
| q20 | What is the total amount of the orders placed from July to December 2026? | number | `orders_total_second_half` |
| q21 | How much did we spend on advertising in 2026? | — | not answerable |
| q22 | How many units of the gizmo are in stock in the warehouse? | — | not answerable |
| q23 | What profit margin did we make on the gadget? | — | not answerable |
| q24 | Which supplier provides the widget? | — | not answerable |

The questions file is `benchmarks/semantic/questions.json`. The customers' names are personal, so the agent
cannot name a customer: q02 is answered with the customer's cell name.

## How to run it

```
python -m benchmarks.semantic.agent_experiment \
    --agent-cmd '<agent CLI> --mcp-config {mcp_config} <flags that allow only the proto_db tools> {question}' \
    --model <the model the agent runs> --out results/agent-experiment.json
```

`{mcp_config}` is replaced by the path of an MCP client configuration (`mcpServers` form) that starts the server
in the run's mode, and `{question}` by the prompt; each stays one argument. The results name the model and the
commit (and whether the tree was dirty); the transcripts are in `results/agent-experiment.transcripts/`. It needs
the optional extra `agent` (`pip install -e .[agent]`).

## Results

One run per question and mode, made on 2026-11-18 at commit `6b0697a` (clean tree):

- **Agent:** Claude Code 2.1.326 in headless mode (`claude -p`), model **`claude-sonnet-5-5`**. It ran with no
  built-in tools (`--tools ""`), only the server's four tools allowed, no user or project settings
  (`--setting-sources ""`), and no slash commands. The exact command is in the results file.
- **Server:** Python 3.14.6, database version 11, built by the demo.
- **Results file:** `benchmarks/results/2026-11-18-6b0697a-agent/full-6b0697a.json`, with each run's transcript
  next to it. `--verify` re-scores it from the transcripts: "the report verifies".

| Mode | Correct | Not answerable detected | Verdicts |
|---|---|---|---|
| catalog | 23 of 24 | 4 of 4 | 23 correct, 1 wrong |
| raw (control) | 24 of 24 | 4 of 4 | 24 correct |

| Id | Expected | Catalog | Raw |
|---|---|---|---|
| q01 | `8` | `8` | `8` |
| q02 | `acme.sales.Customer#570d7c4587bb4734bdc5f7be5fbf80b7` | `acme.sales.Customer#570d7c4587bb4734bdc5f7be5fbf80b7` | `acme.sales.Customer#570d7c4587bb4734bdc5f7be5fbf80b7` |
| q03 | `99229.00` | `99229` | `99229` |
| q04 | `30` | `30` | `30` |
| q05 | `1930.00` | `1930.00` | `1930.00` |
| q06 | `6` | `6` | `6` |
| q07 | `200400.00` | `200400.00` | `200400` |
| q08 | `12.00` | `12.00` | `12.00` |
| q09 | `100.00` | `100.00` | `100.00` |
| q10 | `36` | `36` | `36` |
| q11 | `200.00` | `200` | `200` |
| q12 | `868.00` | `868.00` | `868` |
| q13 | `16` | `16` | `16` |
| q14 | `18` | `18` | `18` |
| q15 | `118.50` | `118.50` | `118.50` |
| q16 | `2026-03-20` | `2026-03-20` | `2026-03-20` |
| q17 | `78` | `78` | `78` |
| q18 | `2` | `2` | `2` |
| q19 | `2026-12-17` | `2026-12-17` | `2026-12-17` |
| q20 | `1165.00` | `1165.50` (wrong) | `1165.00` |
| q21 | `NOT ANSWERABLE` | `NOT ANSWERABLE` | `NOT ANSWERABLE` |
| q22 | `NOT ANSWERABLE` | `NOT ANSWERABLE` | `NOT ANSWERABLE` |
| q23 | `NOT ANSWERABLE` | `NOT ANSWERABLE` | `NOT ANSWERABLE` |
| q24 | `NOT ANSWERABLE` | `NOT ANSWERABLE` | `NOT ANSWERABLE` |

**What this run shows, and what it does not.**

- **Both arms detect the four unanswerable questions**, the advertising question (q21) among them, and neither
  arm declares any answerable question not answerable.
- **The catalog shows no measured advantage in correctness on this question set.** The raw arm answered every
  question: the stored type and attribute names (`price`, `quantity`, `total`, `credit_limit`, `date`) are
  descriptive enough for these questions, and the model reads them as a person would. The catalog's own content
  (meanings, the price agreed at the sale, the computed `amount`, units) is not needed to answer any of them. A
  question set that separates the arms needs questions whose answer depends on what the stored names do not
  say. This run does not test that.
- **The catalog arm's one miss (q20) is the agent's own arithmetic.** It listed the 18 orders with their correct
  totals, then added them by hand to `1165.50`; they sum to `1165.00`. The server's own `sum` gives `1165.00`
  for that query. The same wrong total came back in all three catalog runs of q20 (at `74d314c`, `a14cf8b` and
  `6b0697a`). The raw arm reached the right total in each of the runs where it could see the attributes. Both
  arms added by hand, not with the `sum` aggregate. One possible reason, not confirmed (the transcripts keep only
  the agent's final text, not its tool calls): an ordering comparison of a date with a string constant
  (`date >= "2026-07-01"` instead of `{"date": "2026-07-01"}`) is false by D28's type ranks, so such a query
  returns no rows without an error.
- **One run per question and mode.** No repetition, so nothing here measures the agent's variance.

**An earlier run measured a defect, not the catalog.** At `74d314c` the raw server read no stored attribute at
all. It never imports the model's code, so its records load as opaque atoms, and raw mode only read the fields
of classes it had. The raw arm then answered 7 of 24 (17 declared not answerable), against 23 of 24 for the
catalog arm. That run is kept as the evidence of the defect, not as a result:
`benchmarks/results/2026-11-18-74d314c-agent-control-defect/`. Fixed in `a14cf8b`, `e3bad96` and `6b0697a`,
with tests that read the database in a process without the model's code.
