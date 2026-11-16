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
    never stored), and its `check_answerable` matches stored names only.
- **The agent** is the operator's: any headless agent that takes an MCP configuration. It should be restricted
  to the server's four tools (no files, shell or web); the harness runs it in an empty directory. Only synthetic
  demo data reaches it.
- **Ground truth.** Each answerable question names a query of `docs/kit/semantic/queries.py` (`QUESTIONS`), run
  in catalog mode, and possibly a declared client-side post-step (`POST`) over its rows, for what the MVP's query
  tree cannot say (it has no ordering, minimum or maximum). An argmin or argmax over a tie is refused, so every
  question has one answer.
- **Scoring** never judges by similarity. The agent ends with one line `ANSWER: <answer>`. A number must be a
  plain decimal equal to the ground truth after `Decimal` parsing (`1980` equals `1980.00`; `$1980`, `1,980.00`
  and `1980 units` are unparsable, hence incorrect); a date must be `YYYY-MM-DD` and equal; a cell name must be
  equal character for character. A question the database cannot answer is correct only when the agent declares
  `NOT ANSWERABLE`, and that declaration is incorrect for any other question. A run whose agent exits with an
  error or times out is never correct.
- **Self-check.** Every run's transcript is kept; the harness re-scores every run from its transcript and
  recounts the summary before it writes the results, and `--verify FILE` repeats it. The harness is validated
  by planted agents (`tests/support/planted_agent.py`, `tests/unit/test_semantic_agent_experiment.py`): one that
  echoes the ground truth is scored all correct in both modes; one whose answers are near misses (one unit off in
  the last digit, a date a day later, a cell name one character off) and one that answers a number to the
  questions the database cannot answer are scored incorrect exactly there; one that prints the right answer and
  then exits with an error is never correct.

## The questions

| Id | Question | Answer | Ground truth (query, post-step) |
|---|---|---|---|
| q01 | How many units of the widget did we sell in June 2026? | number | `widget_units_in_june` |
| q02 | Which customer is closest to its credit limit? | cell | `customer_limits`, `argmin(credit_limit - committed)` |
| q03 | How much credit does the customer closest to its credit limit still have available? | number | `customer_limits`, `min(credit_limit - committed)` |
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
| q14 | How many orders were placed in July 2026? | number | `orders_in_july` |
| q15 | What is the total of order number 7? | number | `order_7_total`, `only(total)` |
| q16 | On what date was order number 15 placed? | date | `order_15_date`, `only(date)` |
| q17 | How many lines does order number 12 have? | number | `order_12_line_count` |
| q18 | How many customers have committed more than half of their credit limit? | number | `customers_over_half_their_limit` |
| q19 | What is the number of the most recently dated order? | number | `order_dates`, `number of argmax(date)` |
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

**Results pending: the real-agent run has not been performed yet.** No answer, score or model is reported
here until that run is made; this section will then give, per mode, each question's answer and correctness, the
counts of correct answers and of unanswerable questions detected, the model, the commit, and the results file.
