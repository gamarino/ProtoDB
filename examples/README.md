# proto_db examples

Small, runnable scripts that show the proto_db 2.0 API. Every script checks the results it
prints and exits with an error if one is wrong, and every script runs in the test suite
(`tests/integration/test_examples.py`), so they stay in step with the API.

Run any of them from the repository root (or from anywhere: each script adds the repository
to `sys.path`):

```bash
python examples/simple_example.py
```

| Script | Shows |
|---|---|
| `simple_example.py` | An `ObjectSpace` over `MemoryStorage`, a database, transactions as context managers, `List`, `Dictionary` and `DBObject` versions, the version history. |
| `file_storage.py` | `FileStorage`: write, close and reopen a space; history and a `RangeIndex` survive the reopen, and `explain()` shows the query using the index; the durability modes. |
| `indexed_query.py` | `EqualityIndex` and `RangeIndex` on a `Dictionary`; the same LINQ queries planned with and without indexes, `explain()` output, identical results. |
| `linq_queries.py` | The LINQ front end over plain Python data: `where`, `between` on dates, `order_by`/`take`, `select`, `group_by` with `Agg`, `join`, `select_many`, `distinct`, and a lambda shown as a `PythonFilter`. |
| `transactions_concurrency.py` | Eight threads at once under the precondition model: a read-modify-write conflict and the unit it names, `Database.run` retries, per-key dictionary writes that never conflict, `CountedSet` counts that compose, a `MutableObject` counter, and blind cell writes that conflict instead of overwriting; no update is lost. |
| `vector_search.py` | `VectorIndex` with the exact backend on one collection and the IVF-Flat backend on another: k-nearest, k-nearest among the elements matching a filter, and a similarity threshold on each, with `explain()` showing the backend that ran and every result checked against brute force. Needs numpy (`pip install "proto_db[vectors]"`). |

## The tutorial scripts

`examples/tutorial/` holds one self-checking script per chapter of the three tutorials under
[`docs/tutorial/`](../docs/tutorial/README.md). Each script is generated from its Markdown chapter
(`python tests/docs/test_markdown_tutorials.py --write-scripts`), runs the chapter's code in order, compares
what it prints with the output the chapter shows, exits with status 1 at the first difference, and prints
`<name>: OK` last. `tests/integration/test_examples.py` runs every one of them.

```bash
python examples/tutorial/05_concurrency.py
```

| Directory | Scripts | Tutorial |
|---|---|---|
| `examples/tutorial/` | `01_first_database.py` to `10_cluster.py` | [the complete tutorial](../docs/tutorial/README.md) |
| `examples/tutorial/queries/` | `01_building_queries.py` to `07_performance_pitfalls.py` | [the queries tutorial](../docs/tutorial/queries/README.md) |
| `examples/tutorial/semantic/` | `01_declaring_a_model.py` to `07_history.py` | [the semantic model tutorial](../docs/tutorial/semantic-model/README.md) |

The other files in `examples/tutorial/` are the modules the chapters import: `erp_app.py` (the plain ERP model
and its seed data), `erp_model.py` (the same ERP as a semantic model), the registry modules that extend it
(`erp_labels.py`, `erp_crm.py`, `erp_forecast.py`, `erp_regions.py`) and `memory_store.py` (the in-process
object store of the cluster chapter). `09_vector_search.py` and `queries/05_range_vector_and_custom_indexes.py`
need numpy.

Performance measurements are not examples: the 2.0 performance suite (spec section 12) lives
apart from this directory.
