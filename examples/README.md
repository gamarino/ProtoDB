# ProtoDB examples

Small, runnable scripts that show the ProtoDB 2.0 API. Every script checks the results it
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
| `transactions_concurrency.py` | Eight threads at once: a read-modify-write conflict, `Database.run` retries, blind writes merged per key, and a `MutableObject` counter; no update is lost. |
| `vector_search.py` | `VectorIndex` with the exact backend on one collection and the IVF-Flat backend on another: k-nearest, k-nearest among the elements matching a filter, and a similarity threshold on each, with `explain()` showing the backend that ran and every result checked against brute force. Needs numpy (`pip install "proto_db[vectors]"`). |

Performance measurements are not examples: the 2.0 performance suite (spec section 12) lives
apart from this directory.
