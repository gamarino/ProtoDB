Examples
========

This section shows how to run the example scripts that ship with ProtoBase. All examples can be executed directly
from the repository without installing the package, as each script bootstraps sys.path to include the project root.

Running an example
------------------

From the project root:

.. code-block:: bash

    # Basic usage
    python examples/simple_example.py

    # Task Manager sample app
    python examples/task_manager.py

    # LINQ examples
    python examples/linq_basic.py
    python examples/linq_between.py
    python examples/linq_groupby.py
    python examples/linq_policies.py

    # Performance/benchmarks (use small sizes locally)
    python examples/minimal_benchmark.py
    python examples/simple_performance_benchmark.py --count 100 --queries 10
    python examples/linq_performance.py --size 10000 --runs 3

Indexed access demonstration
----------------------------

The collection_indexing_example.py script contrasts a linear scan against an index‑backed query plan
built over the same List, demonstrating planner push‑down and index usage:

.. code-block:: bash

    python examples/collection_indexing_example.py

It builds a list of users, searches by email with and without a secondary index, and prints both timings and speedup.

Indexed benchmark
-----------------

The indexed_benchmark.py script constructs ad‑hoc indexes over a wrapped list of Python dicts, then compares
filtered range queries and point lookups against a pure Python baseline and a linear WherePlan:

.. code-block:: bash

    python examples/indexed_benchmark.py --items 100000 --queries 50 --out examples/benchmark_results_indexed.json

The output JSON contains total timings, latency percentiles, and computed speedups.
