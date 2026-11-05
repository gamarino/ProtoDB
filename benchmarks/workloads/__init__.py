"""The workloads of the performance suite; importing this package registers all of them.

Import order is the report order: queries, pagination, writes, storage, concurrency, vectors, cluster.
"""
from benchmarks.workloads import queries  # noqa: F401
from benchmarks.workloads import pagination  # noqa: F401
from benchmarks.workloads import writes  # noqa: F401
from benchmarks.workloads import storage  # noqa: F401
from benchmarks.workloads import concurrency  # noqa: F401
from benchmarks.workloads import vector  # noqa: F401
from benchmarks.workloads import cluster  # noqa: F401
from benchmarks.workloads import rqlite  # noqa: F401
