# ProtoBase

[![PyPI version](https://img.shields.io/pypi/v/proto_db.svg)](https://pypi.org/project/proto_db/)
[![License](https://img.shields.io/github/license/yourusername/ProtoBase.svg)](LICENSE)
[![Python Version](https://img.shields.io/pypi/pyversions/proto_db.svg)](https://pypi.org/project/proto_db/)
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen.svg)](https://github.com/yourusername/ProtoBase)
[![Code Coverage](https://img.shields.io/badge/coverage-80%25-yellowgreen.svg)](https://github.com/yourusername/ProtoBase)

ProtoBase is a transactional, object-oriented database system implemented in Python. It provides a flexible and
extensible foundation for building database applications with support for various storage backends, rich data
structures, and a powerful query system.

## Why ProtoBase?

ProtoBase fills a unique niche in the database ecosystem by offering:

- **Lightweight Transactional Object Model**: Get the power of a transactional database without the overhead of a full
  DBMS server. ProtoBase runs within your Python application, making it perfect for embedded use cases.

- **Flexible Storage Options**: Choose from in-memory storage for testing, file-based storage for single-node
  applications, distributed storage for high availability, or cloud storage for scalability. Switch between them with
  minimal code changes.

- **Rich Data Structures**: Unlike simple key-value stores, ProtoBase provides native support for complex data
  structures like dictionaries, lists, and sets that maintain their semantics across transactions.

- **Pythonic Interface**: Work with a natural, Pythonic API that integrates seamlessly with your application code. Model
  your data as native Python classes inheriting from `DBObject`—no SQL, no complex ORM mapping, just Python objects all
  the way down.

- **Extensibility**: Easily extend ProtoBase with custom data types, storage backends, or query capabilities to meet
  your specific needs.

When you need more than SQLite but less than PostgreSQL, when you want transaction safety but don't want to manage a
server, when you need complex data structures but don't want to serialize/deserialize manually - ProtoBase is your
solution.

## Overview

ProtoBase is designed as a modular database system with the following key components:

- **Core Abstractions**: Atoms as the basic unit of data, with support for transactions and persistence.
- **Object-Oriented Data Modeling**: `DBObject` for representing data as Python objects.
- **Storage Backends**: Both in-memory and file-based storage implementations.
- **Data Structures**: Dictionaries, lists, sets, and other collections with transaction support.
- **Query System**: A comprehensive query system with filtering, joining, grouping, and more.

The system is built around the concept of "atoms"—self-contained units of data that can be saved, loaded, and
manipulated within transactions. All operations are performed within transactions, ensuring data consistency and
integrity.

## Key Features

New in 2025: Atom-level caches for faster reads across transactions. See docs (API > Atom-level Caches).

- **Transactional Operations**: All database operations are performed within transactions that can be committed or
  aborted.
- **Object-Oriented Data Modeling**: Define your data models as Python classes inheriting from `DBObject` for a more
  intuitive and readable codebase.
- **Multiple Storage Backends**:
    - `MemoryStorage`: In-memory storage for testing or ephemeral data.
    - `StandaloneFileStorage`: File-based storage with Write-Ahead Logging (WAL).
    - `ClusterFileStorage`: Distributed storage for high availability and horizontal scaling.
    - `CloudFileStorage`: Cloud-based storage using S3-compatible object storage services.
- **Rich Data Structures**:
    - `Dictionary`: Key-value mapping with string keys.
    - `List`: Ordered collection of items.
    - `Set`: Unordered collection of unique items.
    - `HashDictionary`: Dictionary with hash-based lookups.
- **Powerful Query System**:
    - Filtering with complex expressions.
    - Joining multiple data sources.
    - Grouping and aggregation.
    - Sorting and pagination.
- **Extensible Architecture**: Easy to add new storage backends, data structures, and query capabilities.

## Performance Benchmarks

See also docs/performance.md for the indexed benchmark harness and how to run it end-to-end using secondary indexes.

ProtoBase has been benchmarked to evaluate its performance characteristics. The following results were obtained using
the in-memory storage backend with a small dataset (1,000 items):

| Operation | Items/Second | Time per Item (ms) |
|-----------|--------------|--------------------|
| Insert    | 419.33       | 2.38               |
| Read      | 7,574.09     | 0.13               |
| Update    | 551.94       | 1.81               |
| Delete    | 2,846.18     | 0.35               |
| Query     | 2.51 (qps)   | 397.71 (per query) |

### Performance Analysis

- **Read Performance**: ProtoBase excels at read operations, achieving over 7,500 items per second, making it suitable
  for read-heavy workloads.
- **Delete Performance**: Delete operations are also very efficient at nearly 3,000 items per second.
- **Insert and Update**: These operations are moderately fast, with insert achieving around 420 items per second and
  update around 550 items per second.
- **Query Performance**: Complex queries are more resource-intensive, achieving about 2.5 queries per second for the
  test dataset.

### Comparison with Other Platforms

When compared to other database solutions:

1. **vs. SQLite**: ProtoBase offers comparable read performance to SQLite in memory mode but with the added benefits of
   a rich object model and native Python integration. SQLite may perform better for complex queries due to its mature
   query optimizer.

2. **vs. Redis**: Redis typically offers higher throughput for simple operations (100K+ ops/sec) but lacks ProtoBase's
   rich data structures and transactional model. ProtoBase is better suited for complex object relationships and when
   full transaction support is required.

3. **vs. MongoDB**: MongoDB offers better scaling for large datasets and distributed operations. ProtoBase provides a
   more lightweight solution with tighter Python integration, making it ideal for embedded use cases and applications
   that don't require MongoDB's scale.

4. **vs. Pickle/JSON**: Compared to simple file serialization methods like Pickle or JSON, ProtoBase offers
   significantly better performance for partial updates and queries, as it doesn't need to load and save the entire
   dataset for each operation.

ProtoBase is optimized for use cases that require:

- A lightweight embedded database
- Rich object model with native Python integration
- Transactional safety
- Good read performance

For applications requiring extreme write throughput or handling very large datasets (100M+ records), specialized
database systems may be more appropriate.

## LINQ-like Queries (Phase 1)

ProtoBase includes a lazy, composable LINQ-like API that works over Python iterables as well as ProtoBase collections and QueryPlans. It supports filtering, projection, ordering, distinct, paging, grouping with aggregates, and a Between operator with inclusive/exclusive bounds. When running over ProtoBase collections with indexes, supported predicates are pushed down to the query planner (WherePlan/SelectPlan) and can leverage indexes.

Quick example:

```python
from proto_db.linq import from_collection, F

users = [
    {"id": 1, "first_name": "Alice", "last_name": "Zeus", "age": 30, "country": "ES", "status": "active", "email": "a@example.com", "last_login": 5},
    {"id": 2, "first_name": "Bob", "last_name": "Young", "age": 17, "country": "AR", "status": "inactive", "email": "b@example.com", "last_login": 10},
    {"id": 3, "first_name": "Carol", "last_name": "Xavier", "age": 25, "country": "US", "status": "active", "email": "c@example.com", "last_login": 2},
    {"id": 4, "first_name": "Dan", "last_name": "White", "age": 22, "country": "AR", "status": "active", "email": "d@example.com", "last_login": 7},
]

q = (from_collection(users)
     .where((F.age >= 18) & F.country.in_(["ES", "AR"]))
     .order_by(F.last_login, ascending=False)
     .select({"id": lambda x: x["id"], "name": F.first_name + " " + F.last_name})
     .take(20))

res = q.to_list()
```

Between and chained comparisons:

```python
# Inclusive by default
from_collection(rows).where(F.value.between(10, 20)).to_list()

# Lambda automatically translated to a range
from_collection(rows).where(lambda x: 10 <= x["value"] <= 20).count()
```

Policies allow controlling fallback behavior when expressions cannot be translated for pushdown:

```python
from proto_db.linq import Policy

# Error on unsupported
q = from_collection(items).with_policy(Policy(on_unsupported="error"))

# Warn and fallback (local evaluation up to safety limits)
q = (from_collection(items)
     .on_unsupported("warn")
     .where(lambda x: custom_python_check(x))
     .take(100))
```

See the Sphinx docs (API > LINQ-like API) for the full surface and details about explain(), grouping and aggregation.

## Installation

ProtoBase requires Python 3.11 or higher. You can install it directly from PyPI:
