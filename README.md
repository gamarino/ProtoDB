# ProtoBase

[![PyPI version](https://img.shields.io/pypi/v/proto_db.svg)](https://pypi.org/project/proto_db/)
[![License](https://img.shields.io/github/license/yourusername/ProtoBase.svg)](LICENSE)
[![Python Version](https://img.shields.io/pypi/pyversions/proto_db.svg)](https://pypi.org/project/proto_db/)
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen.svg)](https://github.com/yourusername/ProtoBase)
[![Code Coverage](https://img.shields.io/badge/coverage-80%25-yellowgreen.svg)](https://github.com/yourusername/ProtoBase)

ProtoBase is a transactional, object-oriented database system implemented in Python. It provides a flexible and extensible foundation for building database applications with support for various storage backends, rich data structures, and a powerful query system.

## Why ProtoBase?

ProtoBase fills a unique niche in the database ecosystem by offering:

- **Lightweight Transactional Object Model**: Get the power of a transactional database without the overhead of a full DBMS server. ProtoBase runs within your Python application, making it perfect for embedded use cases.

- **Flexible Storage Options**: Choose from in-memory storage for testing, file-based storage for single-node applications, distributed storage for high availability, or cloud storage for scalability. Switch between them with minimal code changes.

- **Rich Data Structures**: Unlike simple key-value stores, ProtoBase provides native support for complex data structures like dictionaries, lists, and sets that maintain their semantics across transactions.

- **Pythonic Interface**: Work with a natural, Pythonic API that integrates seamlessly with your application code. Model your data as native Python classes inheriting from `DBObject`—no SQL, no complex ORM mapping, just Python objects all the way down.

- **Extensibility**: Easily extend ProtoBase with custom data types, storage backends, or query capabilities to meet your specific needs.

When you need more than SQLite but less than PostgreSQL, when you want transaction safety but don't want to manage a server, when you need complex data structures but don't want to serialize/deserialize manually - ProtoBase is your solution.

## Overview

ProtoBase is designed as a modular database system with the following key components:

- **Core Abstractions**: Atoms as the basic unit of data, with support for transactions and persistence.
- **Object-Oriented Data Modeling**: `DBObject` for representing data as Python objects.
- **Storage Backends**: Both in-memory and file-based storage implementations.
- **Data Structures**: Dictionaries, lists, sets, and other collections with transaction support.
- **Query System**: A comprehensive query system with filtering, joining, grouping, and more.

The system is built around the concept of "atoms"—self-contained units of data that can be saved, loaded, and manipulated within transactions. All operations are performed within transactions, ensuring data consistency and integrity.

## Key Features

- **Transactional Operations**: All database operations are performed within transactions that can be committed or aborted.
- **Object-Oriented Data Modeling**: Define your data models as Python classes inheriting from `DBObject` for a more intuitive and readable codebase.
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

## Installation

ProtoBase requires Python 3.11 or higher. You can install it directly from PyPI: