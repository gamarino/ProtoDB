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

- **Pythonic Interface**: Work with a natural, Pythonic API that integrates seamlessly with your application code. No SQL, no ORM mapping - just Python objects all the way down.

- **Extensibility**: Easily extend ProtoBase with custom data types, storage backends, or query capabilities to meet your specific needs.

When you need more than SQLite but less than PostgreSQL, when you want transaction safety but don't want to manage a server, when you need complex data structures but don't want to serialize/deserialize manually - ProtoBase is your solution.

## Overview

ProtoBase is designed as a modular database system with the following key components:

- **Core Abstractions**: Atoms as the basic unit of data, with support for transactions and persistence
- **Storage Backends**: Both in-memory and file-based storage implementations
- **Data Structures**: Dictionaries, lists, sets, and other collections with transaction support
- **Query System**: A comprehensive query system with filtering, joining, grouping, and more

The system is built around the concept of "atoms" - self-contained units of data that can be saved, loaded, and manipulated within transactions. All operations are performed within transactions, ensuring data consistency and integrity.

## Key Features

- **Transactional Operations**: All database operations are performed within transactions that can be committed or aborted
- **Multiple Storage Backends**: 
  - `MemoryStorage`: In-memory storage for testing or ephemeral data
  - `StandaloneFileStorage`: File-based storage with Write-Ahead Logging (WAL)
  - `ClusterFileStorage`: Distributed storage for high availability and horizontal scaling
  - `CloudFileStorage`: Cloud-based storage using S3-compatible object storage services
- **Rich Data Structures**:
  - `Dictionary`: Key-value mapping with string keys
  - `List`: Ordered collection of items
  - `Set`: Unordered collection of unique items
  - `HashDictionary`: Dictionary with hash-based lookups
- **Powerful Query System**:
  - Filtering with complex expressions
  - Joining multiple data sources
  - Grouping and aggregation
  - Sorting and pagination
- **Extensible Architecture**: Easy to add new storage backends, data structures, and query capabilities

## Installation

ProtoBase requires Python 3.11 or higher. You can install it directly from PyPI:

```bash
# Create a virtual environment (recommended)
python -m venv .venv

# Activate the virtual environment
# On Windows:
.venv\Scripts\activate
# On Unix/MacOS:
source .venv/bin/activate

# Install from PyPI
pip install proto_db
```

Alternatively, you can install from the source:

```bash
# Clone the repository
git clone https://github.com/yourusername/ProtoBase.git
cd ProtoBase

# Install in development mode
pip install -e .
```

## Basic Usage

### Creating a Database

```python
from proto_db.memory_storage import MemoryStorage
from proto_db.db_access import ObjectSpace, Database

# Create a storage provider
storage = MemoryStorage()

# Create an object space with the storage provider
object_space = ObjectSpace(storage=storage)

# Create a new database
database = object_space.new_database('MyDatabase')
```

### Working with Transactions

```python
# Create a new transaction
transaction = database.new_transaction()

# Set a root object
transaction.set_root_object('my_key', 'my_value')

# Commit the transaction
transaction.commit()

# Create a new transaction to retrieve the value
transaction2 = database.new_transaction()
value = transaction2.get_root_object('my_key')
print(value)  # Output: my_value
```

### Working with Collections

```python
# Create a new transaction
transaction = database.new_transaction()

# Create a list
my_list = transaction.new_list()
for i in range(10):
    my_list = my_list.set_at(i, i * 2)

# Store the list as a root object
transaction.set_root_object('my_list', my_list)
transaction.commit()

# Retrieve and use the list
transaction2 = database.new_transaction()
retrieved_list = transaction2.get_root_object('my_list')
for item in retrieved_list.as_iterable():
    print(item)
```

### Using the Query System

```python
from proto_db.queries import WherePlan, Expression

# Create a query plan from a list
query_plan = retrieved_list.as_query_plan()

# Filter the list
filter_expression = Expression.compile([
    'value', '>', 5  # Filter items where value > 5
])
filtered_plan = WherePlan(filter=filter_expression, based_on=query_plan)

# Execute the query
for item in filtered_plan.execute():
    print(item)
```

## Architecture

ProtoBase is organized around several key abstractions:

### Core Components

- **Atom**: The base class for all database objects
- **AtomPointer**: A reference to a stored atom
- **SharedStorage**: Interface for storage implementations
- **ObjectSpace**: Container for multiple databases
- **Database**: Container for a single database
- **Transaction**: Context for database operations

### Storage Layer

The storage layer is responsible for persisting atoms to disk or memory. It provides:

- **MemoryStorage**: In-memory storage for testing or ephemeral data
- **StandaloneFileStorage**: File-based storage with Write-Ahead Logging (WAL)
- **ClusterFileStorage**: Distributed storage with support for multiple nodes in a cluster environment
- **CloudFileStorage**: Cloud-based storage using S3-compatible object storage services
- **CloudClusterFileStorage**: Combined cloud and cluster storage for distributed cloud environments

### Data Structures

ProtoBase provides several data structures built on top of atoms:

- **Dictionary**: Key-value mapping with string keys
- **List**: Ordered collection of items
- **Set**: Unordered collection of unique items
- **HashDictionary**: Dictionary with hash-based lookups

### Query System

The query system allows for complex data manipulation:

- **Expression**: Logical expressions for filtering
- **QueryPlan**: Base class for all query plans
- **WherePlan**: Filtering records
- **JoinPlan**: Joining multiple data sources
- **GroupByPlan**: Grouping and aggregation
- **OrderByPlan**: Sorting results
- **SelectPlan**: Projecting specific fields
- **LimitPlan** and **OffsetPlan**: Pagination

## Advanced Storage Options

ProtoBase offers advanced storage options for distributed and cloud environments:

### ClusterFileStorage

`ClusterFileStorage` extends `StandaloneFileStorage` to provide distributed storage capabilities across multiple nodes in a cluster.

**Key Features:**
- **Distributed Coordination**: Nodes communicate to maintain data consistency
- **Vote-based Locking**: Implements distributed locking through a voting mechanism
- **Root Synchronization**: Ensures all nodes have a consistent view of the root object
- **Cached Page Retrieval**: Allows nodes to retrieve pages from other nodes in the cluster

**Use Cases:**
- **High Availability Systems**: Deploy across multiple servers for fault tolerance
- **Horizontal Scaling**: Distribute load across multiple nodes
- **Geographically Distributed Applications**: Maintain data consistency across different locations
- **Real-time Collaborative Applications**: Support concurrent access from multiple users

**Example:**
```python
from proto_db.cluster_file_storage import ClusterFileStorage
from proto_db.file_block_provider import FileBlockProvider
from proto_db.db_access import ObjectSpace, Database

# Create a block provider
block_provider = FileBlockProvider(directory="data")

# Create a cluster storage with multiple servers
storage = ClusterFileStorage(
    block_provider=block_provider,
    server_id="server1",
    host="localhost",
    port=8000,
    servers=[("localhost", 8000), ("localhost", 8001), ("localhost", 8002)]
)

# Create an object space and database as usual
object_space = ObjectSpace(storage=storage)
database = object_space.new_database('ClusterDB')
```

### CloudFileStorage

`CloudFileStorage` extends `ClusterFileStorage` to provide cloud-based storage using both Amazon S3 and Google Cloud Storage services.

**Key Features:**
- **Multiple Cloud Provider Support**: Store data in Amazon S3 or Google Cloud Storage
- **Local Caching**: Maintain a local cache for improved performance
- **Background Uploading**: Asynchronously upload data to cloud storage
- **Batched Operations**: Group operations for efficient processing
- **Proper Thread Management**: Ensures background uploader threads are properly managed

**Use Cases:**
- **Scalable Storage**: Leverage cloud infrastructure for virtually unlimited storage
- **Disaster Recovery**: Ensure data durability through cloud storage redundancy
- **Cost-Effective Archiving**: Store historical data in cost-effective cloud storage
- **Hybrid Cloud Deployments**: Combine on-premises and cloud resources
- **Global Access**: Make data accessible from anywhere with internet connectivity

**Example with Amazon S3:**
```python
from proto_db.cloud_file_storage import CloudFileStorage, CloudBlockProvider, S3Client
from proto_db.db_access import ObjectSpace, Database

# Create an S3 client
s3_client = S3Client(
    bucket="my-protobase-bucket",
    prefix="db-data/",
    endpoint_url="https://s3.amazonaws.com",
    access_key="YOUR_ACCESS_KEY",
    secret_key="YOUR_SECRET_KEY",
    region="us-east-1"
)

# Create a cloud block provider
block_provider = CloudBlockProvider(
    cloud_client=s3_client,
    cache_dir="local-cache",
    cache_size=1024 * 1024 * 1024  # 1GB cache
)

# Create a cloud storage
storage = CloudFileStorage(
    block_provider=block_provider,
    server_id="cloud-server-1"
)

# Create an object space and database as usual
object_space = ObjectSpace(storage=storage)
database = object_space.new_database('CloudDB')
```

**Example with Google Cloud Storage:**
```python
from proto_db.cloud_file_storage import CloudFileStorage, CloudBlockProvider, GoogleCloudClient
from proto_db.db_access import ObjectSpace, Database

# Create a Google Cloud Storage client
gcs_client = GoogleCloudClient(
    bucket="my-protobase-bucket",
    prefix="db-data/",
    project_id="my-project-id",
    credentials_path="/path/to/credentials.json"
)

# Create a cloud block provider
block_provider = CloudBlockProvider(
    cloud_client=gcs_client,
    cache_dir="gcs-cache",
    cache_size=1024 * 1024 * 1024  # 1GB cache
)

# Create a cloud storage
storage = CloudFileStorage(
    block_provider=block_provider,
    server_id="cloud-server-1"
)

# Create an object space and database as usual
object_space = ObjectSpace(storage=storage)
database = object_space.new_database('CloudDB')
```

### CloudClusterFileStorage

`CloudClusterFileStorage` combines the features of `ClusterFileStorage` and `CloudFileStorage` to provide distributed cloud-based storage with support for both Amazon S3 and Google Cloud Storage.

**Key Features:**
- **Distributed Cloud Storage**: Combines distributed coordination with cloud storage capabilities
- **Page Caching**: Maintains a local cache of cloud pages for improved performance
- **Multi-Provider Support**: Works with both Amazon S3 and Google Cloud Storage
- **Fault Tolerance**: Provides high availability through distributed nodes and cloud redundancy
- **Horizontal Scaling**: Distribute load across multiple nodes while leveraging cloud storage

**Use Cases:**
- **Global Distributed Applications**: Deploy across multiple regions with cloud storage as the backbone
- **High-Performance Distributed Systems**: Combine local caching with cloud durability
- **Disaster Recovery Solutions**: Ensure data availability even in case of node failures
- **Hybrid Multi-Cloud Deployments**: Operate across different cloud providers and on-premises infrastructure

**Example:**
```python
from proto_db.cloud_cluster_file_storage import CloudClusterFileStorage
from proto_db.cloud_file_storage import CloudBlockProvider, GoogleCloudClient
from proto_db.db_access import ObjectSpace, Database

# Create a Google Cloud Storage client
gcs_client = GoogleCloudClient(
    bucket="my-protobase-bucket",
    prefix="db-data/",
    project_id="my-project-id",
    credentials_path="/path/to/credentials.json"
)

# Create a cloud block provider
block_provider = CloudBlockProvider(
    cloud_client=gcs_client,
    cache_dir="gcs-cache",
    cache_size=1024 * 1024 * 1024  # 1GB cache
)

# Create a cloud cluster storage
storage = CloudClusterFileStorage(
    block_provider=block_provider,
    server_id="cloud-cluster-node-1",
    host="localhost",
    port=8000,
    servers=[("localhost", 8000), ("localhost", 8001), ("localhost", 8002)],
    page_cache_dir="cloud_page_cache"
)

# Create an object space and database as usual
object_space = ObjectSpace(storage=storage)
database = object_space.new_database('CloudClusterDB')
```

## Development

### Environment Setup

1. **Python Version**: ProtoBase requires Python 3.11 or higher.

2. **Virtual Environment**: It's recommended to use a virtual environment for development:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Dependencies**: The project doesn't have a formal requirements.txt file, but it relies on standard Python libraries. The main external dependencies appear to be:
   - `uuid` - For generating unique identifiers
   - `concurrent.futures` - For asynchronous operations
   - Standard libraries like `io`, `json`, `os`, `struct`, etc.

### Project Structure

The project is organized as a Python package named `proto_db` with the following key components:

- **Core Components**:
  - `proto_db/common.py`: Contains base classes like Atom, Literal, and other foundational types
  - `proto_db/db_access.py`: Database access layer with ObjectSpace, Database, and Transaction classes
  - `proto_db/standalone_file_storage.py`: File-based storage implementation with WAL (Write-Ahead Logging)
  - `proto_db/memory_storage.py`: In-memory storage implementation
  - `proto_db/cluster_file_storage.py`: Distributed storage implementation
  - `proto_db/cloud_file_storage.py`: Cloud-based storage implementation

- **Data Structures**:
  - `proto_db/dictionaries.py` and `proto_db/hash_dictionaries.py`: Dictionary implementations
  - `proto_db/lists.py`: List implementation
  - `proto_db/sets.py`: Set implementation

- **Query System**:
  - `proto_db/queries.py`: Query planning and execution

### Running Tests

Tests are implemented using Python's standard `unittest` framework. To run all tests:

```bash
python -m unittest discover proto_db/tests
```

To run a specific test file:

```bash
python -m unittest proto_db.tests.test_file_name
```

For example:

```bash
python -m unittest proto_db.tests.test_db_access
```

### Writing Tests

1. **Test Structure**: Tests should be organized in classes that inherit from `unittest.TestCase`.

2. **Test Setup**: Use the `setUp` method to initialize test environments.

3. **Test Naming**: Test methods should be named with a descriptive prefix like `test_001_feature_name`.

4. **Test Documentation**: Include docstrings that explain the purpose of each test.

5. **Example Test**:

```python
import unittest
from ..common import Atom, Literal

class TestExample(unittest.TestCase):
    """Example test to demonstrate testing in ProtoBase."""

    def test_literal_creation(self):
        """Test creating and using a Literal."""
        # Create a literal with a string value
        literal = Literal(literal="test_value")

        # Verify the literal's string attribute
        self.assertEqual(literal.string, "test_value")

        # Test string representation
        self.assertEqual(str(literal), "test_value")

        # Test equality comparison
        self.assertEqual(literal, "test_value")
```

### Testing Patterns

1. **Transaction Testing**: Many tests follow a pattern of:
   - Creating a transaction
   - Performing operations
   - Committing the transaction
   - Creating a new transaction to verify persistence
   - Asserting expected results

2. **Mocking**: For testing components in isolation, use `unittest.mock` to create mock objects:

```python
from unittest.mock import Mock, MagicMock

# Create a mock for the BlockProvider
mock_block_provider = Mock()
mock_block_provider.get_new_wal = MagicMock(return_value=(uuid4(), 0))
```

### Code Style

1. **Documentation**: Classes and methods are documented with detailed docstrings in reStructuredText format.

2. **Error Handling**: The project uses custom exception classes defined in `exceptions.py` for different error scenarios.

3. **Concurrency**: The project uses `concurrent.futures` for asynchronous operations, with methods returning `Future` objects.

### Architecture

1. **Storage Abstraction**: The project provides multiple storage backends:
   - `MemoryStorage`: In-memory storage for testing or ephemeral data
   - `StandaloneFileStorage`: File-based storage with Write-Ahead Logging (WAL)
   - `ClusterFileStorage`: Distributed storage with support for multiple nodes in a cluster environment
   - `CloudFileStorage`: Cloud-based storage using S3-compatible object storage services
   - `CloudClusterFileStorage`: Combined cloud and cluster storage for distributed cloud environments

2. **Transaction Model**: Database operations are performed within transactions:
   ```python
   tr = database.new_transaction()
   # Perform operations
   tr.commit()
   ```

3. **Object Model**: The project uses an object model based on the `Atom` class, with specialized classes for different data types:
   - `DBObject`: Base class for database objects
   - `MutableObject`: For objects that can be modified
   - `Literal`: For string literals
   - Various collection types (List, Dictionary, Set)

### Debugging Tips

1. **WAL Inspection**: When debugging file storage issues, check the WAL (Write-Ahead Log) state:
   ```python
   storage = StandaloneFileStorage(block_provider)
   print(f"WAL ID: {storage.current_wal_id}")
   print(f"WAL Base: {storage.current_wal_base}")
   ```

2. **Transaction Verification**: To verify transaction integrity, check if objects are properly persisted:
   ```python
   tr1 = database.new_transaction()
   tr1.set_root_object('key', value)
   tr1.commit()

   tr2 = database.new_transaction()
   retrieved_value = tr2.get_root_object('key')
   assert retrieved_value == value
   ```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

[Add contribution guidelines here]
