# ProtoBase

ProtoBase is a transactional, object-oriented database system implemented in Python. It provides a flexible and extensible foundation for building database applications with support for various storage backends, rich data structures, and a powerful query system.

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

`CloudFileStorage` extends `ClusterFileStorage` to provide cloud-based storage using S3-compatible object storage services.

**Key Features:**
- **Cloud Storage Integration**: Store data in S3-compatible object storage
- **Local Caching**: Maintain a local cache for improved performance
- **Background Uploading**: Asynchronously upload data to cloud storage
- **Batched Operations**: Group operations for efficient processing

**Use Cases:**
- **Scalable Storage**: Leverage cloud infrastructure for virtually unlimited storage
- **Disaster Recovery**: Ensure data durability through cloud storage redundancy
- **Cost-Effective Archiving**: Store historical data in cost-effective cloud storage
- **Hybrid Cloud Deployments**: Combine on-premises and cloud resources
- **Global Access**: Make data accessible from anywhere with internet connectivity

**Example:**
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
    s3_client=s3_client,
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

## Development

### Running Tests

Tests are implemented using Python's standard `unittest` framework:

```bash
python -m unittest discover proto_db/tests
```

### Project Structure

- `proto_db/common.py`: Core abstractions and base classes
- `proto_db/db_access.py`: Database access layer
- `proto_db/standalone_file_storage.py`: File-based storage implementation
- `proto_db/memory_storage.py`: In-memory storage implementation
- `proto_db/cluster_file_storage.py`: Distributed storage implementation
- `proto_db/cloud_file_storage.py`: Cloud-based storage implementation
- `proto_db/dictionaries.py`, `proto_db/lists.py`, `proto_db/sets.py`: Data structures
- `proto_db/queries.py`: Query system
- `proto_db/tests/`: Test cases

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

[Add contribution guidelines here]
