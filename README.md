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

ProtoBase requires Python 3.11 or higher. It's recommended to use a virtual environment:

```bash
# Create a virtual environment
python -m venv .venv

# Activate the virtual environment
# On Windows:
.venv\Scripts\activate
# On Unix/MacOS:
source .venv/bin/activate

# Clone the repository
git clone https://github.com/yourusername/ProtoBase.git
cd ProtoBase

# Install dependencies (if any)
# pip install -r requirements.txt
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
- `proto_db/dictionaries.py`, `proto_db/lists.py`, `proto_db/sets.py`: Data structures
- `proto_db/queries.py`: Query system
- `proto_db/tests/`: Test cases

## License

[Add your license information here]

## Contributing

[Add contribution guidelines here]