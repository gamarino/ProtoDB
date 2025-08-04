# ProtoBase Examples

This directory contains example applications that demonstrate the capabilities of ProtoBase, a transactional,
object-oriented database system implemented in Python.

> **Note**: These examples are provided for illustrative purposes to demonstrate the intended usage of ProtoBase.
> Depending on your specific version of ProtoBase, you may need to make adjustments to the code to run the examples
> successfully.

## Task Manager Example

The Task Manager example demonstrates a simple but powerful task management application built with ProtoBase. It
showcases key features including:

- **Transactional Operations**: All database operations are performed within transactions that ensure data consistency.
- **Object-Oriented Data Modeling**: Using `DBObject` to represent data as Python objects, making the code more
  intuitive and readable.
- **Rich Data Structures**: Employing ProtoBase's `Dictionary` to organize and store `Task` objects.
- **Powerful Query Capabilities**: Filtering and sorting tasks by their object attributes using ProtoBase's query
  system.
- **Persistence**: Storing tasks in a file-based database that persists between application runs.

### Running the Example

To run the Task Manager example:

```bash
# Navigate to the examples directory
cd examples

# Run the task manager example
python task_manager.py
```

The example will:

1. Create a new database (or load an existing one)
2. Add sample tasks with different priorities and due dates
3. Update a task's status
4. Query tasks based on various criteria
5. Delete a task
6. Display the results of each operation

### Understanding the Code

The Task Manager example is organized around the `TaskManager` class, which provides methods for:

- **add_task()**: Add a new task with a title, description, priority, and due date
- **update_task()**: Update an existing task's properties
- **delete_task()**: Remove a task from the database
- **get_task()**: Retrieve a specific task by ID
- **get_all_tasks()**: Get all tasks in the database
- **query_tasks()**: Filter and sort tasks based on criteria

Each method demonstrates ProtoBase's transaction model:

1. Create a new transaction
2. Perform operations within the transaction
3. Commit the transaction to persist changes

### Key Concepts Demonstrated

#### Object-Oriented Modeling with `DBObject`

ProtoBase allows you to model your data using Python classes that inherit from `DBObject`. This makes data manipulation
more intuitive and your code easier to read and maintain.

```python
# Define a Task class that inherits from DBObject
class Task(DBObject):
    """
    A class representing a Task, stored as a DBObject.
    """
    pass

# Create a new Task instance
task = Task(
    id=task_id,
    title="Implement user authentication",
    description="Add login/logout functionality",
    priority="high",
    status="pending"
)

# Access properties like regular object attributes
print(f"Title: {task.title}")
print(f"Status: {task.status}")
```

#### Transactions

All operations that modify the database are performed within a transaction, ensuring atomicity and consistency.

```python
# Create a new transaction
tr = self.database.new_transaction()

# Perform operations
tasks_dict = tr.get_root_object('tasks')
task = Task(**task_data) # task_data is a dict with task properties
tasks_dict = tasks_dict.set(task_id, task)
tr.set_root_object('tasks', tasks_dict)

# Commit the transaction to persist changes
tr.commit()
```

#### Queries on Object Attributes

The query system is powerful and allows you to filter data based on the attributes of your `DBObject` instances.

```python
# Create a query plan from the tasks dictionary
query_plan = tasks_dict.as_query_plan()

# Apply filters based on DBObject attributes
# Note: 'value' refers to the Task object in the dictionary
filter_expression = Expression.compile(['value', '.', 'priority', '==', 'high'])
query_plan = WherePlan(filter=filter_expression, based_on=query_plan)

# Execute the query
for item in query_plan.execute():
    # Process results
    task = item.value
    print(f"High priority task: {task.title}")
```

### Extending the Example

You can extend this example in several ways:

1. **Add More Query Types**: Implement more complex queries using JoinPlan, GroupByPlan, or other query plans.
2. **Use Different Storage Backends**: Modify the example to use ClusterFileStorage or CloudFileStorage.
3. **Add User Interface**: Build a simple CLI or web interface on top of the TaskManager class.
4. **Implement Task Dependencies**: Extend the data model to support task dependencies, for example by adding a
   `tr.new_list()` to a `Task` object.

## Simple Example

The Simple Example (`simple_example.py`) provides a minimal demonstration of ProtoBase's core functionality, showing how
to store and retrieve different data structures within transactions. This example is ideal for getting started with
ProtoBase and understanding its basic concepts.

### Running the Example

To run the Simple Example:

```bash
# Navigate to the examples directory
cd examples

# Run the simple example
python simple_example.py
```

## Performance Benchmarks

The performance benchmark scripts demonstrate how to measure the performance of ProtoBase using standard benchmarks for
object databases. They measure:

1. **Insert performance**: Adding objects to the database
2. **Read performance**: Retrieving objects from the database
3. **Update performance**: Modifying objects in the database
4. **Delete performance**: Removing objects from the database
5. **Query performance**: Filtering and sorting objects

There are several benchmark scripts available, each with a different level of complexity:

- **minimal_benchmark.py**: A very simple benchmark that just tests basic database operations
- **update_benchmark.py**: A focused benchmark that tests update performance
- **simple_performance_benchmark.py**: A comprehensive benchmark that tests all operations with a simple implementation
- **db_performance_benchmark.py**: A more advanced benchmark with additional features
- **performance_benchmark.py**: The most comprehensive benchmark with a class-based implementation

### Running the Benchmarks

To run the benchmarks:

```bash
# Navigate to the examples directory
cd examples

# Run the minimal benchmark
python minimal_benchmark.py

# Run the update benchmark with a specific number of items
python update_benchmark.py --count 10

# Run the simple performance benchmark for a specific operation
python simple_performance_benchmark.py --benchmark insert --count 10

# Run the db performance benchmark with file storage
python db_performance_benchmark.py --storage file --benchmark all --count 10 --queries 5

# Run the performance benchmark with specific parameters
python performance_benchmark.py --storage memory --benchmark query --count 10 --queries 5
```

### Key Concepts Demonstrated

#### Benchmarking Different Operations

The benchmarks measure the performance of different database operations:

```python
# Insert benchmark
tr = database.new_transaction()
items_dict = tr.new_dictionary()
for i in range(count):
    item = BenchmarkItem(id=f"item-{i}", name=f"Item {i}", value=i * 10)
    items_dict = items_dict.set_at(item.id, item)
tr.set_root_object('items', items_dict)
tr.commit()

# Read benchmark
tr = database.new_transaction()
items_dict = tr.get_root_object('items')
for item_id in selected_ids:
    item = items_dict.get_at(item_id)
    # Access attributes to ensure they're loaded
    _ = item.name
    _ = item.value
tr.commit()

# Update benchmark
tr = database.new_transaction()
updated_dict = tr.new_dictionary()
for i, item_id in enumerate(selected_ids):
    updated_item = BenchmarkItem(id=item_id, name=f"Updated Item {i}", value=random.randint(1, 1000))
    updated_dict = updated_dict.set_at(item_id, updated_item)
tr.set_root_object('items', updated_dict)
tr.commit()

# Delete benchmark
tr = database.new_transaction()
items_dict = tr.get_root_object('items')
for item_id in selected_ids:
    items_dict = items_dict.remove_at(item_id)
tr.set_root_object('items', items_dict)
tr.commit()

# Query benchmark
tr = database.new_transaction()
items_dict = tr.get_root_object('items')
results = []
for item_key, item_value in items_dict.as_iterable():
    if item_value.category == category and item_value.status == status:
        results.append(item_value)
tr.commit()
```

#### Performance Measurement

The benchmarks measure the time taken for each operation and calculate the average time per item or query:

```python
start_time = time.time()
# Perform operation
elapsed_time = time.time() - start_time
print(f"Operation completed in {elapsed_time:.4f} seconds")
print(f"Average time per item: {(elapsed_time / count) * 1000:.4f} ms")
```

### Extending the Benchmarks

You can extend these benchmarks in several ways:

1. **Add More Operations**: Implement benchmarks for other operations like bulk operations or transactions.
2. **Use Different Storage Backends**: Modify the benchmarks to use different storage backends like ClusterFileStorage
   or CloudFileStorage.
3. **Add More Complex Data Models**: Extend the data model to include more complex relationships between objects.
4. **Implement Concurrent Benchmarks**: Add benchmarks for concurrent operations to test performance under load.

## Performance Characteristics

Comprehensive performance testing with significant data volumes reveals the following characteristics of ProtoBase:

### Performance Metrics

The following metrics were observed in tests with varying dataset sizes:

#### Small Dataset (1,000 items)

- **Insert**: ~500 items/second
- **Read**: ~25,000 items/second
- **Update**: ~300 items/second
- **Delete**: ~270 items/second
- **Query**: ~1 query/second (with each query processing ~50 items)

#### Medium Dataset (10,000 items)

- **Insert**: ~4,000 items/second
- **Read**: ~20,500 items/second
- **Update**: ~375 items/second
- **Query**: Performance degrades significantly with larger datasets

### Scaling Characteristics

1. **Read Operations**: ProtoBase excels at read operations, which scale well with increasing dataset size. The binary
   search algorithm used in dictionary lookups provides efficient O(log n) performance.

2. **Insert Operations**: Insert performance improves with larger batches, showing good scaling characteristics. This is
   likely due to amortized costs of transaction management.

3. **Update Operations**: Updates maintain consistent performance across dataset sizes, with a slight improvement in
   larger datasets.

4. **Delete Operations**: Delete operations encounter challenges with larger datasets, particularly when removing
   multiple items in sequence. This suggests potential optimization opportunities in the dictionary's remove_at
   implementation.

5. **Query Operations**: Query performance is the main bottleneck for large datasets. The current implementation
   performs full scans of the dictionary for each query, resulting in O(n) complexity that doesn't scale well.

### Storage Considerations

1. **In-Memory Storage**: Provides the best performance for all operations and is recommended for applications with
   moderate data volumes or where persistence isn't critical.

2. **File-Based Storage**: Requires proper serialization of custom objects. Performance is significantly lower than
   in-memory storage but provides persistence.

### Comparison with Other Systems

When compared to other object storage systems:

1. **Relational Databases (e.g., SQLite, PostgreSQL)**:
    - ProtoBase offers superior read performance for direct key lookups
    - SQL databases excel at complex queries and joins, which ProtoBase struggles with
    - SQL databases provide better indexing options for query optimization

2. **Document Stores (e.g., MongoDB)**:
    - Similar performance characteristics for basic CRUD operations
    - Document stores typically offer better query performance on large datasets
    - ProtoBase provides stronger transactional guarantees

3. **Key-Value Stores (e.g., Redis)**:
    - Redis offers faster raw performance for simple operations
    - ProtoBase provides richer object modeling capabilities
    - ProtoBase's transaction model offers stronger consistency guarantees

4. **Object-Relational Mappers (e.g., SQLAlchemy)**:
    - ProtoBase eliminates the object-relational impedance mismatch
    - ORMs typically offer more mature query optimization
    - ProtoBase provides a more natural Python object model

### Recommended Usage Scenarios

Based on performance characteristics, ProtoBase is well-suited for:

1. **Read-Heavy Applications**: Applications that perform frequent reads but infrequent writes
2. **Small to Medium Datasets**: Applications with data volumes in the thousands to tens of thousands of objects
3. **Object-Oriented Domain Models**: Systems where the object model is complex and maintaining object relationships is
   important
4. **Transactional Integrity Requirements**: Applications requiring ACID guarantees for data operations

For applications with very large datasets or complex query requirements, consider:

1. Using ProtoBase with a more optimized query strategy
2. Implementing custom indexes for frequently queried attributes
3. Partitioning data to reduce the size of individual collections
4. Using a hybrid approach with ProtoBase for object storage and a specialized database for complex queries
