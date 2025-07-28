# ProtoBase Examples

This directory contains example applications that demonstrate the capabilities of ProtoBase, a transactional, object-oriented database system implemented in Python.

> **Note**: These examples are provided for illustrative purposes to demonstrate the intended usage of ProtoBase. Depending on your specific version of ProtoBase, you may need to make adjustments to the code to run the examples successfully.

## Task Manager Example

The Task Manager example demonstrates a simple but powerful task management application built with ProtoBase. It showcases key features including:

- **Transactional Operations**: All database operations are performed within transactions that ensure data consistency
- **Rich Data Structures**: Using ProtoBase's Dictionary data structure for storing and retrieving tasks
- **Query Capabilities**: Filtering and sorting tasks using ProtoBase's query system
- **Persistence**: Storing tasks in a file-based database that persists between application runs

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

#### Transactions

```python
# Create a new transaction
tr = self.database.new_transaction()

# Perform operations
tasks_dict = tr.get_root_object('tasks')
tasks_dict = tasks_dict.set(task_id, task)
tr.set_root_object('tasks', tasks_dict)

# Commit the transaction
tr.commit()
```

#### Data Structures

```python
# Create a new dictionary
task = tr.new_dictionary()

# Set properties
task = task.set("title", title)
task = task.set("priority", priority)

# Check if a key exists
if task.has(key):
    value = task.get(key)
```

#### Queries

```python
# Create a query plan
query_plan = tasks_dict.as_query_plan()

# Apply filters
filter_expression = Expression.compile(['value', '.', 'status', '==', status])
query_plan = WherePlan(filter=filter_expression, based_on=query_plan)

# Apply sorting
query_plan = OrderByPlan(
    order_by=[('value', '.', sort_by)], 
    ascending=ascending,
    based_on=query_plan
)

# Execute the query
for item in query_plan.execute():
    # Process results
    task = item.value
```

### Extending the Example

You can extend this example in several ways:

1. **Add More Query Types**: Implement more complex queries using JoinPlan, GroupByPlan, or other query plans
2. **Use Different Storage Backends**: Modify the example to use ClusterFileStorage or CloudFileStorage
3. **Add User Interface**: Build a simple CLI or web interface on top of the TaskManager class
4. **Implement Task Dependencies**: Extend the data model to support task dependencies

## Simple Example

The Simple Example (`simple_example.py`) provides a minimal demonstration of ProtoBase's core functionality:

- **In-Memory Storage**: Using ProtoBase's MemoryStorage for quick, non-persistent data storage
- **Basic Transactions**: Creating and committing transactions
- **Data Structures**: Working with Dictionary and List data structures
- **Root Objects**: Storing and retrieving objects from the database

### Running the Example

To run the Simple Example:

```bash
# Navigate to the examples directory
cd examples

# Run the simple example
python simple_example.py
```

The example will:
1. Create an in-memory database
2. Store a string, a dictionary, and a list in the database
3. Commit the transaction
4. Create a new transaction to retrieve the stored data
5. Display the retrieved data

This example is ideal for getting started with ProtoBase and understanding its basic concepts before moving on to more complex examples.

## Future Examples

More examples will be added in the future to demonstrate other features of ProtoBase, such as:

- Using different storage backends (ClusterFileStorage, CloudFileStorage)
- Working with other data structures (List, Set, HashDictionary)
- Implementing more complex query patterns
- Building real-time collaborative applications
