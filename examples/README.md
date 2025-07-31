# ProtoBase Examples

This directory contains example applications that demonstrate the capabilities of ProtoBase, a transactional, object-oriented database system implemented in Python.

> **Note**: These examples are provided for illustrative purposes to demonstrate the intended usage of ProtoBase. Depending on your specific version of ProtoBase, you may need to make adjustments to the code to run the examples successfully.

## Task Manager Example

The Task Manager example demonstrates a simple but powerful task management application built with ProtoBase. It showcases key features including:

- **Transactional Operations**: All database operations are performed within transactions that ensure data consistency.
- **Object-Oriented Data Modeling**: Using `DBObject` to represent data as Python objects, making the code more intuitive and readable.
- **Rich Data Structures**: Employing ProtoBase's `Dictionary` to organize and store `Task` objects.
- **Powerful Query Capabilities**: Filtering and sorting tasks by their object attributes using ProtoBase's query system.
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

ProtoBase allows you to model your data using Python classes that inherit from `DBObject`. This makes data manipulation more intuitive and your code easier to read and maintain.

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
4. **Implement Task Dependencies**: Extend the data model to support task dependencies, for example by adding a `tr.new_list()` to a `Task` object.

## Simple Example

The Simple Example (`simple_example.py`) provides a minimal demonstration of ProtoBase's core functionality, showing how to store and retrieve different data structures within transactions. This example is ideal for getting started with ProtoBase and understanding its basic concepts.

### Running the Example

To run the Simple Example:

```bash
# Navigate to the examples directory
cd examples

# Run the simple example
python simple_example.py
```