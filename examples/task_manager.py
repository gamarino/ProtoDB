#!/usr/bin/env python3
"""
Task Manager Example Application

This example demonstrates a simple task management application built with ProtoBase.
It showcases key features of ProtoBase including:
- Transactions for data consistency
- Rich data structures (Dictionary, List)
- Query capabilities for filtering and sorting tasks
- Persistence with file-based storage

This application allows you to:
- Create, update, and delete tasks
- Assign priorities and status to tasks
- Query tasks based on various criteria
- Persist tasks between application runs
"""

import os
import sys
import uuid
from datetime import datetime

# Add the parent directory to the path to import proto_db
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proto_db.memory_storage import MemoryStorage
from proto_db.standalone_file_storage import StandaloneFileStorage
from proto_db.file_block_provider import FileBlockProvider
from proto_db.db_access import ObjectSpace, Database
from proto_db.queries import WherePlan, Expression


class TaskManager:
    """
    A task management application built with ProtoBase.

    This class demonstrates how to use ProtoBase to build a simple but powerful
    application with persistent storage, transactions, and query capabilities.
    """

    def __init__(self, storage_type="memory", storage_path="task_manager_data"):
        """
        Initialize the TaskManager with the specified storage type.

        Args:
            storage_type (str): The type of storage to use ("memory" or "file").
            storage_path (str): The directory path for file storage (used only if storage_type is "file").
        """
        # Create the appropriate storage
        if storage_type == "memory":
            self.storage = MemoryStorage()
            print("Using in-memory storage (data will be lost when the application exits)")
        else:
            # Create directory if it doesn't exist
            os.makedirs(storage_path, exist_ok=True)
            block_provider = FileBlockProvider(space_path=storage_path)
            self.storage = StandaloneFileStorage(block_provider=block_provider)
            print(f"Using file-based storage at: {os.path.abspath(storage_path)}")

        # Create object space and database
        self.object_space = ObjectSpace(storage=self.storage)

        # Check if database exists, if not create it
        try:
            self.database = self.object_space.open_database('TaskManager')
            print("Loaded existing database")
        except:
            self.database = self.object_space.new_database('TaskManager')
            print("Created new database")

            # Initialize the database with empty task collections
            tr = self.database.new_transaction()
            tasks_dict = tr.new_dictionary()
            tr.set_root_object('tasks', tasks_dict)
            tr.commit()

    def add_task(self, title, description="", priority="medium", due_date=None):
        """
        Add a new task to the database.

        Args:
            title (str): The title of the task.
            description (str): The description of the task.
            priority (str): The priority of the task ("low", "medium", or "high").
            due_date (str): The due date of the task in YYYY-MM-DD format.

        Returns:
            str: The ID of the newly created task.
        """
        # Validate inputs
        if not title:
            raise ValueError("Task title cannot be empty")

        if priority not in ["low", "medium", "high"]:
            raise ValueError("Priority must be 'low', 'medium', or 'high'")

        # Create a new transaction
        tr = self.database.new_transaction()

        # Get the tasks dictionary
        tasks_dict = tr.get_root_object('tasks')

        # Create a new task dictionary
        task = tr.new_dictionary()

        # Generate a unique ID for the task
        task_id = str(uuid.uuid4())

        # Set task properties
        task = task.set("id", task_id)
        task = task.set("title", title)
        task = task.set("description", description)
        task = task.set("priority", priority)
        task = task.set("status", "pending")
        task = task.set("created_at", datetime.now().isoformat())

        if due_date:
            task = task.set("due_date", due_date)

        # Add the task to the tasks dictionary
        tasks_dict = tasks_dict.set(task_id, task)

        # Update the root object
        tr.set_root_object('tasks', tasks_dict)

        # Commit the transaction
        tr.commit()

        print(f"Added task: {title} (ID: {task_id})")
        return task_id

    def update_task(self, task_id, **kwargs):
        """
        Update an existing task.

        Args:
            task_id (str): The ID of the task to update.
            **kwargs: The task properties to update.

        Returns:
            bool: True if the task was updated, False otherwise.
        """
        # Create a new transaction
        tr = self.database.new_transaction()

        # Get the tasks dictionary
        tasks_dict = tr.get_root_object('tasks')

        # Check if the task exists
        if not tasks_dict.has(task_id):
            print(f"Task with ID {task_id} not found")
            return False

        # Get the task
        task = tasks_dict.get(task_id)

        # Update the task properties
        for key, value in kwargs.items():
            if key in ["title", "description", "priority", "status", "due_date"]:
                task = task.set(key, value)

        # Update the task in the tasks dictionary
        tasks_dict = tasks_dict.set(task_id, task)

        # Update the root object
        tr.set_root_object('tasks', tasks_dict)

        # Commit the transaction
        tr.commit()

        print(f"Updated task: {task_id}")
        return True

    def delete_task(self, task_id):
        """
        Delete a task.

        Args:
            task_id (str): The ID of the task to delete.

        Returns:
            bool: True if the task was deleted, False otherwise.
        """
        # Create a new transaction
        tr = self.database.new_transaction()

        # Get the tasks dictionary
        tasks_dict = tr.get_root_object('tasks')

        # Check if the task exists
        if not tasks_dict.has(task_id):
            print(f"Task with ID {task_id} not found")
            return False

        # Remove the task from the tasks dictionary
        tasks_dict = tasks_dict.remove(task_id)

        # Update the root object
        tr.set_root_object('tasks', tasks_dict)

        # Commit the transaction
        tr.commit()

        print(f"Deleted task: {task_id}")
        return True

    def get_task(self, task_id):
        """
        Get a task by ID.

        Args:
            task_id (str): The ID of the task to get.

        Returns:
            dict: The task as a dictionary, or None if not found.
        """
        # Create a new transaction
        tr = self.database.new_transaction()

        # Get the tasks dictionary
        tasks_dict = tr.get_root_object('tasks')

        # Check if the task exists
        if not tasks_dict.has(task_id):
            print(f"Task with ID {task_id} not found")
            return None

        # Get the task
        task = tasks_dict.get(task_id)

        # Convert the task to a dictionary
        task_dict = {}
        for key in ["id", "title", "description", "priority", "status", "created_at", "due_date"]:
            if task.has(key):
                task_dict[key] = task.get(key)

        return task_dict

    def get_all_tasks(self):
        """
        Get all tasks.

        Returns:
            list: A list of all tasks as dictionaries.
        """
        # Create a new transaction
        tr = self.database.new_transaction()

        # Get the tasks dictionary
        tasks_dict = tr.get_root_object('tasks')

        # Convert the tasks to a list of dictionaries
        tasks = []
        for task_id in tasks_dict.keys():
            task = tasks_dict.get(task_id)
            task_dict = {}
            for key in ["id", "title", "description", "priority", "status", "created_at", "due_date"]:
                if task.has(key):
                    task_dict[key] = task.get(key)
            tasks.append(task_dict)

        return tasks

    def query_tasks(self, status=None, priority=None, sort_by="created_at", ascending=True):
        """
        Query tasks based on criteria.

        This method demonstrates the use of ProtoBase's query system to filter and sort tasks.

        Args:
            status (str): Filter tasks by status.
            priority (str): Filter tasks by priority.
            sort_by (str): Sort tasks by this field.
            ascending (bool): Sort in ascending order if True, descending if False.

        Returns:
            list: A list of tasks matching the criteria.
        """
        # Create a new transaction
        tr = self.database.new_transaction()

        # Get the tasks dictionary
        tasks_dict = tr.get_root_object('tasks')

        # Create a query plan from the tasks dictionary
        query_plan = tasks_dict.as_query_plan()

        # Apply filters if specified
        if status or priority:
            filter_parts = []

            if status:
                filter_parts.extend(['value', '.', 'status', '==', status])

            if priority:
                if filter_parts:
                    filter_parts.append('and')
                filter_parts.extend(['value', '.', 'priority', '==', priority])

            filter_expression = Expression.compile(filter_parts)
            query_plan = WherePlan(filter=filter_expression, based_on=query_plan)

        # Execute the query and convert results to dictionaries
        results = []
        for item in query_plan.execute():
            task = item.value
            task_dict = {}
            for key in ["id", "title", "description", "priority", "status", "created_at", "due_date"]:
                if task.has(key):
                    task_dict[key] = task.get(key)
            results.append(task_dict)

        # Apply manual sorting if specified
        if sort_by and results:
            # Define a key function for sorting
            def get_sort_key(task_dict):
                # Handle missing keys by using a default value
                if sort_by in task_dict:
                    return task_dict[sort_by]
                return "" if ascending else "zzzzzzzzz"  # Default value based on sort direction

            # Sort the results
            results.sort(key=get_sort_key, reverse=not ascending)

        return results


def print_task(task):
    """Print a task in a formatted way."""
    print(f"ID: {task['id']}")
    print(f"Title: {task['title']}")
    print(f"Description: {task.get('description', '')}")
    print(f"Priority: {task['priority']}")
    print(f"Status: {task['status']}")
    print(f"Created: {task['created_at']}")
    if 'due_date' in task:
        print(f"Due: {task['due_date']}")
    print("-" * 40)


def demo():
    """Run a demonstration of the TaskManager."""
    # Create a TaskManager with file-based storage
    manager = TaskManager(storage_type="file")

    # Add some sample tasks
    task1_id = manager.add_task(
        title="Implement user authentication",
        description="Add login/logout functionality with JWT tokens",
        priority="high",
        due_date="2023-12-15"
    )

    task2_id = manager.add_task(
        title="Optimize database queries",
        description="Improve performance of slow queries",
        priority="medium",
        due_date="2023-12-20"
    )

    task3_id = manager.add_task(
        title="Write documentation",
        description="Create user and developer documentation",
        priority="low",
        due_date="2023-12-30"
    )

    # Update a task
    manager.update_task(task1_id, status="in_progress")

    # Query tasks by priority
    print("\nHigh priority tasks:")
    high_priority_tasks = manager.query_tasks(priority="high")
    for task in high_priority_tasks:
        print_task(task)

    # Query tasks by status
    print("\nPending tasks sorted by priority (highest first):")
    pending_tasks = manager.query_tasks(status="pending", sort_by="priority", ascending=False)
    for task in pending_tasks:
        print_task(task)

    # Get all tasks
    print("\nAll tasks:")
    all_tasks = manager.get_all_tasks()
    for task in all_tasks:
        print_task(task)

    # Delete a task
    manager.delete_task(task3_id)

    print("\nAfter deleting a task:")
    all_tasks = manager.get_all_tasks()
    for task in all_tasks:
        print_task(task)


if __name__ == "__main__":
    demo()
