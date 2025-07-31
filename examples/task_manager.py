#!/usr/bin/env python3
"""
Task Manager Example Application

This example demonstrates a simple task management application built with ProtoBase.
It showcases key features of ProtoBase including:
- Transactions for data consistency
- Object-oriented data modeling with DBObject
- Query capabilities for filtering and sorting tasks
- Persistence with file-based storage
"""

import os
import sys
import uuid
from datetime import datetime

# Add the parent directory to the path to import proto_db
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proto_db import ObjectSpace, DBObject
from proto_db.standalone_file_storage import StandaloneFileStorage
from proto_db.file_block_provider import FileBlockProvider
from proto_db.memory_storage import MemoryStorage
from proto_db.queries import WherePlan, Expression


class Task(DBObject):
    """
    A class representing a Task, stored as a DBObject.
    """
    pass


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
            storage_path (str): The directory path for file storage.
        """
        if storage_type == "memory":
            self.storage = MemoryStorage()
            print("Using in-memory storage (data will be lost when the application exits)")
        else:
            os.makedirs(storage_path, exist_ok=True)
            block_provider = FileBlockProvider(space_path=storage_path)
            self.storage = StandaloneFileStorage(block_provider=block_provider)
            print(f"Using file-based storage at: {os.path.abspath(storage_path)}")

        self.object_space = ObjectSpace(storage=self.storage)

        try:
            self.database = self.object_space.open_database('TaskManager')
            print("Loaded existing database")
        except Exception:
            self.database = self.object_space.new_database('TaskManager')
            print("Created new database")
            tr = self.database.new_transaction()
            tr.set_root_object('tasks', tr.new_dictionary())
            tr.commit()

    def add_task(self, title, description="", priority="medium", due_date=None):
        """
        Add a new task to the database.
        """
        if not title:
            raise ValueError("Task title cannot be empty")
        if priority not in ["low", "medium", "high"]:
            raise ValueError("Priority must be 'low', 'medium', or 'high'")

        tr = self.database.new_transaction()
        tasks_dict = tr.get_root_object('tasks')
        task_id = str(uuid.uuid4())

        task_data = {
            "id": task_id,
            "title": title,
            "description": description,
            "priority": priority,
            "status": "pending",
            "created_at": datetime.now().isoformat()
        }
        if due_date:
            task_data["due_date"] = due_date

        task = Task(**task_data)
        tasks_dict = tasks_dict.set(task_id, task)
        tr.set_root_object('tasks', tasks_dict)
        tr.commit()

        print(f"Added task: {title} (ID: {task_id})")
        return task_id

    def update_task(self, task_id, **kwargs):
        """
        Update an existing task.
        """
        tr = self.database.new_transaction()
        tasks_dict = tr.get_root_object('tasks')

        if not tasks_dict.has(task_id):
            print(f"Task with ID {task_id} not found")
            return False

        old_task = tasks_dict.get(task_id)
        task_props = {
            'id': old_task.id,
            'title': old_task.title,
            'description': old_task.description,
            'priority': old_task.priority,
            'status': old_task.status,
            'created_at': old_task.created_at
        }
        if hasattr(old_task, 'due_date'):
            task_props['due_date'] = old_task.due_date

        for key, value in kwargs.items():
            if key in ["title", "description", "priority", "status", "due_date"]:
                task_props[key] = value

        new_task = Task(**task_props)
        tasks_dict = tasks_dict.set(task_id, new_task)
        tr.set_root_object('tasks', tasks_dict)
        tr.commit()

        print(f"Updated task: {task_id}")
        return True

    def delete_task(self, task_id):
        """
        Delete a task.
        """
        tr = self.database.new_transaction()
        tasks_dict = tr.get_root_object('tasks')

        if not tasks_dict.has(task_id):
            print(f"Task with ID {task_id} not found")
            return False

        tasks_dict = tasks_dict.remove(task_id)
        tr.set_root_object('tasks', tasks_dict)
        tr.commit()

        print(f"Deleted task: {task_id}")
        return True

    def _task_to_dict(self, task):
        """Converts a Task DBObject to a Python dictionary."""
        task_dict = {
            'id': task.id,
            'title': task.title,
            'description': task.description,
            'priority': task.priority,
            'status': task.status,
            'created_at': task.created_at,
        }
        if hasattr(task, 'due_date'):
            task_dict['due_date'] = task.due_date
        return task_dict

    def get_task(self, task_id):
        """
        Get a task by ID.
        """
        tr = self.database.new_transaction()
        tasks_dict = tr.get_root_object('tasks')

        if not tasks_dict.has(task_id):
            print(f"Task with ID {task_id} not found")
            return None

        task = tasks_dict.get(task_id)
        return self._task_to_dict(task)

    def get_all_tasks(self):
        """
        Get all tasks.
        """
        tr = self.database.new_transaction()
        tasks_dict = tr.get_root_object('tasks')
        tasks = []
        for task_id in tasks_dict.keys():
            task = tasks_dict.get(task_id)
            tasks.append(self._task_to_dict(task))
        return tasks

    def query_tasks(self, status=None, priority=None, sort_by="created_at", ascending=True):
        """
        Query tasks based on criteria.
        """
        tr = self.database.new_transaction()
        tasks_dict = tr.get_root_object('tasks')
        query_plan = tasks_dict.as_query_plan()

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

        results = [self._task_to_dict(item.value) for item in query_plan.execute()]

        if sort_by and results:
            def get_sort_key(task_dict):
                return task_dict.get(sort_by, "" if ascending else "zzzzzzzzz")

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
    manager = TaskManager(storage_type="file")

    task1_id = manager.add_task(
        title="Implement user authentication",
        description="Add login/logout functionality with JWT tokens",
        priority="high",
        due_date="2025-08-15"
    )
    task2_id = manager.add_task(
        title="Optimize database queries",
        description="Improve performance of slow queries",
        priority="medium",
        due_date="2025-08-20"
    )
    task3_id = manager.add_task(
        title="Write documentation",
        description="Create user and developer documentation",
        priority="low",
        due_date="2025-08-30"
    )

    manager.update_task(task1_id, status="in_progress")

    print("\nHigh priority tasks:")
    for task in manager.query_tasks(priority="high"):
        print_task(task)

    print("\nPending tasks sorted by due_date:")
    for task in manager.query_tasks(status="pending", sort_by="due_date"):
        print_task(task)

    manager.delete_task(task3_id)

    print("\nAll tasks after deletion:")
    for task in manager.get_all_tasks():
        print_task(task)


if __name__ == "__main__":
    demo()