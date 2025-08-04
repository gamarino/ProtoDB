#!/usr/bin/env python3
"""
Minimal ProtoBase Performance Benchmark

This is a simplified version of the performance benchmark that just tests
basic database operations to ensure they work correctly.
"""

import os
import sys

# Add the parent directory to the path to import proto_db
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proto_db import ObjectSpace, DBObject
from proto_db.memory_storage import MemoryStorage


class TestItem(DBObject):
    """
    A class representing a test item.
    """
    pass


def main():
    """Run a minimal benchmark."""
    print("ProtoBase Minimal Benchmark")
    print("==========================")

    # Create an in-memory storage
    print("\nCreating in-memory storage...")
    storage = MemoryStorage()

    # Create an object space with the storage
    print("Creating object space...")
    object_space = ObjectSpace(storage=storage)

    # Create a new database
    print("Creating database...")
    database = object_space.new_database('MinimalBenchmarkDB')

    # Create a new transaction
    print("\nCreating transaction...")
    tr = database.new_transaction()

    # Create a dictionary to store items
    print("Creating a dictionary...")
    items_dict = tr.new_dictionary()

    # Add some items to the dictionary
    print("Adding items to the dictionary...")
    for i in range(5):
        item_id = f"item-{i}"
        item = TestItem(
            id=item_id,
            name=f"Test Item {i}",
            value=i * 10
        )
        items_dict = items_dict.set_at(item_id, item)

    # Store the dictionary as a root object
    print("Setting root object...")
    tr.set_root_object('items', items_dict)

    # Commit the transaction
    print("Committing transaction...")
    tr.commit()

    # Create a new transaction to read the items
    print("\nCreating new transaction to read items...")
    tr = database.new_transaction()

    # Get the items dictionary
    print("Getting items dictionary...")
    items_dict = tr.get_root_object('items')

    # Print the items
    print("\nItems in the database:")
    for item_key, item_value in items_dict.as_iterable():
        print(f"  {item_key}: {item_value.name} (value: {item_value.value})")

    # Commit the transaction
    print("\nCommitting read transaction...")
    tr.commit()

    print("\nMinimal benchmark completed successfully!")


if __name__ == "__main__":
    main()
