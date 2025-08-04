#!/usr/bin/env python3
"""
ProtoBase Update Benchmark

This example demonstrates how to measure the performance of updating items
in a ProtoBase database. It follows a simple pattern that ensures compatibility
with the ProtoBase API.
"""

import os
import random
import sys
import time

# Add the parent directory to the path to import proto_db
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proto_db import ObjectSpace, DBObject
from proto_db.memory_storage import MemoryStorage


class BenchmarkItem(DBObject):
    """
    A class representing an item for benchmarking.
    """
    pass


def main():
    """Run the update benchmark."""
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='ProtoBase Update Benchmark')
    parser.add_argument('--count', type=int, default=10,
                        help='Number of items for benchmark')
    args = parser.parse_args()

    count = args.count

    print("\n" + "=" * 50)
    print("UPDATE BENCHMARK")
    print("=" * 50)
    print(f"Updating {count} items")

    # Create storage
    print("\nCreating in-memory storage...")
    storage = MemoryStorage()

    # Create object space
    print("Creating object space...")
    object_space = ObjectSpace(storage=storage)

    # Create database
    print("Creating database...")
    database = object_space.new_database('UpdateBenchmarkDB')

    # Create a transaction to set up the database
    print("\nCreating initial transaction...")
    tr = database.new_transaction()

    # Create a dictionary to store items
    print("Creating a dictionary...")
    items_dict = tr.new_dictionary()

    # Add items to the dictionary
    print(f"Adding {count} items to the dictionary...")
    item_ids = []
    for i in range(count):
        item_id = f"item-{i}"
        item_ids.append(item_id)
        item = BenchmarkItem(
            id=item_id,
            name=f"Item {i}",
            value=i * 10,
            category=f"Category {i % 5}",
            status="active"
        )
        items_dict = items_dict.set_at(item_id, item)

    # Store the dictionary as a root object
    print("Setting root object...")
    tr.set_root_object('items', items_dict)

    # Commit the transaction
    print("Committing initial transaction...")
    tr.commit()

    # Start timing
    print("\nStarting update benchmark...")
    start_time = time.time()

    # Create a new transaction for updating
    tr = database.new_transaction()

    # Get the items dictionary
    items_dict = tr.get_root_object('items')

    # Create a new dictionary for updated items
    updated_dict = tr.new_dictionary()

    # Update all items
    for i, item_id in enumerate(item_ids):
        # Create a new item with updated properties
        updated_item = BenchmarkItem(
            id=item_id,
            name=f"Updated Item {i}",
            value=random.randint(1, 1000),
            category=f"Category {i % 5}",
            status="updated"
        )

        # Add to the updated dictionary
        updated_dict = updated_dict.set_at(item_id, updated_item)

    # Store the updated dictionary
    tr.set_root_object('items', updated_dict)

    # Commit the transaction
    tr.commit()

    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    print(f"Update completed in {elapsed_time:.4f} seconds")
    print(f"Average time per item: {(elapsed_time / count) * 1000:.4f} ms")

    # Verify the updates
    print("\nVerifying updates...")
    tr = database.new_transaction()
    items_dict = tr.get_root_object('items')

    # Print a few items to verify
    print("\nUpdated items (first 3):")
    for i, (item_key, item_value) in enumerate(items_dict.as_iterable()):
        if i >= 3:
            break
        print(f"  {item_key}: {item_value.name} (value: {item_value.value}, status: {item_value.status})")

    # Commit the transaction
    tr.commit()

    print("\nUpdate benchmark completed successfully!")


if __name__ == "__main__":
    main()
