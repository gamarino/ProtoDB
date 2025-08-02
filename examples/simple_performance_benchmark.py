#!/usr/bin/env python3
"""
ProtoBase Simple Performance Benchmark

This example demonstrates how to measure the performance of ProtoBase
using standard benchmarks for object databases. It follows a simple pattern
that ensures compatibility with the ProtoBase API.
"""

import os
import sys
import time
import uuid
import random
import argparse
from datetime import datetime

# Add the parent directory to the path to import proto_db
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proto_db import ObjectSpace, DBObject
from proto_db.standalone_file_storage import StandaloneFileStorage
from proto_db.file_block_provider import FileBlockProvider
from proto_db.memory_storage import MemoryStorage
from proto_db.queries import WherePlan, Expression


class BenchmarkItem(DBObject):
    """
    A class representing an item for benchmarking.
    """
    pass


def run_insert_benchmark(count=10):
    """Run the insert benchmark."""
    print("\n" + "=" * 50)
    print("INSERT BENCHMARK")
    print("=" * 50)
    print(f"Inserting {count} items")

    # Create storage
    print("\nCreating in-memory storage...")
    storage = MemoryStorage()

    # Create object space
    print("Creating object space...")
    object_space = ObjectSpace(storage=storage)

    # Create database
    print("Creating database...")
    database = object_space.new_database('InsertBenchmarkDB')

    # Create categories and statuses for random item generation
    categories = ["category1", "category2", "category3", "category4", "category5"]
    statuses = ["active", "inactive", "pending", "archived"]

    # Start timing
    start_time = time.time()

    # Create a transaction
    tr = database.new_transaction()

    # Create a dictionary to store items
    items_dict = tr.new_dictionary()

    # Add items to the dictionary
    for _ in range(count):
        item_id = str(uuid.uuid4())
        item = BenchmarkItem(
            id=item_id,
            name=f"Item-{random.randint(1000, 9999)}",
            value=random.randint(1, 1000),
            category=random.choice(categories),
            status=random.choice(statuses),
            created_at=datetime.now().isoformat(),
            tags=[f"tag{i}" for i in range(random.randint(1, 5))]
        )
        items_dict = items_dict.set_at(item_id, item)

    # Store the dictionary as a root object
    tr.set_root_object('items', items_dict)

    # Commit the transaction
    tr.commit()

    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    print(f"Insert completed in {elapsed_time:.4f} seconds")
    print(f"Average time per item: {(elapsed_time / count) * 1000:.4f} ms")

    return elapsed_time


def run_read_benchmark(count=10):
    """Run the read benchmark."""
    print("\n" + "=" * 50)
    print("READ BENCHMARK")
    print("=" * 50)
    print(f"Reading {count} items")

    # Create storage
    print("\nCreating in-memory storage...")
    storage = MemoryStorage()

    # Create object space
    print("Creating object space...")
    object_space = ObjectSpace(storage=storage)

    # Create database
    print("Creating database...")
    database = object_space.new_database('ReadBenchmarkDB')

    # Create categories and statuses for random item generation
    categories = ["category1", "category2", "category3", "category4", "category5"]
    statuses = ["active", "inactive", "pending", "archived"]

    # Create a transaction to set up the database
    tr = database.new_transaction()

    # Create a dictionary to store items
    items_dict = tr.new_dictionary()

    # Add items to the dictionary
    print(f"Adding {count} items to the database...")
    for i in range(count):
        item_id = f"item-{i}"
        item = BenchmarkItem(
            id=item_id,
            name=f"Item-{random.randint(1000, 9999)}",
            value=random.randint(1, 1000),
            category=random.choice(categories),
            status=random.choice(statuses),
            created_at=datetime.now().isoformat(),
            tags=[f"tag{i}" for i in range(random.randint(1, 5))]
        )
        items_dict = items_dict.set_at(item_id, item)

    # Store the dictionary as a root object
    tr.set_root_object('items', items_dict)

    # Commit the transaction
    tr.commit()

    # Start timing
    start_time = time.time()

    # Create a new transaction for reading
    tr = database.new_transaction()

    # Get the items dictionary
    items_dict = tr.get_root_object('items')

    # Read all items
    for item_key, item_value in items_dict.as_iterable():
        # Access some attributes to ensure they're loaded
        _ = item_value.name
        _ = item_value.value
        _ = item_value.category

    # Commit the transaction
    tr.commit()

    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    print(f"Read completed in {elapsed_time:.4f} seconds")
    print(f"Average time per item: {(elapsed_time / count) * 1000:.4f} ms")

    return elapsed_time


def run_update_benchmark(count=10):
    """Run the update benchmark."""
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

    # Create categories and statuses for random item generation
    categories = ["category1", "category2", "category3", "category4", "category5"]
    statuses = ["active", "inactive", "pending", "archived"]

    # Create a transaction to set up the database
    tr = database.new_transaction()

    # Create a dictionary to store items
    items_dict = tr.new_dictionary()

    # Add items to the dictionary
    print(f"Adding {count} items to the database...")
    item_ids = []
    for i in range(count):
        item_id = f"item-{i}"
        item_ids.append(item_id)
        item = BenchmarkItem(
            id=item_id,
            name=f"Item-{random.randint(1000, 9999)}",
            value=random.randint(1, 1000),
            category=random.choice(categories),
            status=random.choice(statuses),
            created_at=datetime.now().isoformat(),
            tags=[f"tag{i}" for i in range(random.randint(1, 5))]
        )
        items_dict = items_dict.set_at(item_id, item)

    # Store the dictionary as a root object
    tr.set_root_object('items', items_dict)

    # Commit the transaction
    tr.commit()

    # Start timing
    start_time = time.time()

    # Create a new transaction for updating
    tr = database.new_transaction()

    # Get the items dictionary (just to verify it exists)
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
            category=random.choice(categories),
            status=random.choice(statuses),
            created_at=datetime.now().isoformat(),
            tags=[f"tag{i}" for i in range(random.randint(1, 5))]
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

    return elapsed_time


def run_delete_benchmark(count=10):
    """Run the delete benchmark."""
    print("\n" + "=" * 50)
    print("DELETE BENCHMARK")
    print("=" * 50)
    print(f"Deleting {count} items")

    # Create storage
    print("\nCreating in-memory storage...")
    storage = MemoryStorage()

    # Create object space
    print("Creating object space...")
    object_space = ObjectSpace(storage=storage)

    # Create database
    print("Creating database...")
    database = object_space.new_database('DeleteBenchmarkDB')

    # Create categories and statuses for random item generation
    categories = ["category1", "category2", "category3", "category4", "category5"]
    statuses = ["active", "inactive", "pending", "archived"]

    # Create a transaction to set up the database
    tr = database.new_transaction()

    # Create a dictionary to store items
    items_dict = tr.new_dictionary()

    # Add items to the dictionary
    print(f"Adding {count} items to the database...")
    item_ids = []
    for i in range(count):
        item_id = f"item-{i}"
        item_ids.append(item_id)
        item = BenchmarkItem(
            id=item_id,
            name=f"Item-{random.randint(1000, 9999)}",
            value=random.randint(1, 1000),
            category=random.choice(categories),
            status=random.choice(statuses),
            created_at=datetime.now().isoformat(),
            tags=[f"tag{i}" for i in range(random.randint(1, 5))]
        )
        items_dict = items_dict.set_at(item_id, item)

    # Store the dictionary as a root object
    tr.set_root_object('items', items_dict)

    # Commit the transaction
    tr.commit()

    # Start timing
    start_time = time.time()

    # Create a new transaction for deleting
    tr = database.new_transaction()

    # Get the items dictionary
    items_dict = tr.get_root_object('items')

    # Delete all items
    for item_id in item_ids:
        items_dict = items_dict.remove_at(item_id)

    # Store the updated dictionary
    tr.set_root_object('items', items_dict)

    # Commit the transaction
    tr.commit()

    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    print(f"Delete completed in {elapsed_time:.4f} seconds")
    print(f"Average time per item: {(elapsed_time / count) * 1000:.4f} ms")

    return elapsed_time


def run_query_benchmark(count=10, num_queries=5):
    """Run the query benchmark."""
    print("\n" + "=" * 50)
    print("QUERY BENCHMARK")
    print("=" * 50)
    print(f"Running {num_queries} queries on {count} items")

    # Create storage
    print("\nCreating in-memory storage...")
    storage = MemoryStorage()

    # Create object space
    print("Creating object space...")
    object_space = ObjectSpace(storage=storage)

    # Create database
    print("Creating database...")
    database = object_space.new_database('QueryBenchmarkDB')

    # Create categories and statuses for random item generation
    categories = ["category1", "category2", "category3", "category4", "category5"]
    statuses = ["active", "inactive", "pending", "archived"]

    # Create a transaction to set up the database
    tr = database.new_transaction()

    # Create a dictionary to store items
    items_dict = tr.new_dictionary()

    # Add items to the dictionary
    print(f"Adding {count} items to the database...")
    for i in range(count):
        item_id = f"item-{i}"
        item = BenchmarkItem(
            id=item_id,
            name=f"Item-{random.randint(1000, 9999)}",
            value=random.randint(1, 1000),
            category=random.choice(categories),
            status=random.choice(statuses),
            created_at=datetime.now().isoformat(),
            tags=[f"tag{i}" for i in range(random.randint(1, 5))]
        )
        items_dict = items_dict.set_at(item_id, item)

    # Store the dictionary as a root object
    tr.set_root_object('items', items_dict)

    # Commit the transaction
    tr.commit()

    # Start timing
    start_time = time.time()

    # Run queries
    for i in range(num_queries):
        # Create a new transaction for each query
        tr = database.new_transaction()

        # Get the items dictionary
        items_dict = tr.get_root_object('items')

        # Randomly choose query parameters
        category = random.choice(categories)
        status = random.choice(statuses)

        # Simple manual filtering
        results = []
        for item_key, item_value in items_dict.as_iterable():
            if hasattr(item_value, 'category') and hasattr(item_value, 'status'):
                if item_value.category == category and item_value.status == status:
                    results.append(item_value)

        # Commit the transaction
        tr.commit()

    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    print(f"Queries completed in {elapsed_time:.4f} seconds")
    print(f"Average time per query: {(elapsed_time / num_queries) * 1000:.4f} ms")

    return elapsed_time


def run_all_benchmarks(count=10, query_count=5):
    """Run all benchmarks."""
    print("\n" + "=" * 50)
    print("RUNNING ALL BENCHMARKS")
    print("=" * 50)

    insert_time = run_insert_benchmark(count)
    read_time = run_read_benchmark(count)
    update_time = run_update_benchmark(count)
    delete_time = run_delete_benchmark(count)
    query_time = run_query_benchmark(count, query_count)

    # Print summary
    print("\n" + "=" * 50)
    print("BENCHMARK SUMMARY")
    print("=" * 50)
    print(f"Insert {count} items: {insert_time:.4f} seconds ({(insert_time / count) * 1000:.4f} ms per item)")
    print(f"Read {count} items: {read_time:.4f} seconds ({(read_time / count) * 1000:.4f} ms per item)")
    print(f"Update {count} items: {update_time:.4f} seconds ({(update_time / count) * 1000:.4f} ms per item)")
    print(f"Delete {count} items: {delete_time:.4f} seconds ({(delete_time / count) * 1000:.4f} ms per item)")
    print(f"Execute {query_count} queries: {query_time:.4f} seconds ({(query_time / query_count) * 1000:.4f} ms per query)")


def main():
    """Run the performance benchmark."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='ProtoBase Simple Performance Benchmark')
    parser.add_argument('--count', type=int, default=10,
                        help='Number of items for benchmarks')
    parser.add_argument('--queries', type=int, default=5,
                        help='Number of queries for query benchmark')
    parser.add_argument('--benchmark', choices=['insert', 'read', 'update', 'delete', 'query', 'all'],
                        default='all', help='Which benchmark to run')
    args = parser.parse_args()

    # Run selected benchmark
    if args.benchmark == 'insert':
        run_insert_benchmark(args.count)
    elif args.benchmark == 'read':
        run_read_benchmark(args.count)
    elif args.benchmark == 'update':
        run_update_benchmark(args.count)
    elif args.benchmark == 'delete':
        run_delete_benchmark(args.count)
    elif args.benchmark == 'query':
        run_query_benchmark(args.count, args.queries)
    else:
        run_all_benchmarks(args.count, args.queries)


if __name__ == "__main__":
    main()
