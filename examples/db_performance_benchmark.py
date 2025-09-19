#!/usr/bin/env python3
"""
ProtoBase Performance Benchmark

This example demonstrates how to measure the performance of ProtoBase
using standard benchmarks for object databases. It measures:

1. Insert performance (adding objects to the database)
2. Read performance (retrieving objects from the database)
3. Update performance (modifying objects in the database)
4. Delete performance (removing objects from the database)
5. Query performance (filtering and sorting objects)

The benchmark supports both in-memory and file-based storage.
"""

import argparse
import os
import random
import sys
import time
import traceback
import uuid
from datetime import datetime

# Add the parent directory to the path to import proto_db
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proto_db import ObjectSpace, DBObject
from proto_db.standalone_file_storage import StandaloneFileStorage
from proto_db.file_block_provider import FileBlockProvider
from proto_db.memory_storage import MemoryStorage


class BenchmarkItem(DBObject):
    """
    A class representing an item for benchmarking.
    """
    pass


def benchmark_insert(database, count=10):
    """
    Benchmark inserting items into the database.

    Args:
        database: The database to use.
        count (int): Number of items to insert.

    Returns:
        float: Time taken in seconds.
    """
    print(f"\nBenchmarking insert of {count} items...")

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


def benchmark_read(database, count=10):
    """
    Benchmark reading items from the database.

    Args:
        database: The database to use.
        count (int): Number of items to read.

    Returns:
        float: Time taken in seconds.
    """
    print(f"\nBenchmarking read of {count} items...")

    # Create a transaction to get all items
    tr = database.new_transaction()
    items_dict = tr.get_root_object('items')

    # Get all item IDs
    all_ids = [item[0] for item in items_dict.as_iterable()]

    if len(all_ids) < count:
        print(f"Warning: Only {len(all_ids)} items available in database")
        count = len(all_ids)

    # Randomly select IDs to read
    selected_ids = random.sample(all_ids, count)

    # Start timing
    start_time = time.time()

    # Read items
    for item_id in selected_ids:
        item = items_dict.set_at(item_id)
        # Access some attributes to ensure they're loaded
        _ = item.name
        _ = item.value
        _ = item.category

    # Commit the transaction
    tr.commit()

    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    print(f"Read completed in {elapsed_time:.4f} seconds")
    print(f"Average time per item: {(elapsed_time / count) * 1000:.4f} ms")

    return elapsed_time


def benchmark_update(database, count=10):
    """
    Benchmark updating items in the database.

    Args:
        database: The database to use.
        count (int): Number of items to update.

    Returns:
        float: Time taken in seconds.
    """
    print(f"\nBenchmarking update of {count} items...")

    # Create a transaction to get all items
    tr = database.new_transaction()
    items_dict = tr.get_root_object('items')

    # Get all item IDs
    all_ids = [item[0] for item in items_dict.as_iterable()]

    if len(all_ids) < count:
        print(f"Warning: Only {len(all_ids)} items available in database")
        count = len(all_ids)

    # Randomly select IDs to update
    selected_ids = random.sample(all_ids, count)

    # Get some information about the items to update
    categories = ["category1", "category2", "category3", "category4", "category5"]
    statuses = ["active", "inactive", "pending", "archived"]

    # Start timing
    start_time = time.time()

    # Create a new transaction for updates
    tr = database.new_transaction()

    # Get the items dictionary (just to verify it exists)
    tr.get_root_object('items')

    # Create a new dictionary for updated items
    updated_dict = tr.new_dictionary()

    # Update all items
    for i, item_id in enumerate(selected_ids):
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


def benchmark_delete(database, count=10):
    """
    Benchmark deleting items from the database.

    Args:
        database: The database to use.
        count (int): Number of items to delete.

    Returns:
        float: Time taken in seconds.
    """
    print(f"\nBenchmarking delete of {count} items...")

    # Create a transaction to get all items
    tr = database.new_transaction()
    items_dict = tr.get_root_object('items')

    # Get all item IDs
    all_ids = [item[0] for item in items_dict.as_iterable()]

    if len(all_ids) < count:
        print(f"Warning: Only {len(all_ids)} items available in database")
        count = len(all_ids)

    # Randomly select IDs to delete
    selected_ids = random.sample(all_ids, count)

    # Start timing
    start_time = time.time()

    # Create a new transaction for deletions
    tr = database.new_transaction()
    items_dict = tr.get_root_object('items')

    # Delete items
    for item_id in selected_ids:
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


def benchmark_query(database, num_queries=5):
    """
    Benchmark querying items from the database.

    Args:
        database: The database to use.
        num_queries (int): Number of queries to perform.

    Returns:
        float: Time taken in seconds.
    """
    print(f"\nBenchmarking {num_queries} queries...")

    # Define categories and statuses for queries
    categories = ["category1", "category2", "category3", "category4", "category5"]
    statuses = ["active", "inactive", "pending", "archived"]

    # Start timing
    start_time = time.time()

    # Perform queries
    for _ in range(num_queries):
        # Create a transaction for the query
        tr = database.new_transaction()
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


def ensure_items_exist(database, min_count):
    """
    Ensure that at least min_count items exist in the database.
    If not, add more items to reach the minimum count.
    """
    # First, try to get the items dictionary
    tr = database.new_transaction()

    try:
        # Try to get the items dictionary
        items_dict = tr.get_root_object('items')
        current_count = len(list(item[0] for item in items_dict.as_iterable()))
        tr.commit()
    except Exception:
        # If 'items' doesn't exist, create it in a new transaction
        tr.commit()  # Commit the failed transaction

        # Create a new transaction for initialization
        tr = database.new_transaction()
        items_dict = tr.new_dictionary()
        tr.set_root_object('items', items_dict)
        tr.commit()

        current_count = 0

    # If we need more items, add them
    if current_count < min_count:
        items_to_add = min_count - current_count
        print(f"Adding {items_to_add} items to reach minimum count of {min_count}")
        benchmark_insert(database, items_to_add)


def run_all_benchmarks(database, item_count=10, query_count=5):
    """
    Run all benchmarks.

    Args:
        database: The database to use.
        item_count (int): Number of items for insert/read/update/delete benchmarks.
        query_count (int): Number of queries for query benchmark.
    """
    # First, ensure we have enough items in the database
    ensure_items_exist(database, item_count)

    # Run all benchmarks
    insert_time = benchmark_insert(database, item_count)
    read_time = benchmark_read(database, item_count)
    update_time = benchmark_update(database, item_count)
    query_time = benchmark_query(database, query_count)
    delete_time = benchmark_delete(database, item_count)

    # Print summary
    print("\n" + "=" * 50)
    print("BENCHMARK SUMMARY")
    print("=" * 50)
    print(f"Insert {item_count} items: {insert_time:.4f} seconds ({(insert_time / item_count) * 1000:.4f} ms per item)")
    print(f"Read {item_count} items: {read_time:.4f} seconds ({(read_time / item_count) * 1000:.4f} ms per item)")
    print(f"Update {item_count} items: {update_time:.4f} seconds ({(update_time / item_count) * 1000:.4f} ms per item)")
    print(f"Delete {item_count} items: {delete_time:.4f} seconds ({(delete_time / item_count) * 1000:.4f} ms per item)")
    print(
        f"Execute {query_count} queries: {query_time:.4f} seconds ({(query_time / query_count) * 1000:.4f} ms per query)")


def main():
    """Run the performance benchmark."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='ProtoBase Performance Benchmark')
    parser.add_argument('--storage', choices=['memory', 'file'], default='memory',
                        help='Storage type (memory or file)')
    parser.add_argument('--count', type=int, default=10,
                        help='Number of items for benchmarks')
    parser.add_argument('--queries', type=int, default=5,
                        help='Number of queries for query benchmark')
    parser.add_argument('--benchmark', choices=['insert', 'read', 'update', 'delete', 'query', 'all'],
                        default='insert', help='Which benchmark to run')
    args = parser.parse_args()

    print("\n" + "=" * 50)
    print("PROTOBASE PERFORMANCE BENCHMARK")
    print("=" * 50)
    print(f"Storage: {args.storage}")
    print(f"Item count: {args.count}")
    print(f"Query count: {args.queries}")
    print(f"Benchmark: {args.benchmark}")
    print("=" * 50)

    try:
        # Create storage
        if args.storage == "memory":
            print("\nCreating in-memory storage...")
            storage = MemoryStorage()
        else:
            storage_path = "benchmark_data"
            print(f"\nCreating file-based storage at {storage_path}...")
            os.makedirs(storage_path, exist_ok=True)
            block_provider = FileBlockProvider(space_path=storage_path)
            storage = StandaloneFileStorage(block_provider=block_provider)

        # Create object space
        print("Creating object space...")
        object_space = ObjectSpace(storage=storage)

        # Create database
        print("Creating database...")
        database = object_space.new_database('BenchmarkDB')

        # Run selected benchmark
        if args.benchmark == 'insert':
            insert_time = benchmark_insert(database, args.count)
            print(
                f"\nInsert {args.count} items: {insert_time:.4f} seconds ({(insert_time / args.count) * 1000:.4f} ms per item)")
        elif args.benchmark == 'read':
            ensure_items_exist(database, args.count)
            read_time = benchmark_read(database, args.count)
            print(
                f"\nRead {args.count} items: {read_time:.4f} seconds ({(read_time / args.count) * 1000:.4f} ms per item)")
        elif args.benchmark == 'update':
            ensure_items_exist(database, args.count)
            update_time = benchmark_update(database, args.count)
            print(
                f"\nUpdate {args.count} items: {update_time:.4f} seconds ({(update_time / args.count) * 1000:.4f} ms per item)")
        elif args.benchmark == 'delete':
            ensure_items_exist(database, args.count)
            delete_time = benchmark_delete(database, args.count)
            print(
                f"\nDelete {args.count} items: {delete_time:.4f} seconds ({(delete_time / args.count) * 1000:.4f} ms per item)")
        elif args.benchmark == 'query':
            ensure_items_exist(database, max(args.count, 20))  # Ensure enough items for meaningful queries
            query_time = benchmark_query(database, args.queries)
            print(
                f"\nExecute {args.queries} queries: {query_time:.4f} seconds ({(query_time / args.queries) * 1000:.4f} ms per query)")
        elif args.benchmark == 'all':
            run_all_benchmarks(database, item_count=args.count, query_count=args.queries)

        print("\nBenchmark completed successfully!")
    except Exception as e:
        print(f"\nError during benchmark: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
