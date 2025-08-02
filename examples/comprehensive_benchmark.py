#!/usr/bin/env python3
"""
ProtoBase Comprehensive Performance Benchmark

This script runs comprehensive performance benchmarks on ProtoBase with a significant
number of elements to establish its expected characteristics. It tests:

1. Insert performance (adding objects to the database)
2. Read performance (retrieving objects from the database)
3. Update performance (modifying objects in the database)
4. Delete performance (removing objects from the database)
5. Query performance (filtering and sorting objects)

The benchmark supports both in-memory and file-based storage and tests with
different dataset sizes to establish scaling characteristics.
"""

import os
import sys
import time
import uuid
import random
import argparse
import traceback
import json
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


def benchmark_insert(database, count=1000):
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
    print(f"Items per second: {count / elapsed_time:.2f}")

    return elapsed_time


def benchmark_read(database, count=1000):
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
        item = items_dict.get_at(item_id)
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
    print(f"Items per second: {count / elapsed_time:.2f}")

    return elapsed_time


def benchmark_update(database, count=1000):
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

    # Get the items dictionary
    items_dict = tr.get_root_object('items')

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

        # Update the item in the dictionary
        items_dict = items_dict.set_at(item_id, updated_item)

    # Store the updated dictionary
    tr.set_root_object('items', items_dict)

    # Commit the transaction
    tr.commit()

    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    print(f"Update completed in {elapsed_time:.4f} seconds")
    print(f"Average time per item: {(elapsed_time / count) * 1000:.4f} ms")
    print(f"Items per second: {count / elapsed_time:.2f}")

    return elapsed_time


def benchmark_delete(database, count=1000):
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
        try:
            items_dict = items_dict.remove_at(item_id)
        except Exception as e:
            print(f"Error removing item {item_id}: {e}")
            # Continue with the next item

    # Store the updated dictionary
    tr.set_root_object('items', items_dict)

    # Commit the transaction
    tr.commit()

    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    print(f"Delete completed in {elapsed_time:.4f} seconds")
    print(f"Average time per item: {(elapsed_time / count) * 1000:.4f} ms")
    print(f"Items per second: {count / elapsed_time:.2f}")

    return elapsed_time


def benchmark_query(database, num_queries=50, items_per_query=100):
    """
    Benchmark querying items from the database.

    Args:
        database: The database to use.
        num_queries (int): Number of queries to perform.
        items_per_query (int): Expected number of items per query result.

    Returns:
        float: Time taken in seconds.
    """
    print(f"\nBenchmarking {num_queries} queries (expecting ~{items_per_query} items per query)...")

    # Define categories and statuses for queries
    categories = ["category1", "category2", "category3", "category4", "category5"]
    statuses = ["active", "inactive", "pending", "archived"]

    # Start timing
    start_time = time.time()

    # Perform queries
    total_items_retrieved = 0
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

        total_items_retrieved += len(results)
        # Commit the transaction
        tr.commit()

    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    print(f"Queries completed in {elapsed_time:.4f} seconds")
    print(f"Average time per query: {(elapsed_time / num_queries) * 1000:.4f} ms")
    print(f"Queries per second: {num_queries / elapsed_time:.2f}")
    print(f"Total items retrieved: {total_items_retrieved}")
    print(f"Average items per query: {total_items_retrieved / num_queries:.2f}")

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


def run_all_benchmarks(database, item_count=1000, query_count=50):
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
    results = {}

    # Insert benchmark
    insert_time = benchmark_insert(database, item_count)
    results["insert"] = {
        "total_time": insert_time,
        "items": item_count,
        "time_per_item_ms": (insert_time / item_count) * 1000,
        "items_per_second": item_count / insert_time
    }

    # Read benchmark
    read_time = benchmark_read(database, item_count)
    results["read"] = {
        "total_time": read_time,
        "items": item_count,
        "time_per_item_ms": (read_time / item_count) * 1000,
        "items_per_second": item_count / read_time
    }

    # Update benchmark
    update_time = benchmark_update(database, item_count)
    results["update"] = {
        "total_time": update_time,
        "items": item_count,
        "time_per_item_ms": (update_time / item_count) * 1000,
        "items_per_second": item_count / update_time
    }

    # Query benchmark
    query_time = benchmark_query(database, query_count)
    results["query"] = {
        "total_time": query_time,
        "queries": query_count,
        "time_per_query_ms": (query_time / query_count) * 1000,
        "queries_per_second": query_count / query_time
    }

    # Delete benchmark
    delete_time = benchmark_delete(database, item_count)
    results["delete"] = {
        "total_time": delete_time,
        "items": item_count,
        "time_per_item_ms": (delete_time / item_count) * 1000,
        "items_per_second": item_count / delete_time
    }

    # Print summary
    print("\n" + "=" * 50)
    print("BENCHMARK SUMMARY")
    print("=" * 50)
    print(f"Insert {item_count} items: {insert_time:.4f} seconds ({(insert_time / item_count) * 1000:.4f} ms per item, {item_count / insert_time:.2f} items/sec)")
    print(f"Read {item_count} items: {read_time:.4f} seconds ({(read_time / item_count) * 1000:.4f} ms per item, {item_count / read_time:.2f} items/sec)")
    print(f"Update {item_count} items: {update_time:.4f} seconds ({(update_time / item_count) * 1000:.4f} ms per item, {item_count / update_time:.2f} items/sec)")
    print(f"Delete {item_count} items: {delete_time:.4f} seconds ({(delete_time / item_count) * 1000:.4f} ms per item, {item_count / delete_time:.2f} items/sec)")
    print(f"Execute {query_count} queries: {query_time:.4f} seconds ({(query_time / query_count) * 1000:.4f} ms per query, {query_count / query_time:.2f} queries/sec)")

    return results


def main():
    """Run the comprehensive performance benchmark."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='ProtoBase Comprehensive Performance Benchmark')
    parser.add_argument('--storage', choices=['memory', 'file'], default='memory',
                        help='Storage type (memory or file)')
    parser.add_argument('--size', choices=['small', 'medium', 'large'], default='medium',
                        help='Dataset size: small (1,000), medium (10,000), or large (100,000)')
    parser.add_argument('--benchmark', choices=['insert', 'read', 'update', 'delete', 'query', 'all'],
                        default='all', help='Which benchmark to run')
    parser.add_argument('--output', type=str, default='benchmark_results.json',
                        help='Output file for benchmark results (JSON format)')
    args = parser.parse_args()

    # Determine item count based on dataset size
    if args.size == 'small':
        item_count = 1000
        query_count = 50
    elif args.size == 'medium':
        item_count = 10000
        query_count = 100
    else:  # large
        item_count = 100000
        query_count = 200

    print("\n" + "=" * 50)
    print("PROTOBASE COMPREHENSIVE PERFORMANCE BENCHMARK")
    print("=" * 50)
    print(f"Storage: {args.storage}")
    print(f"Dataset size: {args.size} ({item_count} items)")
    print(f"Query count: {query_count}")
    print(f"Benchmark: {args.benchmark}")
    print("=" * 50)

    results = {
        "config": {
            "storage": args.storage,
            "dataset_size": args.size,
            "item_count": item_count,
            "query_count": query_count,
            "benchmark": args.benchmark,
            "timestamp": datetime.now().isoformat()
        },
        "benchmarks": {}
    }

    try:
        # Create storage
        if args.storage == "memory":
            print("\nCreating in-memory storage...")
            storage = MemoryStorage()
        else:
            storage_path = f"benchmark_data_{args.size}"
            print(f"\nCreating file-based storage at {storage_path}...")
            os.makedirs(storage_path, exist_ok=True)
            block_provider = FileBlockProvider(space_path=storage_path)
            storage = StandaloneFileStorage(block_provider=block_provider)

        # Create object space
        print("Creating object space...")
        object_space = ObjectSpace(storage=storage)

        # Create database
        print("Creating database...")
        database = object_space.new_database('ComprehensiveBenchmarkDB')

        # Run selected benchmark
        if args.benchmark == 'insert':
            insert_time = benchmark_insert(database, item_count)
            results["benchmarks"]["insert"] = {
                "total_time": insert_time,
                "items": item_count,
                "time_per_item_ms": (insert_time / item_count) * 1000,
                "items_per_second": item_count / insert_time
            }
        elif args.benchmark == 'read':
            ensure_items_exist(database, item_count)
            read_time = benchmark_read(database, item_count)
            results["benchmarks"]["read"] = {
                "total_time": read_time,
                "items": item_count,
                "time_per_item_ms": (read_time / item_count) * 1000,
                "items_per_second": item_count / read_time
            }
        elif args.benchmark == 'update':
            ensure_items_exist(database, item_count)
            update_time = benchmark_update(database, item_count)
            results["benchmarks"]["update"] = {
                "total_time": update_time,
                "items": item_count,
                "time_per_item_ms": (update_time / item_count) * 1000,
                "items_per_second": item_count / update_time
            }
        elif args.benchmark == 'delete':
            ensure_items_exist(database, item_count)
            delete_time = benchmark_delete(database, item_count)
            results["benchmarks"]["delete"] = {
                "total_time": delete_time,
                "items": item_count,
                "time_per_item_ms": (delete_time / item_count) * 1000,
                "items_per_second": item_count / delete_time
            }
        elif args.benchmark == 'query':
            ensure_items_exist(database, max(item_count, 1000))  # Ensure enough items for meaningful queries
            query_time = benchmark_query(database, query_count)
            results["benchmarks"]["query"] = {
                "total_time": query_time,
                "queries": query_count,
                "time_per_query_ms": (query_time / query_count) * 1000,
                "queries_per_second": query_count / query_time
            }
        elif args.benchmark == 'all':
            benchmark_results = run_all_benchmarks(database, item_count=item_count, query_count=query_count)
            results["benchmarks"] = benchmark_results

        print("\nBenchmark completed successfully!")

        # Save results to file
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output}")

    except Exception as e:
        print(f"\nError during benchmark: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
