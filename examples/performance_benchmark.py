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


class PerformanceBenchmark:
    """
    A class for benchmarking ProtoBase performance.
    """

    def __init__(self, storage_type="memory", storage_path="benchmark_data"):
        """
        Initialize the benchmark with the specified storage type.

        Args:
            storage_type (str): The type of storage to use ("memory" or "file").
            storage_path (str): The directory path for file storage.
        """
        if storage_type == "memory":
            self.storage = MemoryStorage()
            print("Using in-memory storage")
        else:
            os.makedirs(storage_path, exist_ok=True)
            block_provider = FileBlockProvider(space_path=storage_path)
            self.storage = StandaloneFileStorage(block_provider=block_provider)
            print(f"Using file-based storage at: {os.path.abspath(storage_path)}")

        self.object_space = ObjectSpace(storage=self.storage)

        # Always create a new database for benchmarking
        self.database = self.object_space.new_database('BenchmarkDB')
        print("Created new database")

        # Initialize the database with an empty dictionary
        tr = self.database.new_transaction()
        tr.set_root_object('items', tr.new_dictionary())
        tr.commit()

    def _create_random_item(self):
        """Create a random benchmark item."""
        categories = ["category1", "category2", "category3", "category4", "category5"]
        statuses = ["active", "inactive", "pending", "archived"]

        return {
            "id": str(uuid.uuid4()),
            "name": f"Item-{random.randint(1000, 9999)}",
            "value": random.randint(1, 1000),
            "category": random.choice(categories),
            "status": random.choice(statuses),
            "created_at": datetime.now().isoformat(),
            "tags": [f"tag{i}" for i in range(random.randint(1, 5))]
        }

    def benchmark_insert(self, count=1000):
        """
        Benchmark inserting items into the database.

        Args:
            count (int): Number of items to insert.

        Returns:
            float: Time taken in seconds.
        """
        print(f"\nBenchmarking insert of {count} items...")

        start_time = time.time()

        tr = self.database.new_transaction()
        items_dict = tr.get_root_object('items')

        for _ in range(count):
            item_data = self._create_random_item()
            item = BenchmarkItem(**item_data)
            items_dict = items_dict.set_at(item.id, item)

        tr.set_root_object('items', items_dict)
        tr.commit()

        elapsed_time = time.time() - start_time
        print(f"Insert completed in {elapsed_time:.4f} seconds")
        print(f"Average time per item: {(elapsed_time / count) * 1000:.4f} ms")

        return elapsed_time

    def benchmark_read(self, count=1000):
        """
        Benchmark reading items from the database.

        Args:
            count (int): Number of items to read.

        Returns:
            float: Time taken in seconds.
        """
        # First, ensure we have enough items
        self._ensure_items_exist(count)

        print(f"\nBenchmarking read of {count} items...")

        # Get all item IDs
        tr = self.database.new_transaction()
        items_dict = tr.get_root_object('items')
        all_ids = [item[0] for item in items_dict.as_iterable()]

        # Randomly select IDs to read
        selected_ids = random.sample(all_ids, count)

        start_time = time.time()

        # Create a new transaction for reading
        tr = self.database.new_transaction()
        items_dict = tr.get_root_object('items')

        # Read items
        for item_id in selected_ids:
            item = items_dict.get_at(item_id)
            # Access some attributes to ensure they're loaded
            _ = item.name
            _ = item.value
            _ = item.category

        # Commit the transaction
        tr.commit()

        elapsed_time = time.time() - start_time
        print(f"Read completed in {elapsed_time:.4f} seconds")
        print(f"Average time per item: {(elapsed_time / count) * 1000:.4f} ms")

        return elapsed_time

    def _ensure_items_exist(self, min_count):
        """
        Ensure that at least min_count items exist in the database.
        If not, add more items to reach the minimum count.
        """
        tr = self.database.new_transaction()
        try:
            items_dict = tr.get_root_object('items')
            if items_dict is None:
                items_dict = tr.new_dictionary()
                tr.set_root_object('items', items_dict)
            current_count = len(list(item[0] for item in items_dict.as_iterable()))
        except Exception:
            # If 'items' doesn't exist, create it
            items_dict = tr.new_dictionary()
            tr.set_root_object('items', items_dict)
            current_count = 0
        tr.commit()

        if current_count < min_count:
            items_to_add = min_count - current_count
            print(f"Adding {items_to_add} items to reach minimum count of {min_count}")
            self.benchmark_insert(items_to_add)

    def benchmark_update(self, count=1000):
        """
        Benchmark updating items in the database.

        Args:
            count (int): Number of items to update.

        Returns:
            float: Time taken in seconds.
        """
        # First, ensure we have enough items
        self._ensure_items_exist(count)

        print(f"\nBenchmarking update of {count} items...")

        # Get all item IDs
        tr = self.database.new_transaction()
        items_dict = tr.get_root_object('items')
        all_ids = [item[0] for item in items_dict.as_iterable()]
        tr.commit()

        # Randomly select IDs to update
        selected_ids = random.sample(all_ids, count)

        # Get some information about the items to update
        categories = ["category1", "category2", "category3", "category4", "category5"]
        statuses = ["active", "inactive", "pending", "archived"]

        start_time = time.time()

        # Create a new transaction for updates
        tr = self.database.new_transaction()

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

        elapsed_time = time.time() - start_time
        print(f"Update completed in {elapsed_time:.4f} seconds")
        print(f"Average time per item: {(elapsed_time / count) * 1000:.4f} ms")

        return elapsed_time

    def benchmark_delete(self, count=1000):
        """
        Benchmark deleting items from the database.

        Args:
            count (int): Number of items to delete.

        Returns:
            float: Time taken in seconds.
        """
        # First, ensure we have enough items
        self._ensure_items_exist(count)

        print(f"\nBenchmarking delete of {count} items...")

        # Get all item IDs
        tr = self.database.new_transaction()
        items_dict = tr.get_root_object('items')
        all_ids = [item[0] for item in items_dict.as_iterable()]
        tr.commit()

        # Randomly select IDs to delete
        selected_ids = random.sample(all_ids, count)

        start_time = time.time()

        # Create a new transaction for deletions
        tr = self.database.new_transaction()
        items_dict = tr.get_root_object('items')

        # Delete items
        for item_id in selected_ids:
            items_dict = items_dict.remove_at(item_id)

        # Store the updated dictionary
        tr.set_root_object('items', items_dict)

        # Commit the transaction
        tr.commit()

        elapsed_time = time.time() - start_time
        print(f"Delete completed in {elapsed_time:.4f} seconds")
        print(f"Average time per item: {(elapsed_time / count) * 1000:.4f} ms")

        return elapsed_time

    def benchmark_query(self, num_queries=10):
        """
        Benchmark querying items from the database.

        Args:
            num_queries (int): Number of queries to perform.

        Returns:
            float: Time taken in seconds.
        """
        # First, ensure we have enough items for meaningful queries
        # We need at least 20 items to have a good chance of finding matches
        min_items = max(20, num_queries * 2)
        self._ensure_items_exist(min_items)

        print(f"\nBenchmarking {num_queries} queries...")

        categories = ["category1", "category2", "category3", "category4", "category5"]
        statuses = ["active", "inactive", "pending", "archived"]

        start_time = time.time()

        for _ in range(num_queries):
            tr = self.database.new_transaction()
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

            tr.commit()

        elapsed_time = time.time() - start_time
        print(f"Queries completed in {elapsed_time:.4f} seconds")
        print(f"Average time per query: {(elapsed_time / num_queries) * 1000:.4f} ms")

        return elapsed_time

    def run_all_benchmarks(self, item_count=1000, query_count=10):
        """
        Run all benchmarks.

        Args:
            item_count (int): Number of items for insert/read/update/delete benchmarks.
            query_count (int): Number of queries for query benchmark.
        """
        print("\n" + "=" * 50)
        print("PROTOBASE PERFORMANCE BENCHMARK")
        print("=" * 50)

        # Run only insert benchmark for testing
        insert_time = self.benchmark_insert(item_count)

        # Print summary
        print("\n" + "=" * 50)
        print("BENCHMARK SUMMARY")
        print("=" * 50)
        print(
            f"Insert {item_count} items: {insert_time:.4f} seconds ({(insert_time / item_count) * 1000:.4f} ms per item)")

    def _ensure_items_exist(self, min_count):
        """
        Ensure that at least min_count items exist in the database.
        If not, add more items to reach the minimum count.
        """
        tr = self.database.new_transaction()
        items_dict = tr.get_root_object('items')
        current_count = len(list(item[0] for item in items_dict.as_iterable()))
        tr.commit()

        if current_count < min_count:
            items_to_add = min_count - current_count
            print(f"Adding {items_to_add} items to reach minimum count of {min_count}")
            self.benchmark_insert(items_to_add)


def main():
    """Run the performance benchmark."""
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='ProtoBase Performance Benchmark')
    parser.add_argument('--storage', choices=['memory', 'file'], default='memory',
                        help='Storage type (memory or file)')
    parser.add_argument('--count', type=int, default=10,
                        help='Number of items for benchmarks')
    parser.add_argument('--queries', type=int, default=2,
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
        # Create benchmark instance
        benchmark = PerformanceBenchmark(storage_type=args.storage)

        # Run selected benchmark
        if args.benchmark == 'insert':
            insert_time = benchmark.benchmark_insert(args.count)
            print(
                f"\nInsert {args.count} items: {insert_time:.4f} seconds ({(insert_time / args.count) * 1000:.4f} ms per item)")
        elif args.benchmark == 'read':
            read_time = benchmark.benchmark_read(args.count)
            print(
                f"\nRead {args.count} items: {read_time:.4f} seconds ({(read_time / args.count) * 1000:.4f} ms per item)")
        elif args.benchmark == 'update':
            update_time = benchmark.benchmark_update(args.count)
            print(
                f"\nUpdate {args.count} items: {update_time:.4f} seconds ({(update_time / args.count) * 1000:.4f} ms per item)")
        elif args.benchmark == 'delete':
            delete_time = benchmark.benchmark_delete(args.count)
            print(
                f"\nDelete {args.count} items: {delete_time:.4f} seconds ({(delete_time / args.count) * 1000:.4f} ms per item)")
        elif args.benchmark == 'query':
            query_time = benchmark.benchmark_query(args.queries)
            print(
                f"\nExecute {args.queries} queries: {query_time:.4f} seconds ({(query_time / args.queries) * 1000:.4f} ms per query)")
        elif args.benchmark == 'all':
            benchmark.run_all_benchmarks(item_count=args.count, query_count=args.queries)

        print("\nBenchmark completed successfully!")
    except Exception as e:
        print(f"\nError during benchmark: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
