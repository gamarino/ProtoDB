#!/usr/bin/env python3
"""
Simple ProtoBase Example

This example demonstrates the basic functionality of ProtoBase, a transactional,
object-oriented database system implemented in Python.

It shows how to:
1. Create an in-memory database
2. Perform basic operations within transactions
3. Store and retrieve data
4. Use ProtoBase's data structures
"""

import os
import sys

# Add the parent directory to the path to import proto_db
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proto_db.memory_storage import MemoryStorage
from proto_db.db_access import ObjectSpace


def main():
    """Run a simple demonstration of ProtoBase."""
    print("ProtoBase Simple Example")
    print("=======================")

    # Create an in-memory storage
    print("\nCreating in-memory storage...")
    storage = MemoryStorage()

    # Create an object space with the storage
    print("Creating object space...")
    object_space = ObjectSpace(storage=storage)

    # Create a new database
    print("Creating database...")
    database = object_space.new_database('SimpleDB')

    # Create a new transaction
    print("\nCreating transaction...")
    tr = database.new_transaction()

    # Store a simple string value
    print("Storing a string value...")
    tr.set_root_object('greeting', 'Hello, ProtoBase!')

    # Create and store a dictionary
    print("Creating a dictionary...")
    user = tr.new_dictionary()
    user = user.set_at('name', 'John Doe')
    user = user.set_at('email', 'john@example.com')
    user = user.set_at('role', 'admin')
    tr.set_root_object('user', user)

    # Create and store a list
    print("Creating a list...")
    numbers = tr.new_list()
    for i in range(5):
        numbers = numbers.set_at(i, i * 10)
    tr.set_root_object('numbers', numbers)

    # Commit the transaction
    print("\nCommitting transaction...")
    tr.commit()
    print("Transaction committed successfully!")

    # Create a new transaction to retrieve the values
    print("\nCreating a new transaction to retrieve values...")
    tr2 = database.new_transaction()

    # Retrieve the string value
    greeting = tr2.get_root_object('greeting')
    print(f"\nRetrieved greeting: {greeting}")

    # Retrieve and display the dictionary
    user = tr2.get_root_object('user')
    print("\nRetrieved user:")
    print(f"  Name: {user.get_at('name')}")
    print(f"  Email: {user.get_at('email')}")
    print(f"  Role: {user.get_at('role')}")

    # Retrieve and display the list
    numbers = tr2.get_root_object('numbers')
    print("\nRetrieved numbers:")
    for i in range(numbers.length()):
        print(f"  numbers[{i}] = {numbers.get_at(i)}")

    print("\nExample completed successfully!")


if __name__ == "__main__":
    main()
