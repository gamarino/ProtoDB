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

from proto_db import ObjectSpace, DBObject, MutableObject
from proto_db.memory_storage import MemoryStorage


class User(DBObject):
    """
    A class representing a User
    """


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

    # Create and store a dictionary
    print("Creating a list of users")
    users = tr.new_list()

    user1 = User(
        name='John Doe',
        email='john@example.com',
        role='admin'
    )
    users = users.append_last(user1)

    user2 = User(
        name='Jane Doe',
        email='jane@example.com',
        role='user'
    )
    users = users.append_last(user2)

    user3 = User(
        name='Dan Doe',
        email='dan@example.com',
        role='user',
        friends=tr.new_list().append_last(user2).append_last(user1)
    )
    users = users.append_last(user3)

    tr.set_root_object('users', users)
    tr.commit()

    tr = database.new_transaction()
    read_users = tr.get_root_object('users')

    for u in read_users.as_iterable():
        print(f'Name: {u.name}')
        print(f'Email: {u.email}')
        print(f'Role: {u.role}')
        if u.friends:
            for f in u.friends.as_iterable():
                print(f'Friend: {f.name}')

    tr.commit()

    print("\nExample completed successfully!")


if __name__ == "__main__":
    main()
