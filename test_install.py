"""
Test script to verify the local installation of proto_db.
"""
import os
import shutil
import subprocess
import sys
import tempfile


def main():
    # Create a temporary directory
    temp_dir = tempfile.mkdtemp()
    try:
        # Change to the temporary directory
        os.chdir(temp_dir)

        # Create a virtual environment
        subprocess.check_call([sys.executable, "-m", "venv", "venv"])

        # Determine the path to the pip executable in the virtual environment
        if os.name == 'nt':  # Windows
            pip_path = os.path.join(temp_dir, "venv", "Scripts", "pip")
        else:  # Unix/MacOS
            pip_path = os.path.join(temp_dir, "venv", "bin", "pip")

        # Install the package in development mode
        subprocess.check_call([pip_path, "install", "-e", os.path.dirname(os.path.abspath(__file__))])

        # Create a test script
        test_script = """
import threading

import proto_db
from proto_db import MemoryStorage, ObjectSpace

assert threading.active_count() == 1, "importing proto_db must not start threads"
space = ObjectSpace(MemoryStorage())
database = space.new_database('TestDatabase')
with database.new_transaction() as transaction:
    transaction.set_root_object('test_key', 'test_value')

transaction = database.new_transaction()
value = transaction.get_root_object('test_key')
transaction.abort()
space.close()
assert value == 'test_value', value
print(f"proto_db {proto_db.__version__}: retrieved value: {value}")
print("Installation test successful!")
"""
        with open(os.path.join(temp_dir, "test_script.py"), "w") as f:
            f.write(test_script)

        # Determine the path to the python executable in the virtual environment
        if os.name == 'nt':  # Windows
            python_path = os.path.join(temp_dir, "venv", "Scripts", "python")
        else:  # Unix/MacOS
            python_path = os.path.join(temp_dir, "venv", "bin", "python")

        # Run the test script
        subprocess.check_call([python_path, "test_script.py"])

        print("\nPackage installation and basic functionality test passed!")

    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
        print("Package installation or functionality test failed.")
        return 1
    finally:
        # Clean up the temporary directory
        shutil.rmtree(temp_dir)

    return 0


if __name__ == "__main__":
    sys.exit(main())
