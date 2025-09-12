# Ensure package-context friendly test execution under various runners.
# When running `python -m unittest discover proto_db/tests`, the package context
# may be missing causing relative imports (`from ..module import ...`) to fail.
# Importing tests here is unnecessary and can also break discovery.
# Keep this file minimal and avoid side-effect imports.
