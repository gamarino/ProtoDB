"""The top-level ``proto_db`` package: the public API of ProtoDB 2.0 (Task 8.5, D47).

Catches: a public name that no longer resolves or resolves to the wrong object, a public
name without a docstring, ``import proto_db`` importing submodules or optional
third-party packages or starting threads (the 0.1 package started 24 executor threads on
import), a 0.1 module still importable, and the package version drifting from
pyproject.toml.
"""
import importlib
import os
import subprocess
import sys
import sysconfig
import textwrap
import tomllib

import pytest

import proto_db

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

#: The 0.1 modules removed by Task 8.5 (and the 0.1 test package).
REMOVED_MODULES = [
    "common", "db_access", "memory_storage", "standalone_file_storage", "file_block_provider",
    "cluster_file_storage", "cloud_file_storage", "cloud_cluster_file_storage", "atom_cache", "lists",
    "dictionaries", "hash_dictionaries", "sets", "indexes", "queries", "linq", "vector_index", "vectors",
    "hybrid_executor", "exceptions", "parallel", "fsm", "arrow_bridge", "tests",
]

#: Optional third-party packages that ``import proto_db`` must not import.
OPTIONAL = ["boto3", "botocore", "google.cloud.storage", "numpy", "pyarrow", "hnswlib"]


def fresh_python(code: str) -> str:
    """Run ``code`` in a fresh interpreter of this session (same GIL mode); return its stdout."""
    free_threaded_build = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    flags = ["-X", "gil=1"] if free_threaded_build and sys._is_gil_enabled() else []
    proc = subprocess.run([sys.executable, *flags, "-c", textwrap.dedent(code)], cwd=REPO, capture_output=True,
                          text=True, timeout=120, env=dict(os.environ, PYTHONPATH=REPO))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    return proc.stdout


@pytest.mark.parametrize("name", proto_db.__all__)
def test_every_public_name_resolves_and_is_documented(name):
    value = getattr(proto_db, name)
    assert value is not None
    doc = getattr(value, "__doc__", None)
    assert doc and doc.strip(), f"proto_db.{name} has no docstring"


def test_public_names_are_the_defining_objects():
    from proto_db import errors, transactions, values
    from proto_db.collections.dictionaries import Dictionary
    from proto_db.core.dbobject import MutableObject
    from proto_db.indexing.equality import EqualityIndex
    from proto_db.query.linq import F, from_collection
    from proto_db.storage.memory import MemoryStorage
    assert proto_db.ObjectSpace is transactions.ObjectSpace
    assert proto_db.Transaction is transactions.Transaction
    assert proto_db.Dictionary is Dictionary
    assert proto_db.EqualityIndex is EqualityIndex
    assert proto_db.MemoryStorage is MemoryStorage
    assert proto_db.from_collection is from_collection and proto_db.F is F
    assert proto_db.MISSING is values.MISSING
    # The 2.0 handle, not the 0.1 MutableObject(Atom) the old package exported.
    assert proto_db.MutableObject is MutableObject is values.MutableObject
    assert not issubclass(proto_db.MutableObject, proto_db.Atom)
    # D12: the 0.1 exception names remain aliases of the 2.0 classes for one release.
    assert proto_db.ProtoBaseException is proto_db.ProtoDBError is errors.ProtoDBError
    assert proto_db.ProtoLockingException is proto_db.ProtoLockingError


def test_unknown_names_raise_attribute_error():
    with pytest.raises(AttributeError):
        proto_db.NoSuchName  # noqa: B018
    assert "ObjectSpace" in dir(proto_db) and "TieredStorage" in dir(proto_db)


def test_star_import_exports_all():
    namespace = {}
    exec("from proto_db import *", namespace)
    assert set(proto_db.__all__) <= set(namespace)


def test_version_matches_pyproject():
    with open(os.path.join(REPO, "pyproject.toml"), "rb") as f:
        assert proto_db.__version__ == tomllib.load(f)["project"]["version"]


@pytest.mark.parametrize("module", REMOVED_MODULES)
def test_0_1_modules_are_gone(module):
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(f"proto_db.{module}")


def test_import_starts_no_thread_and_imports_nothing_else():
    out = fresh_python(f"""
        import sys, threading
        before = threading.active_count()
        modules = set(sys.modules)
        import proto_db
        new = sorted(m for m in set(sys.modules) - modules if m.startswith("proto_db"))
        optional = [m for m in {OPTIONAL!r} if m in sys.modules]
        print(threading.active_count() - before, new, optional)
    """)
    assert out.split() == ["0", "['proto_db']", "[]"], out


def test_resolving_every_name_starts_no_thread_and_no_optional_package():
    out = fresh_python(f"""
        import sys, threading
        before = threading.active_count()
        import proto_db
        for name in proto_db.__all__:
            getattr(proto_db, name)
        optional = [m for m in {OPTIONAL!r} if m in sys.modules]
        print(threading.active_count() - before, optional)
    """)
    assert out.split() == ["0", "[]"], out
