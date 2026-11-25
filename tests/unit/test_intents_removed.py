"""The intents mode is removed (R53-R55).

Catches: a mode parameter that is still accepted by ``new_transaction`` or ``run`` (R53); a space written by an
older release, holding an ``__intentLog`` root, that no longer opens or reads (R55); a leftover ``__intentLog``
that survives the next commit that writes (R55, a reserved-root write under R37).  The space is the conformance
vector ``catalog_database``, which holds an ``__intentLog`` (its digests stay pinned, R54).
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from proto_db.catalog.atoms import INTENT_LOG_ROOT
from proto_db.storage.file import FileStorage
from proto_db.transactions import ObjectSpace
from tests.support.roots import reserved_root_names

HERE = Path(__file__).resolve().parent.parent / "conformance"


@pytest.fixture
def intent_log_space(tmp_path):
    """A writable copy of the conformance vector space, whose last commit wrote an ``__intentLog``."""
    entry = json.loads((HERE / "manifest.json").read_text())["catalog_database"]
    work = tmp_path / "db"
    shutil.copytree(HERE / "vectors" / entry["dir"], work)
    space = ObjectSpace(FileStorage(str(work)))
    try:
        yield space
    finally:
        space.close()


@pytest.fixture
def db(tmp_path):
    from proto_db.storage.memory import MemoryStorage
    space = ObjectSpace(MemoryStorage())
    try:
        yield space.new_database("db")
    finally:
        space.close()


def test_new_transaction_has_no_intents_parameter(db):
    with pytest.raises(TypeError):
        db.new_transaction(intents=True)


def test_run_has_no_intents_parameter(db):
    with pytest.raises(TypeError):
        db.run(lambda tr: None, intents=True)


def test_space_with_intent_log_root_opens_and_reads(intent_log_space):
    db = intent_log_space.open_database("db")
    assert INTENT_LOG_ROOT in reserved_root_names(db)
    tr = db.new_transaction()
    try:
        assert tr.get_root_object("plain") is not None
    finally:
        tr.abort()


def test_leftover_intent_log_dropped_on_next_commit(intent_log_space):
    db = intent_log_space.open_database("db")
    db.run(lambda tr: tr.set_root_object("x", 1))
    assert INTENT_LOG_ROOT not in reserved_root_names(db)
    assert "x" in reserved_root_names(db)
