# Intents-mode removal inventory (R54)

Written before any deletion (Task 1 of the precondition concurrency plan). "Replay-dependent" means the site
exists only because the commit replays a logged operation list; "mode" means it exists only to serve
`intents=True`. Line numbers are those of the tree at the time of writing.

## 1. The mode (removed)

| Site | What it is | Replay-dependent |
|---|---|---|
| `proto_db/semantic/intents.py` (whole file, 639 lines) | `IntentLog`, `Cells` (record view), `Tracking`, `Entry`, `ReplaySession`, `without_intent_log`, `_changed_state`, `CREATED` | yes |
| `proto_db/transactions.py:97-106` | module docstring paragraph describing the mode | yes |
| `proto_db/transactions.py:261-264` | `_intents()` lazy import | yes |
| `proto_db/transactions.py:285-287` | `_check_intents_flag` | mode |
| `proto_db/transactions.py:2539-2545` | `Database.new_transaction(intents=)` | mode |
| `proto_db/transactions.py:2646-2679` | `Database.run(intents=)` | mode |
| `proto_db/transactions.py:2746`, `2764-2776` | `Transaction` docstrings and the coordinator refusal in `__init__` | mode |
| `proto_db/transactions.py:2845-2846` | `self._intent_log` | mode |
| `proto_db/transactions.py:2893-2895` | `Transaction.intents` property | mode |
| `proto_db/transactions.py:2897-2910` | `_check_not_logged`, `_check_not_written_by_log` | mode |
| `proto_db/transactions.py:3053,3269,3290` | `_check_not_logged` calls (`new_mutable`, `set_mutable`, `remove_mutable`) | mode |
| `proto_db/transactions.py:3074,3091` | `_check_not_written_by_log` calls (`get_mutable`, `snapshot_mutable`) | mode |
| `proto_db/transactions.py:3312-3313` | `mutable_names` lists the log's created cells | replay |
| `proto_db/transactions.py:3760` | `_commit` early-return guard on `_intent_log.entries` | replay |
| `proto_db/transactions.py:3867-3872` | `_build_root`: `_intent_log.replay(...)` | replay |
| `proto_db/semantic/session.py:20-25, 68-70, 81-86, 96-101, 103-118, 120-127, 169-180` | `_log`, `_store`, `_new_cell`, `_record_read`, `_writing`, tracker in `_operations`, docstring | mode |
| `proto_db/semantic/session.py:235-294` | `create/update/link/unlink/relate/invoke` branches `if log is not None` | mode |
| `proto_db/semantic/entities.py:10, 86, 98, 271, 487-489, 730-743, 834-839, 862, 877` | `tracker`, `Tracking`, `_track`, `Version.record` fine read | mode |
| `proto_db/semantic/classes.py:42, 68, 121` | docstring and the `PRIMITIVES` names refused for intents (kept: they also guard `invoke` names) | mode (doc only) |
| `proto_db/semantic/descriptors.py:242` | docstring reference | mode (doc only) |
| `benchmarks/semantic/credit_experiment.py:9, 187, 318-338, 398-410, 551` | `--mode intents`, `IntentLog` import, `intents=` | mode |
| `docs/kit/semantic/semantic_demo.py:366` | demo uses `intents=True` | mode |

## 2. Kept (R54, R55)

| Site | Why |
|---|---|
| `proto_db/catalog/atoms.py` `Intent` (type id 31), `IntentRecord` (type id 34), `INTENT_LOG_ROOT`, `intent_log_root`, `intent_records` | catalog types and the on-disk root; format-stable. `IntentRecord` is written only by the replay, so nothing writes it after this task, but a space may hold it |
| `proto_db/core/atom.py:243,284`, `proto_db/storage/format.py:77` | type registry and ids |
| `tests/conformance/generate.py:980-1003`, `test_conformance.py:387-401`, `vectors/catalog/*`, `manifest.json` | the `catalog_database` vector holds an `__intentLog`; digests are pinned |
| `tests/unit/test_catalog_atoms.py` (`IntentRecord`, `intent_log_root` cases) | catalog atom behavior, no mode |
| `proto_db/transactions.py:3873-3874` (the `elif` of the `_build_root` replay) | R55: a leftover `__intentLog` is dropped by the next commit that writes (a reserved-root write, R37) |
| Semantic intents as a language feature (`@intent`, `invoke`, `IntentContext`, `requires`) | not the mode; they run through the cell path |

## 3. Tests using the mode

| Test | Disposition |
|---|---|
| `tests/unit/test_semantic_intents.py` (27 tests) | rewritten to the cell path; mechanics-only tests replaced by `tests/unit/test_intents_removed.py` |
| `tests/unit/test_semantic_constraints.py:105-429` | `intents=True` variants converted to the cell path (R53) |
| `tests/unit/test_semantic_history_verifier.py:96, 213-344` | the `intents` mode of the experiment removed; direct kept |
| `tests/concurrency/test_semantic_intents_concurrency.py` | rewritten to the cell path |
| `tests/unit/test_semantic_relations.py:1077-1097` | `intents` parameter dropped |
| `tests/unit/test_catalog_atoms.py` | unchanged unless it fails |

## 4. Docs mentioning the mode (not edited by this task)

`docs/source/concurrency-model.rst:110,118`, `docs/source/performance.rst:935-1096` (measurements of a mode that
no longer exists; historical), `docs/source/format-spec.rst:1706,1771,1975,2011,2060-2068`, `docs/decisions.md:1083-1088`.
