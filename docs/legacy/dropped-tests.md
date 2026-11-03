# The 0.1 test suite (`proto_db/tests/`) and where its contracts went

Task 8.5 removed the 0.1 implementation and its 50 test modules (decision D48). Each module
was reviewed: a contract that still applies to 2.0 is either already covered by the 2.0 suite
under `tests/` (the covering tests are named) or was ported in Task 8.5; everything else is
dropped with the reason given. "0.1-only API" means the module tests classes, private methods
or behaviours that have no 2.0 counterpart by design (spec sections 3 to 10), usually through
mocks of 0.1 internals.

| 0.1 module | Outcome | Where / why |
|---|---|---|
| `test_arrow_parquet.py` | covered | `tests/unit/test_arrow.py`, `tests/unit/test_arrow_missing.py` (2.0 bridge, D42; `ArrowNotAvailable` became `ProtoNotSupportedError`) |
| `test_atom_cache.py` | dropped | 0.1-only API: `AtomObjectCache`/`AtomBytesCache` 2Q eviction and single-flight are implementation details of the removed cache; the 2.0 bounded cache is covered by `tests/unit/test_page_cache.py` and concurrent lazy loads by `tests/concurrency/test_lazy_load_race.py` |
| `test_atom_cache_write_through.py` | dropped | 0.1-only API (mocked `StandaloneFileStorage` write-through); read-after-write through the storage is covered by `tests/contracts/storage_contract.py` (`test_atom_round_trip`, `test_flush_keeps_data_readable`) |
| `test_between_range.py` | covered | `tests/unit/test_terms.py::test_between_variants`, `tests/unit/test_expressions.py::test_compile_every_operator`, `tests/unit/test_planner.py` (range plans) |
| `test_cloud_cluster_file_storage.py` | dropped | 0.1-only API (`CloudClusterFileStorage` internals via mocks); 2.0 cloud + cluster behaviour is covered by `tests/unit/test_cloud_storage.py`, `tests/unit/test_tiered_storage.py`, `tests/unit/test_cluster_*.py`, `tests/concurrency/test_cloud_concurrency.py` |
| `test_cloud_cluster_file_storage_with_gcs.py` | dropped | as above, with the 0.1 `MockGoogleCloudClient`; `GCSObjectStore` is covered by `tests/unit/test_cloud_storage.py` against a fake SDK client |
| `test_cloud_file_storage.py` | dropped | 0.1-only API (background uploader, WAL flush, object-key layout of `CloudFileStorage`); 2.0 contract in `tests/unit/test_cloud_storage.py` (D43) |
| `test_cloud_file_storage_with_gcs.py` | dropped | as above, GCS flavour |
| `test_cluster_file_storage.py` | dropped | 0.1-only API (`ClusterFileStorage` vote/lock protocol, replaced by D44-D46); covered by `tests/unit/test_cluster_*.py`, `tests/concurrency/test_cluster_*.py` |
| `test_collections_properties.py` | covered | `tests/property/test_collections_model.py` (stateful models of list/dict/set/Counter), `tests/property/test_avl.py` |
| `test_concurrency.py` | covered / ported | dictionary counters: `tests/concurrency/test_counters.py::test_dictionary_key_counter`, `tests/unit/test_transactions.py::test_concurrent_counters_lose_no_update`; concurrent adds to one key of a `RepeatedKeysDictionary` (test_102) ported in Task 8.5 fix round 1 as `tests/concurrency/test_counters.py::test_repeated_keys_blind_adds_merge` (32 threads, real commits, memory and file storage) |
| `test_counted_set.py` | covered | `tests/unit/test_counted_set.py`, `tests/unit/test_content_equals.py` |
| `test_db_access.py` | covered | `tests/unit/test_transactions.py` (databases, roots), `tests/unit/test_list.py::test_large_bulk_list_has_no_recursion_problem` |
| `test_db_access_with_standalone_file_storage.py` | covered | `tests/integration/test_end_to_end.py`, `tests/unit/test_file_storage.py` |
| `test_dbobject_recursion.py` | covered | `tests/unit/test_atom.py::test_dbobject_persist_and_lazy_reload` (nested DBObject child reloaded lazily), `test_save_children_first_and_once` |
| `test_dictionaries.py` | covered | `tests/unit/test_dictionary.py`, `tests/unit/test_repeated_keys_dictionary.py` |
| `test_dictionary_commit_merge.py` | covered | ported earlier to `tests/unit/test_transactions.py` (`test_untouched_keys_keep_their_values`, `test_integer_write_stores_the_written_value`, `test_removal_is_persisted`, `test_blind_write_merges_only_its_own_keys`) |
| `test_example.py` | dropped | 0.1-only API: the global `Literal` catalog was removed (D7) |
| `test_expressions.py` | covered | `tests/unit/test_expressions.py`, `tests/unit/test_terms.py` |
| `test_file_block_provider.py` | dropped | 0.1-only API (`FileBlockProvider`, `FileReaderFactory`, `PageCache` internals); 2.0 file storage in `tests/unit/test_file_storage.py`, page cache in `tests/unit/test_page_cache.py` |
| `test_format_indicators.py` | dropped | 0.1-only API: JSON/MessagePack/legacy format indicators; 2.0 has one framed format (D4), covered by `tests/unit/test_format.py`, `tests/unit/test_self_validating_framing.py` |
| `test_fsm.py` | covered | `tests/unit/test_fsm.py` (2.0 FSM, D41) |
| `test_google_cloud_client.py` | dropped | 0.1-only API (`MockGoogleCloudClient`, `CloudStorageError`); `GCSObjectStore` covered by `tests/unit/test_cloud_storage.py` |
| `test_group_by_plan.py` | covered | `tests/unit/test_plans.py` (`test_group_by_with_aggregations`, `test_group_by_merges_equal_keys_and_keeps_missing_apart`, `test_group_by_rejects_bad_specs_and_values`) |
| `test_hash_dictionaries.py` | covered | `tests/unit/test_hash_dictionary.py`; `merge`, `_get_first`/`_get_last` and `_rebalance` are 0.1-only API |
| `test_hash_dictionary.py` | covered | `tests/unit/test_hash_dictionary.py`, `tests/property/test_avl.py` (balance invariants) |
| `test_hybrid_executor.py` | dropped | 0.1-only API: `HybridExecutor` is not used by 2.0 (D41) and was deleted |
| `test_integration.py` | covered | `tests/integration/test_end_to_end.py::test_reopen_keeps_roots_history_and_indexes` |
| `test_ivf_flat_index.py` | covered | `tests/unit/test_vector_index.py` (`test_ivf_flat_*`, `test_ivf_flat_round_trip_through_file_storage_keeps_centroids`) |
| `test_join_plan.py` | covered / dropped | inner and left joins: `tests/unit/test_plans.py::test_join_inner_and_left`, `tests/unit/test_linq.py::test_join_inner_and_left`; right, outer and "external" joins dropped: the 2.0 `Join` plan implements `JOIN_KINDS = ("inner", "left")` |
| `test_linq_between.py` | covered | ported earlier to `tests/unit/test_linq.py` (`test_between_*`, `test_datetime_between`; the chained-comparison lambda in `test_errors_raised_by_user_code_propagate`) |
| `test_linq_groupby.py` | covered | ported earlier: `tests/unit/test_linq.py::test_group_by_with_aggregates` |
| `test_linq_indexed_fastpath.py` | covered | ported earlier: `tests/unit/test_linq.py::test_indexed_*` |
| `test_linq_like.py` | covered | ported earlier: `tests/unit/test_linq.py` (`test_distinct_and_count`, `test_filter_projection_pagination`, `test_lambda_where_with_window`; policies were removed, D38) |
| `test_lists.py` | covered | `tests/unit/test_list.py`; `ListQueryPlan` and the balance-factor internals are 0.1-only API |
| `test_memory_storage.py` | covered | `tests/unit/test_memory_storage.py`, `tests/contracts/storage_contract.py` (concurrent pushers, racing publishers, root lock) |
| `test_new_query_features.py` | covered / dropped | prefix-syntax compile and NOT_TRUE: `tests/unit/test_expressions.py`; unnest: `tests/unit/test_plans.py::test_unnest`; OR merge: `tests/unit/test_planner.py::test_or_fully_indexed_uses_union`; `CollectionFieldPlan` dropped (0.1-only API, expressible as a `select` with a nested query) |
| `test_parallel.py` | covered | `tests/unit/test_parallel.py` (2.0 parallel scan, D40; work stealing replaced by a shared cursor) |
| `test_queries.py` | covered | `tests/unit/test_plans.py` (project, count, filter), `tests/unit/test_planner.py` (predicate ordering and residuals), `tests/unit/test_linq.py::test_errors_raised_by_user_code_propagate` |
| `test_recursive_plan.py` | dropped | 0.1-only API: the `traverse` (recursive) operator is not part of the 2.0 query layer (spec section 10) |
| `test_select_many_plan.py` | covered / ported | flattening: `tests/unit/test_linq.py::test_select_many`; the top-N-per-category use case was ported in Task 8.5 as `tests/unit/test_linq.py::test_select_many_top_n_per_category` |
| `test_sets.py` | covered | `tests/unit/test_set.py` |
| `test_singlethread_concurrency.py` | covered | `tests/unit/test_transactions.py` (conflicts on read roots, blind merges, `run` retries), `tests/concurrency/test_counters.py`; the repeated-keys cases (test_097/098) by `tests/concurrency/test_counters.py::test_repeated_keys_blind_adds_merge` (memory and file storage) and `tests/unit/test_repeated_keys_dictionary.py` (rebase) |
| `test_standalone_file_storage.py` | dropped | 0.1-only API (`StandaloneFileStorage`, `WALState`); 2.0 contract in `tests/unit/test_file_storage.py`, `tests/crash/` |
| `test_vectors.py` | covered / dropped | exact search and cosine: `tests/unit/test_vector_index.py`; the `Vector` class is 0.1-only API; `near` without a vector index now raises `ProtoNotSupportedError` instead of a linear fallback (D28: `Term.matches` raises for `near[]`, ruling R17; D36: `Filter` rejects `near[]`; D37: no supporting index raises) |
| `test_vectors_buffers.py` | dropped | 0.1-only API (`Vector.from_buffer`); zero-copy numpy vectors for Arrow are covered by `tests/unit/test_arrow.py::test_vectors_from_numpy_are_zero_copy` |
| `test_wal_durability.py` | covered | ported earlier to `tests/integration/test_end_to_end.py` (`test_committed_data_survives_a_crash`, `test_data_committed_after_a_read_is_readable`, `test_page_read_at_end_of_segment_sees_later_commits`) |
| `test_where_plan.py` | covered | `tests/unit/test_plans.py::test_filter_*`, `tests/unit/test_planner.py` (indexed lookups, OR union, fallback to a scan for an unindexed branch) |
