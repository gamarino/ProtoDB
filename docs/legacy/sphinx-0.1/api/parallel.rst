Parallel (Work-Stealing and Adaptive Chunking)
==============================================

.. module:: proto_db.parallel

Overview
--------
This module provides optional components for parallel scans:

- ParallelConfig: configuration with sane defaults and environment overrides.
- AdaptiveChunkController: adaptive chunk sizing with EMA smoothing and clamping.
- WorkStealingPool: per-worker local deques with top-steal strategy.
- parallel_scan: convenience helper to run a map/filter scan over a logical range.

API
---

ParallelConfig
~~~~~~~~~~~~~~
.. autoclass:: proto_db.parallel.ParallelConfig
    :members:

AdaptiveChunkController
~~~~~~~~~~~~~~~~~~~~~~~
.. autoclass:: proto_db.parallel.AdaptiveChunkController
    :members:

WorkStealingPool
~~~~~~~~~~~~~~~~
.. autoclass:: proto_db.parallel.WorkStealingPool
    :members:

Helper
~~~~~~
.. autofunction:: proto_db.parallel.parallel_scan
