Evidence: the residency design measurements
===========================================

*An evidence note of the design guide. It records, for the figures of* :doc:`../residency` *that come from the
Task 8.3m design (step 1), the commit they were measured at, the command that produced them and the script,
so that a reader can repeat them. Figures that the repository records elsewhere (D53, the logs under*
``benchmarks/results/``) *are cited there and not repeated here.*

Common conditions
-----------------

* Code under test: a ``git archive`` of commit ``26f3b85`` (before residency), extracted to a directory on ZFS;
  every command below runs from the root of that extract, with the scripts of this note copied anywhere.
* Machine: AMD Ryzen 5 5500U (6 cores), CPython 3.14.6 free-threaded, ``-X gil=0`` unless stated.
* Each figure is a **single measurement**, taken once during the design, with no repeated runs and no range.
  The figures support an order-of-magnitude choice between mechanisms (a cost of zero against hundreds of
  nanoseconds per edge); they are not benchmark results and must not be quoted as such.

The edge census
---------------

Command::

   python -X gil=0 census.py 20 /path/to/new/workdir

The script builds the benchmark inventory (``benchmarks/workloads/inventory.py``) at 20 sites with the default
50,000-entry cache, then walks breadth-first from the cache entries through loaded atoms only, and prints a
JSON summary. Output of the design run (kept outside the repository, quoted here; it was produced by a version
of the script that did not yet print the ``resident`` and ``stats`` keys, which do not affect these figures):

* 68,841 objects; 50,000 cache entries, 49,999 of them loaded;
* 199,335 loaded atoms alive, every one reachable from the cache entries; 149,336 of them are not entries;
  1 stub alive; breadth-first depth 21;
* 317,710 edges from a loaded parent to a loaded child, of which 119,964 (38 %) are atoms nested inside
  tuples: ``CountedKeyNode.key`` and ``CountedKeyNode.value`` to ``DBObject`` (58,777 each: an index member
  ``(element, count)`` and its member key, which embeds the element's exact token), and
  ``CountedKeyNode.value`` to ``CountedKeyNode`` (2,410, key groups). The largest direct classes are
  ``KeyNode.value`` to ``DBObject`` (69,121) and the ``left`` / ``right`` links of both node classes.

**A correction.** The design record summed the same table as "386,000 edges, 120,000 (31 %) nested", and D53
repeated the 31 % until the post-C1 touch-up, which corrected it there. The listed counts sum to 317,710 and 119,964, that is 38 %. The conclusion does not change:
more than a third of the pinning edges are inside tuples, out of reach of any per-attribute rule.

.. literalinclude:: scripts/census.py
   :language: python
   :caption: census.py

Per-edge costs of the edge rules
--------------------------------

Command::

   python -X gil=0 micro.py

Each variant reads ``node.left`` 2,000,000 times over 1,000 objects, in 1 thread and in 8 threads; the figure is
the best of 3 runs, in aggregate nanoseconds per access (wall time divided by all accesses). Figures of the
design run:

=====================================================  ===========  ============
Variant                                                1 thread     8 threads
=====================================================  ===========  ============
direct attribute (today)                               49 ns        not quoted
(a) pointer link resolved through ``BoundedCache``     745 ns       2,175 ns
(b) weak reference to the child                        337 ns       458 ns
=====================================================  ===========  ============

The 8-thread cost of (a) is high because every resolution takes the cache's one lock.

.. literalinclude:: scripts/micro.py
   :language: python
   :caption: micro.py

Point read and decode
---------------------

Commands::

   python -X gil=0 pointread.py
   python -X gil=1 pointread.py

The script builds a 100,000-entry ``Dictionary`` in memory and measures a warm ``get_at`` (50,000 random keys,
same transaction), the number of edges a lookup crosses (tree depth plus the value), and, on a space without a
decoded-atom cache, the cold time of 5,000 lookups divided by the number of nodes they decoded. Figures of the
design run: a warm lookup of about 5.7 us crossing **16.7 edges**; about **28 us** per decoded node with the GIL
off and **44 us** with it on. The decode figure is an approximation: the cold time includes the walk itself.

From these figures the guide derives the per-lookup costs of the rules. For (a), 16.7 edges at 696 to 745 ns
each (the extra cost over a direct read, or the whole resolution) add 11.6 to 12.4 us to a 5.7 us lookup: about
3x. For (b), 16.7 edges at 288 to 337 ns add 4.8 to 5.6 us: about +85 to +100 %.

.. literalinclude:: scripts/pointread.py
   :language: python
   :caption: pointread.py
