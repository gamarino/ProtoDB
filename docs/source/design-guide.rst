The design guide
================

This guide explains *why* ProtoDB is built the way it is. It is written for students and practitioners,
and it is meant to be taught from. The reference pages (:doc:`concepts`, :doc:`concurrency-model`,
:doc:`durability`, :doc:`format-spec`) say what the system does; ``docs/decisions.md`` is the terse record of
every decision. This guide tells the story behind the decisions that were hard: what went wrong first, what
was measured, which obvious solution failed, and which mistakes were made on the way. The mistakes are kept
on purpose. They are the most instructive part.

Two audiences
-------------

The guide has two kinds of reader in mind:

* the **systems programmer**, who wants the representation, the caches, the concurrency arguments and the
  cluster protocol;
* the **application developer**, who may not see what a mechanism makes possible from a description of the
  mechanism alone.

One running example joins them: a small ERP. Every chapter opens with something the ERP can do because of
the property the chapter explains (a question it answers, or a feature that needs no extra machinery), then
explains how the property works, and closes by returning to the ERP.

The running example
-------------------

The :doc:`ERP tutorial <tutorials/erp>` models four kinds of thing, and nothing else (no ERP features):

* **users**, which are *entities*: a user's name can be corrected, and every record that refers to the user
  should show the current name. A user is a ``MutableObject`` cell;
* **groups** of users. The tutorial does not build them yet; in this guide a group is a record that holds the
  handles of its users, and it appears only in prose and in sketches that are marked as such;
* **persons** (contacts), which are plain records in a ``Dictionary``;
* **opportunities**, which are *values*: each change produces a new version that points at the version it
  replaced (``previous``) and records who made it (``changed_by``). The collection holds the current
  versions.

The tutorial's code runs in the test suite, and the outputs this guide quotes from it are the outputs of that
run.

How every chapter is built
--------------------------

After the ERP opening, each chapter states, in this order:

1. **the problem**, with the symptom that was measured;
2. **why the obvious solution fails**;
3. **the alternatives**, with what each was measured to cost;
4. **the choice**, and the invariant that makes it correct;
5. **its cost**, stated honestly;
6. **what was tried and rejected**, and why.

It then closes by tying the mechanism back to the ERP capability it opened with. A chapter that has no
measurement for a step says so instead of filling the gap.

How figures are cited
---------------------

Every figure names the commit it was measured at and the record it comes from: an entry of
``docs/decisions.md`` (cited by its number, for example D53), a log under ``benchmarks/results/``, or a task
report or review. The task reports and reviews are the project's working records; they live in
``.superpowers/sdd/2026-11-01-protodb-v2/`` and are not part of the repository, so a figure that appears only
there is cited by the report's name (for example "Task 8.3m report"). Rulings (R-numbers) are recorded in the
same place.

Unless a chapter says otherwise, every figure was measured on one laptop: an AMD Ryzen 5 5500U (6 cores),
CPython 3.14.6 free-threaded, with the GIL off unless "GIL on" is stated, and work directories on ZFS. The
:doc:`performance` page describes that machine and its noise. A figure from a single run is marked as one.

The guide is not organised by course yet. The planned courses (the object database, queries and their
optimization, distribution, and the runtime class registry) will reorganise it once the runtime design is
closed.

Chapters
--------

The chapters can be read in order: the data model first, then queries, then the cluster, then how the system
is evaluated. The residency chapter is the most complete worked example of the method and is the one to read
first if you read only one.

.. toctree::
   :maxdepth: 1

   design-guide/immutability
   design-guide/cache-layers
   design-guide/residency
   design-guide/history-links
   design-guide/modelling
   design-guide/referential-integrity
   design-guide/pager-and-rank
   design-guide/coordinator
   design-guide/pipelined-permits
   design-guide/unknown-outcomes
   design-guide/comparisons

The diagrams are Graphviz sources in ``docs/source/design-guide/diagrams/`` (``*.dot``), rendered to the
``*.svg`` files the pages show with ``dot -Tsvg``. Edit the source and render again; never edit the SVG.
