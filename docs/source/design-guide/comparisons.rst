Comparisons by workload profile
===============================

*Ruling R100 (and R97, R112); decision D49; principle P6. The figures are on* :doc:`/performance`\ *, which is
being regenerated after Task 8.3m; this chapter quotes only a few, each with its tag and commit, to teach the
method.*

What it gives the ERP
---------------------

"Should our ERP run on ``proto_db``, on SQLite, or on rqlite?" has no single answer, and a guide that gave one
would be wrong for half its readers. It has an answer *per workload*: the repeated pipeline report, the stable
paginated report and the "as of" questions are one kind of work; the nightly import, an ad-hoc analytics
query and a write-heavy cluster are others. The ERP team can decide by asking which profiles dominate its day,
and it can see, for each, where the other system is the better choice.

The problem
-----------

``proto_db`` and the systems it is compared with are built on different models. A single ratio averages a model's
strengths with its weaknesses and says nothing about either.

**Measured symptom** (D56, Task 8.4b; :doc:`pager-and-rank`): on one and the same deep-page workload, SQLite
was **7 times faster** than ``proto_db`` at offset 0, and ``proto_db`` was **170 times faster** at offset 9 x 10^5. Any
single figure for "pagination" is either of those, or a meaningless average of them. Across the suite the
spread is wider still: SQLite bulk-loads 112 times faster than ``proto_db`` at 10^5 objects ([S0], ``4aab0f2``) and
134 times at 10^6 ([INV], ``8b439cd``), while a ``proto_db``
reader under write load answers from its own snapshot in 18-22 us at the median with one reader per node
([SES], ``8b439cd``).

Why the obvious solution fails
------------------------------

The obvious presentation is a **headline**: one benchmark, one ratio, one winner. The maintainer rejected it
(ruling R100): *rqlite and proto_db have conceptual differences that suit them to different loads; a flat
comparison is not conclusive.* A headline also invites the two classic benchmarking errors this project
committed and had to correct (below): measuring different work on the two sides, and reporting runs that did
not do the work.

The method
----------

*This chapter departs from the structure of the guide: in place of "the alternatives and their costs" and "the
choice" it presents the method that was chosen; the rejected presentations are listed at the end.*

Every comparison is organised by **workload profile**, and each profile states three things (R100):

1. **the conceptual reason** a system fits the work: the model, not the tuning;
2. **the measured figures for both systems on the same work**, every repetition verified;
3. **plainly, where the other system is the better choice.**

The rules that make the figures trustworthy (D49, principle P6):

* **every benchmark verifies the work it reports**, in every repetition: against a brute-force scan, a model,
  a read-back by a new instance, or a strongly consistent query on rqlite. A failed run is reported, never
  averaged in;
* **the rival runs the same work**, with its documented settings, verified from its documentation and not
  from memory (ruling R97);
* **every figure records its commit** and a clean flag checked against a hash of every exported file.

A few profiles, as the method presents them
-------------------------------------------

From :doc:`/performance` (tags and commits as on that page; GIL off; figures that predate Task 8.3m):

* **Deep pages** (D56). *Fit:* a ``proto_db`` pager seeks by rank on a snapshot that never changes. *Figures:* flat
  about 215 us per page at any offset ([S0], ``4aab0f2``; :doc:`pager-and-rank` quotes 232-253 us from D56,
  a different run at another commit, which is why the two figures differ). *Where SQLite is better:* next-page
  browsing by keyset, about 6 times faster, and the first pages by ``OFFSET``.
* **Repeated queries** (D56). *Fit:* a result cached by the pointers it read stays valid until they change.
  *Figures:* a hit in about 3.5 ms against SQLite's 10-105 ms re-execution ([S0]). *Where SQLite is better:* a
  first, ad-hoc query, 8 times faster at 10^5 rows.
* **Mixed reads and writes** (R97). *Fit:* ``proto_db`` readers never wait and never leave their node. *Figures:*
  per-read p50 18-22 us with one reader per node ([SES], ``8b439cd``). *Where rqlite is better:* when reads
  must be linearizable, and when readers share the writers' CPU: ``proto_db``'s writers lost 34 % (one reader per
  node) to 79 % (four), rqlite's 5 % to 40 %.
* **Writes in a cluster.** *Where rqlite is better:* 1.8-19 times the rows per second on every commit shape
  measured ([SES], ``8b439cd``, and [S0]).

The page has ten such profiles, and in several of them ``proto_db`` is not the better choice.

Its cost, stated honestly
-------------------------

* **It is more work and more text.** Each profile needs the rival run on the same shape, verification on both
  sides, and a paragraph of reasons. Readers who want one number do not get it.
* **It is never finished.** Ruling R112 stops adding comparisons: further profiles at the edges are mentioned,
  not measured, because the purpose is to show where each model fits, not to win a table.
* **One machine.** Every peer and every rqlite node shares one laptop's CPU and disk; a real cluster has a
  machine per node. The page says so next to the figures.

Mistakes the reviews caught
---------------------------

* **A wrong mechanism for the rival** (Task 9 review). The first performance page explained rqlite's throughput
  by a write queue that is opt-in in rqlite and that the workload did not use; the workload's own docstring said
  each request was one Raft entry. Fixed; since then, statements about another system are checked against its
  documentation and marked as inferences when they are inferences.
* **A median over failed runs** (Task 9c review, finding I2). A contention cell with the GIL on was quoted as a
  median of five runs, four of which had failed (writer threads stopped on ``ProtoOutcomeUnknownError``). The
  harness's summary included them. The page now reports that cell run by run, with its rates taken only from
  verified runs.
* **Verification of the last repetition only** (Task 9 review). A sample that failed silently in an earlier
  repetition would have been reported as a success. Every repetition is verified now.

Each mistake is the same lesson in a different place: **a benchmark that does not prove its own work measures
nothing**, and a comparison that does not run the rival's real configuration compares nothing.

What was tried and rejected
---------------------------

* A single headline ratio, or a geometric mean across the suite: rejected by R100.
* Quoting the rival's figures from its documentation instead of running it: never done; rqlite is measured,
  never quoted.
* Adding profiles until ``proto_db`` wins more of them: rejected by R112.

Back to the ERP
---------------

The ERP team reads the profiles against its own day. The morning's repeated pipeline reports, the stable
paginated reports and the "as of" questions are profiles where ``proto_db``'s model fits. The nightly bulk import,
ad-hoc analytical SQL over every version, and a write-heavy cluster are profiles where SQLite or rqlite fits
better, and the page says so in plain words. The decision is theirs, made with figures that did the work they
claim, on the work that matters to them.
