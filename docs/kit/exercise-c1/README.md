# Exercise: the lost acknowledged commit

A planted-bug exercise on the ProtoDB cluster coordinator. The bug is real: an adversarial review found it in
this code (finding C1 of the second re-review of Task 8.3n), and it was fixed. The exercise puts it back in a scratch copy of the repository, gives you a test that
fails, and asks you to find out why and to fix it.

Expected effort: two to four hours for a student who has seen Paxos or Raft. Solving it does not require reading
the whole coordinator (about 4,300 lines); it requires one function and one argument.

## Getting the exercise

From a git checkout of the repository:

```
python docs/kit/exercise-c1/make_exercise.py ~/c1-exercise
cd ~/c1-exercise
python -m pytest exercise_c1/test_c1_exercise.py
```

The script exports the committed tree into the directory you name (it refuses a directory inside the
repository, and never writes into the repository), plants the bug, and copies this README and the test into
`exercise_c1/`. It removes from the copy what describes the fix: `docs/`, `CHANGELOG.md`, the C1 regression
tests and the catalogue of planted defects. The published documentation describes the fix too: do not read the
section "The 8.3n round 3 mistake" of the coordinator protocol document, step 3 of its "Choosing the base of a
term's first root", or the design guide's "The mistake: a recovery round that ended too early" until you have an
answer, and do not read the git history of the repository's cluster code. A few comments and test docstrings in
the copy may still hint at it; work from the code and the failing test.

The test needs Python 3.11 or later and the test dependencies of the repository (`pytest`, `pytest-timeout`).
On the planted copy it runs in under a second. A fix that is safe but stalls the recovery round takes
about 5 seconds, because the test waits a bounded time for replication to finish.

## The setting

Several processes write one ProtoDB space through a cluster of peers. Each commit produces a new **root** (the
newest version of the whole database), and the cluster must agree on a single chain of roots.

* **Voters** are the nodes that vote. A **majority** of voters must accept a root before the commit is
  **acknowledged**, that is, before `run()` returns to the application.
* A node writes under a **lease** for a **term** (a ballot number). It asks the other voters to grant it; a voter
  that grants **promises** never to accept a root of a lower term again, and replies with the root it has
  accepted so far. This is Paxos phase 1. The lease's `root` is the highest root its granters reported.
* Each root is **labelled** with the term it was replicated under. Roots are ordered by `(label, epoch)`, like a
  Raft log by `(term, index)`.
* **The ballot rule.** A voter accepts the first root of a newer term over whatever root it holds. So the first
  root a new term commits decides what the history keeps: everything that root is not built on is dropped.
* When a lease is **certain** (every granter knows its root's outcome), the new term builds on the lease's root.
  When it is **not certain**, the holder runs a **recovery round** (`ClusterCoordinator.recover_root`): it asks
  the other voters for their roots, ends once a majority has answered, and takes the highest root.

**The failure model** (principle P5 of the design): messages may be delayed, lost, duplicated or reordered
without bound; a node's timer may run slow or stand still (a suspended machine); at most a minority of nodes
fails. **No safety property may depend on timing.** Timers may decide when the system makes progress, never
whether it is correct.

## The invariant

> A commit that was acknowledged is never lost: every root committed afterwards is built on it.

## The task

1. Run the test. It fails with a message of the form *"an acknowledged root (R', ...) was superseded by a
   committed root not built on it"*.
2. Read the schedule in the test's docstring. It is a deterministic run of the real coordinator: five voters, a
   network that holds every message until the test delivers it, and one fake clock.
3. Explain, in a paragraph, **why** the schedule loses R'. Name the step at which the protocol made a wrong
   decision, and the information it had and did not use.
4. The code justifies its choice with an argument (look at the docstring of `recover_root`). Say precisely which
   step of that argument is false.
5. Fix the code. Your fix must not use time: no timeout, no clock comparison, no "wait a little longer".
6. Argue that your fix is correct for every schedule the failure model allows, not only for this one.

## Checking an answer

* The exercise test passes. It judges **safety only**: it fails when an acknowledged root is lost. Your fix may
  also change the schedule, for example so that R' is never acknowledged, or so that a does not commit. Nothing
  acknowledged is lost then, so the test passes and reports each step that did not complete as a warning that
  starts with "not completed (safe)" (shown in pytest's warnings summary, or with `-rw`):

  ```
  python -m pytest -rw exercise_c1/test_c1_exercise.py
  ```

* The exercise does not require that this schedule completes, but a fix that stops everything is safe and
  useless. If the test reports "not completed (safe)", explain in writing why your fix still makes progress in
  general. The cluster tests around the commit path check that the common paths still make progress (about 15
  seconds):

  ```
  python -m pytest tests/unit/test_cluster_transactions.py tests/unit/test_cluster_chain_rule.py tests/unit/test_cluster_send_reserve.py
  ```

* Run both with `-X gil=0` and `-X gil=1` on a free-threaded build if you have one.
* Your written argument names the step that fails (task 4) and does not assume anything about clocks.

A passing test is necessary, not sufficient. The test checks one schedule; the argument is what covers the rest.
The instructor holds `SOLUTION.md`, which is not in your copy.

## Hints

Read them one at a time, and only when you are stuck.

<details>
<summary>Hint 1: where to look</summary>

The base of a commit under a lease is chosen in `_lease_base` in `proto_db/transactions.py`. Read what it does
when the lease is not certain.
</details>

<details>
<summary>Hint 2: what the recovery query carries</summary>

Look at the `query` message a recovery round sends, and at what an answering voter does when it receives one. Does
answering change what that voter will accept afterwards? Compare with what a voter does when it grants a lease.
</details>

<details>
<summary>Hint 3: the majority argument</summary>

"Every acknowledged root was accepted by a majority, and that majority intersects the majority that answered the
recovery round." True. But *when* did the member in the intersection accept the root, and when did it answer?
</details>

<details>
<summary>Hint 4: the information that was there</summary>

In step 4 of the schedule, what is `lease_a.root`? Which voters reported it, and what had they promised before they
reported it?
</details>
