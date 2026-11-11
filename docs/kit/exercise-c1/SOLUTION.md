# Solution: the lost acknowledged commit (instructor's copy)

Do not hand this file out. `make_exercise.py` never copies it into the exercise.

Sources, all in the repository: the coordinator protocol document (`docs/source/coordinator-protocol.rst`,
"Choosing the base of a term's first root" and "The 8.3n round 3 mistake"), the design guide
(`docs/source/design-guide/unknown-outcomes.rst`, "The mistake: a recovery round that ended too early"), the
docstring of `_phase_1_floor` in `proto_db/transactions.py`, the D46 amendment in `docs/decisions.md`, and the
commits `1b3a188` (the change that made the hole the normal path), `276a256` (the fix) and `db1a4c8` (the
liveness follow-up). Nothing below goes beyond them.

## The mechanism

The planted code is `_lease_base` as it was before `276a256`: when the lease is not certain, the base of the new
term's first root is **the result of the recovery round, alone**. `lease.root`, the value phase 1 reported, is
not used.

The schedule of the test, and what happens at each step:

1. a commits r1 (label 4096). Time passes.
2. e owns T' = 4100, granted by b and c. e's timer stands still (a timer-rate fault, allowed by P5).
3. e replicates R'. b accepts it. The copy for c is held back by the network.
4. On b's and c's clocks, the promises to e have expired. a owns T = 8192, granted by b and d. **Phase 1 reports
   R'** (b accepted it), so `lease_a.root` is R' and the lease is not certain.
5. a's recovery round is answered by c and d. Neither has accepted R' yet, so the round returns r1. With a, that
   is a majority of witnesses, so the round ends.
6. e's delayed message reaches c. **c has promised nothing to T**: a recovery query carries ballot 0, and the
   coordinator routes only queries with a ballot to the acceptor lane (`coordinator.py`, the dispatcher's
   `kind == "query" and msg["ballot"]` test). So `_may_accept` finds no higher term granted on c and c accepts
   R'. e counts b, c and itself: 3 of 5, while its frozen timer says its lease is valid. **R' is acknowledged.**
7. a builds the first root of T on r1 and replicates it to c and d. It is the first root of a newer term, so each
   voter accepts it over its accepted root under the ballot rule alone. a's commit is acknowledged.
   **R' is not in its history.**

The wrong decision is at step 5: a chose a base below a root it already knew about (R', in `lease.root`), on the
strength of answers that said nothing about the future.

## Why the majority-intersection argument fails

The pre-fix docstring of `recover_root`, which the exercise copy restores, says:

> Every committed root was acknowledged by a majority, which intersects the answering majority, so the result is
> the newest committed root or a root built on it.

The intersection is real: R' was acknowledged by {b, c, e}, the round was answered by {a, c, d}, and they share c.
The argument fails on **time**. Intersection proves that the member in the intersection accepted every root that
was chosen *before it answered*. It says nothing about roots it accepts *after* it answered, unless the answer
was a promise. A ballot-0 answer promises nothing. c answered in step 5 and accepted R' in step 6.

That is what Paxos phase 1 exists for: the promise is what keeps an answer true afterwards. And the phase-1
information was there: a's lease had been granted by b and d, each of which had promised T before replying, and
the highest root they reported was R'.

Two remarks for the discussion:

* **Ending at a majority was not the cause; it made the hole the normal path.** Before `1b3a188` the round
  waited for every voter, but went on with a majority after `request_timeout` when an answer was late. Under P5 an
  answer may always be late, so the same schedule was already possible. A ballot-0 answer is no evidence, at any
  count, about what its sender accepts afterwards.
* **The only sentence that excused it was about clocks.** The decision record justified the round as "sent under
  the lease, i.e. after every earlier lease ended". That holds only if e's lease ended on e's timer when it ended
  on its granters' timers: a rate assumption. Under P5 (safety never depends on time) it carries no weight; e's
  timer standing still is exactly the fault the test injects.

## The fix

Never let the base of an uncertain lease order below the phase-1 value. The base is the **higher**, in
`compare_roots` order, of `lease.root` (or a permission's `root`) and the recovery result (`276a256`):

```python
known, current, label = coordinator.recover_root()
if _phase_1_floor(lease) and (not known or compare_roots(lease.root_term, lease.root, label, current) > 0):
    # The recovery answers promised nothing and may predate a root accepted afterwards;
    # the phase-1 root may not.
    known, current, label = True, lease.root, lease.root_term
```

`_phase_1_floor(lease)` is true when the lease knows a root (`lease.root is not None or lease.root_term`). A
student's inline version of the same condition is a correct answer; it was checked to make the exercise test pass.

### Why it is sufficient (the argument in `_phase_1_floor`'s docstring)

Let C be a root chosen (accepted by a majority Q_c) in a term T_c < T. The granting majority Q_g of T shares a
member m with Q_c. m promised T before it replied to the grant, and an acceptor refuses every root of a term below
its promise; so m accepted C *before* it replied, and reported a root that orders at or above C (an acceptor's
accepted root only moves up). Hence `lease.root` orders at or above C. By induction over terms (every earlier
first root obeyed the rule), and because within one term every acceptor accepts only roots that extend its own
accepted root (D55, V2') and the roots of a term lie on one chain (S2, R79), any root that orders at or above C
extends C. So a base at or above `lease.root` extends every root chosen below T. A recovery result that orders
higher is safe to adopt for the same reason: some voter accepted it, and it orders above C. No timing premise
appears.

(The docstring also covers the one-lost-disk case: an acceptor that lost its disk regains a root at or above
what it had before it votes again, through its rejoin's ballot round, R78a.)

### Other correct answers

**Waiting for every granter (rejected in the product).** Ending the recovery round only once **every granter** of
the current lease has answered is also safe: the granters promised before answering. It was rejected because it
brings back the stall `1b3a188` removed: whenever a granter dies after granting, every recovery waits
`request_timeout` inside the commit's lease.

**A recovery query that carries a ballot (a promise).** Hint 2 leads naturally here: send the recovery `query`
with the lease's term as its ballot, so that each answering voter promises the term before it answers. The
coordinator already handles such a query: `_on_ballot_query`, used by a rejoin (R78a), raises the promise, makes
it durable and only then answers. The change is two lines: a `ballot` parameter on `recover_root`, passed as
`lease.term` by `_lease_base`.

- **Why it is safe.** The answering round is now a real phase 1 for the term. Every root chosen below T was
  accepted by a majority that intersects the answering majority; the member in the intersection promised T
  before it answered, so it accepted that root before answering and reported a root at or above it. A ballot-0
  answer said nothing about the future; a promise does.
- **What it does to the test's schedule.** c promises T in step 5, so in step 6 it refuses R'. R' reaches only b
  and e: it is never acknowledged, and dropping it is legal (an unknown outcome for e's writer). The exercise test
  passes and reports "not completed (safe): R' was never acknowledged". Nothing *blocks* in the protocol: e's
  replication would end when e's lease expires on e's timer, and the test, whose e timer stands still, lets it
  expire after a bounded wait. a's commit completes.
- **Checked** on a planted copy with this change: the exercise test passes in both GIL modes with that one
  warning, and the README's regression set passes (28 tests, GIL off).
- **Its cost, read from the code** (it was not measured, and the decision record does not discuss it): every
  recovery round makes each answering voter write a durable promise to its acceptor log before it answers, where a
  ballot-0 answer writes nothing. The floor adds no message and no write to the round (its own cost was a
  liveness one, below).
- It is a better answer to task 3 than to the product's constraints; discuss with the student why the product
  kept the cheaper ballot-0 round and floored its result instead.

### What the fix cost (a good follow-up question)

The floor was not free. The next re-review found that a floor root whose records live only on members that are
now down cannot be re-committed: every round of the term based on it ended "none", the same base was issued
again, and the term stalled while a majority was alive. `db1a4c8` makes such a round raise `_BaseUnreachable`,
end the permission "unknown", and abandon the term, so a fresh phase 1 runs on the live members (R133; the
argument is in the `_BaseUnreachable` docstring). This is worth asking students: *does your fix still make
progress when the node that holds R' is the one that dies?*

## Grading notes

* Full marks need the step-6 observation (a ballot-0 answer is not a promise) and the use of the phase-1 value.
  Answers that wait longer, add a timeout or compare clocks fail requirement 5 and P5.
* An answer that waits for every voter, or for every granter, is safe; ask what it costs when one of them is dead.
* An answer that makes the recovery query carry the lease's term as a ballot is safe and deserves full marks if
  the argument says why a promise makes the answer stay true. Its test run passes with "not completed (safe): R'
  was never acknowledged"; ask the student why that is legal, and what the promise write costs.
* Any other answer needs its own argument at the level of the one above; the exercise test alone does not grade it.
