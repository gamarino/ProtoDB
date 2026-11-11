Glossary of the cluster protocol
================================

*A reference chapter, not one of the R107 chapters: it has no ERP opening and no problem-to-choice story. It
defines the protocol vocabulary that* :doc:`coordinator`, :doc:`pipelined-permits` *and* :doc:`unknown-outcomes`
*use. The full protocol, step by step, is* :doc:`/coordinator-protocol`. *Code references are to*
``proto_db/cluster/coordinator.py`` *and* ``proto_db/cluster/lease.py``; *decision numbers to*
``docs/decisions.md``.

Members and roles
-----------------

member
   A process (a *node*) of the cluster, with ``proto_db`` embedded. Membership is a static list of peers.

voter, learner
   A fixed, odd set of members (three by default) orders roots: they grant leases and accept roots. The other
   members are *learners*: they read and commit, but do not vote (D54).

rank
   A member's index among the sorted peer ids. In the five-voter example of :doc:`unknown-outcomes`, a to e have
   ranks 0 to 4.

``f``
   The number of members besides the committer that make a commit's records durable (the *data replicas*). A
   commit's records live on ``f + 1`` members, not on every member (D54).

Terms, ballots and labels
-------------------------

term, ballot
   A number that names one attempt to own the cluster's write turn; the two words are used for the same number.
   A ballot is *proposer-qualified*: its low 12 bits (``RANK_BITS``) are the proposer's rank, so two members
   never ask for the same term. ``ballot_after(floor, rank)`` is the smallest ballot of ``rank`` above
   ``floor``. Examples: 4096 = 1 x 4096 + 0 (rank 0, member a), 4100 = 1 x 4096 + 4 (rank 4, member e),
   8192 = 2 x 4096 + 0 (rank 0, member a, a later attempt).

label
   The term of the lease under which a root was replicated, stored in the root (``SpaceRoot.cluster_term``).

epoch
   The commit counter of a root. Roots are ordered by ``(label, epoch)``, as Raft orders a log by
   ``(term, index)`` (D46); two different roots with the same label and epoch are a protocol violation.

Leases and Paxos phase 1
------------------------

lease, grant, granter
   A member asks the voters for a term; each voter that agrees sends a *grant* and is a *granter*. With grants
   from a majority the member holds the *lease* of that term, for a duration measured on its own clock
   (D45). The holder of the lease is the term's *owner*.

promise
   A granter's durable commitment, made before its grant leaves: it refuses every root of a lower term from
   then on (``granted_top``). The ballot rule: a voter refuses a replicated root whose term is below the highest
   term it granted.

phase 1
   The grant round is phase 1 of Paxos: each granter promises the term and reports the root it has accepted.
   The highest reported root (``best_root`` in ``lease.py``) becomes ``lease.root``, the *phase-1 value*.

certain, uncertain
   A lease is *certain* when every granter knows a root and none reported a grant whose outcome it could not
   know; then ``lease.root`` is used as the base directly. Otherwise it is *uncertain*, and the holder runs a
   recovery round to choose its base.

recovery round
   A query to the voters for the roots they hold (``recover_root``); the highest answer raises the base, which is floored at the lease's phase-1 root. The
   query carries ballot 0: it *promises nothing*, which is the point of finding C1 in
   :doc:`unknown-outcomes`.

witness
   A member whose answers count: its acceptor log is loaded and carries the *rejoined* marker. Recovery and rejoin
   rounds count only witnesses' answers (D46).

suspect term, abandoned term
   A term whose outcome a member could not learn is *suspect*: that member's later grant replies are
   uncertain. An owner that cannot sequence an outcome *abandons* its term (D50, D55 O6).

Roots and commits
-----------------

accepted root
   The newest root a voter has durably accepted (logged and synced in its acceptor log before it replies).

committed root
   A root that a majority of the voters has durably accepted. A commit is *acknowledged* to its caller when its
   holder observed that majority within its permission (D46, D50).

base
   The root a commit is built on: the lease's or permission's root (``lease.root``, ``permission.root``) when it is
   certain or a proposal, else the result of a recovery round (``_lease_base`` in ``transactions.py``).

fencing
   The object store's conditional write of the persisted root. In a cluster it never arbitrates; it only refuses a
   stale writer (D44).

Permissions and messages
------------------------

permission, holder
   The owner of a term grants short *permissions* to commit, numbered in sequence; the member that holds one is
   its *holder*. Each node groups its threads' commits into one round per permission (D50).

``replicate``
   The holder sends its new root (and records) to the voters; a voter that accepts it replies with an
   acknowledgement.

``done``
   The holder reports the outcome of its permission to the owner and the other members: committed, none, or
   unknown.

``propose``
   With pipelining (D55): a holder whose round had no conflict sends its built root to the owner, which may then
   issue the next permission on it (``after``), before the root is committed.

``warm``
   Records of a new root sent ahead to the members, so that the next holder finds them in its cache.

``store``
   A data-stage message that carries records to a data replica, which syncs them as a store-only entry and
   answers ``ack(ok=False, stored=True)``. No code path can accept a ``store`` as a root (V1).

gate
   The holder-side condition of D55: a pipelined holder logs its own acceptance and sends its root only once its
   node knows the predecessor root committed and holds it as its accepted root (rule H5).

Rule names of D55
-----------------

O1-O8
   The owner's sequencing rules for its pipeline of at most ``DEPTH + 1`` issued permissions (``DEPTH = 1``),
   among them: issue on the newest proposal once (O2), retire on
   a committed ``done`` (O3), ignore unknown or duplicate proposals and abandon on an inconsistent one (O4),
   restore the committed base only after a ``none`` of a lone entry that did not propose (O5), abandon on any
   outcome it cannot sequence (O6), give a retried request the same permit (O7), end the term only with an empty
   pipeline (O8).

V1
   Voter rule: a ``store`` is never accepted, never moves the receiver's accepted or committed root, and is never
   replayed as an acceptance.

V2'
   Voter rule (Raft's log matching on roots): a root of the voter's own accepted term is accepted only if it is a
   duplicate of the accepted root or extends it. A root of a newer term is accepted only if it is the *first* root
   of its term, which the voter recognises by its ``pred`` carrying an older label (``coordinator.py``, the check
   ``first_of_term``). The voter cannot check where that first root's base came from: see
   :doc:`pipelined-permits`.

H1, H2, H5, H7
   Holder rules: a proposing permission runs no further round (H1); a permission on a proposal base is never
   certain, is never re-proposed, and its base is not learnt committed when the permit arrives (H2); the gate (H5); and the outcome rule (H7): ``ProtoConflictError`` only when
   nothing a voter could accept left the node, ``ProtoOutcomeUnknownError`` otherwise.
