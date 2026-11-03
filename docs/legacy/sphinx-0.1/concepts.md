# ProtoDB Fundamental Concepts

This guide explains the core mental model behind ProtoDB so you can use it correctly and efficiently. We use simple analogies and highlight critical behaviors to avoid surprises.

## 1. The Core Idea: The World is Made of Immutable Atoms

In ProtoDB, everything is an Atom: a self-contained, immutable piece of data.

- Think of an Atom like a Git commit — once written, it never changes.
- “Modifying” an object does not mutate it in place. Instead, a new Atom is created with the new content, and references are updated to point to it.
- Immutability makes concurrency safer and enables copy-on-write semantics.

## 2. Stable Identity: The AtomPointer

Python’s `id()` is process-local and not stable across time or machines. ProtoDB needs a durable identity to support hashing, equality, and indexing that remain consistent across sessions.

- AtomPointer is the stable identity for an Atom: `(transaction_id, offset)`.
- It acts like a birth certificate. When an Atom is persisted, it gets an AtomPointer.
- Stable identity underpins Atom’s `hash()` and equality (`==`) semantics.

## 3. The Atom.hash() Contract and its Critical Side-Effect

- The stable hash of an Atom is derived from its AtomPointer.
- To obtain an AtomPointer, an Atom must be persisted (saved to storage).
- Therefore, calling `hash(my_new_atom)` will trigger a write to storage so the Atom gains an AtomPointer.

Why this is necessary:
- Stable identity is essential for data structures, deduplication, and secondary indexes.

Why it can be problematic:
- Hashing a temporary Atom you don’t intend to persist will cause unnecessary writes and WAL growth. If that Atom never becomes reachable from a root, it still consumed I/O during the transaction.

## 4. The Solution: Ephemeral vs. Persistent State in Collections

ProtoDB collections (notably `Set` and `CountedSet`) embrace a dual-state model to avoid the `hash()` pitfall:

- `_new_objects`: a staging area (draft space) for newly added elements during a transaction. Lives only in memory.
- `content`: the persisted elements that are part of the committed object graph.

Only when a collection is part of the final committed graph do staged objects get promoted from `_new_objects` into `content`. This allows you to:

- Use Sets for intermediate results without writing anything if you do not persist the Set.
- Avoid accidental persistence from hashing new Atoms while doing temporary calculations.

## 5. Transactions: Your Isolated Sandbox

A transaction is your safe, isolated workspace.

Lifecycle:

1. `tr = db.new_transaction()`
2. Perform operations (create objects, update roots, query)
3. `tr.commit()` applies your changes atomically (or `tr.abort()` discards them)

Notes:
- Transactions are snapshot-based (copy-on-write). Your changes are invisible to others until commit.
- Only objects reachable from updated roots (and modified mutables) are persisted. Unreachable objects created during the transaction are discarded at commit time.
