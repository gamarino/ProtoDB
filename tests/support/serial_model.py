"""Serial model of a database for the serializability checker (spec section 12 item 2).

A program is a list of :class:`Op`.  The same program runs two ways:

* :func:`execute` runs it in a real :class:`~proto_db.transactions.Transaction` and returns
  the value each operation read;
* :meth:`Model.run` runs it on an in-memory replica of roots, cells and collections, with the
  serial semantics of spec sections 5 and 6, and returns the values it reads there.

:func:`check_history` replays the committed programs in commit order (the database version
each one published) on the model and asserts that every value a committed program read, and
the final state, are what the serial execution gives.  A transaction that published nothing
(R23) is checked against the state of the version it read.

The operation set is closed: Tasks 2-4 (object roots, mutable cells, Dictionary and
HashDictionary roots).  Later tasks add their collections to :data:`COLLECTIONS`, their
operations to the ``_real_*`` / ``Model._*`` pairs, and their kinds to the checker's mix.
"""
import copy
from collections import namedtuple
from dataclasses import dataclass, field

from proto_db.collections.dictionaries import Dictionary
from proto_db.collections.hash_dictionaries import HashDictionary
from proto_db.errors import ProtoValidationError

#: The kind of an object root (R1: any value that is not a collection).
OBJECT = "object"
#: The collection kinds of fine-grained roots, by class name.
COLLECTIONS = {"Dictionary": Dictionary, "HashDictionary": HashDictionary}
#: Collection kinds whose keys have a position (R17).
POSITIONAL = ("Dictionary",)

#: One operation of a program: ``kind`` names it; ``target`` is a root or cell name; ``key``
#: a key, an attribute name or a position; ``value`` the value written.  Unused fields are None.
Op = namedtuple("Op", "kind target key value", defaults=(None, None, None))

#: The operations of Tasks 2-4, grouped by what they act on.
ROOT_OPS = ("root_get", "root_names", "root_remove", "root_reset")
OBJECT_OPS = ("obj_get", "obj_set")
DICTIONARY_OPS = ("d_get", "d_has", "d_set", "d_remove", "d_len", "d_items", "d_key_at", "d_pos")
CELL_OPS = ("c_get", "c_put", "c_inc", "c_remove", "c_names")
DICTIONARY_AND_CELL_OPS = ROOT_OPS + OBJECT_OPS + DICTIONARY_OPS + CELL_OPS


@dataclass(frozen=True)
class StateNames:
    """The state the programs act on: root name -> kind (:data:`OBJECT` or a key of
    :data:`COLLECTIONS`), the keys (also the object attributes), the cell names, and the
    range of values written."""
    roots: dict
    keys: tuple
    cells: tuple
    values: range = range(100)

    def roots_of(self, kinds) -> tuple:
        return tuple(name for name, kind in self.roots.items() if kind in kinds)


@dataclass
class Record:
    """One committed transaction: ``version`` is the database version it published (its place
    in the commit order) or, when it published nothing (``read_only``), the version it read."""
    version: int
    program: list
    reads: list
    read_only: bool = False
    thread: int = -1


# -- program generation -------------------------------------------------------------------------

def random_program(rng, state_names: StateNames, ops, max_length: int = 6) -> list:
    """A program of 1 to ``max_length`` operations drawn from ``ops`` with ``rng``.

    An operation whose targets do not exist in ``state_names`` (a positional read with no
    Dictionary root, for instance) is drawn again."""
    program = []
    for _ in range(rng.randint(1, max_length)):
        while True:
            op = _draw(rng, state_names, rng.choice(tuple(ops)))
            if op is not None:
                program.append(op)
                break
    return program


def _draw(rng, names: StateNames, kind: str):
    value = rng.choice(names.values)
    key = rng.choice(names.keys)
    if kind == "root_names" or kind == "c_names":
        return Op(kind)
    if kind in ROOT_OPS:
        target = rng.choice(tuple(names.roots))
        if kind == "root_reset":
            chosen = rng.sample(names.keys, rng.randint(0, len(names.keys)))
            return Op(kind, target, value=tuple(sorted((k, rng.choice(names.values)) for k in chosen)))
        return Op(kind, target)
    if kind in OBJECT_OPS:
        targets = names.roots_of((OBJECT,))
        if not targets:
            return None
        return Op(kind, rng.choice(targets), key, value if kind == "obj_set" else None)
    if kind in DICTIONARY_OPS:
        targets = names.roots_of(POSITIONAL if kind in ("d_key_at", "d_pos") else tuple(COLLECTIONS))
        if not targets:
            return None
        target = rng.choice(targets)
        if kind == "d_key_at":
            return Op(kind, target, rng.randrange(len(names.keys)))
        if kind in ("d_len", "d_items"):
            return Op(kind, target)
        return Op(kind, target, key, value if kind == "d_set" else None)
    if kind in CELL_OPS:
        return Op(kind, rng.choice(names.cells), value=value if kind == "c_put" else None)
    raise ValueError(f"Unknown operation kind {kind!r}")


# -- the real execution -------------------------------------------------------------------------

def _attributes(obj, keys) -> tuple:
    """The attributes of an object root as a sorted tuple of pairs (values are never None)."""
    return tuple((k, obj[k]) for k in sorted(keys) if obj[k] is not None)


def _fresh(tr, kind: str, items: dict):
    """A new value of ``kind`` holding ``items``, not linked to any root (R11)."""
    if kind == OBJECT:
        return tr.new_object(**items)
    collection = COLLECTIONS[kind](context=tr)
    for k, v in sorted(items.items()):
        collection = collection.set_at(k, v)
    return collection


def execute(tr, program: list, names: StateNames) -> list:
    """Run ``program`` in transaction ``tr``; return the value each operation read (None for a write)."""
    return [_REAL[op.kind](tr, op, names) for op in program]


def _real_root_get(tr, op, names):
    value = tr.get_root_object(op.target)
    if value is None:
        return None
    if names.roots[op.target] == OBJECT:
        return _attributes(value, names.keys)
    return type(value).__name__     # a collection root: only presence and class are read


def _real_root_names(tr, op, names):
    return tuple(tr.root_names())


def _real_root_remove(tr, op, names):
    tr.remove_root_object(op.target)


def _real_root_reset(tr, op, names):
    tr.set_root_object(op.target, _fresh(tr, names.roots[op.target], dict(op.value)))


def _real_obj_get(tr, op, names):
    obj = tr.get_root_object(op.target)
    return None if obj is None else obj[op.key]


def _real_obj_set(tr, op, names):
    obj = tr.get_root_object(op.target)
    obj = tr.new_object(**{op.key: op.value}) if obj is None else obj.with_(**{op.key: op.value})
    tr.set_root_object(op.target, obj)


def _real_d_get(tr, op, names):
    d = tr.get_root_object(op.target)
    return None if d is None else d.get_at(op.key)


def _real_d_has(tr, op, names):
    d = tr.get_root_object(op.target)
    return None if d is None else d.has(op.key)


def _real_d_set(tr, op, names):
    d = tr.get_root_object(op.target)
    if d is None:
        d = _fresh(tr, names.roots[op.target], {op.key: op.value})
    else:
        d = d.set_at(op.key, op.value)
    tr.set_root_object(op.target, d)


def _real_d_remove(tr, op, names):
    d = tr.get_root_object(op.target)
    if d is not None:
        tr.set_root_object(op.target, d.remove_at(op.key))


def _real_d_len(tr, op, names):
    d = tr.get_root_object(op.target)
    return None if d is None else len(d)


def _real_d_items(tr, op, names):
    d = tr.get_root_object(op.target)
    if d is None:
        return None
    items = tuple(d.items())
    # A Dictionary iterates in key order; a HashDictionary's order is not part of its contract.
    return items if names.roots[op.target] in POSITIONAL else tuple(sorted(items))


def _real_d_key_at(tr, op, names):
    d = tr.get_root_object(op.target)
    if d is None:
        return None
    n = len(d)
    return (n, None) if n == 0 else (n, d.key_at_position(op.key % n))


def _real_d_pos(tr, op, names):
    d = tr.get_root_object(op.target)
    if d is None:
        return None
    return d.position_of(op.key) if d.has(op.key) else -1


def _real_c_get(tr, op, names):
    return tr.get_mutable(op.target)


def _real_c_put(tr, op, names):
    # A blind write: whether the cell exists is decided by the view, which records the cell.
    try:
        tr.set_mutable(op.target, op.value)
    except ProtoValidationError:
        tr.new_mutable(op.target, op.value)


def _real_c_inc(tr, op, names):
    value = tr.get_mutable(op.target)
    if value is None:
        tr.new_mutable(op.target, 1)
    else:
        tr.set_mutable(op.target, value + 1)
    return value


def _real_c_remove(tr, op, names):
    try:
        tr.remove_mutable(op.target)
    except ProtoValidationError:
        pass                        # absent in the view: nothing to remove (the read is recorded)


def _real_c_names(tr, op, names):
    return tuple(tr.mutable_names())


_REAL = {name[len("_real_"):]: fn for name, fn in globals().items() if name.startswith("_real_")}


# -- the serial model ---------------------------------------------------------------------------

@dataclass
class Model:
    """An in-memory replica: ``roots`` maps each present root name to its content (a dict of
    attributes or of items), ``cells`` each present cell to its value; ``version`` is the
    database version this state is."""
    names: StateNames
    roots: dict = field(default_factory=dict)
    cells: dict = field(default_factory=dict)
    version: int = 0

    def copy(self) -> "Model":
        return Model(self.names, copy.deepcopy(self.roots), dict(self.cells), self.version)

    def state(self) -> tuple:
        """What two models compare on (the version is not part of the content)."""
        return self.roots, self.cells

    def run(self, program: list) -> list:
        """Apply ``program`` serially; return the value each operation read."""
        return [self.apply(op) for op in program]

    def apply(self, op: Op):
        """Apply one operation with its serial semantics (sections 5 and 6); return what it read."""
        return getattr(self, "_" + op.kind)(op)

    def _kind(self, op) -> str:
        return self.names.roots[op.target]

    def _root_get(self, op):
        content = self.roots.get(op.target)
        if content is None:
            return None
        if self._kind(op) == OBJECT:
            return tuple(sorted(content.items()))
        return self._kind(op)

    def _root_names(self, op):
        return tuple(sorted(self.roots))

    def _root_remove(self, op):
        self.roots.pop(op.target, None)

    def _root_reset(self, op):
        self.roots[op.target] = dict(op.value)

    def _obj_get(self, op):
        content = self.roots.get(op.target)
        return None if content is None else content.get(op.key)

    def _obj_set(self, op):
        self.roots.setdefault(op.target, {})[op.key] = op.value

    def _d_get(self, op):
        content = self.roots.get(op.target)
        return None if content is None else content.get(op.key)

    def _d_has(self, op):
        content = self.roots.get(op.target)
        return None if content is None else op.key in content

    def _d_set(self, op):
        self.roots.setdefault(op.target, {})[op.key] = op.value

    def _d_remove(self, op):
        content = self.roots.get(op.target)
        if content is not None:
            content.pop(op.key, None)

    def _d_len(self, op):
        content = self.roots.get(op.target)
        return None if content is None else len(content)

    def _d_items(self, op):
        content = self.roots.get(op.target)
        return None if content is None else tuple(sorted(content.items()))

    def _d_key_at(self, op):
        content = self.roots.get(op.target)
        if content is None:
            return None
        n = len(content)
        return (n, None) if n == 0 else (n, sorted(content)[op.key % n])

    def _d_pos(self, op):
        content = self.roots.get(op.target)
        if content is None:
            return None
        return sorted(content).index(op.key) if op.key in content else -1

    def _c_get(self, op):
        return self.cells.get(op.target)

    def _c_put(self, op):
        self.cells[op.target] = op.value

    def _c_inc(self, op):
        value = self.cells.get(op.target)
        self.cells[op.target] = 1 if value is None else value + 1
        return value

    def _c_remove(self, op):
        self.cells.pop(op.target, None)

    def _c_names(self, op):
        return tuple(sorted(self.cells))


def capture(tr, names: StateNames) -> Model:
    """The state transaction ``tr`` reads, as a :class:`Model` (``version``: the version it reads)."""
    model = Model(names, version=tr.database.snapshot_at(tr.snapshot_ref).version)
    present = set(tr.root_names())
    unknown = present - set(names.roots)
    assert not unknown, f"roots outside the model: {sorted(unknown)}"
    for name in sorted(present):
        value = tr.get_root_object(name)
        kind = names.roots[name]
        if kind == OBJECT:
            model.roots[name] = dict(_attributes(value, names.keys))
        else:
            assert type(value).__name__ == kind, f"root {name!r} holds a {type(value).__name__}, not a {kind}"
            model.roots[name] = dict(value.items())
    for name in tr.mutable_names():
        model.cells[name] = tr.get_mutable(name)
    return model


# -- the checker --------------------------------------------------------------------------------

def _describe(record: Record) -> str:
    return (f"version {record.version} ({'read-only' if record.read_only else 'published'}, "
            f"thread {record.thread}): program {record.program}")


def check_history(records: list, initial: Model, *, final: Model) -> None:
    """Assert that ``records`` is serializable in commit order.

    The committed writers' versions must be ``initial.version + 1`` onward with no gap (no
    commit left out of the history).  Their programs are re-run on a copy of ``initial`` in
    version order: each must read exactly what it read in the database, and the state after the
    last one must equal ``final``.  A read-only record (R23) must read what the state at its
    version gives.  The first mismatch fails with the record, the expected and actual reads,
    and the records before it.
    """
    writers = sorted((r for r in records if not r.read_only), key=lambda r: r.version)
    versions = [r.version for r in writers]
    expected_versions = list(range(initial.version + 1, initial.version + 1 + len(writers)))
    assert versions == expected_versions, \
        f"commit versions are not consecutive from {initial.version + 1}: {versions[:20]}..."
    readers = {}
    for r in records:
        if r.read_only:
            readers.setdefault(r.version, []).append(r)
    last = initial.version + len(writers)
    stray = sorted(v for v in readers if not initial.version <= v <= last)
    assert not stray, f"read-only transactions read versions outside the history: {stray}"

    model = initial.copy()
    history = []

    def check(record, before):
        """Run ``record``'s program on ``before`` (changed in place) and compare its reads."""
        state = f"roots={before.roots} cells={before.cells}"
        reads = before.run(record.program)
        if reads != record.reads:
            context = "\n".join("    " + _describe(r) + f" read {r.reads}" for r in history[-5:])
            raise AssertionError(
                f"not serializable at {_describe(record)}\n"
                f"  database read: {record.reads}\n"
                f"  serial read:   {reads}\n"
                f"  state before:  {state}\n"
                f"  previous commits:\n{context}")

    for reader in readers.get(model.version, ()):
        check(reader, model.copy())
    for record in writers:
        check(record, model)
        model.version = record.version
        history.append(record)
        for reader in readers.get(model.version, ()):
            check(reader, model.copy())
    assert model.state() == final.state(), \
        f"final state differs from the serial one:\n  database: {final.state()}\n  serial:   {model.state()}"
