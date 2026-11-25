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

A root's kind (:data:`OBJECT` or a key of :data:`COLLECTIONS`) is part of the state, not of
the root's name: ``root_reset`` draws a kind, ``obj_set`` writes an object over a collection
and ``d_set`` a collection over an object, so a linked collection write can meet a concurrent
class change (R12).  Every operation looks at the root's current kind; one that does not apply
to it reads ``("kind", <kind>)`` and writes nothing.

The operation set is closed: Tasks 2-4 (object roots, mutable cells, Dictionary and
HashDictionary roots).  A later task adds its collections to :data:`COLLECTIONS`, its kinds to
an ops tuple and to :data:`ALL_OPS`, and each operation as a ``@_real``-registered function
plus a ``Model._<kind>`` method; the import-time check below rejects a kind missing either.
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
#: Every kind a root can hold.
KINDS = (OBJECT,) + tuple(COLLECTIONS)
#: Collection kinds whose keys have a position (R17).
POSITIONAL = ("Dictionary",)

#: One operation of a program: ``kind`` names it; ``target`` is a root or cell name; ``key``
#: a key, an attribute name, a position or (``root_reset``) the kind written; ``value`` the
#: value written.  Unused fields are None.
Op = namedtuple("Op", "kind target key value", defaults=(None, None, None))

#: The operations of Tasks 2-4, grouped by what they act on.
ROOT_OPS = ("root_get", "root_names", "root_remove", "root_reset")
OBJECT_OPS = ("obj_get", "obj_set")
DICTIONARY_OPS = ("d_get", "d_has", "d_set", "d_remove", "d_len", "d_items", "d_key_at", "d_pos")
CELL_OPS = ("c_get", "c_put", "c_inc", "c_remove", "c_names")
DICTIONARY_AND_CELL_OPS = ROOT_OPS + OBJECT_OPS + DICTIONARY_OPS + CELL_OPS
#: Every operation kind the model knows (each must have a real and a model implementation).
ALL_OPS = DICTIONARY_AND_CELL_OPS

#: Operations that need no target.
_UNTARGETED = ("root_names", "c_names")


@dataclass(frozen=True)
class StateNames:
    """The state the programs act on: root name -> home kind (the kind it starts with, and the
    kind a collection write creates on an absent or object root), the keys (also the object
    attributes), the cell names, and the range of values written."""
    roots: dict
    keys: tuple
    cells: tuple
    values: range = range(100)

    def collection_home(self, root: str) -> str:
        """The collection kind a collection write creates on ``root`` when it holds no collection."""
        kind = self.roots[root]
        return kind if kind in COLLECTIONS else next(iter(COLLECTIONS))


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

def _drawable(kind: str, names: StateNames) -> bool:
    if kind in _UNTARGETED:
        return True
    if kind in CELL_OPS:
        return bool(names.cells)
    return bool(names.roots) and bool(names.keys)


def random_program(rng, state_names: StateNames, ops, max_length: int = 6) -> list:
    """A program of 1 to ``max_length`` operations drawn from ``ops`` with ``rng``.

    Raises ``ValueError`` for an unknown kind, or when no kind of ``ops`` has a target in
    ``state_names`` (no cell for the cell operations, no root or key for the others)."""
    ops = tuple(ops)
    unknown = [kind for kind in ops if kind not in ALL_OPS]
    if unknown:
        raise ValueError(f"Unknown operation kinds {unknown}")
    drawable = tuple(kind for kind in ops if _drawable(kind, state_names))
    if not drawable:
        raise ValueError(f"No operation of {ops} has a target in {state_names}")
    return [_draw(rng, state_names, rng.choice(drawable)) for _ in range(rng.randint(1, max_length))]


def _draw(rng, names: StateNames, kind: str) -> Op:
    if kind in _UNTARGETED:
        return Op(kind)
    if kind in CELL_OPS:
        return Op(kind, rng.choice(names.cells), value=rng.choice(names.values) if kind == "c_put" else None)
    target = rng.choice(tuple(names.roots))
    key = rng.choice(names.keys)
    value = rng.choice(names.values)
    if kind == "root_reset":
        chosen = rng.sample(names.keys, rng.randint(0, len(names.keys)))
        return Op(kind, target, rng.choice(KINDS), tuple(sorted((k, rng.choice(names.values)) for k in chosen)))
    if kind in ("root_get", "root_remove", "d_len", "d_items"):
        return Op(kind, target)
    if kind == "d_key_at":
        return Op(kind, target, rng.randrange(len(names.keys)))
    return Op(kind, target, key, value if kind in ("obj_set", "d_set") else None)


# -- the real execution -------------------------------------------------------------------------

_REAL = {}


def _real(fn):
    """Register ``fn`` (named ``_real_<kind>``) as the real execution of operation ``<kind>``."""
    assert fn.__name__.startswith("_real_"), fn.__name__
    _REAL[fn.__name__[len("_real_"):]] = fn
    return fn


def kind_of(value):
    """The kind of a root value (None when absent)."""
    if value is None:
        return None
    name = type(value).__name__
    return name if name in COLLECTIONS else OBJECT


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


@_real
def _real_root_get(tr, op, names):
    value = tr.get_root_object(op.target)
    kind = kind_of(value)
    if kind == OBJECT:
        return (OBJECT, _attributes(value, names.keys))
    return kind     # a collection root: only presence and class are read


@_real
def _real_root_names(tr, op, names):
    return tuple(tr.root_names())


@_real
def _real_root_remove(tr, op, names):
    tr.remove_root_object(op.target)


@_real
def _real_root_reset(tr, op, names):
    tr.set_root_object(op.target, _fresh(tr, op.key, dict(op.value)))


@_real
def _real_obj_get(tr, op, names):
    obj = tr.get_root_object(op.target)
    kind = kind_of(obj)
    if kind is None:
        return None
    return obj[op.key] if kind == OBJECT else ("kind", kind)


@_real
def _real_obj_set(tr, op, names):
    obj = tr.get_root_object(op.target)
    if kind_of(obj) == OBJECT:
        obj = obj.with_(**{op.key: op.value})
    else:
        obj = tr.new_object(**{op.key: op.value})     # absent or a collection: replaced by an object
    tr.set_root_object(op.target, obj)


def _collection(tr, op, kinds=tuple(COLLECTIONS)):
    """``(root value, marker)``: the marker is what the operation reads when the root does not
    hold one of ``kinds`` (None when absent, ``("kind", <kind>)`` otherwise), else None."""
    d = tr.get_root_object(op.target)
    kind = kind_of(d)
    if kind is None:
        return None, None
    if kind not in kinds:
        return None, ("kind", kind)
    return d, None


@_real
def _real_d_get(tr, op, names):
    d, marker = _collection(tr, op)
    return marker if d is None else d.get_at(op.key)


@_real
def _real_d_has(tr, op, names):
    d, marker = _collection(tr, op)
    return marker if d is None else d.has(op.key)


@_real
def _real_d_set(tr, op, names):
    d = tr.get_root_object(op.target)
    if kind_of(d) in COLLECTIONS:
        d = d.set_at(op.key, op.value)
    else:
        d = _fresh(tr, names.collection_home(op.target), {op.key: op.value})    # absent or an object
    tr.set_root_object(op.target, d)


@_real
def _real_d_remove(tr, op, names):
    d, _ = _collection(tr, op)
    if d is not None:
        tr.set_root_object(op.target, d.remove_at(op.key))


@_real
def _real_d_len(tr, op, names):
    d, marker = _collection(tr, op)
    return marker if d is None else len(d)


@_real
def _real_d_items(tr, op, names):
    d, marker = _collection(tr, op)
    if d is None:
        return marker
    items = tuple(d.items())
    # A Dictionary iterates in key order; a HashDictionary's order is not part of its contract.
    return items if kind_of(d) in POSITIONAL else tuple(sorted(items))


@_real
def _real_d_key_at(tr, op, names):
    d, marker = _collection(tr, op, POSITIONAL)
    if d is None:
        return marker
    n = len(d)
    return (n, None) if n == 0 else (n, d.key_at_position(op.key % n))


@_real
def _real_d_pos(tr, op, names):
    d, marker = _collection(tr, op, POSITIONAL)
    if d is None:
        return marker
    return d.position_of(op.key) if d.has(op.key) else -1


@_real
def _real_c_get(tr, op, names):
    return tr.get_mutable(op.target)


@_real
def _real_c_put(tr, op, names):
    # A blind write: whether the cell exists is decided by the view, which records the cell.
    try:
        tr.set_mutable(op.target, op.value)
    except ProtoValidationError:
        tr.new_mutable(op.target, op.value)


@_real
def _real_c_inc(tr, op, names):
    value = tr.get_mutable(op.target)
    if value is None:
        tr.new_mutable(op.target, 1)
    else:
        tr.set_mutable(op.target, value + 1)
    return value


@_real
def _real_c_remove(tr, op, names):
    try:
        tr.remove_mutable(op.target)
    except ProtoValidationError:
        pass                        # absent in the view: nothing to remove (the read is recorded)


@_real
def _real_c_names(tr, op, names):
    return tuple(tr.mutable_names())


# -- the serial model ---------------------------------------------------------------------------

@dataclass
class Model:
    """An in-memory replica: ``roots`` maps each present root name to ``(kind, content)``, the
    content a dict of attributes (an object) or of items (a collection); ``cells`` maps each
    present cell to its value; ``version`` is the database version this state is."""
    names: StateNames
    roots: dict = field(default_factory=dict)
    cells: dict = field(default_factory=dict)
    version: int = 0

    def copy(self) -> "Model":
        return Model(self.names, copy.deepcopy(self.roots), dict(self.cells), self.version)

    def state(self) -> tuple:
        """What two models compare on (the version is not part of the content)."""
        return self.roots, self.cells

    def kinds(self) -> dict:
        """Root name -> its current kind (absent roots left out)."""
        return {name: kind for name, (kind, _) in self.roots.items()}

    def run(self, program: list) -> list:
        """Apply ``program`` serially; return the value each operation read."""
        return [self.apply(op) for op in program]

    def apply(self, op: Op):
        """Apply one operation with its serial semantics (sections 5 and 6); return what it read."""
        return getattr(self, "_" + op.kind)(op)

    def _content(self, op, kinds=tuple(COLLECTIONS)):
        """``(content, marker)`` as :func:`_collection` gives them for the real root."""
        kind, content = self.roots.get(op.target, (None, None))
        if kind is None:
            return None, None
        if kind not in kinds:
            return None, ("kind", kind)
        return content, None

    def _root_get(self, op):
        kind, content = self.roots.get(op.target, (None, None))
        if kind == OBJECT:
            return (OBJECT, tuple(sorted(content.items())))
        return kind

    def _root_names(self, op):
        return tuple(sorted(self.roots))

    def _root_remove(self, op):
        self.roots.pop(op.target, None)

    def _root_reset(self, op):
        self.roots[op.target] = (op.key, dict(op.value))

    def _obj_get(self, op):
        content, marker = self._content(op, (OBJECT,))
        return marker if content is None else content.get(op.key)

    def _obj_set(self, op):
        content, _ = self._content(op, (OBJECT,))
        if content is None:
            self.roots[op.target] = (OBJECT, {op.key: op.value})
        else:
            content[op.key] = op.value

    def _d_get(self, op):
        content, marker = self._content(op)
        return marker if content is None else content.get(op.key)

    def _d_has(self, op):
        content, marker = self._content(op)
        return marker if content is None else op.key in content

    def _d_set(self, op):
        content, _ = self._content(op)
        if content is None:
            self.roots[op.target] = (self.names.collection_home(op.target), {op.key: op.value})
        else:
            content[op.key] = op.value

    def _d_remove(self, op):
        content, _ = self._content(op)
        if content is not None:
            content.pop(op.key, None)

    def _d_len(self, op):
        content, marker = self._content(op)
        return marker if content is None else len(content)

    def _d_items(self, op):
        content, marker = self._content(op)
        return marker if content is None else tuple(sorted(content.items()))

    def _d_key_at(self, op):
        content, marker = self._content(op, POSITIONAL)
        if content is None:
            return marker
        n = len(content)
        return (n, None) if n == 0 else (n, sorted(content)[op.key % n])

    def _d_pos(self, op):
        content, marker = self._content(op, POSITIONAL)
        if content is None:
            return marker
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


def _check_implementations() -> None:
    """Import-time check: every operation kind has exactly one real and one model implementation."""
    real, modelled = set(_REAL), {kind for kind in ALL_OPS if callable(getattr(Model, "_" + kind, None))}
    missing_real = sorted(set(ALL_OPS) - real)
    missing_model = sorted(set(ALL_OPS) - modelled)
    stray_real = sorted(real - set(ALL_OPS))
    if missing_real or missing_model or stray_real:
        raise ImportError(f"serial_model operations out of step: no real implementation {missing_real}, "
                          f"no model implementation {missing_model}, real but not in ALL_OPS {stray_real}")


_check_implementations()


def capture(tr, names: StateNames) -> Model:
    """The state transaction ``tr`` reads, as a :class:`Model` (``version``: the version it reads)."""
    model = Model(names, version=tr.database.snapshot_at(tr.snapshot_ref).version)
    present = set(tr.root_names())
    unknown = present - set(names.roots)
    assert not unknown, f"roots outside the model: {sorted(unknown)}"
    for name in sorted(present):
        value = tr.get_root_object(name)
        kind = kind_of(value)
        if kind == OBJECT:
            model.roots[name] = (kind, dict(_attributes(value, names.keys)))
        else:
            model.roots[name] = (kind, dict(value.items()))
    for name in tr.mutable_names():
        model.cells[name] = tr.get_mutable(name)
    return model


# -- the checker --------------------------------------------------------------------------------

def _describe(record: Record) -> str:
    return (f"version {record.version} ({'read-only' if record.read_only else 'published'}, "
            f"thread {record.thread}): program {record.program}")


def _writers(records: list, initial: Model) -> list:
    """The published records in version order; asserts the versions run from
    ``initial.version + 1`` with no gap (no commit left out of the history)."""
    writers = sorted((r for r in records if not r.read_only), key=lambda r: r.version)
    versions = [r.version for r in writers]
    expected_versions = list(range(initial.version + 1, initial.version + 1 + len(writers)))
    assert versions == expected_versions, \
        f"commit versions are not consecutive from {initial.version + 1}: {versions[:20]}..."
    return writers


def check_history(records: list, initial: Model, *, final: Model) -> None:
    """Assert that ``records`` is serializable in commit order.

    The committed writers' versions must be ``initial.version + 1`` onward with no gap (no
    commit left out of the history).  Their programs are re-run on a copy of ``initial`` in
    version order: each must read exactly what it read in the database, and the state after the
    last one must equal ``final``.  A read-only record (R23) must read what the state at its
    version gives.  The first mismatch fails with the record, the expected and actual reads,
    and the records before it.
    """
    writers = _writers(records, initial)
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


def kinds_by_version(records: list, initial: Model) -> dict:
    """Version -> the kind of each present root after that version's commit (``initial.version``
    included), from a serial replay of the published records.  For statistics: which change of
    a root a conflicting attempt met."""
    model = initial.copy()
    kinds = {model.version: model.kinds()}
    for record in _writers(records, initial):
        model.run(record.program)
        kinds[record.version] = model.kinds()
    return kinds
