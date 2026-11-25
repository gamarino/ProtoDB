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

The operation set is closed: Tasks 2-4, 6 and 7 (object roots, mutable cells, Dictionary,
HashDictionary, Set, CountedSet, RepeatedKeysDictionary and List roots).  A later task adds its
collections to :data:`COLLECTIONS`, its kinds to an ops tuple and to :data:`ALL_OPS`, and each
operation as a ``@_real``-registered function plus a ``Model._<kind>`` method; the import-time
check below rejects a kind missing either.

Each collection family has its operations (``d_*`` dictionaries, ``s_*`` Set, ``cs_*``
CountedSet, ``r_*`` RepeatedKeysDictionary, ``l_*`` List).  A read of a root holding another kind
reads the marker; a write that creates (``d_set``, ``s_add``, ``cs_add``, ``r_add``,
``l_append``) replaces an absent or object root with a new collection of the family; any other
write on a root of another kind writes nothing.  Model contents: a dictionary ``{key: value}``,
a Set ``{element: True}``, a CountedSet ``{element: count}``, a RepeatedKeysDictionary
``{key: {record: count}}``, a List ``[item, ...]``.  A List operation on a position outside the
list (``ProtoValidationError``) reads :data:`OUT_OF_RANGE` and writes nothing.

Task 9 adds equality lookups through an index (``x_eq``, ``x_in``; family ``x_``, the
dictionaries): every Dictionary and HashDictionary the model builds carries the EqualityIndex
:data:`GROUP_INDEX` over the extractor ``value // GROUP_WIDTH`` (:data:`GROUP_EXTRACTOR`: four
groups over the 100 values, dense enough that lookups and writes of one group meet), so every
dictionary write maintains an index from precomputed keys, and a lookup reads the sorted values
of the groups it names (``Unit(INDEX_GROUP)``, R28).  The extractor counts the calls that found
the space root held (:func:`extractor_calls_under_root`, which must stay 0: P-C).

Task 10 adds range reads (R67, R68): every dictionary also carries the RangeIndex
:data:`VALUE_INDEX` over the value itself, ``x_range`` reads the values in ``[low, high]`` through it
(a RangeIndex range scan when the planner picks it: one ``KeyRange`` of index keys), and the
Dictionary windows (family ``w_``, Dictionary only): ``w_after`` reads at most ``value`` keys after
``key`` with ``keys(after=key)`` (``value`` None: all of them; a window range, to the end when
exhausted) and ``w_window`` the values at positions ``key .. key+value-1`` through the planner's ``limit``/``offset`` window (the
range from the first key, or the whole dictionary when the window reaches the end).
:func:`range_reads` counts the range reads that recorded a range rather than the whole root.
"""
import copy
import threading
from itertools import islice
from collections import namedtuple
from dataclasses import dataclass, field

from proto_db.collections.dictionaries import Dictionary, RepeatedKeysDictionary
from proto_db.collections.hash_dictionaries import HashDictionary
from proto_db.collections.lists import List
from proto_db.collections.sets import CountedSet, Set
from proto_db.errors import ProtoValidationError
from proto_db.indexing.equality import EqualityIndex
from proto_db.indexing.keyspec import KeySpec, register_extractor
from proto_db.indexing.range import RangeIndex
from proto_db.query.planner import plan_query
from proto_db.query.terms import BETWEEN, EQ, IN, Term

#: The kind of an object root (R1: any value that is not a collection).
OBJECT = "object"
#: The collection kinds of fine-grained roots, by class name.
COLLECTIONS = {"Dictionary": Dictionary, "HashDictionary": HashDictionary, "Set": Set, "CountedSet": CountedSet,
               "RepeatedKeysDictionary": RepeatedKeysDictionary, "List": List}
#: Every kind a root can hold.
KINDS = (OBJECT,) + tuple(COLLECTIONS)
#: Collection kinds whose keys have a position (R17).
POSITIONAL = ("Dictionary",)
#: The kinds each family of collection operations acts on (the first is its default home).
DICTIONARIES = ("Dictionary", "HashDictionary")
SETS = ("Set",)
COUNTED_SETS = ("CountedSet",)
MULTIMAPS = ("RepeatedKeysDictionary",)
LISTS = ("List",)
#: Positions a List operation draws from (some beyond the list, on purpose), and the depth of a
#: negative ``get_at``.
LIST_POSITIONS = 6
NEGATIVE_DEPTH = 3
#: What a List operation on a position outside the list reads.
OUT_OF_RANGE = "out of range"
#: How many distinct records a RepeatedKeysDictionary operation draws from (collisions on purpose).
RECORDS = 3

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
#: The operations of Task 6.
SET_OPS = ("s_has", "s_add", "s_remove", "s_union", "s_difference", "s_intersection", "s_len")
COUNTED_SET_OPS = ("cs_count", "cs_has", "cs_add", "cs_remove", "cs_items")
MULTIMAP_OPS = ("r_get", "r_has", "r_add", "r_remove_record", "r_remove_key", "r_len")
#: The operations of Task 7 (``l_insert``/``l_remove`` are structural: the whole list, R14).
LIST_OPS = ("l_get", "l_get_neg", "l_set", "l_append", "l_extend", "l_insert", "l_remove", "l_len")
#: The operations of Task 9: equality lookups through :data:`GROUP_INDEX` (``key``: one group,
#: or a tuple of groups for ``x_in``).
LOOKUP_OPS = ("x_eq", "x_in")
#: The operations of Task 10: range reads (``x_range`` through :data:`VALUE_INDEX`, ``key`` the
#: pair ``(low, high)``; the Dictionary windows ``w_after`` and ``w_window``).
RANGE_OPS = ("x_range", "w_after", "w_window")
#: Every operation kind the model knows (each must have a real and a model implementation).
ALL_OPS = DICTIONARY_AND_CELL_OPS + SET_OPS + COUNTED_SET_OPS + MULTIMAP_OPS + LIST_OPS + LOOKUP_OPS + RANGE_OPS
#: The kinds each family's operations act on, by operation prefix.
_FAMILIES = {"d_": DICTIONARIES, "s_": SETS, "cs_": COUNTED_SETS, "r_": MULTIMAPS, "l_": LISTS, "x_": DICTIONARIES,
             "w_": POSITIONAL}

#: The extractor of the dictionaries' index: the group of a value, ``value // GROUP_WIDTH``
#: (values are the ints of ``StateNames.values``, 0..99: :data:`GROUPS` groups).
GROUP_EXTRACTOR = "tests.serial_model.group"
GROUP_WIDTH = 25
GROUPS = 4
#: The name of that index.
GROUP_INDEX = "by_group"
#: Calls of the extractor, and those that found the space root held (P-C: must stay 0).
_extractor_calls = [0, 0]
_extractor_lock = threading.Lock()


def group_of(value) -> int:
    """The index key of a dictionary value: its group."""
    return value // GROUP_WIDTH


def _group(value):
    from proto_db.transactions import Transaction
    held = Transaction._holding_root_lock()
    with _extractor_lock:
        _extractor_calls[0] += 1
        _extractor_calls[1] += held
    return group_of(value)


register_extractor(GROUP_EXTRACTOR, _group)


def extractor_calls_under_root() -> tuple:
    """``(calls, calls with the space root held)`` of the index extractor so far."""
    with _extractor_lock:
        return tuple(_extractor_calls)


def group_index() -> EqualityIndex:
    """The equality index every dictionary of the model carries."""
    return EqualityIndex(GROUP_INDEX, KeySpec.extractor(GROUP_EXTRACTOR))


#: The name of the RangeIndex over the value every dictionary carries (Task 10).
VALUE_INDEX = "by_value"
#: Width bounds of an ``x_range`` interval over the values 0..99: narrow enough that the planner
#: answers it by the range index rather than by a filtered scan of a few keys.
RANGE_WIDTH = (3, 20)
#: How many keys a ``w_after`` reads (None: to the end, so the window is exhausted and covers
#: every later key).
W_AFTER_COUNTS = (1, 2, None)
#: The offsets of a one-row ``w_window``: small, so the window mostly ends before the last key of
#: a dictionary of a few keys and records a range (one reaching the end records the whole root),
#: and mostly after the first key, so a change before the window (which shifts its positions)
#: must conflict.
W_WINDOW_OFFSETS = (1,)


def with_indexes(d):
    """``d`` with the indexes every dictionary of the model carries."""
    return d.add_index(group_index()).add_index(RangeIndex(VALUE_INDEX, KeySpec.element()))


#: Range reads by operation kind: ``[recorded a range, recorded the whole root]``.
_range_reads: dict = {}
_range_lock = threading.Lock()


def _count_range_read(tr, kind: str, ranges_before: int) -> None:
    ranged = len(tr._read_log.ranges()) > ranges_before
    with _range_lock:
        _range_reads.setdefault(kind, [0, 0])[0 if ranged else 1] += 1


def range_reads() -> dict:
    """``{kind: (reads that recorded a range, reads that recorded none: the whole root or a
    range already held)}`` so far (``reset_range_reads`` clears)."""
    with _range_lock:
        return {kind: tuple(counts) for kind, counts in sorted(_range_reads.items())}


def reset_range_reads() -> None:
    with _range_lock:
        _range_reads.clear()
#: Operations that read or write several elements at once (``value`` is a tuple of keys).
_MULTI = ("s_union", "s_difference", "s_intersection", "l_extend")
#: Operations that only read the whole collection, or the root (no key).
_KEYLESS = ("root_get", "root_remove", "d_len", "d_items", "s_len", "cs_items", "r_len", "l_len")

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

    def collection_home(self, root: str, family=DICTIONARIES) -> str:
        """The collection kind a write of ``family`` creates on ``root`` when it holds no collection:
        the root's home kind if it belongs to the family, else the family's first kind."""
        kind = self.roots[root]
        return kind if kind in family else family[0]


@dataclass
class Record:
    """One committed transaction: ``version`` is the database version it published (its place
    in the commit order) or, when it published nothing (``read_only``), the version it read;
    ``base`` is the version its snapshot read (for statistics: what it was reapplied over)."""
    version: int
    program: list
    reads: list
    read_only: bool = False
    thread: int = -1
    base: int = -1


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


def family_of(kind: str):
    """The collection kinds operation ``kind`` acts on, or None (root, object and cell operations)."""
    for prefix, family in _FAMILIES.items():
        if kind.startswith(prefix):
            return family
    return None


def _draw(rng, names: StateNames, kind: str) -> Op:
    if kind in _UNTARGETED:
        return Op(kind)
    if kind in CELL_OPS:
        return Op(kind, rng.choice(names.cells), value=rng.choice(names.values) if kind == "c_put" else None)
    roots = tuple(names.roots)
    family = family_of(kind)
    homes = tuple(name for name in roots if names.roots[name] in (family or ()))
    # A family operation mostly targets a root of its family, sometimes any root (class changes).
    target = rng.choice(homes if homes and rng.random() < 0.8 else roots)
    key = rng.choice(names.keys)
    value = rng.choice(names.values)
    if kind == "root_reset":
        chosen = rng.sample(names.keys, rng.randint(0, len(names.keys)))
        return Op(kind, target, rng.choice(KINDS), tuple(sorted((k, rng.choice(names.values)) for k in chosen)))
    if kind in _KEYLESS:
        return Op(kind, target)
    if kind == "d_key_at":
        return Op(kind, target, rng.randrange(len(names.keys)))
    if kind == "l_extend":
        return Op(kind, target, value=tuple(rng.choice(names.values) for _ in range(rng.randint(0, 3))))
    if kind in _MULTI:
        return Op(kind, target, value=tuple(sorted(rng.sample(names.keys, rng.randint(0, 3)))))
    if kind in ("l_get", "l_remove"):
        return Op(kind, target, rng.randrange(LIST_POSITIONS))
    if kind == "l_get_neg":
        return Op(kind, target, -rng.randint(1, NEGATIVE_DEPTH))
    if kind in ("l_set", "l_insert"):
        return Op(kind, target, rng.randrange(LIST_POSITIONS), value)
    if kind == "l_append":
        return Op(kind, target, value=value)
    if kind in ("r_add", "r_remove_record"):
        return Op(kind, target, key, rng.randrange(RECORDS))
    if kind == "x_eq":
        return Op(kind, target, rng.randrange(GROUPS))
    if kind == "x_in":
        return Op(kind, target, tuple(sorted(rng.sample(range(GROUPS), rng.randint(1, 2)))))
    if kind == "x_range":
        low = rng.choice(names.values)
        return Op(kind, target, (low, low + rng.randint(*RANGE_WIDTH)))
    if kind == "w_after":
        return Op(kind, target, key, rng.choice(W_AFTER_COUNTS))
    if kind == "w_window":
        return Op(kind, target, rng.choice(W_WINDOW_OFFSETS), 1)
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


def fresh_content(kind: str, items: dict) -> dict:
    """The model content of a new value of ``kind`` built from ``items`` (key -> value), as
    :func:`_fresh` builds it: a Set holds the keys, a CountedSet each key ``value % 3 + 1``
    times, a RepeatedKeysDictionary the record ``value % RECORDS`` under each key, a List the
    values in key order."""
    if kind == "List":
        return [v for _, v in sorted(items.items())]
    if kind == "Set":
        return {k: True for k in items}
    if kind == "CountedSet":
        return {k: v % 3 + 1 for k, v in items.items()}
    if kind == "RepeatedKeysDictionary":
        return {k: {v % RECORDS: 1} for k, v in items.items()}
    return dict(items)


def _fresh(tr, kind: str, items: dict):
    """A new value of ``kind`` holding ``items`` (see :func:`fresh_content`), not linked to any root (R11)."""
    if kind == OBJECT:
        return tr.new_object(**items)
    collection = COLLECTIONS[kind](context=tr)
    if kind == "List":
        return collection.extend(v for _, v in sorted(items.items()))
    for k, v in sorted(items.items()):
        if kind == "Set":
            collection = collection.add(k)
        elif kind == "CountedSet":
            for _ in range(v % 3 + 1):
                collection = collection.add(k)
        elif kind == "RepeatedKeysDictionary":
            collection = collection.set_at(k, v % RECORDS)
        else:
            collection = collection.set_at(k, v)
    return with_indexes(collection) if kind in DICTIONARIES else collection


def content_of(value) -> dict:
    """The model content of a collection value."""
    kind = kind_of(value)
    if kind == "Set":
        return {e: True for e in value.elements()}
    if kind == "RepeatedKeysDictionary":
        return {k: dict(records.items()) for k, records in value.items()}
    if kind == "List":
        return list(value.elements())
    return dict(value.items())


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


def _collection(tr, op, kinds=DICTIONARIES):
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


def _creating_write(tr, op, names, family, write, items):
    """A write that creates: through the root's linked collection when it holds one of ``family``
    (``write(collection)``), a new collection of the family holding ``items`` (see
    :func:`fresh_content`) when the root is absent or an object, nothing on another collection."""
    value = tr.get_root_object(op.target)
    kind = kind_of(value)
    if kind in family:
        tr.set_root_object(op.target, write(value))
    elif kind in (None, OBJECT):
        tr.set_root_object(op.target, _fresh(tr, names.collection_home(op.target, family), items))


def _plain_write(tr, op, family, write):
    """A write through the root's linked collection when it holds one of ``family``; else nothing."""
    value, _ = _collection(tr, op, family)
    if value is not None:
        tr.set_root_object(op.target, write(value))


@_real
def _real_d_set(tr, op, names):
    _creating_write(tr, op, names, DICTIONARIES, lambda d: d.set_at(op.key, op.value), {op.key: op.value})


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


def _lookup(tr, op, term):
    d, marker = _collection(tr, op)
    return marker if d is None else tuple(sorted(plan_query(d, term).execute()))


@_real
def _real_x_eq(tr, op, names):
    return _lookup(tr, op, Term("@" + GROUP_EXTRACTOR, EQ, op.key))


@_real
def _real_x_in(tr, op, names):
    return _lookup(tr, op, Term("@" + GROUP_EXTRACTOR, IN, op.key))


@_real
def _real_x_range(tr, op, names):
    d, marker = _collection(tr, op)
    if d is None:
        return marker
    before = len(tr._read_log.ranges())
    found = tuple(sorted(plan_query(d, Term(None, BETWEEN, op.key)).execute()))
    _count_range_read(tr, op.kind, before)
    return found


@_real
def _real_w_after(tr, op, names):
    d, marker = _collection(tr, op, POSITIONAL)
    if d is None:
        return marker
    before = len(tr._read_log.ranges())
    found = tuple(islice(d.keys(after=op.key), op.value))
    _count_range_read(tr, op.kind, before)
    return found


@_real
def _real_w_window(tr, op, names):
    d, marker = _collection(tr, op, POSITIONAL)
    if d is None:
        return marker
    before = len(tr._read_log.ranges())
    found = tuple(plan_query(d, limit=op.value, offset=op.key).execute())
    _count_range_read(tr, op.kind, before)
    return found


@_real
def _real_s_has(tr, op, names):
    s, marker = _collection(tr, op, SETS)
    return marker if s is None else s.has(op.key)


@_real
def _real_s_add(tr, op, names):
    # A new Set over an absent or object root: fresh_content's Set of {key}.
    _creating_write(tr, op, names, SETS, lambda s: s.add(op.key), {op.key: 0})


@_real
def _real_s_remove(tr, op, names):
    _plain_write(tr, op, SETS, lambda s: s.remove(op.key))


@_real
def _real_s_union(tr, op, names):
    _plain_write(tr, op, SETS, lambda s: s.union(op.value))


@_real
def _real_s_difference(tr, op, names):
    _plain_write(tr, op, SETS, lambda s: s.difference(op.value))


@_real
def _real_s_intersection(tr, op, names):
    _plain_write(tr, op, SETS, lambda s: s.intersection(op.value))


@_real
def _real_s_len(tr, op, names):
    s, marker = _collection(tr, op, SETS)
    return marker if s is None else len(s)


@_real
def _real_cs_count(tr, op, names):
    c, marker = _collection(tr, op, COUNTED_SETS)
    return marker if c is None else c.count_of(op.key)


@_real
def _real_cs_has(tr, op, names):
    c, marker = _collection(tr, op, COUNTED_SETS)
    return marker if c is None else c.has(op.key)


@_real
def _real_cs_add(tr, op, names):
    # A new CountedSet over an absent or object root: fresh_content's {key: 1}.
    _creating_write(tr, op, names, COUNTED_SETS, lambda c: c.add(op.key), {op.key: 0})


@_real
def _real_cs_remove(tr, op, names):
    _plain_write(tr, op, COUNTED_SETS, lambda c: c.remove(op.key))


@_real
def _real_cs_items(tr, op, names):
    c, marker = _collection(tr, op, COUNTED_SETS)
    return marker if c is None else tuple(sorted(c.items()))


@_real
def _real_r_get(tr, op, names):
    r, marker = _collection(tr, op, MULTIMAPS)
    return marker if r is None else tuple(sorted(r.get_at(op.key).items()))


@_real
def _real_r_has(tr, op, names):
    r, marker = _collection(tr, op, MULTIMAPS)
    return marker if r is None else r.has(op.key)


@_real
def _real_r_add(tr, op, names):
    _creating_write(tr, op, names, MULTIMAPS, lambda r: r.set_at(op.key, op.value), {op.key: op.value})


@_real
def _real_r_remove_record(tr, op, names):
    _plain_write(tr, op, MULTIMAPS, lambda r: r.remove_record(op.key, op.value))


@_real
def _real_r_remove_key(tr, op, names):
    _plain_write(tr, op, MULTIMAPS, lambda r: r.remove_key(op.key))


@_real
def _real_r_len(tr, op, names):
    r, marker = _collection(tr, op, MULTIMAPS)
    return marker if r is None else len(r)


def _list_op(tr, op, fn):
    """``fn(list)`` on the root's linked List (None on another kind): what it reads, or
    :data:`OUT_OF_RANGE` when the position is outside the list (``ProtoValidationError``)."""
    lst, _ = _collection(tr, op, LISTS)
    if lst is None:
        return None
    try:
        return fn(lst)
    except ProtoValidationError as e:
        assert "out of range" in str(e), e
        return OUT_OF_RANGE


def _list_read(tr, op, fn):
    lst, marker = _collection(tr, op, LISTS)
    return marker if lst is None else _list_op(tr, op, fn)


def _list_write(tr, op, write):
    """``write(list)`` through the root's linked List, assigned to the root; reads None, or
    :data:`OUT_OF_RANGE` (nothing assigned)."""
    def assign(lst):
        tr.set_root_object(op.target, write(lst))
    return _list_op(tr, op, assign)


@_real
def _real_l_get(tr, op, names):
    return _list_read(tr, op, lambda lst: lst.get_at(op.key))


@_real
def _real_l_get_neg(tr, op, names):
    return _list_read(tr, op, lambda lst: lst.get_at(op.key))


@_real
def _real_l_set(tr, op, names):
    return _list_write(tr, op, lambda lst: lst.set_at(op.key, op.value))


@_real
def _real_l_append(tr, op, names):
    # A new List over an absent or object root: fresh_content's List of [value].
    _creating_write(tr, op, names, LISTS, lambda lst: lst.append_last(op.value), {0: op.value})


@_real
def _real_l_extend(tr, op, names):
    return _list_write(tr, op, lambda lst: lst.extend(op.value))


@_real
def _real_l_insert(tr, op, names):
    return _list_write(tr, op, lambda lst: lst.insert_at(op.key, op.value))


@_real
def _real_l_remove(tr, op, names):
    return _list_write(tr, op, lambda lst: lst.remove_at(op.key))


@_real
def _real_l_len(tr, op, names):
    return _list_read(tr, op, len)


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

    def _content(self, op, kinds=DICTIONARIES):
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
        self.roots[op.target] = (op.key, fresh_content(op.key, dict(op.value)))

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

    def _x_eq(self, op):
        content, marker = self._content(op)
        return marker if content is None else tuple(sorted(v for v in content.values() if group_of(v) == op.key))

    def _x_in(self, op):
        content, marker = self._content(op)
        return marker if content is None else tuple(sorted(v for v in content.values() if group_of(v) in op.key))

    def _x_range(self, op):
        content, marker = self._content(op)
        low, high = op.key
        return marker if content is None else tuple(sorted(v for v in content.values() if low <= v <= high))

    def _w_after(self, op):
        content, marker = self._content(op, POSITIONAL)
        return marker if content is None else tuple(sorted(k for k in content if k > op.key)[:op.value])

    def _w_window(self, op):
        content, marker = self._content(op, POSITIONAL)
        if content is None:
            return marker
        return tuple(content[k] for k in sorted(content)[op.key:op.key + op.value])

    def _creating(self, op, family):
        """The content a creating write of ``family`` changes, as :func:`_creating_write`: the
        root's, a new empty one over an absent or object root (the write then adds what
        ``_creating_write``'s fresh collection holds), or None on another collection."""
        kind, content = self.roots.get(op.target, (None, None))
        if kind in family:
            return content
        if kind in (None, OBJECT):
            home = self.names.collection_home(op.target, family)
            self.roots[op.target] = (home, fresh_content(home, {}))
            return self.roots[op.target][1]
        return None

    def _d_set(self, op):
        content = self._creating(op, DICTIONARIES)
        if content is not None:
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

    def _s_has(self, op):
        content, marker = self._content(op, SETS)
        return marker if content is None else op.key in content

    def _s_add(self, op):
        content = self._creating(op, SETS)
        if content is not None:
            content[op.key] = True

    def _s_remove(self, op):
        content, _ = self._content(op, SETS)
        if content is not None:
            content.pop(op.key, None)

    def _s_union(self, op):
        content, _ = self._content(op, SETS)
        if content is not None:
            content.update((k, True) for k in op.value)

    def _s_difference(self, op):
        content, _ = self._content(op, SETS)
        if content is not None:
            for k in op.value:
                content.pop(k, None)

    def _s_intersection(self, op):
        content, _ = self._content(op, SETS)
        if content is not None:
            for k in [k for k in content if k not in op.value]:
                del content[k]

    def _s_len(self, op):
        content, marker = self._content(op, SETS)
        return marker if content is None else len(content)

    def _cs_count(self, op):
        content, marker = self._content(op, COUNTED_SETS)
        return marker if content is None else content.get(op.key, 0)

    def _cs_has(self, op):
        content, marker = self._content(op, COUNTED_SETS)
        return marker if content is None else op.key in content

    def _cs_add(self, op):
        content = self._creating(op, COUNTED_SETS)
        if content is not None:
            content[op.key] = content.get(op.key, 0) + 1

    def _cs_remove(self, op):
        content, _ = self._content(op, COUNTED_SETS)
        if content is not None and op.key in content:    # a decrement never goes below 0 (D26)
            content[op.key] -= 1
            if not content[op.key]:
                del content[op.key]

    def _cs_items(self, op):
        content, marker = self._content(op, COUNTED_SETS)
        return marker if content is None else tuple(sorted(content.items()))

    def _r_get(self, op):
        content, marker = self._content(op, MULTIMAPS)
        return marker if content is None else tuple(sorted(content.get(op.key, {}).items()))

    def _r_has(self, op):
        content, marker = self._content(op, MULTIMAPS)
        return marker if content is None else op.key in content

    def _r_add(self, op):
        content = self._creating(op, MULTIMAPS)
        if content is not None:
            records = content.setdefault(op.key, {})
            records[op.value] = records.get(op.value, 0) + 1

    def _r_remove_record(self, op):
        content, _ = self._content(op, MULTIMAPS)
        records = None if content is None else content.get(op.key)
        if records is not None and op.value in records:
            records[op.value] -= 1
            if not records[op.value]:
                del records[op.value]
            if not records:
                del content[op.key]

    def _r_remove_key(self, op):
        content, _ = self._content(op, MULTIMAPS)
        if content is not None:
            content.pop(op.key, None)

    def _r_len(self, op):
        content, marker = self._content(op, MULTIMAPS)
        return marker if content is None else sum(len(records) for records in content.values())

    def _l_get(self, op):
        content, marker = self._content(op, LISTS)
        if content is None:
            return marker
        return content[op.key] if -len(content) <= op.key < len(content) else OUT_OF_RANGE

    _l_get_neg = _l_get

    def _l_set(self, op):
        content, _ = self._content(op, LISTS)
        if content is None:
            return None
        if not 0 <= op.key < len(content):
            return OUT_OF_RANGE
        content[op.key] = op.value

    def _l_append(self, op):
        content = self._creating(op, LISTS)
        if content is not None:
            content.append(op.value)

    def _l_extend(self, op):
        content, _ = self._content(op, LISTS)
        if content is not None:
            content.extend(op.value)

    def _l_insert(self, op):
        content, _ = self._content(op, LISTS)
        if content is None:
            return None
        if not 0 <= op.key <= len(content):
            return OUT_OF_RANGE
        content.insert(op.key, op.value)

    def _l_remove(self, op):
        content, _ = self._content(op, LISTS)
        if content is None:
            return None
        if not 0 <= op.key < len(content):
            return OUT_OF_RANGE
        del content[op.key]

    def _l_len(self, op):
        content, marker = self._content(op, LISTS)
        return marker if content is None else len(content)

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
            model.roots[name] = (kind, content_of(value))
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


def roots_by_version(records: list, initial: Model) -> dict:
    """Version -> a copy of the roots (name -> (kind, content)) after that version's commit
    (``initial.version`` included), from a serial replay of the published records."""
    model = initial.copy()
    roots = {model.version: copy.deepcopy(model.roots)}
    for record in _writers(records, initial):
        model.run(record.program)
        roots[record.version] = copy.deepcopy(model.roots)
    return roots


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
