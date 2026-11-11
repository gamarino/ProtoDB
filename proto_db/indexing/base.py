"""The index framework base: :class:`Index` and its hooks (spec section 8.3, decision D29).

Concrete indexes (equality, range, vector, and any new kind) subclass
:class:`Index`.  The extension procedure is documented on the class.
"""
from __future__ import annotations

from abc import ABCMeta, abstractmethod
from collections import Counter
from collections.abc import Iterable

from ..core.atom import RESERVED_NAMES, Atom, PersistenceContext
from ..errors import ProtoCorruptionError, ProtoNotSupportedError, ProtoUserError, ProtoValidationError
from ..query.plans import Estimate, QueryPlan
from ..query.terms import EQ, IS_NONE, NOT_NONE, Term, is_orderable
from ..values import MISSING, check_text, fold, is_atom, order_key
from .keyspec import KeySpec

__all__ = ["Index", "element_identity"]


def element_identity(element):
    """A hashable identity of an element, equal for equal elements; for comparing result multisets.

    Atoms are identified by oid; orderable values by :func:`order_key` (so
    ``1`` and ``1.0`` are one element); lists, tuples and dicts item by item at any depth (an
    explicit stack; a list never equals a tuple); NaN equals NaN.
    """
    return fold(element, _identity_expand)


def _identity_expand(element):
    if isinstance(element, list):
        return element, _list_identity
    if isinstance(element, tuple):
        return element, _tuple_identity
    if isinstance(element, dict):
        return [x for pair in element.items() for x in pair], _dict_identity
    if not is_atom(element) and element != element:
        return None, ("nan",)
    return None, order_key(element)


def _list_identity(items: list) -> tuple:
    return ("list", tuple(items))


def _tuple_identity(items: list) -> tuple:
    return ("tuple", tuple(items))


def _dict_identity(items: list) -> tuple:
    return ("dict", frozenset(zip(items[0::2], items[1::2])))


class Index(Atom, metaclass=ABCMeta):
    """Base class of every index: a persistent, immutable atom maintained by its collection.

    Persisted state: its public instance attributes -- the definition fields
    named in ``_definition`` (at least ``name``, a non-empty str, and
    ``key_spec``, a :class:`KeySpec`) plus whatever content the subclass keeps
    (typically persistent collections such as a ``HashDictionary`` or a keyed
    AVL of ``CountedSet`` s).  Every hook returns a NEW index and never changes
    the receiver; collections call the element hooks from their mutators
    (``IndexSet.on_add/on_remove/on_replace``) and build an index with
    ``index.empty().build(elements)``.

    Writing a new index kind
    ------------------------
    1. Subclass :class:`Index` and give it a stable ``__type_name__`` in the
       class body (required for every concrete class, ``ProtoUserError``
       otherwise; abstract intermediate classes may omit it).
    2. ``__init__(self, name, key_spec, <extra definition fields>, *, context=None)``
       must create an EMPTY index: call ``super().__init__(name, key_spec,
       context=context)`` and set every other public field with
       ``object.__setattr__`` (indexes are immutable).  Extra definition fields
       (e.g. a vector metric) are accepted as keyword arguments and listed in
       ``_definition = Index._definition + ("metric",)``, so :meth:`empty` can
       rebuild the definition.  Never name a public field after a method or
       class attribute.
    3. Implement the abstract hooks:

       * ``with_added(element)`` / ``with_removed(element)`` -- the index after
         one more / one fewer occurrence of ``element``.  Elements may repeat
         (a List holds duplicates), so keep a multiset.  Use
         ``self.key_spec.extract(element)`` for the key; MISSING (absent key)
         must be recorded too, so that ``?N`` agrees with a scan.  Build the
         new version with :meth:`_with`.
       * ``supports(term)`` -- start with ``self._on_key(term)`` (an index on
         ``city`` never answers a term on ``zip``) and accept only operators
         whose answer equals :meth:`Term.matches` exactly.
       * ``estimate(term) -> Estimate(rows, cost)`` for the planner; an
         unsupported term raises ``ProtoNotSupportedError`` (use
         ``self._check_supported(term)``).
       * ``plan(term) -> IndexPlan`` -- call ``self._check_supported(term)``
         first (an unsupported term raises ``ProtoNotSupportedError``) and
         return ``IndexPlan(self, term, producer, kind=...)`` where
         ``producer()`` returns a fresh iterator over the matching elements,
         each occurrence once.  Capture immutable state only, so the plan can
         run several times and from several threads.

    4. Ordered iteration (used by ``order_by``): an index that can yield its
       elements in key order overrides ``supports_order(key_spec)`` (True only
       for keys it can order, normally ``key_spec == self.key_spec``) and
       ``ordered_plan(descending=False)``: every stored occurrence, ascending by
       ``order_key`` of the extracted key (MISSING < None < bool < number < ...,
       unorderable keys after every orderable one); descending is the exact
       reverse of that key-group order; the order inside one key group is
       unspecified.  Return ``IndexPlan(self, None, producer, kind=...,
       estimate=Estimate(...))``.  The defaults declare no ordering.
    5. Residual predicates (``near[]`` with ``k``, ruling R27): an index whose
       answer to a term depends on the whole collection (k nearest) cannot be
       post-filtered without changing the meaning of ``term AND P``.  Such an
       index overrides ``supports_filtered(term)`` (True for the terms it can
       restrict) and ``filtered_plan(term, where)``: the answer to ``term``
       computed over only the elements satisfying the Expression ``where``.
       The planner hands every other conjunct of a ``near[]`` conjunction with
       ``k`` to it (D37 amendment).  The defaults declare no such support.
    6. Override defaults when the structure allows better:
       ``with_replaced`` (default: remove then add), ``build`` (default: fold
       of ``with_added`` starting from :meth:`empty`; bulk-load for speed),
       ``validate`` (default: plan == scan for every supported ``==`` on the
       elements' keys, ``?N`` and ``?!N``; add structural checks), ``explain``
       (add details, keep the ``index``/``kind``/``key`` entries) and
       ``empty`` (only if ``__init__`` cannot be called with the definition).
    7. Prove it: register an ``IndexContract`` for the new kind in
       ``tests/contracts/index_contract.py`` (index factory, element strategy,
       optional term strategy and oracle).  The contract checks plan == scan,
       mutation sequences, persistence, concurrent readers, unsupported terms,
       attachment to every registered collection, immutability of the
       receiver, ``empty``, ordered iteration and the default equivalences.  A kind that passes
       it implements every hook correctly.
    """

    __type_name__ = "proto_db.indexing.Index"
    #: Public fields that define the index (kept by :meth:`empty`).
    _definition: tuple = ("name", "key_spec")
    #: True when every plan of this index is a deterministic function of the index's stored
    #: content and the plan's arguments, so its results may be cached by the index's pointer
    #: (D56).  False by default: every class opts in explicitly, subclasses included.
    _plan_cacheable: bool = False
    #: Public fields holding a :class:`KeySpec`, persisted as plain data.
    _keyspec_fields: tuple = ("key_spec",)

    def __init_subclass__(cls, **kwargs):
        missing = {"name", "key_spec"} - set(cls._definition)
        if missing:
            raise ProtoUserError(f"{cls.__name__}._definition must include {sorted(missing)}")
        # ABCMeta computes __abstractmethods__ only after this hook, so look for abstract members directly.
        abstract = any(getattr(getattr(cls, n, None), "__isabstractmethod__", False) for n in dir(cls))
        if not abstract and "__type_name__" not in cls.__dict__:
            raise ProtoUserError(f"Concrete index class {cls.__qualname__} must set __type_name__ in its own "
                                 f"body: persisted indexes are found by that stable name")
        super().__init_subclass__(**kwargs)

    def __init__(self, name: str, key_spec: KeySpec, *, context: PersistenceContext | None = None):
        """Create the definition part of an index; subclasses add their (empty) content."""
        if not isinstance(name, str) or not name:
            raise ProtoValidationError(f"An index needs a non-empty str name, not {name!r}")
        check_text(name)
        if not isinstance(key_spec, KeySpec):
            raise ProtoValidationError(f"key_spec must be a KeySpec, not {type(key_spec).__name__}")
        super().__init__(context=context)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "key_spec", key_spec)

    # -- immutability and persistence ------------------------------------------------------

    def __setattr__(self, name, value):
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        raise ProtoUserError(f"{type(self).__name__} is immutable; cannot set {name!r}")

    def __delattr__(self, name):
        if name.startswith("_"):
            object.__delattr__(self, name)
            return
        raise ProtoUserError(f"{type(self).__name__} is immutable; cannot delete {name!r}")

    def _public_state(self) -> dict:
        # One complete copy of the loaded state, safe across an unload by the atom cache (8.3m).
        return {k: v for k, v in self._snapshot().items()
                if not k.startswith("_") and k not in RESERVED_NAMES}

    def _state(self) -> dict:
        state = self._public_state()
        for name in self._keyspec_fields:
            if isinstance(state.get(name), KeySpec):
                state[name] = state[name].to_data()
        return state

    def _restore(self, state: dict) -> None:
        state = dict(state)
        for name in self._keyspec_fields:
            if name in state:
                state[name] = KeySpec.from_data(state[name])
        super()._restore(state)

    def _with(self, **changes) -> "Index":
        """Return a new index of the same type: this one's public state updated by ``changes``.

        Only existing public fields may be changed (``ProtoValidationError``
        otherwise).  The receiver is never modified.
        """
        state = self._public_state()
        unknown = sorted(set(changes) - set(state))
        if unknown:
            raise ProtoValidationError(f"{type(self).__name__}._with got unknown fields {unknown}")
        state.update(changes)
        new = object.__new__(type(self))
        Atom.__init__(new, context=self.context)
        for name, value in state.items():
            object.__setattr__(new, name, value)
        return new

    # -- helpers for subclasses ----------------------------------------------------------

    def _plans_deterministic(self) -> bool:
        """Whether this index version's plans may be cached by its pointer (D56); default
        ``_plan_cacheable``.  An index whose answers depend on more than its stored content
        (process state, randomness) returns False.  The flag is read from the index's own
        class only: a subclass may change what plans compute, so it opts in by restating
        ``_plan_cacheable = True`` (R99 M3)."""
        return vars(type(self)).get("_plan_cacheable", False) is True

    def _on_key(self, term: Term) -> bool:
        """True iff ``term`` names this index's key (its path is ``key_spec.term_path``)."""
        return term.key_spec == self.key_spec

    def _check_supported(self, term: Term) -> None:
        """Raise ``ProtoNotSupportedError`` unless :meth:`supports` accepts ``term``."""
        if not self.supports(term):
            raise ProtoNotSupportedError(
                f"Index {self.name!r} ({type(self).__name__} on {self.key_spec.describe()}) "
                f"does not support {term.op!r} on {term.path!r}")

    # -- abstract hooks ------------------------------------------------------------------

    @abstractmethod
    def with_added(self, element) -> "Index":
        """Return the index after adding one occurrence of ``element``."""

    @abstractmethod
    def with_removed(self, element) -> "Index":
        """Return the index after removing one occurrence of ``element`` (which it holds)."""

    @abstractmethod
    def supports(self, term: Term) -> bool:
        """True iff :meth:`plan` can answer ``term`` exactly (same result as a scan)."""

    @abstractmethod
    def estimate(self, term: Term) -> Estimate:
        """Planner estimate for answering a supported ``term``; ``ProtoNotSupportedError`` otherwise."""

    @abstractmethod
    def plan(self, term: Term) -> QueryPlan:
        """An executable plan (an ``IndexPlan``) yielding the elements matching ``term``.

        Raises ``ProtoNotSupportedError`` when :meth:`supports` is False.
        """

    # -- defaults --------------------------------------------------------------------------

    def supports_order(self, key_spec: KeySpec) -> bool:
        """True iff :meth:`ordered_plan` yields the elements ordered by ``key_spec`` (default False)."""
        return False

    def ordered_plan(self, descending: bool = False) -> QueryPlan:
        """Every element in key order (see the class docstring); default ``ProtoNotSupportedError``."""
        raise ProtoNotSupportedError(
            f"Index {self.name!r} ({type(self).__name__}) does not provide ordered iteration")

    def supports_filtered(self, term: Term) -> bool:
        """True iff :meth:`filtered_plan` answers ``term`` restricted to a predicate (default False)."""
        return False

    def filtered_plan(self, term: Term, where) -> QueryPlan:
        """``term`` answered over only the elements satisfying ``where``; default ``ProtoNotSupportedError``."""
        raise ProtoNotSupportedError(
            f"Index {self.name!r} ({type(self).__name__}) cannot answer {term.op!r} restricted to a predicate")

    def with_replaced(self, old, new) -> "Index":
        """Return the index after replacing one occurrence of ``old`` by ``new`` (default: remove + add)."""
        return self.with_removed(old).with_added(new)

    def has_current_tokens(self) -> bool:
        """Whether this index may be changed in place.  An index whose stored member keys were
        written under an older token scheme returns False, and its collection rebuilds it from
        its elements before its next change (D58).  Default True."""
        return True

    def empty(self) -> "Index":
        """An empty index with the same definition: ``type(self)(**definition, context=...)``."""
        definition = {name: getattr(self, name) for name in self._definition}
        return type(self)(**definition, context=self.context)

    def build(self, elements: Iterable) -> "Index":
        """An index over exactly ``elements``: a fold of :meth:`with_added` starting from :meth:`empty`.

        The receiver's own content is never kept, so building a populated index
        is safe.
        """
        index = self.empty()
        for element in elements:
            index = index.with_added(element)
        return index

    def explain(self) -> dict:
        """``{"index": name, "kind": class name, "key": key description}``."""
        return {"index": self.name, "kind": type(self).__name__, "key": self.key_spec.describe()}

    def validate(self, elements: Iterable) -> None:
        """Check this index against a full scan of ``elements``; ``ProtoCorruptionError`` on mismatch.

        For every supported term among ``==`` on each distinct orderable key of
        the elements, ``?N`` and ``?!N``, the plan's result multiset must equal
        the scan's.  Subclasses may add structural checks.
        """
        elements = list(elements)
        path = self.key_spec.term_path
        keys: dict = {}
        for element in elements:
            key = self.key_spec.extract(element)
            if key is not MISSING and is_orderable(key):
                keys.setdefault(order_key(key), key)
        terms = [Term(path, EQ, key) for key in keys.values()] + [Term(path, IS_NONE), Term(path, NOT_NONE)]
        for term in terms:
            if not self.supports(term):
                continue
            expected = Counter(element_identity(e) for e in elements if term.matches(e))
            got = Counter(element_identity(e) for e in self.plan(term).execute())
            if got != expected:
                raise ProtoCorruptionError(
                    f"Index {self.name!r} ({type(self).__name__}) is inconsistent for {term.op!r} "
                    f"{term.value!r} on {self.key_spec.describe()}: it returns {sum(got.values())} "
                    f"elements, a scan {sum(expected.values())} ({sum((got - expected).values())} extra, "
                    f"{sum((expected - got).values())} missing)")
