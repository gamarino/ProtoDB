"""VectorIndex: k-nearest-neighbour and threshold search on one key (spec section 8.3, D39, ruling R27).

``VectorIndex(name, key_spec, dim, metric="cosine", backend="exact", **params)``
answers ``Term(path, "near[]", NearQuery(vector, k, threshold, metric))`` on its
key.  It is the third built-in index kind, written purely against the
:class:`proto_db.indexing.base.Index` hooks.

Persisted state (public fields; every structure is immutable and shared between versions):

* the definition -- ``name``, ``key_spec``, ``dim``, ``metric``, ``backend``
  and ``params`` (the validated backend parameters, a sorted tuple of pairs);
* ``store`` -- a keyed AVL tree mapping ``(list number, member key)`` to
  ``(element, count, vector)``: the persistent id -> vector store.  The member
  key is the exact member key of :mod:`.keyed` (ruling R19), so an element held
  twice in a List is one member with count 2 and plans return the stored
  elements themselves.  The list number is the IVF inverted list (-1 while an
  IVF index is untrained; 0 for the other backends);
* ``invalid`` -- the group (as in :mod:`.keyed`) of the elements whose key is
  MISSING, None or not a vector of ``dim`` finite numbers: never returned by a
  search, counted by ``explain`` and checked by ``validate``;
* ``total_count`` -- every stored occurrence; ``centroids`` and
  ``trained_size`` -- the IVF training (empty / 0 for the other backends).

Search semantics (R27; scores and backends in :mod:`.vector_backends`):

* ``k`` -- the ``k`` nearest members (ties broken by member key), each with
  all its occurrences; ``threshold`` -- every member within it (cosine:
  similarity >= threshold, l2: distance <= threshold, ip: score >= threshold);
  both -- the ``k`` nearest among those within the threshold.  Results come
  nearest first.
* ``NearQuery.metric`` None means the index metric; another metric, or a query
  of another dimension, is not supported (``supports`` is False).
* ``filtered_plan(term, where)`` answers the query among the elements
  satisfying the Expression ``where`` only: ``near(k) AND P`` is the k nearest
  among the elements satisfying P (the planner passes the other conjuncts here
  when ``k`` is set, D37 amendment).

:func:`near_oracle` is the brute-force definition of these semantics, used by
the index contract in place of ``Term.matches`` (which cannot evaluate
``near[]``).
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterable

from ..collections import avl
from ..core.atom import PersistenceContext
from ..errors import ProtoCorruptionError, ProtoValidationError
from ..query.expressions import Expression, evaluate, iter_terms
from ..query.plans import Estimate, IndexPlan
from ..query.terms import NEAR, NearQuery, Term
from ..values import exact_token, order_key
from .base import Index, element_identity
from .keyed import (EMPTY_GROUP, MEMBER_TOKENS, check_member_key, group_add, group_remove, member_key,
                    member_key_matches)
from .keyspec import KeySpec
from .vector_backends import (METRICS, backend_for, canonical_vector, iter_entries,
                              normalize_params, rank_entries, store_key_okey)

__all__ = ["VectorIndex", "near_oracle"]


#: MessagePack containers a store key ``(list number, member key)`` adds around the member key
#: (an ``@tup``): an element with a valid vector may nest 2 containers less than in a key-group
#: index (format spec, "Value nesting depth").
STORE_KEY_WRAPPING = 2


def _check_dim(dim) -> int:
    if isinstance(dim, bool) or not isinstance(dim, int) or dim < 1:
        raise ProtoValidationError(f"A vector index needs an int dim >= 1, not {dim!r}")
    return dim


def _check_metric(metric) -> str:
    if metric not in METRICS:
        raise ProtoValidationError(f"Unknown vector metric {metric!r}; expected one of {list(METRICS)}")
    return metric


def _predicate(where) -> Callable | None:
    """``where`` (None, an Expression, the 0.1 list syntax or a callable) as ``element -> bool``."""
    if where is None:
        return None
    if callable(where) and not isinstance(where, Expression):
        return lambda e: bool(where(e))
    expr = Expression.compile(where)
    return lambda e: evaluate(expr, e)


def near_oracle(elements: Iterable, key_spec: KeySpec, near_query: NearQuery, metric: str,
                predicate=None) -> list:
    """The exact answer to ``near_query`` over ``elements`` by brute force (the vector scan oracle).

    ``metric`` is the index metric, used when ``near_query.metric`` is None;
    ``predicate`` (None, an Expression, the 0.1 list syntax or a callable)
    restricts the search to the elements satisfying it.  Keys that are not
    vectors of the query's dimension are ignored.  Elements are grouped into
    members by exact member key; the result lists each selected member's
    occurrences, nearest member first (module docstring).
    """
    if not isinstance(near_query, NearQuery):
        raise ProtoValidationError(f"near_oracle needs a NearQuery, not {near_query!r}")
    metric = _check_metric(near_query.metric or metric)
    if not isinstance(key_spec, KeySpec):
        raise ProtoValidationError(f"near_oracle needs a KeySpec, not {key_spec!r}")
    q, dim = near_query.vector, len(near_query.vector)
    satisfies = _predicate(predicate)
    members: dict = {}
    for element in elements:
        vector = canonical_vector(key_spec.extract(element), dim)
        if vector is None or (satisfies is not None and not satisfies(element)):
            continue
        mk = member_key(element)
        slot = members.get(okey := order_key(mk))
        if slot is None:
            members[okey] = [mk, element, 1, vector]
        else:
            slot[2] += 1
    ranked = rank_entries((tuple(m) for m in members.values()), q, metric, near_query.k, near_query.threshold)
    return [element for element, count in ranked for _ in range(count)]


class VectorIndex(Index):
    """Vector index (module docstring): ``VectorIndex(name, key_spec, dim, metric, backend, **params)``.

    ``key_spec`` is a :class:`KeySpec` or a dotted path str; ``dim`` an int >= 1;
    ``metric`` one of ``"cosine"``, ``"l2"``, ``"ip"``; ``backend`` one of
    ``"exact"``, ``"ivf_flat"``, ``"hnsw"``; ``params`` the backend's
    parameters (see :mod:`.vector_backends`; unknown names raise
    ``ProtoValidationError``).
    """

    __type_name__ = "proto_db.indexing.VectorIndex"
    _definition = Index._definition + ("dim", "metric", "backend", "params")
    _plan_cacheable = True
    _required_fields = ("name", "key_spec", "dim", "metric", "backend", "params", "store", "invalid",
                        "total_count", "centroids", "trained_size")

    def __init__(self, name: str, key_spec: KeySpec | str, dim: int, metric: str = "cosine",
                 backend: str = "exact", *, context: PersistenceContext | None = None, **params):
        """Create an EMPTY vector index (``params`` are the backend parameters)."""
        if isinstance(key_spec, str):
            key_spec = KeySpec.path(key_spec)
        super().__init__(name, key_spec, context=context)
        osa = object.__setattr__
        osa(self, "dim", _check_dim(dim))
        osa(self, "metric", _check_metric(metric))
        backend_for(backend)
        osa(self, "backend", backend)
        osa(self, "params", normalize_params(backend, params))
        osa(self, "store", None)
        osa(self, "invalid", EMPTY_GROUP)
        osa(self, "total_count", 0)
        osa(self, "centroids", ())
        osa(self, "trained_size", 0)
        osa(self, "member_tokens", MEMBER_TOKENS)

    def has_current_tokens(self) -> bool:
        """False for an index written by format 3 (Python-text member tokens): its collection
        rebuilds it before changing it (D58)."""
        return getattr(self, "member_tokens", None) == MEMBER_TOKENS

    def empty(self) -> "VectorIndex":
        """An empty index with this definition (parameters included)."""
        return type(self)(self.name, self.key_spec, self.dim, self.metric, self.backend,
                           context=self.context, **dict(self.params))

    def _impl(self):
        return backend_for(self.backend)

    def _plans_deterministic(self) -> bool:
        """Cacheable by pointer (D56) unless the backend's search is not deterministic (hnsw) or
        a subclass did not opt in (it must restate ``_plan_cacheable``)."""
        return super()._plans_deterministic() and self._impl().deterministic

    def _vector(self, element):
        return canonical_vector(self.key_spec.extract(element), self.dim)

    # -- element hooks ------------------------------------------------------------------------

    def with_added(self, element) -> "VectorIndex":
        """The index after one more occurrence of ``element`` (an invalid key goes to ``invalid``)."""
        vector = self._vector(element)
        mk = member_key(element)
        if vector is None:
            check_member_key(self, element, mk)
            return self._with(invalid=group_add(self.invalid, element, mk), total_count=self.total_count + 1)
        check_member_key(self, element, mk, STORE_KEY_WRAPPING)
        impl = self._impl()
        list_no = impl.list_for(self, vector)
        key = (list_no, mk)
        entry = avl.key_get(self.store, key)
        value = (element, 1, vector) if entry is None else (entry[0], entry[1] + 1, entry[2])
        grown = self._with(store=avl.key_put(self.store, key, value)[0], total_count=self.total_count + 1)
        if entry is None and impl.needs_training(grown):
            return grown._retrained()
        return grown

    def with_removed(self, element) -> "VectorIndex":
        """The index after one occurrence of ``element`` fewer; ``ProtoValidationError`` if it holds none.

        Only the member whose store key is ``(list of its vector, its member key)`` is removed,
        never an equal member (D58, as in :mod:`.keyed`).
        """
        vector = self._vector(element)
        if vector is None:
            invalid = group_remove(self.invalid, element)
            if invalid is None:
                self._not_held(element)
            return self._with(invalid=invalid, total_count=self.total_count - 1)
        list_no = self._impl().list_for(self, vector)
        key = (list_no, member_key(element))
        entry = avl.key_get(self.store, key)
        if entry is None:
            self._not_held(element)
        store = (avl.key_remove(self.store, key)[0] if entry[1] == 1
                 else avl.key_put(self.store, key, (entry[0], entry[1] - 1, entry[2]))[0])
        return self._with(store=store, total_count=self.total_count - 1)

    def _not_held(self, element):
        raise ProtoValidationError(
            f"Index {self.name!r} (VectorIndex on {self.key_spec.describe()}) does not hold {element!r}; "
            f"an index can only remove an element it holds")

    def build(self, elements: Iterable) -> "VectorIndex":
        """An index over exactly ``elements``, bulk-built: members grouped, trained once, then balanced trees."""
        empty = self.empty()
        members: dict = {}
        invalid: dict = {}
        total = 0
        for element in elements:
            total += 1
            vector = empty._vector(element)
            mk = member_key(element)
            check_member_key(self, element, mk, 0 if vector is None else STORE_KEY_WRAPPING)
            target = invalid if vector is None else members
            slot = target.get(okey := order_key(mk))
            if slot is None:
                target[okey] = [mk, element, 1, vector]
            else:
                slot[2] += 1
        invalid_group = EMPTY_GROUP
        if invalid:
            ordered = sorted(invalid.items(), key=_first)
            invalid_group = (avl.key_from_sorted_okeys((ok, mk, (e, n)) for ok, (mk, e, n, _) in ordered),
                             sum(n for _, (_, _, n, _) in ordered))
        index = empty._with(invalid=invalid_group, total_count=total)
        return index._with_members(sorted(members.items(), key=_first))

    def _with_members(self, ordered: list) -> "VectorIndex":
        """This index trained on and holding exactly ``[(member okey, [mk, element, count, vector])]``
        (sorted by member okey); the backend decides whether there is anything to train."""
        impl = self._impl()
        centroids = impl.train(self, [v for _, (_, _, _, v) in ordered])
        index = self._with(centroids=centroids, trained_size=len(ordered) if centroids else 0)
        triples = []
        for okey, (mk, element, count, vector) in ordered:
            list_no = impl.list_for(index, vector)
            triples.append((store_key_okey(list_no, okey), (list_no, mk), (element, count, vector)))
        triples.sort(key=_first)
        return index._with(store=avl.key_from_sorted_okeys(triples))

    def _retrained(self) -> "VectorIndex":
        """This index retrained on its own members (IVF retrain policy, D39)."""
        ordered = sorted(((order_key(mk), [mk, e, n, v]) for mk, e, n, v in iter_entries(self.store)), key=_first)
        return self._with_members(ordered)

    # -- terms and plans ----------------------------------------------------------------------------

    def supports(self, term: Term) -> bool:
        """``near[]`` on this key, of this index's dimension, with the index metric (or None)."""
        if not self._on_key(term) or term.op != NEAR:
            return False
        query = term.value
        return (query.metric is None or query.metric == self.metric) and \
            canonical_vector(query.vector, self.dim) is not None

    def supports_filtered(self, term: Term) -> bool:
        """Every supported term can be restricted to a residual predicate (ruling R27)."""
        return self.supports(term)

    def estimate(self, term: Term) -> Estimate:
        """The backend's estimate (D30 units)."""
        self._check_supported(term)
        return self._impl().estimate(self, term.value.k, term.value.threshold, False)

    def plan(self, term: Term) -> IndexPlan:
        """The members answering ``term`` (module docstring), nearest first, each occurrence once."""
        self._check_supported(term)
        return self._plan(term, None)

    def filtered_plan(self, term: Term, where) -> IndexPlan:
        """``term`` answered among the elements satisfying the Expression ``where`` only (R27)."""
        self._check_supported(term)
        expr = Expression.compile(where)
        if any(t.op == NEAR for t in iter_terms(expr)):
            raise ProtoValidationError(f"A residual predicate cannot hold a 'near[]' term ({expr!r})")
        return self._plan(term, expr)

    def _plan(self, term: Term, where) -> IndexPlan:
        query = term.value
        q, k, threshold = canonical_vector(query.vector, self.dim), query.k, query.threshold
        impl = self._impl()
        index = self
        eligible = None if where is None else _predicate(where)

        def produce():
            for element, count in impl.search(index, q, k, threshold, eligible):
                for _ in range(count):
                    yield element

        details = {"backend": self.backend, "metric": self.metric}
        if where is not None:
            details["where"] = where.describe()
        return IndexPlan(self, term, produce, kind=impl.kind,
                         estimate=impl.estimate(self, k, threshold, where is not None), details=details,
                         cache_args=(("where", where),))

    # -- explain and validate ---------------------------------------------------------------------------

    def explain(self) -> dict:
        """The base entries plus the definition and counts: ``entries`` (occurrences), ``vectors``
        (occurrences with a valid vector), ``members`` (distinct ones), ``invalid`` and backend details."""
        info = super().explain()
        members = 0 if self.store is None else self.store.count
        info.update(dim=self.dim, metric=self.metric, backend=self.backend, params=dict(self.params),
                    entries=self.total_count, vectors=self.total_count - self.invalid[1], members=members,
                    invalid=self.invalid[1])
        info.update(self._impl().explain(self))
        return info

    def _corrupt(self, detail: str):
        raise ProtoCorruptionError(f"Index {self.name!r} (VectorIndex on {self.key_spec.describe()}) is "
                                   f"corrupt: {detail}")

    def _check_centroids(self) -> None:
        centroids, trained = self.centroids, self.trained_size
        if not isinstance(centroids, tuple) or type(trained) is not int or trained < 0:
            self._corrupt(f"malformed training state {centroids!r} / {trained!r}")
        if self.backend != "ivf_flat":
            if centroids or trained:
                self._corrupt(f"a {self.backend!r} index holds IVF centroids")
            return
        if centroids and len(centroids) != dict(self.params)["nlist"]:
            self._corrupt(f"{len(centroids)} centroids for nlist={dict(self.params)['nlist']}")
        if bool(centroids) != bool(trained):
            self._corrupt(f"trained_size {trained} does not match {len(centroids)} centroids")
        for c in centroids:
            if canonical_vector(c, self.dim) != c:
                self._corrupt(f"malformed centroid {c!r}")

    def _check_store(self, stored: Counter) -> int:
        avl.check_invariants(self.store)
        current = self.has_current_tokens()
        impl = self._impl()
        seen = 0
        for key, entry in avl.key_iter(self.store):
            if not isinstance(key, tuple) or len(key) != 2 or type(key[0]) is not int:
                self._corrupt(f"malformed store key {key!r}")
            if not isinstance(entry, tuple) or len(entry) != 3:
                self._corrupt(f"malformed store entry {entry!r}")
            element, n, vector = entry
            if type(n) is not int or n < 1:
                self._corrupt(f"element {element!r} has the count {n!r}")
            if not member_key_matches(key[1], element, current):
                self._corrupt(f"element {element!r} is stored under the wrong member key {key[1]!r}")
            actual = self._vector(element)
            if actual is None or actual != vector:
                self._corrupt(f"element {element!r} is stored with the vector {vector!r}, its key gives {actual!r}")
            expected_list = impl.list_for(self, vector)
            if key[0] != expected_list:
                self._corrupt(f"element {element!r} is in list {key[0]}, its vector belongs to {expected_list}")
            stored[("vector", element_identity(element), exact_token(element))] += n
            seen += n
        return seen

    def _check_invalid(self, stored: Counter) -> int:
        group = self.invalid
        if not isinstance(group, tuple) or len(group) != 2:
            self._corrupt(f"malformed invalid group {group!r}")
        members, total = group
        avl.check_invariants(members)
        current = self.has_current_tokens()
        seen = 0
        for key, entry in avl.key_iter(members):
            if not isinstance(entry, tuple) or len(entry) != 2:
                self._corrupt(f"malformed invalid entry {entry!r}")
            element, n = entry
            if type(n) is not int or n < 1:
                self._corrupt(f"element {element!r} has the count {n!r}")
            if not member_key_matches(key, element, current):
                self._corrupt(f"element {element!r} is kept under the wrong member key {key!r}")
            if self._vector(element) is not None:
                self._corrupt(f"element {element!r} has a valid vector but is kept as invalid")
            stored[("invalid", element_identity(element), exact_token(element))] += n
            seen += n
        if total != seen:
            self._corrupt(f"the invalid group records {total!r} occurrences but holds {seen}")
        return seen

    def validate(self, elements: Iterable) -> None:
        """Structural check against ``elements`` (``ProtoCorruptionError`` on any mismatch).

        Checks the AVL invariants, the training state, that every stored member
        is under its own member key and list with its own vector and a positive
        count, that invalid keys are kept aside, that the totals add up, and that
        the stored multiset of (valid or not, element, exact token) equals that
        of ``elements``.  That determines every answer of the exact backends.  On
        an index written by format 3 the member keys' tokens are not compared
        (as in :meth:`KeyGroupIndex.validate`).
        """
        self._check_centroids()
        stored: Counter = Counter()
        total = self._check_store(stored) + self._check_invalid(stored)
        if total != self.total_count:
            self._corrupt(f"total_count is {self.total_count!r} but {total} occurrences are stored")
        expected = Counter(("invalid" if self._vector(e) is None else "vector", element_identity(e), exact_token(e))
                           for e in elements)
        if stored != expected:
            extra, missing = stored - expected, expected - stored
            self._corrupt(f"{sum(extra.values())} stored occurrences no element accounts for, "
                          f"{sum(missing.values())} element occurrences not stored")


def _first(item):
    return item[0]

