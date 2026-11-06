"""KeySpec: the persistable description of an indexed key (spec section 8.3, decision D29).

A key spec is a small frozen value of one of three kinds:

* ``KeySpec.path("a.b")`` -- a dotted attribute path.  Each step reads a
  persisted attribute of a :class:`DBObject` (or another atom) or a key of a
  mapping (``FrozenDict``/``dict``); an absent attribute or key at any level,
  or a step through any other value (``None``, a number, ...), yields
  :data:`MISSING`.  Private (``_x``) and reserved (``oid``, ``atom_pointer``,
  ``context``) attribute names are never read.  An explicit ``None`` stays
  ``None``: absence and ``None`` are different keys.
* ``KeySpec.element()`` -- the element itself.
* ``KeySpec.extractor(name)`` -- a named function registered in this process
  with :func:`register_extractor`.  Only the name is persisted, never the
  function; the name is resolved at extraction time, and an unregistered name
  raises ``ProtoValidationError``.  Results are normalised with
  :func:`proto_db.values.canonical`.

Key specs persist as plain data (:meth:`KeySpec.to_data`, a ``(kind, arg)``
tuple) and compare by value.  Terms name a key by its :attr:`KeySpec.term_path`:
the dotted path, ``None`` for the element, ``"@name"`` for an extractor.
"""
from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass, field

from ..core.atom import RESERVED_NAMES, Atom
from ..core.dbobject import DBObject
from ..errors import ProtoCorruptionError, ProtoValidationError
from ..values import MISSING, canonical

__all__ = ["KeySpec", "register_extractor", "unregister_extractor", "extractor_generation",
           "PATH", "ELEMENT", "EXTRACTOR"]

PATH = "path"
ELEMENT = "element"
EXTRACTOR = "extractor"
_KINDS = (PATH, ELEMENT, EXTRACTOR)

_extractors: dict[str, Callable] = {}
#: Registration generation of every registered name (D56): a process-wide counter value taken at
#: registration, so a name registered again after :func:`unregister_extractor` gets a new one.
_generations: dict[str, int] = {}
_next_generation = 1
_extractors_lock = threading.Lock()


def _check_extractor_name(name) -> None:
    if (not isinstance(name, str) or not name or name.startswith("@")
            or any(c.isspace() for c in name)):
        raise ProtoValidationError(
            f"An extractor name must be a non-empty str without whitespace or a leading '@', not {name!r}")


def register_extractor(name: str, fn: Callable) -> None:
    """Register ``fn(element) -> key`` under ``name`` for :meth:`KeySpec.extractor`.

    Registration is process-wide and must happen (typically at import time)
    before any index or term naming the extractor is used.  Registering the same
    function again is a no-op; binding a registered name to another function
    raises ``ProtoValidationError``, because persisted indexes rely on the
    name keeping its meaning.  Thread-safe.

    ``fn`` must be a pure function of the element: the same element always gives the
    same key, whatever the time, the process or any state outside the element.  Indexes
    store the keys it computed, and the query result cache (D56) serves a result computed
    once for as long as the versions it read are asked for: an impure extractor (one that
    reads the clock, say) gives stale cached results, silently.  Pager tokens identify the
    function by :func:`extractor_fingerprint` and refuse to resume when it changed.
    """
    _check_extractor_name(name)
    if not callable(fn):
        raise ProtoValidationError(f"Extractor {name!r} must be callable, not {type(fn).__name__}")
    global _next_generation
    with _extractors_lock:
        current = _extractors.get(name)
        if current is not None and current is not fn:
            raise ProtoValidationError(f"Extractor {name!r} is already registered with another function")
        if current is None:
            _extractors[name] = fn
            _generations[name] = _next_generation
            _next_generation += 1


def unregister_extractor(name: str) -> None:
    """Remove the registration of ``name`` (``ProtoValidationError`` if it is not registered).

    Until ``name`` is registered again, indexes and terms naming it raise on use.  The
    name's generation is forgotten, and a new registration takes a fresh one
    (:func:`extractor_generation`), so query results cached under the old function are
    never served for the new one (D56).  Stored indexes
    built with the old function are not rebuilt: rebinding a name to a function with another
    meaning makes them disagree with their elements, as the registry docstring warns.
    """
    _check_extractor_name(name)
    with _extractors_lock:
        if _extractors.pop(name, None) is None:
            raise ProtoValidationError(f"No key extractor is registered under {name!r}")
        del _generations[name]


def extractor_generation(name: str) -> int:
    """The registration generation of ``name`` in this process (``ProtoValidationError`` if unregistered).

    Generations are unique within a process: registering a name again after
    :func:`unregister_extractor` gives a different value, whatever the function."""
    with _extractors_lock:
        generation = _generations.get(name)
    if generation is None:
        _registered(name)  # raises the usual "not registered" error
    return generation


def extractor_fingerprint(name: str) -> str:
    """A process-independent identity of the function registered under ``name`` (D56, R99 M4):
    a digest of its module, qualified name and code (bytecode, constants, names), the same
    in every process that registers the same function.  Pager tokens carry it.  Captured
    state (closure cells, globals) is not covered: extractors must be pure (see
    :func:`register_extractor`)."""
    import hashlib
    fn = _registered(name)
    code = getattr(fn, "__code__", None)
    parts = [getattr(fn, "__module__", None) or "", getattr(fn, "__qualname__", None) or type(fn).__qualname__]
    if code is not None:
        parts += [code.co_code.hex(), repr(code.co_consts), repr(code.co_names)]
    return hashlib.blake2b("\x1f".join(parts).encode(), digest_size=16).hexdigest()


def _registered(name: str) -> Callable:
    with _extractors_lock:
        fn = _extractors.get(name)
    if fn is None:
        raise ProtoValidationError(
            f"No key extractor is registered under {name!r}; call register_extractor({name!r}, fn) "
            f"in this process before using an index or term that names it")
    return fn


def _step(value, name: str):
    """One path step: attribute ``name`` of a record/atom or key ``name`` of a mapping, else MISSING."""
    if isinstance(value, dict):
        return value.get(name, MISSING)
    if isinstance(value, Atom):
        if name.startswith("_") or name in RESERVED_NAMES:
            return MISSING
        if isinstance(value, DBObject):
            return getattr(value, name) if value.has_defined_attr(name) else MISSING
        value._load()
        return value.__dict__.get(name, MISSING)
    return MISSING


@dataclass(frozen=True)
class KeySpec:
    """Persistable description of an indexed key: ``kind`` (path, element, extractor) and ``arg``.

    Build one with :meth:`path`, :meth:`element` or :meth:`extractor`; direct
    construction ``KeySpec(kind, arg)`` is validated the same way.
    """

    kind: str
    arg: str | None = None
    _segments: tuple = field(init=False, repr=False, compare=False, default=())

    def __post_init__(self):
        if self.kind not in _KINDS:
            raise ProtoValidationError(f"Unknown key spec kind {self.kind!r}; expected one of {_KINDS}")
        if self.kind == ELEMENT:
            if self.arg is not None:
                raise ProtoValidationError("KeySpec.element() takes no argument")
        elif self.kind == EXTRACTOR:
            _check_extractor_name(self.arg)
        else:
            path = self.arg
            if not isinstance(path, str) or not path or path.startswith("@"):
                raise ProtoValidationError(f"A key path must be a non-empty str not starting with '@', "
                                           f"not {path!r}")
            segments = tuple(path.split("."))
            if any(not s or s != s.strip() or any(c.isspace() for c in s) for s in segments):
                raise ProtoValidationError(f"Invalid key path {path!r}: empty or blank segment")
            object.__setattr__(self, "_segments", segments)

    # -- constructors ---------------------------------------------------------------------

    @classmethod
    def path(cls, path: str) -> "KeySpec":
        """Key = the value at dotted attribute/mapping path ``path`` (MISSING when absent)."""
        return cls(PATH, path)

    @classmethod
    def element(cls) -> "KeySpec":
        """Key = the element itself."""
        return cls(ELEMENT)

    @classmethod
    def extractor(cls, name: str) -> "KeySpec":
        """Key = ``fn(element)`` for the function registered under ``name`` (resolved at use)."""
        return cls(EXTRACTOR, name)

    @classmethod
    def for_term_path(cls, path) -> "KeySpec":
        """The key spec a term path names: ``None`` -> element, ``"@name"`` -> extractor, else a path."""
        if path is None:
            return cls.element()
        if not isinstance(path, str):
            raise ProtoValidationError(f"A term path must be a str or None, not {type(path).__name__}")
        if path.startswith("@"):
            return cls.extractor(path[1:])
        return cls.path(path)

    # -- use ------------------------------------------------------------------------------

    def extract(self, element):
        """Return the key of ``element``, or ``MISSING`` when it has none."""
        if self.kind == ELEMENT:
            return element
        if self.kind == EXTRACTOR:
            return canonical(_registered(self.arg)(element))
        value = element
        for name in self._segments:
            value = _step(value, name)
            if value is MISSING:
                return MISSING
        return value

    @property
    def term_path(self) -> str | None:
        """How a term names this key: the path, ``None`` (element) or ``"@name"`` (extractor)."""
        if self.kind == ELEMENT:
            return None
        if self.kind == EXTRACTOR:
            return "@" + self.arg
        return self.arg

    def describe(self) -> str:
        """Human-readable form used by ``explain()``: the path, ``"<element>"`` or ``"@name"``."""
        return "<element>" if self.kind == ELEMENT else self.term_path

    # -- persistence ----------------------------------------------------------------------

    def to_data(self) -> tuple:
        """Plain-data form ``(kind, arg)``, storable by the codec."""
        return (self.kind, self.arg)

    @classmethod
    def from_data(cls, data) -> "KeySpec":
        """Inverse of :meth:`to_data`; ``ProtoCorruptionError`` on malformed data."""
        if not isinstance(data, (tuple, list)) or len(data) != 2:
            raise ProtoCorruptionError(f"Malformed persisted key spec {data!r}")
        try:
            return cls(data[0], data[1])
        except ProtoValidationError as e:
            raise ProtoCorruptionError(f"Malformed persisted key spec {data!r}: {e.message}") from e
