from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Any, Dict, Tuple, FrozenSet


@dataclass(frozen=True)
class IndexDefinition:
    """
    Defines a secondary index.
    - name: index name
    - extractor: callable(item) ->
        - a single key value (applies to this index name), or
        - an iterable of key values (applies to this index name), or
        - an iterable of (index_name, key_value) tuples to feed multiple indexes
    """
    name: str
    extractor: Callable[[Any], Any]


class IndexRegistry:
    """
    Immutable registry of secondary indexes.
    data: dict[index_name -> dict[key_value -> frozenset[obj_id]]]
    defs: tuple[IndexDefinition, ...]
    """
    __slots__ = ("_data", "_defs")

    def __init__(self, data: Dict[str, Dict[Any, FrozenSet[Any]]] | None = None,
                 defs: Tuple[IndexDefinition, ...] | None = None):
        self._data: Dict[str, Dict[Any, FrozenSet[Any]]] = data or {}
        self._defs: Tuple[IndexDefinition, ...] = defs or tuple()

    def with_defs(self, defs: Iterable[IndexDefinition]) -> "IndexRegistry":
        return IndexRegistry(data=self._data, defs=tuple(defs))

    @property
    def defs(self) -> Tuple[IndexDefinition, ...]:
        return self._defs

    def get(self, index_name: str) -> Dict[Any, FrozenSet[Any]]:
        return self._data.get(index_name, {})

    @property
    def data(self) -> Dict[str, Dict[Any, FrozenSet[Any]]]:
        return self._data

    def _normalize_extractions(self, item: Any) -> Iterable[Tuple[str, Any]]:
        """
        Normalize extractor outputs to an iterable of (index_name, key_value).
        Accepts:
        - single key (applies to each def.name individually if a single def provided)
        - iterable of keys
        - iterable of (index_name, key)
        """
        results: list[Tuple[str, Any]] = []
        for d in self._defs:
            extracted = d.extractor(item)
            # If it's a tuple of (name,key) or iterable of such
            if isinstance(extracted, tuple) and len(extracted) == 2:
                results.append((extracted[0], extracted[1]))
            elif isinstance(extracted, dict):
                for k, v in extracted.items():
                    results.append((k, v))
            else:
                try:
                    it = iter(extracted)
                    # Heuristic: if first element is a tuple(name,key), treat accordingly
                    # Otherwise, treat as keys for d.name
                    for e in it:
                        if isinstance(e, tuple) and len(e) == 2:
                            results.append((e[0], e[1]))
                        else:
                            results.append((d.name, e))
                except TypeError:
                    # Non-iterable: single key value
                    results.append((d.name, extracted))
        return results

    def _with_update(self, updates: Iterable[Tuple[str, Any, str, Any]]) -> "IndexRegistry":
        """
        Apply a set of updates described as (op, index_name, key, obj_id)
        where op in {"add","remove"}, producing a new IndexRegistry.
        """
        new_data: Dict[str, Dict[Any, FrozenSet[Any]]] = {idx: dict(map_.items()) for idx, map_ in self._data.items()}
        for op, index_name, key, obj_id in updates:
            bucket = new_data.get(index_name)
            if bucket is None:
                bucket = {}
                new_data[index_name] = bucket
            current: FrozenSet[Any] = bucket.get(key, frozenset())
            if op == "add":
                if obj_id in current:
                    continue
                bucket[key] = frozenset(set(current) | {obj_id})
            elif op == "remove":
                if not current or obj_id not in current:
                    continue
                after = set(current)
                after.discard(obj_id)
                if after:
                    bucket[key] = frozenset(after)
                else:
                    # remove empty key bucket to keep map clean
                    bucket.pop(key, None)
        return IndexRegistry(data=new_data, defs=self._defs)

    def with_add(self, obj_id: Any, item: Any) -> "IndexRegistry":
        pairs = self._normalize_extractions(item)
        updates = [("add", name, key, obj_id) for (name, key) in pairs]
        return self._with_update(updates)

    def with_remove(self, obj_id: Any, item: Any) -> "IndexRegistry":
        pairs = self._normalize_extractions(item)
        updates = [("remove", name, key, obj_id) for (name, key) in pairs]
        return self._with_update(updates)

    def with_replace(self, obj_id: Any, old_item: Any, new_item: Any) -> "IndexRegistry":
        reg = self.with_remove(obj_id, old_item)
        return reg.with_add(obj_id, new_item)
