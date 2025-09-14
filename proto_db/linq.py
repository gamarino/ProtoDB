from __future__ import annotations

import time
import warnings
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Iterator, Generic, TypeVar, Optional, Dict, List, Set as TSet, Tuple
import inspect
import ast

# ProtoBase query infrastructure
from .common import QueryPlan, DBCollections
from .queries import ListPlan, WherePlan, SelectPlan, Expression as PBExpression, FromPlan as PBFromPlan

T = TypeVar('T')
K = TypeVar('K')
U = TypeVar('U')
V = TypeVar('V')
Number = float | int


@dataclass
class Policy:
    """Execution policy for Queryable pipelines.

    on_unsupported: 'error' | 'warn' | 'fallback'
    Limits are enforced when local Python fallback is used.
    """
    on_unsupported: str = "fallback"  # 'error' | 'warn' | 'fallback'
    max_rows_local: int = 100_000
    max_memory_mb: int = 256
    timeout_ms: int = 0  # 0 = no timeout


class Grouping(Generic[K, U]):
    def __init__(self, key: K, elements: Iterable[U]):
        self.key = key
        self._elements = list(elements)

    def __iter__(self) -> Iterator[U]:
        return iter(self._elements)


# Minimal DSL Expression for in-memory fallback and composition
class _Pred:
    def __init__(self, fn: Callable[[Any], bool], pb_tokens: Optional[list] = None):
        self.fn = fn
        self.pb_tokens = pb_tokens  # Optional Expression.compile token stream
        self._compiled_expr: Optional[PBExpression] = None
    def __call__(self, x):
        return self.fn(x)
    def get_compiled(self) -> Optional[PBExpression]:
        if self.pb_tokens is None:
            return None
        if self._compiled_expr is None:
            try:
                self._compiled_expr = PBExpression.compile(self.pb_tokens)
            except Exception:
                self._compiled_expr = None
        return self._compiled_expr
    def __and__(self, other: ' _Pred | Callable[[Any], bool]') -> '_Pred':
        other_fn = other.fn if isinstance(other, _Pred) else other
        # Merge pb tokens into And expression if available
        tokens = None
        if isinstance(other, _Pred) and self.pb_tokens is not None and other.pb_tokens is not None:
            tokens = ['&', self.pb_tokens, other.pb_tokens]
        return _Pred(lambda x: self.fn(x) and bool(other_fn(x)), tokens)
    def __or__(self, other: ' _Pred | Callable[[Any], bool]') -> '_Pred':
        other_fn = other.fn if isinstance(other, _Pred) else other
        tokens = None
        if isinstance(other, _Pred) and self.pb_tokens is not None and other.pb_tokens is not None:
            tokens = ['|', self.pb_tokens, other.pb_tokens]
        return _Pred(lambda x: self.fn(x) or bool(other_fn(x)), tokens)
    def __invert__(self) -> '_Pred':
        tokens = None
        if self.pb_tokens is not None:
            tokens = ['!', self.pb_tokens]
        return _Pred(lambda x: not self.fn(x), tokens)

DEFAULT_ALIAS = 'r'

def _prefix_alias(attr: str) -> str:
    if not attr:
        return DEFAULT_ALIAS
    if attr.startswith(DEFAULT_ALIAS + '.'):
        return attr
    return f"{DEFAULT_ALIAS}.{attr}"

class _Field:
    def __init__(self, path: Tuple[str, ...] = ()):  # empty is root
        self._path = path
        self._pending_between: Optional[tuple[Any, Any, tuple[bool,bool]]] = None

    def __getattr__(self, item: str) -> '_Field':
        return _Field(self._path + (item,))

    def __getitem__(self, item: str) -> '_Field':
        return _Field(self._path + (item,))

    def _resolve(self, rec: Any) -> Any:
        cur = rec
        for p in self._path:
            if cur is None:
                return None
            if isinstance(cur, dict):
                cur = cur.get(p)
            else:
                cur = getattr(cur, p, None)
        return cur

    # comparison and boolean operators build callables and PB tokens for pushdown
    def _cmp(self, other: Any, op: Callable[[Any, Any], bool], op_token: Optional[str] = None) -> _Pred:
        pb = None
        if op_token is not None:
            pb_attr = _prefix_alias('.'.join(self._path))
            pb = [pb_attr, op_token, other]
        return _Pred(lambda x: op(self._resolve(x), other), pb)

    def __eq__(self, other):
        return self._cmp(other, lambda a, b: a == b, '==')

    def __ne__(self, other):
        return self._cmp(other, lambda a, b: a != b, '!=')

    def __gt__(self, other):
        return self._cmp(other, lambda a, b: a is not None and b is not None and a > b, '>')

    def __ge__(self, other):
        return self._cmp(other, lambda a, b: a is not None and b is not None and a >= b, '>=')

    def __lt__(self, other):
        return self._cmp(other, lambda a, b: a is not None and b is not None and a < b, '<')

    def __le__(self, other):
        return self._cmp(other, lambda a, b: a is not None and b is not None and a <= b, '<=')

    def in_(self, seq: Iterable[Any]):
        s = set(seq)
        # Note: store original iterable in tokens if list/tuple; else fall back to list for safety
        vals = list(seq) if not isinstance(seq, (list, tuple)) else seq
        pb = [_prefix_alias('.'.join(self._path)), 'in', list(vals)]
        return _Pred(lambda x: self._resolve(x) in s, pb)

    # Between DSL
    def between(self, lo: Any, hi: Any, inclusive: tuple[bool,bool] = (True, True)) -> _Pred:
        if lo is None or hi is None:
            return _Pred(lambda x: False)
        l_inc, r_inc = inclusive
        # Local predicate behavior (None field -> False)
        def _pred(x):
            v = self._resolve(x)
            if v is None:
                return False
            if lo > hi:
                # validation: empty by default
                return False
            if l_inc:
                if v < lo:
                    return False
            else:
                if v <= lo:
                    return False
            if r_inc:
                if v > hi:
                    return False
            else:
                if v >= hi:
                    return False
            return True
        # Build PB tokens for pushdown
        bounds_token = 'between[]' if l_inc and r_inc else (
            'between()' if (not l_inc and not r_inc) else (
                'between[)' if (l_inc and not r_inc) else 'between(]'
            )
        )
        attr = _prefix_alias('.'.join(self._path))
        pb_tokens = [attr, bounds_token, lo, hi]
        return _Pred(_pred, pb_tokens)

    def between_closed(self, lo: Any, hi: Any) -> _Pred:
        return self.between(lo, hi, inclusive=(True, True))

    def between_open(self, lo: Any, hi: Any) -> _Pred:
        return self.between(lo, hi, inclusive=(False, False))

    def between_left_open(self, lo: Any, hi: Any) -> _Pred:
        return self.between(lo, hi, inclusive=(False, True))

    def between_right_open(self, lo: Any, hi: Any) -> _Pred:
        return self.between(lo, hi, inclusive=(True, False))

    def range(self, lo: Any, hi: Any, bounds: str = "[]") -> _Pred:
        bounds = bounds.strip()
        mapping = {
            "[]": (True, True),
            "()": (False, False),
            "(]": (False, True),
            "[)": (True, False),
        }
        inc = mapping.get(bounds)
        if inc is None:
            raise ValueError("bounds must be one of '[]','()','(]','[)'")
        return self.between(lo, hi, inclusive=inc)

    def contains(self, sub: Any):
        pb = [_prefix_alias('.'.join(self._path)), 'contains', sub]
        return _Pred(lambda x: (self._resolve(x) or "").__contains__(sub), pb)

    def startswith(self, prefix: str):
        return _Pred(lambda x: (self._resolve(x) or "").startswith(prefix))

    def endswith(self, suffix: str):
        return _Pred(lambda x: (self._resolve(x) or "").endswith(suffix))

    # arithmetic for select
    def _arith(self, other: Any, op: Callable[[Any, Any], Any]):
        return lambda x: op(self._resolve(x), other if not callable(other) else other(x))

    def __add__(self, other):
        return self._arith(other, lambda a, b: (a or 0) + (b or 0))

    def __radd__(self, other):
        # support chaining where left side is a callable or literal
        if callable(other):
            return lambda x: (other(x) or 0) + (self._resolve(x) or 0)
        return lambda x: (other or 0) + (self._resolve(x) or 0)

    def __sub__(self, other):
        return self._arith(other, lambda a, b: (a or 0) - (b or 0))

    def __mul__(self, other):
        return self._arith(other, lambda a, b: (a or 0) * (b or 0))

    def __truediv__(self, other):
        return self._arith(other, lambda a, b: (a or 1) / (b or 1))

    def lower(self):
        return lambda x: (self._resolve(x) or "").lower()

    def upper(self):
        return lambda x: (self._resolve(x) or "").upper()

    def length(self):
        return lambda x: len(self._resolve(x) or [])

    def abs(self):
        return lambda x: abs(self._resolve(x) or 0)


class _FProxy:
    def __getattr__(self, item) -> _Field:
        return _Field((item,))

    def __getitem__(self, item) -> _Field:
        return _Field((item,))

    # Aggregation pseudo functions to be used inside group_by selects
    def key(self):
        def _fn(g):
            if isinstance(g, Grouping):
                return g.key
            if isinstance(g, dict) and 'key' in g:
                return g['key']
            return None
        return _fn

    def count(self):
        def _fn(g):
            if isinstance(g, Grouping):
                return len(list(g))
            if isinstance(g, dict) and 'orders' in g:
                # heuristic for selected dict
                v = g.get('orders')
                return len(v) if hasattr(v, '__len__') else int(v)
            if isinstance(g, dict) and 'count' in g:
                return g['count']
            return 0
        return _fn

    def sum(self, selector: Optional[Callable[[Any], Number]] = None):
        def _fn(g):
            if isinstance(g, Grouping):
                it = (selector(e) if selector else e for e in g)
                total: Number = 0
                for v in it:
                    total += v or 0
                return total
            if isinstance(g, dict) and 'sum' in g:
                return g['sum']
            return 0
        return _fn

    def average(self, selector: Optional[Callable[[Any], Number]] = None):
        def _fn(g):
            if isinstance(g, Grouping):
                vals = [selector(e) if selector else e for e in g]
                return (sum(v or 0 for v in vals) / len(vals)) if vals else 0.0
            if isinstance(g, dict) and 'avg' in g:
                return g['avg']
            if isinstance(g, dict) and 'average' in g:
                return g['average']
            return 0.0
        return _fn

F = _FProxy()


def _to_callable(expr_or_fn: Optional[Callable[[T], Any] | Any]) -> Optional[Callable[[T], Any]]:
    if expr_or_fn is None:
        return None
    if isinstance(expr_or_fn, _Pred):
        return expr_or_fn.fn
    if isinstance(expr_or_fn, _Field):
        return lambda x: expr_or_fn._resolve(x)
    if callable(expr_or_fn):
        return expr_or_fn
    if isinstance(expr_or_fn, dict):
        # map dict of selectors (support Field, Pred, callables, literals)
        def mapper(x):
            out = {}
            for k, v in expr_or_fn.items():
                vf = _to_callable(v)
                out[k] = vf(x) if vf is not None else v
            return out
        return mapper
    if isinstance(expr_or_fn, (list, tuple)):
        def mapper2(x):
            vals = [v(x) if callable(v) else v for v in expr_or_fn]
            return tuple(vals) if isinstance(expr_or_fn, tuple) else vals
        return mapper2
    # literal value: wrap as constant function
    return lambda _: expr_or_fn


def _field_path_from_ast(node: ast.AST) -> Optional[Tuple[str,...]]:
    # Supports x.attr or x['attr'] chains
    parts: list[str] = []
    cur = node
    while True:
        if isinstance(cur, ast.Attribute):
            parts.append(cur.attr)
            cur = cur.value
            continue
        if isinstance(cur, ast.Subscript):
            try:
                # Python 3.11: slice in cur.slice
                s = cur.slice
                if isinstance(s, ast.Constant) and isinstance(s.value, str):
                    parts.append(s.value)
                elif isinstance(s, ast.Index) and isinstance(s.value, ast.Constant) and isinstance(s.value.value, str):
                    parts.append(s.value.value)
                else:
                    return None
            except Exception:
                return None
            cur = cur.value
            continue
        if isinstance(cur, ast.Name):
            # Reached the parameter name
            break
        return None
    if not parts:
        return None
    parts.reverse()
    return tuple(parts)


def _translate_lambda_between(fn: Callable) -> Optional[list]:
    try:
        src = inspect.getsource(fn)
    except Exception:
        return None
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return None
    # Find the Lambda node or a FunctionDef with a return
    lam: Optional[ast.Lambda] = None
    class _Find(ast.NodeVisitor):
        def visit_Lambda(self, node: ast.Lambda):
            nonlocal lam
            lam = node
    _Find().visit(tree)
    expr_node: Optional[ast.AST] = lam.body if lam else None
    if expr_node is None:
        # Try function def
        class _FindRet(ast.NodeVisitor):
            def __init__(self):
                self.ret = None
            def visit_Return(self, node: ast.Return):
                self.ret = node.value
        fr = _FindRet()
        fr.visit(tree)
        expr_node = fr.ret
    if not isinstance(expr_node, ast.Compare):
        return None
    # Pattern: lo <= center <= hi or lo < center < hi
    if len(expr_node.ops) != 2 or len(expr_node.comparators) != 2:
        return None
    lo_node = expr_node.left
    center = expr_node.comparators[0]
    hi_node = expr_node.comparators[1]
    # Ensure center is a field access on the lambda arg
    path = _field_path_from_ast(center)
    if not path:
        return None
    # Extract constants for lo/hi
    def _const_val(n):
        if isinstance(n, ast.Constant):
            return n.value
        return None
    lo = _const_val(lo_node)
    hi = _const_val(hi_node)
    if lo is None or hi is None:
        return None
    # Determine inclusivity
    left_op = expr_node.ops[0]
    right_op = expr_node.ops[1]
    l_inc = isinstance(left_op, ast.LtE)
    r_inc = isinstance(right_op, ast.LtE)
    if not (isinstance(left_op, (ast.Lt, ast.LtE)) and isinstance(right_op, (ast.Lt, ast.LtE))):
        return None
    bounds_token = 'between[]' if l_inc and r_inc else (
        'between()' if (not l_inc and not r_inc) else (
            'between[)' if (l_inc and not r_inc) else 'between(]'
        )
    )
    return ['.'.join(path), bounds_token, lo, hi]


class FunctionExpression(PBExpression):
    """Adapter Expression that delegates to a Python callable predicate.
    Allows WherePlan to accept our DSL/lambda predicates without building a token tree.
    """
    def __init__(self, fn: Callable[[Any], bool]):
        self._fn = fn
    def match(self, record) -> bool:
        try:
            return bool(self._fn(record))
        except Exception:
            return False


class Queryable(Generic[T]):
    def __init__(self, source: Iterable[T] | QueryPlan | DBCollections, plan: Optional[list] = None, policy: Optional[Policy] = None):
        # If source is a ProtoBase plan/collection, keep a base QueryPlan
        self._base_plan: Optional[QueryPlan] = None
        if isinstance(source, QueryPlan):
            self._base_plan = source
            self._source = None
        elif isinstance(source, DBCollections):
            try:
                self._base_plan = source.as_query_plan()
                self._source = None
            except Exception:
                self._base_plan = None
                self._source = source.as_iterable()
        elif isinstance(source, list):
            # Keep plain Python lists as local iterables to preserve record shape in results
            # Plan-based features (indexes/alias) are not applicable to plain lists
            self._base_plan = None
            self._source = source
        else:
            self._source = source  # generic iterable fallback
        self._ops: list[tuple[str, tuple, dict]] = plan[:] if plan else []
        self._policy = policy or Policy()

    def with_policy(self, policy: Policy) -> 'Queryable[T]':
        q = Queryable(self._base_plan or self._source, self._ops, policy)
        return q

    def on_unsupported(self, mode: str) -> 'Queryable[T]':
        pol = Policy(**{**self._policy.__dict__, 'on_unsupported': mode})
        return self.with_policy(pol)

    # Intermediate operators (lazy)
    def where(self, predicate: Callable[[T], bool] | Any) -> 'Queryable[T]':
        self._ops.append(("where", (predicate,), {}))
        return self

    def select(self, selector: Callable[[T], U] | Any) -> 'Queryable[U]':
        self._ops.append(("select", (selector,), {}))
        return self  # type: ignore

    def select_many(self, selector: Callable[[T], Iterable[U]] | Any) -> 'Queryable[U]':
        self._ops.append(("select_many", (selector,), {}))
        return self  # type: ignore

    def order_by(self, key_selector: Callable[[T], K] | Any, ascending: bool = True, nulls_last: bool = True) -> 'Queryable[T]':
        self._ops.append(("order_by", (key_selector,), {"ascending": ascending, "nulls_last": nulls_last}))
        return self

    def then_by(self, key_selector: Callable[[T], K] | Any, ascending: bool = True, nulls_last: bool = True) -> 'Queryable[T]':
        self._ops.append(("then_by", (key_selector,), {"ascending": ascending, "nulls_last": nulls_last}))
        return self

    def distinct(self, key_selector: Optional[Callable[[T], K] | Any] = None) -> 'Queryable[T]':
        self._ops.append(("distinct", (key_selector,), {}))
        return self

    def take(self, n: int) -> 'Queryable[T]':
        self._ops.append(("take", (n,), {}))
        return self

    def skip(self, n: int) -> 'Queryable[T]':
        self._ops.append(("skip", (n,), {}))
        return self

    def group_by(self, key_selector: Callable[[T], K] | Any, element_selector: Optional[Callable[[T], U] | Any] = None) -> 'Queryable[Grouping[K, U]]':
        self._ops.append(("group_by", (key_selector, element_selector), {}))
        return self  # type: ignore

    # Terminal operators
    def _execute(self) -> Iterator[Any]:
        # Apply simple optimizations: push where early
        where_ops = [(i, op) for i, op in enumerate(self._ops) if op[0] == 'where']
        others = [op for op in self._ops if op[0] != 'where']
        ops_in_order = [* [op for _, op in where_ops], *others]

        # If we have a base QueryPlan, translate the prefix of ops into a QueryPlan chain
        it: Iterable[Any]
        start_time = time.time()

        # Build a plan prefix if possible
        plan_prefix_len = 0
        current_plan: Optional[QueryPlan] = self._base_plan
        if current_plan is not None:
            for (name, args, kwargs) in ops_in_order:
                if name == 'where':
                    arg0 = args[0]
                    # Ensure alias-wrapped base for consistent index field names
                    try:
                        cur_indexes = getattr(current_plan, 'indexes', None)
                        current_plan = PBFromPlan(alias=DEFAULT_ALIAS, indexes=cur_indexes, based_on=current_plan,
                                                  transaction=getattr(current_plan, 'transaction', None))
                    except Exception:
                        pass
                    # Prefer PB Expression tokens if available for pushdown/index usage
                    if isinstance(arg0, _Pred) and arg0.pb_tokens is not None:
                        compiled = arg0.get_compiled()
                        if compiled is not None:
                            current_plan = WherePlan(filter=compiled, based_on=current_plan)
                        else:
                            current_plan = WherePlan(filter_spec=arg0.pb_tokens, based_on=current_plan)
                        plan_prefix_len += 1
                        continue
                    # Try translating lambda chained comparisons to Between
                    if callable(arg0):
                        lam_tokens = _translate_lambda_between(arg0)
                        if lam_tokens is not None:
                            current_plan = WherePlan(filter_spec=lam_tokens, based_on=current_plan)
                            plan_prefix_len += 1
                            continue
                    pred = _to_callable(arg0)
                    if pred is None:
                        plan_prefix_len += 1
                        continue
                    current_plan = WherePlan(filter=FunctionExpression(pred), based_on=current_plan)
                    plan_prefix_len += 1
                elif name == 'select':
                    sel = args[0]
                    if isinstance(sel, dict):
                        # Convert dict values to callables or keep strings
                        fields: Dict[str, Any] = {}
                        ok = True
                        for k, v in sel.items():
                            if isinstance(v, str):
                                fields[k] = v
                            else:
                                fn = _to_callable(v)
                                if fn is None:
                                    ok = False
                                    break
                                fields[k] = fn
                        if ok:
                            current_plan = SelectPlan(fields=fields, based_on=current_plan)
                            plan_prefix_len += 1
                        else:
                            break
                    else:
                        break  # unsupported for plan
                else:
                    break  # we don't have plan nodes for other ops (order_by, skip, take, etc.)
        # Execute plan prefix if built
        remaining_ops = ops_in_order
        if current_plan is not None and plan_prefix_len > 0:
            # Execute the plan prefix to an iterable and continue locally with the rest
            try:
                it = current_plan.optimize().execute()
            except Exception:
                # In case of any issue, fallback to local execution over base iterable
                it = self._source if self._source is not None else []
            remaining_ops = ops_in_order[plan_prefix_len:]
        else:
            # No plan or nothing translatable: start with source iterable
            it = self._source if self._source is not None else (self._base_plan.execute() if self._base_plan else [])

        def check_limits(count: int):
            if self._policy.timeout_ms and (time.time() - start_time) * 1000 > self._policy.timeout_ms:
                raise TimeoutError("Query execution exceeded timeout")
            if count > self._policy.max_rows_local:
                raise RuntimeError("Local execution exceeded max_rows_local")

        count = 0
        pending_order: list[Tuple[Callable[[Any], Any], bool, bool]] = []
        skip_n = 0
        take_n: Optional[int] = None

        for (name, args, kwargs) in remaining_ops:
            if name == 'where':
                # unsupported detection for policy
                arg0 = args[0]
                is_supported = isinstance(arg0, _Pred)
                if not is_supported:
                    if callable(arg0):
                        tokens = _translate_lambda_between(arg0)
                        if tokens is None:
                            if self._policy.on_unsupported == 'error':
                                raise ValueError('Unsupported where predicate; use F DSL (e.g., F.field == 1) or set on_unsupported("warn"|"fallback")')
                            elif self._policy.on_unsupported == 'warn':
                                warnings.warn('Falling back to local Python evaluation for where()', RuntimeWarning)
                        # If tokens detected, proceed silently with local callable
                    else:
                        if self._policy.on_unsupported == 'error':
                            raise ValueError('Unsupported where predicate; use F DSL (e.g., F.field == 1) or set on_unsupported("warn"|"fallback")')
                        elif self._policy.on_unsupported == 'warn':
                            warnings.warn('Falling back to local Python evaluation for where()', RuntimeWarning)
                pred = _to_callable(arg0)
                if pred is None:
                    continue
                def _safe(pred_fn):
                    def _f(x):
                        try:
                            return bool(pred_fn(x))
                        except Exception:
                            return False
                    return _f
                it = (x for x in it if _safe(pred)(x))
            elif name == 'select':
                sel = _to_callable(args[0]) or (lambda x: x)
                it = (sel(x) for x in it)
            elif name == 'select_many':
                selm = _to_callable(args[0]) or (lambda x: x)
                def _flat(_it):
                    for x in _it:
                        for y in selm(x):
                            yield y
                it = _flat(it)
            elif name in ('order_by', 'then_by'):
                key_fn = _to_callable(args[0]) or (lambda x: x)
                pending_order.append((key_fn, kwargs.get('ascending', True), kwargs.get('nulls_last', True)))
            elif name == 'distinct':
                key_fn = _to_callable(args[0]) if args and args[0] is not None else None
                seen: set = set()
                def _dedup(_it):
                    nonlocal seen
                    for x in _it:
                        k = key_fn(x) if key_fn else x
                        if k not in seen:
                            seen.add(k)
                            yield x
                it = _dedup(it)
            elif name == 'take':
                take_n = min(take_n, args[0]) if take_n is not None else args[0]
            elif name == 'skip':
                skip_n += args[0]
            elif name == 'group_by':
                key_fn = _to_callable(args[0]) or (lambda x: x)
                elem_fn = _to_callable(args[1]) if len(args) > 1 else None
                groups: Dict[Any, List[Any]] = {}
                for x in it:
                    k = key_fn(x)
                    v = elem_fn(x) if elem_fn else x
                    groups.setdefault(k, []).append(v)
                it = (Grouping(k, v) for k, v in groups.items())
            else:
                raise NotImplementedError(name)

        # apply order/take/skip at end respecting stability
        if pending_order:
            def compose_key(x):
                vals = []
                for fn, asc, nulls_last in pending_order:
                    v = fn(x)
                    vals.append((v is None, v if asc else (None if v is None else _Neg(v))))
                return tuple(vals)

            class _Neg:
                __slots__ = ("v",)
                def __init__(self, v): self.v=v
                def __lt__(self, other): return self.v>other.v
                def __gt__(self, other): return self.v<other.v
                def __eq__(self, other): return self.v==other.v
                def __le__(self, other): return self.v>=other.v
                def __ge__(self, other): return self.v<=other.v
                def __hash__(self): return hash(self.v)

            arr = list(it)
            arr.sort(key=compose_key)
            it = arr

        # apply skip/take
        if skip_n:
            def _skip(_it):
                s = skip_n
                for x in _it:
                    if s>0:
                        s-=1
                        continue
                    yield x
            it = _skip(it)
        if take_n is not None:
            def _take(_it):
                n = take_n
                for x in _it:
                    if n<=0: break
                    yield x
                    n-=1
            it = _take(it)

        for x in it:
            count += 1
            check_limits(count)
            yield x

    def to_list(self) -> List[T]:
        return list(self._execute())

    def to_set(self) -> TSet[T]:
        return set(self._execute())

    def to_dict(self, key_selector: Callable[[T], K] | Any, value_selector: Callable[[T], V] | Any = lambda x: x) -> Dict[K, V]:
        kf = _to_callable(key_selector)
        if kf is None:
            raise ValueError("to_dict requires key_selector")
        vf = _to_callable(value_selector) or (lambda x: x)
        out: Dict[K, V] = {}
        for x in self._execute():
            k = kf(x)
            if k in out:
                raise ValueError("Duplicate key in to_dict: %r" % (k,))
            out[k] = vf(x)
        return out

    def first(self, predicate: Optional[Callable[[T], bool] | Any] = None) -> T:
        it = self.where(predicate) if predicate is not None else self
        for x in it._execute():
            return x
        raise ValueError("first() with empty sequence")

    def first_or_default(self, default: Optional[T] = None, predicate: Optional[Callable[[T], bool] | Any] = None) -> Optional[T]:
        it = self.where(predicate) if predicate is not None else self
        for x in it._execute():
            return x
        return default

    def any(self, predicate: Optional[Callable[[T], bool] | Any] = None) -> bool:
        it = self.where(predicate) if predicate is not None else self
        for _ in it._execute():
            return True
        return False

    def all(self, predicate: Callable[[T], bool] | Any) -> bool:
        pred = _to_callable(predicate) or (lambda x: True)
        for x in self._execute():
            if not pred(x):
                return False
        return True

    def count(self, predicate: Optional[Callable[[T], bool] | Any] = None) -> int:
        it = self.where(predicate) if predicate is not None else self
        c = 0
        for _ in it._execute():
            c += 1
        return c

    def sum(self, selector: Optional[Callable[[T], Number] | Any] = None) -> Number:
        sel = _to_callable(selector) or (lambda x: x)
        total: Number = 0
        for x in self._execute():
            total += sel(x) or 0
        return total

    def min(self, selector: Optional[Callable[[T], Any] | Any] = None) -> Any:
        sel = _to_callable(selector) or (lambda x: x)
        it = self._execute()
        try:
            first = next(it)
        except StopIteration:
            raise ValueError("min() of empty sequence")
        m = sel(first)
        for x in it:
            v = sel(x)
            if v < m:
                m = v
        return m

    def max(self, selector: Optional[Callable[[T], Any] | Any] = None) -> Any:
        sel = _to_callable(selector) or (lambda x: x)
        it = self._execute()
        try:
            first = next(it)
        except StopIteration:
            raise ValueError("max() of empty sequence")
        m = sel(first)
        for x in it:
            v = sel(x)
            if v > m:
                m = v
        return m

    def average(self, selector: Optional[Callable[[T], Number] | Any] = None) -> float:
        sel = _to_callable(selector) or (lambda x: x)
        total = 0.0
        c = 0
        for x in self._execute():
            total += float(sel(x) or 0)
            c += 1
        return total / c if c else 0.0

    def explain(self, format: str = "text") -> str | dict:
        ops = [op for op in self._ops]
        # Build a plan prefix mirroring _execute()
        plan_prefix_len = 0
        current_plan: Optional[QueryPlan] = self._base_plan
        if current_plan is not None:
            for (name, args, kwargs) in ops:
                if name == 'where':
                    arg0 = args[0]
                    if isinstance(arg0, _Pred) and arg0.pb_tokens is not None:
                        compiled = arg0.get_compiled()
                        if compiled is not None:
                            current_plan = WherePlan(filter=compiled, based_on=current_plan)
                        else:
                            current_plan = WherePlan(filter_spec=arg0.pb_tokens, based_on=current_plan)
                        plan_prefix_len += 1
                        continue
                    if callable(arg0):
                        lam_tokens = _translate_lambda_between(arg0)
                        if lam_tokens is not None:
                            current_plan = WherePlan(filter_spec=lam_tokens, based_on=current_plan)
                            plan_prefix_len += 1
                            continue
                    # Fallback cannot be planned
                    break
                elif name == 'select' and isinstance(args[0], dict):
                    fields: Dict[str, Any] = {}
                    ok = True
                    for k, v in args[0].items():
                        if isinstance(v, str):
                            fields[k] = v
                        else:
                            fn = _to_callable(v)
                            if fn is None:
                                ok = False
                                break
                            fields[k] = fn
                    if ok:
                        current_plan = SelectPlan(fields=fields, based_on=current_plan)
                        plan_prefix_len += 1
                        continue
                    break
                else:
                    break
        optimized_node = None
        try:
            if current_plan is not None and plan_prefix_len > 0:
                optimized_node = current_plan.optimize()
        except Exception:
            optimized_node = None
        node_name = optimized_node.__class__.__name__ if optimized_node is not None else None
        if format == 'json':
            return {
                "base": "QueryPlan" if self._base_plan is not None else "Iterable",
                "plan_prefix": [
                    {"op": name, "args": [str(a) for a in args], **kwargs}
                    for (name, args, kwargs) in ops[:plan_prefix_len]
                ],
                "optimized_node": node_name,
                "local_ops": [
                    {"op": name, "args": [str(a) for a in args], **kwargs}
                    for (name, args, kwargs) in ops[plan_prefix_len:]
                ],
            }
        # text
        segments = []
        if plan_prefix_len:
            segments.append("plan:" + " -> ".join(f"{name}" for (name,_,_) in ops[:plan_prefix_len]))
            if node_name:
                segments.append(f"optimized:{node_name}")
        if plan_prefix_len < len(ops):
            segments.append("local:" + " -> ".join(f"{name}" for (name,_,_) in ops[plan_prefix_len:]))
        return " | ".join(segments) if segments else ("plan: <none>" if self._base_plan is not None else "local: <none>")


def from_collection(source: Iterable[T] | QueryPlan | DBCollections) -> Queryable[T]:
    """Entry point to build a LINQ-like queryable from a collection, QueryPlan or iterable."""
    return Queryable(source)
