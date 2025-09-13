from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import cast, TYPE_CHECKING

from .common import Atom, QueryPlan, AtomPointer, DBObject
from .exceptions import ProtoValidationException
from .hybrid_executor import HybridExecutor

if TYPE_CHECKING:
    from .db_access import ObjectTransaction
    from .dictionaries import Dictionary, DictionaryItem, RepeatedKeysDictionary
    from .sets import Set

# Executor for async operations
max_workers = (os.cpu_count() or 1) * 5
executor_pool = HybridExecutor(base_num_workers=max_workers // 5, sync_multiplier=5)


class Expression(ABC):
    """
    Abstract base class for all expression types used to filter or match records.

    Expressions represent logical conditions that can be applied to records, such as
    "and", "or", "not", or specific comparisons (terms). Subclasses must implement
    the `match` method to define their behavior for evaluating a record.

    The `Expression` structure is highly flexible and supports composition of multiple
    logical operators for complex filtering.
    """

    @staticmethod
    def compile(expression: list) -> Expression:
        """
        Compile a nested list of expressions into an `Expression` object, flattening
        nested AND/OR trees into a canonical shape.
        """
        index = 0

        def collect_expression(local_index: int) -> tuple[Expression, int]:
            if expression[local_index] == '!':
                local_index += 1
                following_expression, local_index = collect_expression(local_index)
                # Build NOT over the (possibly optimized) child
                return NotExpression(following_expression), local_index
            elif expression[local_index] == '&':
                local_index += 1
                first_operand, local_index = collect_expression(local_index)
                second_operand, local_index = collect_expression(local_index)

                # Flatten AND expressions
                new_terms: list[Expression] = []
                for term in (first_operand, second_operand):
                    if isinstance(term, AndExpression):
                        new_terms.extend(term.terms)
                    else:
                        new_terms.append(term)
                return AndExpression(new_terms), local_index

            elif expression[local_index] == '|':
                local_index += 1
                first_operand, local_index = collect_expression(local_index)
                second_operand, local_index = collect_expression(local_index)

                # Flatten OR expressions
                new_terms: list[Expression] = []
                for term in (first_operand, second_operand):
                    if isinstance(term, OrExpression):
                        new_terms.extend(term.terms)
                    else:
                        new_terms.append(term)
                return OrExpression(new_terms), local_index
            else:
                # Support nested sub-expressions and plain terms.
                current = expression[local_index]
                # If current token is a nested expression (list), compile it recursively
                if isinstance(current, list):
                    compiled = Expression.compile(current)
                    local_index += 1
                    return compiled, local_index
                # Otherwise, it should be a plain term in prefix stream: [attr, op, value?]
                try:
                    attr = expression[local_index]
                    op_token = expression[local_index + 1]
                except Exception:
                    raise ProtoValidationException(message=f'Invalid term at position {local_index}: {expression}')
                operand = Operator.get_operator(op_token)
                if operand.parameter_count == 1:
                    local_index += 2
                    return Term(attr, operand, None), local_index
                elif operand.parameter_count == 2:
                    try:
                        value = expression[local_index + 2]
                    except Exception:
                        raise ProtoValidationException(message=f'Operand {op_token} expects binary term [attr, op, value]')
                    local_index += 3
                    return Term(attr, operand, value), local_index
                elif operand.parameter_count == 3:
                    try:
                        lo = expression[local_index + 2]
                        hi = expression[local_index + 3]
                    except Exception:
                        raise ProtoValidationException(message=f'Operand {op_token} expects ternary term [attr, op, lo, hi]')
                    local_index += 4
                    return Term(attr, operand, (lo, hi)), local_index
                else:
                    raise ProtoValidationException(message=f'Unsupported parameter_count={operand.parameter_count} for operator {op_token}')

        default_and_expression: list[Expression] = []
        while index < len(expression):
            new_expression, index = collect_expression(index)
            default_and_expression.append(new_expression)

        if len(default_and_expression) > 1:
            # Flatten top-level implicit AND
            final_terms: list[Expression] = []
            for expr in default_and_expression:
                if isinstance(expr, AndExpression):
                    final_terms.extend(expr.terms)
                else:
                    final_terms.append(expr)
            return AndExpression(final_terms)
        elif default_and_expression:
            # Single element: return it (already possibly flattened)
            single_expr = default_and_expression[0]
            if isinstance(single_expr, AndExpression):
                # Ensure no nested AndExpression inside
                flat_terms: list[Expression] = []
                for t in single_expr.terms:
                    if isinstance(t, AndExpression):
                        flat_terms.extend(t.terms)
                    else:
                        flat_terms.append(t)
                return AndExpression(flat_terms)
            if isinstance(single_expr, OrExpression):
                flat_terms: list[Expression] = []
                for t in single_expr.terms:
                    if isinstance(t, OrExpression):
                        flat_terms.extend(t.terms)
                    else:
                        flat_terms.append(t)
                return OrExpression(flat_terms)
            return single_expr
        else:
            # Empty expression: return a neutral TrueTerm
            return TrueTerm()

    def optimize(self) -> "Expression":
        """
        Recursively optimize the expression tree by flattening nested AND/OR nodes.
        """
        if isinstance(self, AndExpression):
            new_terms: list[Expression] = []
            for term in self.terms:
                optimized_term = term.optimize() if isinstance(term, Expression) else term
                if isinstance(optimized_term, AndExpression):
                    new_terms.extend(optimized_term.terms)
                else:
                    new_terms.append(optimized_term)
            return AndExpression(new_terms)
        if isinstance(self, OrExpression):
            new_terms: list[Expression] = []
            for term in self.terms:
                optimized_term = term.optimize() if isinstance(term, Expression) else term
                if isinstance(optimized_term, OrExpression):
                    new_terms.extend(optimized_term.terms)
                else:
                    new_terms.append(optimized_term)
            return OrExpression(new_terms)
        if isinstance(self, NotExpression):
            return NotExpression(self.negated_expression.optimize())
        return self

    def filter_by_alias(self, alias: set[str]):
        if isinstance(self, Term):
            atribute_alias = self.target_attribute.split('.', maxsplit=1)[0]
            if atribute_alias in alias:
                return self
            else:
                return TrueTerm()
        elif isinstance(self, AndExpression):
            new_operands: list[Expression] = list()
            for operand in self.terms:
                new_operands.append(operand.filter_by_alias(alias))
            return AndExpression(new_operands)
        elif isinstance(self, OrExpression):
            # OR cannot be safely pushed down by alias without full context; return neutral TrueTerm
            return TrueTerm()
        elif isinstance(self, NotExpression):
            return NotExpression(self.negated_expression.filter_by_alias(alias))
        else:
            raise ProtoValidationException(
                message=f"It's not possible to filter {self} expression by alias!"
            )

    @abstractmethod
    def match(self, record) -> bool:
        """
        Matches a given record against specific criteria defined by the implementation.

        This method is abstract and must be implemented by a subclass to provide the
        logic for determining whether the provided record satisfies the required
        conditions. The exact nature of the matching depends on the subclass
        implementation.

        :param record: The record to be evaluated.
        :type record: Any
        :return: A boolean value indicating whether the record matches the criteria.
        :rtype: bool
        """


class AndExpression(Expression):
    """
    Logical 'AND' expression for combining multiple conditions.

    Evaluates as True only if all terms in the `terms` list evaluate to True for a record.

    Attributes:
    - terms (list[Expression]): Sub-expressions that must all evaluate as True.
    """

    terms: list[Expression]

    def __init__(self, terms: list[Expression]):
        self.terms = terms

    def match(self, record):
        """
        Evaluate if the given record matches all sub-term conditions.

        :param record: The input record to evaluate.
        :type record: Any
        :return: True if all terms match the record, False otherwise.
        :rtype: bool
        """
        for term in self.terms:
            if not term.match(record):
                return False
        return True


class OrExpression(Expression):
    terms: list[Expression]

    def __init__(self, terms: list[Expression]):
        self.terms = terms

    def match(self, record):
        for term in self.terms:
            if term.match(record):
                return True
        return False


class NotExpression(Expression):
    negated_expression: Expression

    def __init__(self, negated_expression: Expression):
        self.negated_expression = negated_expression

    def match(self, record):
        return not self.negated_expression.match(record)


class Operator(ABC):
    """
    Abstract base class for comparison operators.

    Operators define how a source value should be compared to another value
    to produce a boolean result. Subclasses implement different kinds of
    comparisons, such as equality, inequality, or inclusion.

    Attributes:
    - parameter_count (int): The number of parameters this operator expects for comparison.
    """

    parameter_count: int = 2

    @staticmethod
    def get_operator(string: str):
        """
        Factory method to get an operator instance based on a string representation.

        This allows dynamic resolution of operator behavior (e.g., '==', '!=', 'contains')
        at runtime. If the operator string is not recognized, an exception is raised.

        :param string: The string representation of the operator (e.g., '==', 'in').
        :type string: str
        :return: An instance of a subclass of `Operator`.
        :rtype: Operator
        """

        if string == '==':
            return Equal()
        elif string == '!=':
            return NotEqual()
        elif string == '>':
            return Greater()
        elif string == '>=':
            return GreaterOrEqual()
        elif string == '<':
            return Lower()
        elif string == '<=':
            return LowerOrEqual()
        elif string == 'contains':
            return Contains()
        elif string == 'in':
            return In()
        elif string == '?T':
            return IsTrue()
        elif string == '?!T':
            return NotTrue()
        elif string == '?N':
            return IsNone()
        elif string == '?!N':
            return NotNone()
        elif string == 'between[]':
            return Between(include_lower=True, include_upper=True)
        elif string == 'between()':
            return Between(include_lower=False, include_upper=False)
        elif string == 'between(]':
            return Between(include_lower=False, include_upper=True)
        elif string == 'between[)':
            return Between(include_lower=True, include_upper=False)
        elif string == 'near[]':
            return Near(metric='cosine')
        else:
            raise ProtoValidationException(
                message=f'Unknown operator: {string}!'
            )

    @abstractmethod
    def match(self, source, value=None) -> bool:
        """
        This method is an abstract method that should be implemented in derived
        classes. It is designed to match the provided source against a specific
        value and return a result based on this matching process. The exact
        implementation and criteria of the match are dependent on the
        implementation in the subclass.

        :param source: The primary input against which the matching process
            will be performed. The exact nature of 'source' depends on the
            implementation in the subclass.
        :param value: Optional value against which 'source' will be matched.
            If not provided, the behavior may vary based on the subclass
            implementation.
        :return: The result of the matching process. The exact type and
            content of the returned value depend on the specific implementation
            in the subclass.
        """


class Equal(Operator):
    parameter_count: int = 2

    def match(self, source, value=None):
        return source == value


class NotEqual(Operator):
    parameter_count: int = 2

    def match(self, source, value=None):
        return source != value


class Greater(Operator):
    parameter_count: int = 2

    def match(self, source, value=None):
        return source > value


class GreaterOrEqual(Operator):
    parameter_count: int = 2

    def match(self, source, value=None):
        return source >= value


class Lower(Operator):
    parameter_count: int = 2

    def match(self, source, value=None):
        return source < value


class LowerOrEqual(Operator):
    parameter_count: int = 2

    def match(self, source, value=None):
        return source <= value


class Contains(Operator):
    parameter_count: int = 2

    def match(self, source, value=None):
        return value in source


class In(Operator):
    parameter_count: int = 2

    def match(self, source, value=None):
        return source in value


class IsTrue(Operator):
    parameter_count: int = 1

    def match(self, source, value=None):
        return bool(source)


class NotTrue(Operator):
    parameter_count: int = 1

    def match(self, source, value=None):
        return not bool(source)


class IsNone(Operator):
    parameter_count: int = 1

    def match(self, source, value=None):
        return source is None


class NotNone(Operator):
    parameter_count: int = 1

    def match(self, source, value=None):
        return source is not None


class Between(Operator):
    """
    Range comparison operator with configurable bound inclusivity.
    `value` must be a tuple (lo, hi).
    """
    parameter_count: int = 3

    def __init__(self, include_lower: bool, include_upper: bool):
        self.include_lower = include_lower
        self.include_upper = include_upper

    def match(self, source, value=None):
        try:
            lo, hi = value if isinstance(value, tuple) else (None, None)
            if lo is None or hi is None:
                return False
            # If lo > hi, consider empty set (no match)
            if lo > hi:
                return False
            # Lower bound
            if self.include_lower:
                if source < lo:
                    return False
            else:
                if source <= lo:
                    return False
            # Upper bound
            if self.include_upper:
                if source > hi:
                    return False
            else:
                if source >= hi:
                    return False
            return True
        except Exception:
            # Non-comparable types -> no match
            return False


class Near(Operator):
    """
    Similarity threshold operator for vector fields.
    Usage in Expression.compile: ['field', 'near[]', query_vector, threshold]
    - value is treated as a tuple (query_vector, threshold)
    - metric defaults to cosine
    """
    parameter_count: int = 3

    def __init__(self, metric: str = 'cosine'):
        self.metric = metric

    def match(self, source, value=None) -> bool:
        try:
            if not isinstance(value, tuple) or len(value) < 2:
                return False
            query_vec, threshold = value[0], value[1]
            # Accept both plain lists and Vector objects
            from .vectors import Vector, cosine_similarity, l2_distance
            # Extract raw data for source
            src = source
            if isinstance(src, Vector):
                src_iter = src.data
            else:
                src_iter = list(src)
            if self.metric == 'cosine':
                score = cosine_similarity(src_iter, query_vec.data if isinstance(query_vec, Vector) else query_vec)
                return score >= float(threshold)
            elif self.metric == 'l2':
                # For l2, interpret threshold as max distance
                # Near should be distance <= threshold
                # Use -distance as score if needed elsewhere
                # Compute distance
                # Reuse cosine helpers module for l2
                from .vectors import l2_distance as _l2
                dist = _l2(src_iter, query_vec.data if isinstance(query_vec, Vector) else query_vec)
                return dist <= float(threshold)
            else:
                return False
        except Exception:
            return False


class Term(Expression):
    target_attribute: str
    operation: Operator
    value: object

    def __init__(self, target_attribute: str, operation: Operator, value: object):
        self.target_attribute = target_attribute
        self.operation = operation
        self.value = value

    def match(self, record):
        # Support dotted-paths and dict/DBObject records
        def resolve(obj, path: str):
            parts = path.split('.') if isinstance(path, str) else [path]
            cur = obj
            for p in parts:
                if cur is None:
                    return None
                if isinstance(cur, dict):
                    cur = cur.get(p)
                else:
                    try:
                        cur = getattr(cur, p)
                    except Exception:
                        return None
            return cur
        source_value = resolve(record, self.target_attribute)
        return self.operation.match(source_value, self.value)


class TrueTerm(Expression):
    def match(self, record):
        return True


class FalseTerm(Expression):
    def match(self, record):
        return False


class ListPlan(QueryPlan):
    """
    Create a QueryPlan from a python list
    """
    base_list: list

    def __init__(self,
                 base_list: list,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.base_list = base_list

    def execute(self) -> list:
        for item in self.base_list:
            yield item

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        return self


class IndexedQueryPlan(QueryPlan):
    """
    IndexedQueryPlan is a specialized version of QueryPlan.

    It provides functionality for creating and managing indexed query plans
    that optimize query execution based on indexed data sources. This class
    extends the base functionality of QueryPlan to incorporate the use of
    indices in query operations.

    """
    indexes: object

    def __init__(
            self,
            indexes = None,
            based_on: QueryPlan = None,
            atom_pointer: AtomPointer = None,
            transaction: ObjectTransaction = None,
            **kwargs):
        super().__init__(based_on=based_on, atom_pointer=atom_pointer, transaction=transaction, **kwargs)
        if indexes is None:
            # Lazy import to avoid circular dependency; only used if someone constructs
            # an IndexedQueryPlan without providing indexes.
            from .dictionaries import Dictionary as _Dictionary
            self.indexes = _Dictionary(transaction=self.transaction)
        else:
            self.indexes = indexes

    def execute(self) -> list:
        # Delegate execution to the underlying plan; indexes are only metadata for optimization
        if self.based_on is None:
            return []
        for rec in self.based_on.execute():
            yield rec

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        return IndexedQueryPlan(
            indexes=self.indexes,
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )

    def add_index(self,
                  field_name: str) -> IndexedQueryPlan:
        """
        Adds an index to the database for optimizing query performance on specified columns. This method
        creates a new index with the given name on the columns specified in the list. Indexing can
        significantly improve the efficiency of certain queries, particularly for large datasets.

        :param field_name: field the index will be created on
        :return: An indexed query plan that contains details of the created index and its application
                 to the underlying query structure.
        :rtype: IndexedQueryPlan
        """
        if self.indexes.has(field_name):
            return self

        # Reindex the current content on the added field
        from .dictionaries import RepeatedKeysDictionary
        new_index = RepeatedKeysDictionary(transaction=self.transaction)
        for record in self.execute():
            if record.has(field_name):
                # Use native key types (no string conversion)
                key = record[field_name]
                new_index = new_index.set_at(key, record)

        return IndexedQueryPlan(
            indexes=self.indexes.set_at(field_name, new_index),
            based_on=self.based_on,
            transaction=self.transaction
        )

    def update_indexes_on_remove(self, removed_record: Atom) -> IndexedQueryPlan:
        """
        Update indexes when specific data is removed from a collection or database.

        This function updates the internal indexes to maintain consistency
        after the specified data is removed. It ensures that subsequent
        queries reflect the correct indexed structure.

        :param removed_record: The data item that was removed, for which the
            indexes need to be updated.
        :return: An updated query plan reflecting the state of indexes
            after the removal.
        :rtype: IndexedQueryPlan
        """
        new_indexes = self.indexes
        for field_name, index in self.indexes.as_iterable():
            # index is expected to support remove_record_at(key, record)
            if removed_record[field_name]:
                key = removed_record[field_name]
                new_indexes.set_at(field_name, index.remove_record_at(key, removed_record))

        return IndexedQueryPlan(
            indexes=new_indexes,
            based_on=self.based_on,
            transaction=self.transaction
        )

    def update_indexes_on_add(self, added_record: Atom) -> IndexedQueryPlan:
        """
        Updates the indexed query plan when an item is added to the dataset.

        The method ensures that the internal indexes are recalibrated after
        removing any existing data that would conflict with the new item's
        location or plan alignment in the indexed structure. It recalculates
        and returns the updated query plan that reflects the modifications.

        :param added_record: the added data.
        :return: The updated IndexedQueryPlan object after modification to
            reflect changes caused by the addition operation.
        :rtype: IndexedQueryPlan
        """
        new_indexes = self.indexes
        for field_name, index in self.indexes.as_iterable():
            index = cast(RepeatedKeysDictionary, index)
            if added_record[field_name]:
                key = added_record[field_name]
                new_indexes.set_at(field_name, index.set_at(key, added_record))

        return IndexedQueryPlan(
            indexes=new_indexes,
            based_on=self.based_on,
            transaction=self.transaction
        )

    def position_at(self, field_name: str, value) -> int:
        self._load()

        if self.indexes and self.indexes.has(field_name):
            idx_dict = cast(Dictionary, self.indexes.get_at(field_name))
            if idx_dict is None:
                return 0

            def _ok(v):
                # use same ordering as DictionaryItem
                from .dictionaries import DictionaryItem as _DI
                return _DI._order_key(v)

            left = 0
            right = idx_dict.content.count - 1
            target_ok = _ok(value)

            while left <= right:
                center = (left + right) // 2

                item = idx_dict.content.get_at(center)
                if item is None:
                    break
                item_ok = _ok(item.key)
                if item_ok == target_ok and item.key == value:
                    return center

                if item_ok > target_ok:
                    right = center - 1
                else:
                    left = center + 1

            return left
        else:
            raise ProtoValidationException(
                message=f'No index on field {field_name}!'
            )

    def yield_from_index(self, field_name: str, index: int) -> list:
        idx_dict = cast(Dictionary, self.indexes.get_at(field_name))
        if idx_dict is None:
            return
        while index < idx_dict.content.count:
            item = cast(DictionaryItem, idx_dict.content.get_at(index))
            index += 1
            if item is None:
                continue
            value_set = cast(Set, item.value)
            for record in value_set.as_iterable():
                yield record

    def get_greater_than(self, field_name: str, value: object) -> list:
        idx_dict = cast(Dictionary, self.indexes.get_at(field_name))
        if idx_dict is None:
            return []
        index = self.position_at(field_name, value)
        if index < idx_dict.content.count:
            item = cast(DictionaryItem, idx_dict.content.get_at(index))
            if item and item.key == value:
                index += 1
        return self.yield_from_index(field_name, index)

    def get_greater_or_equal_than(self, field_name: str, value: object) -> list:
        index = self.position_at(field_name, value)
        return self.yield_from_index(field_name, index)

    def get_equal_than(self, field_name: str, value: object) -> list:
        if self.indexes and self.indexes.has(field_name):
            idx_dict = cast(Dictionary, self.indexes.get_at(field_name))
            if idx_dict is None:
                return []

            # Native key match without string conversion
            value_set = None
            for k, v in idx_dict.as_iterable():
                if k == value:
                    value_set = cast(Set, v)
                    break
            if value_set is None:
                return []
            for record in value_set.as_iterable():
                yield record
        else:
            raise ProtoValidationException(
                message=f'No index on field {field_name}!'
            )

    def yield_up_to_index(self, field_name: str, index_up_to: int) -> list:
        idx_dict = cast(Dictionary, self.indexes.get_at(field_name))
        if idx_dict is None:
            return
        index = 0
        while index < idx_dict.content.count and index < index_up_to:
            item = cast(DictionaryItem, idx_dict.content.get_at(index))
            index += 1
            if item is None:
                continue
            value_set = cast(Set, item.value)
            for record in value_set.as_iterable():
                yield record

    def get_lower_than(self, field_name: str, value: object) -> list:
        idx_dict = cast(Dictionary, self.indexes.get_at(field_name))
        if idx_dict is None:
            return []
        index = self.position_at(field_name, value)
        # Exclusive: do not include the bucket equal to value
        return self.yield_up_to_index(field_name, index)

    def get_lower_or_equal_than(self, field_name: str, value: object) -> list:
        idx_dict = cast(Dictionary, self.indexes.get_at(field_name))
        if idx_dict is None:
            return []
        index = self.position_at(field_name, value)
        # Inclusive: if exact match at position, include it by advancing one
        if index < idx_dict.content.count:
            item = cast(DictionaryItem, idx_dict.content.get_at(index))
            if item and item.key == value:
                index += 1
        return self.yield_up_to_index(field_name, index)

    def get_range(self, field_name: str, lo: object, hi: object, include_lower: bool, include_upper: bool):
        """
        Iterate records whose indexed key is within [lo, hi] with bound inclusivity flags.
        Uses native-type comparisons; avoids converting keys/values to strings.
        Performs a binary search to the first matching index, then a sequential scan until hi.
        """
        self._load()
        if not (self.indexes and self.indexes.has(field_name)):
            raise ProtoValidationException(message=f'No index on field {field_name}!')
        idx_dict = cast(Dictionary, self.indexes.get_at(field_name))
        if idx_dict is None:
            return

        from .dictionaries import DictionaryItem as _DI
        def _ok(v):
            return _DI._order_key(v)

        count = idx_dict.content.count
        # Binary search lower bound position
        left, right = 0, count - 1
        target_ok = _ok(lo)
        pos = 0
        while left <= right:
            center = (left + right) // 2
            item = cast(DictionaryItem, idx_dict.content.get_at(center))
            if item is None:
                break
            iok = _ok(item.key)
            if iok >= target_ok:
                right = center - 1
                pos = center
            else:
                left = center + 1
        # Adjust for exclusivity on lower bound
        if pos < count:
            item = cast(DictionaryItem, idx_dict.content.get_at(pos))
            if item is not None:
                if not include_lower and item.key == lo:
                    pos += 1

        # Sequential scan until upper bound
        i = pos
        hi_ok = _ok(hi)
        while i < count:
            item = cast(DictionaryItem, idx_dict.content.get_at(i))
            i += 1
            if item is None:
                continue
            key_ok = _ok(item.key)
            # Stop if beyond upper bound (respect inclusivity)
            if key_ok > hi_ok or (key_ok == hi_ok and not include_upper and item.key == hi):
                break
            # Skip items below lower bound (in case of edge adjustments)
            if key_ok < target_ok or (key_ok == target_ok and not include_lower and item.key == lo):
                continue
            value_set = cast(Set, item.value)
            for record in value_set.as_iterable():
                yield record


class IndexedSearchPlan(IndexedQueryPlan):
    field_to_scan: str
    operator: Operator
    value: str

    def __init__(
            self,
            field_to_scan: str,
            operator: Operator = None,
            value: str = None,
            indexes: Dictionary = None,
            based_on: QueryPlan = None,
            atom_pointer: AtomPointer = None,
            transaction: ObjectTransaction = None,
            **kwargs):
        super().__init__(
            indexes=indexes,
            based_on=based_on,
            atom_pointer=atom_pointer,
            transaction=transaction, **kwargs)
        if not field_to_scan:
            raise ProtoValidationException(
                message=f'The field to scan should be specified!'
            )
        if not operator:
            raise ProtoValidationException(
                message=f'The operator should be specified!'
            )

        self.field_to_scan = field_to_scan
        if not isinstance(operator, (
                Lower, LowerOrEqual, Equal, GreaterOrEqual, Greater)):
            raise ProtoValidationException(
                message=f'The operator is not valid ({operator})!'
            )
        self.operator = operator
        self.value = value

    def execute(self) -> list:
        if isinstance(self.operator, Equal):
            return self.get_equal_than(self.field_to_scan, self.value)
        elif isinstance(self.operator, Greater):
            return self.get_greater_than(self.field_to_scan, self.value)
        elif isinstance(self.operator, GreaterOrEqual):
            return self.get_greater_or_equal_than(self.field_to_scan, self.value)
        elif isinstance(self.operator, Lower):
            return self.get_lower_than(self.field_to_scan, self.value)
        elif isinstance(self.operator, LowerOrEqual):
            return self.get_lower_or_equal_than(self.field_to_scan, self.value)
        else:
            return []

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        return IndexedSearchPlan(
            field_to_scan=self.field_to_scan,
            operator=self.operator,
            value=self.value,
            indexes=self.indexes,
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )

    def count(self) -> int:
        """
        Fast count leveraging indexes when possible.
        - For equality over an indexed field, return the set count directly.
        - Otherwise, fallback to executing and counting.
        """
        try:
            if isinstance(self.operator, Equal) and self.indexes and self.indexes.has(self.field_to_scan):
                idx_dict = cast(Dictionary, self.indexes.get_at(self.field_to_scan))
                if idx_dict is None:
                    return 0
                # Try native-key lookup by scanning keys to avoid string conversion
                value_set = None
                for k, v in idx_dict.as_iterable():
                    if k == self.value:
                        value_set = cast(Set, v)
                        break
                if value_set is None:
                    return 0
                return value_set.count
        except Exception:
            # On any unexpected shape, fallback to generic count
            pass
        return sum(1 for _ in self.execute())

    def get_references(self) -> frozenset[int]:
        """
        Return a frozenset of stable references (preferably AtomPointer.hash()) for records
        matching this indexed predicate, without materializing full objects.
        """
        refs: set[int] = set()
        try:
            if not (self.indexes and self.indexes.has(self.field_to_scan)):
                return frozenset()
            idx_dict = cast(Dictionary, self.indexes.get_at(self.field_to_scan))
            if idx_dict is None:
                return frozenset()

            def _ref_of(rec) -> int:
                try:
                    if isinstance(rec, Atom) and hasattr(rec, 'atom_pointer') and rec.atom_pointer:
                        return rec.atom_pointer.hash()
                except Exception:
                    pass
                try:
                    return rec.hash()  # custom wrapper may implement
                except Exception:
                    return hash(rec)

            # Handle equality natively; others fallback to execute()
            if isinstance(self.operator, Equal):
                bucket = None
                for k, v in idx_dict.as_iterable():
                    if k == self.value:
                        bucket = cast(Set, v)
                        break
                if bucket is None:
                    return frozenset()
                for rec in bucket.as_iterable():
                    refs.add(_ref_of(rec))
                return frozenset(refs)

            # Fallback: execute and convert to references (less efficient)
            for rec in self.execute():
                refs.add(_ref_of(rec))
            return frozenset(refs)
        except Exception:
            return frozenset()


class OrMerge(QueryPlan):
    or_queries: list[QueryPlan]

    def __init__(self,
                 or_queries: list[QueryPlan] = None,
                 based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.or_queries = or_queries or []

    def execute(self) -> list:
        """
        Execute all sub-queries and yield the concatenated union of results without de-duplication.
        De-duplication is responsibility of count() or higher-level consumers when needed.
        """
        for q in self.or_queries:
            for rec in q.execute():
                yield rec

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        return OrMerge(
            or_queries=[q.optimize(full_plan) for q in self.or_queries],
            based_on=self.based_on.optimize(full_plan) if self.based_on else None,
            transaction=self.transaction
        )

    def count(self) -> int:
        """
        Count unique records across sub-queries.
        Prefer fast path with get_references()/keys_iterator() when available; otherwise fall back to hashing
        materialized items from execute().
        """
        # Try fast path using get_references when present
        uniq: set[int] = set()
        any_fast = False
        for q in self.or_queries:
            if hasattr(q, 'get_references'):
                try:
                    refs = getattr(q, 'get_references')()
                    if refs:
                        any_fast = True
                        uniq.update(refs)
                        continue
                except Exception:
                    pass
            # Try keys_iterator if defined
            if hasattr(q, 'keys_iterator'):
                try:
                    for k in q.keys_iterator():
                        uniq.add(hash(k))
                    any_fast = True
                    continue
                except Exception:
                    pass
        if any_fast:
            return len(uniq)
        # Fallback: materialize and hash
        def _ref_of(rec) -> int:
            try:
                if isinstance(rec, Atom) and getattr(rec, 'atom_pointer', None):
                    return rec.atom_pointer.hash()
            except Exception:
                pass
            try:
                return rec.hash()
            except Exception:
                return hash(rec)
        for rec in self.execute():
            try:
                uniq.add(_ref_of(rec))
            except Exception:
                continue
        return len(uniq)


class AndMerge(QueryPlan):
    and_queries: list[QueryPlan]
    residual_filters: list[Expression] | None

    def __init__(self,
                 and_queries: list[QueryPlan] = None,
                 based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 residual_filters: list[Expression] | None = None,
                 **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.and_queries = and_queries
        self.residual_filters = residual_filters or []

    def execute(self) -> list:
        """
        Intersect sub-queries by stable references to minimize materialization.
        Each sub-plan is asked for get_references(); if unavailable, we fallback to hashing
        materialized records. After computing the intersection, we materialize only from the
        smallest contributing sub-plan and yield records whose reference is in the intersection.
        Residual filters (non-indexable terms) are applied at the end to the few remaining records.
        """
        if not self.and_queries:
            return
        # Collect reference sets and keep a plan to materialize from
        ref_sets: list[tuple[frozenset[int], QueryPlan]] = []
        for q in self.and_queries:
            refs: frozenset[int] | None = None
            if hasattr(q, 'get_references'):
                try:
                    refs = cast(frozenset[int], getattr(q, 'get_references')())
                except Exception:
                    refs = None
            if refs is None or len(refs) == 0:
                # Fallback: derive refs from materialized records (less efficient)
                hset: set[int] = set()
                try:
                    for rec in q.execute():
                        try:
                            if isinstance(rec, Atom) and getattr(rec, 'atom_pointer', None):
                                hset.add(rec.atom_pointer.hash())
                            else:
                                try:
                                    hset.add(rec.hash())
                                except Exception:
                                    hset.add(hash(rec))
                        except Exception:
                            continue
                    refs = frozenset(hset)
                except Exception:
                    refs = frozenset()
            ref_sets.append((refs, q))

        # Early exit if any is empty
        if any(len(rs[0]) == 0 for rs in ref_sets):
            return

        # Sort by ascending size for efficient intersection
        ref_sets.sort(key=lambda t: len(t[0]))
        base_refs, base_plan = ref_sets[0]
        intersection = set(base_refs)
        for rs, _ in ref_sets[1:]:
            intersection.intersection_update(rs)
            if not intersection:
                return

        # Materialize minimally: iterate the smallest plan and filter by intersection
        def _ref_of(rec) -> int:
            try:
                if isinstance(rec, Atom) and getattr(rec, 'atom_pointer', None):
                    return rec.atom_pointer.hash()
            except Exception:
                pass
            try:
                return rec.hash()
            except Exception:
                return hash(rec)

        def _matches_residual(rec) -> bool:
            if not self.residual_filters:
                return True
            try:
                for expr in self.residual_filters:
                    if not expr.match(rec):
                        return False
                return True
            except Exception:
                return False

        for rec in base_plan.execute():
            try:
                if _ref_of(rec) in intersection and _matches_residual(rec):
                    yield rec
            except Exception:
                continue

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        return AndMerge(
            and_queries=self.and_queries,
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )

    def count(self) -> int:
        """
        Calculates the count of the intersection of sub-queries.
        It optimizes by iterating the smaller result set and checking for
        existence in the larger one, without materializing full objects.
        """
        if not self.and_queries:
            return 0

        # Optimize sub-queries first
        optimized_queries = [q.optimize(self) for q in self.and_queries]

        # Get iterators of IDs, not full objects
        id_iterators = [q.keys_iterator() for q in optimized_queries]

        # Sort by potential size (if a count() method is available)
        id_iterators.sort(key=lambda it: it.count() if hasattr(it, 'count') else float('inf'))

        # Use the smallest set as the base for iteration
        base_ids = set(id_iterators[0])
        if not base_ids:
            return 0

        # Intersect with the other sets
        for other_iterator in id_iterators[1:]:
            base_ids.intersection_update(other_iterator)
            if not base_ids:
                return 0

        return len(base_ids)


class IndexedRangeSearchPlan(IndexedQueryPlan):
    field_to_scan: str
    include_lower: bool
    include_upper: bool
    lo: object
    hi: object

    def __init__(self, field_to_scan: str, lo, hi, include_lower: bool, include_upper: bool,
                 indexes: Dictionary = None, based_on: QueryPlan = None,
                 atom_pointer: AtomPointer = None, transaction: ObjectTransaction = None, **kwargs):
        super().__init__(indexes=indexes, based_on=based_on, atom_pointer=atom_pointer, transaction=transaction, **kwargs)
        self.field_to_scan = field_to_scan
        self.lo = lo
        self.hi = hi
        self.include_lower = include_lower
        self.include_upper = include_upper

    def execute(self):
        # Prefer using the index if available; otherwise, fallback to based_on
        if self.indexes and self.indexes.has(self.field_to_scan):
            yield from self.get_range(self.field_to_scan, self.lo, self.hi, self.include_lower, self.include_upper)
            return
        # Fallback should not normally happen if optimizer set this, but guard anyway
        if self.based_on is not None:
            for rec in self.based_on.execute():
                yield rec
        return

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        return IndexedRangeSearchPlan(
            field_to_scan=self.field_to_scan,
            lo=self.lo,
            hi=self.hi,
            include_lower=self.include_lower,
            include_upper=self.include_upper,
            indexes=self.indexes,
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )

    def count(self) -> int:
        return sum(1 for _ in self.execute())

    def get_references(self) -> frozenset[int]:
        """
        Return a frozenset of references for all records with field value in [lo, hi]
        according to inclusivity flags, without materializing full objects.
        Performs a lower-bound binary search on the index (AVL-backed List) and then
        scans sequentially until the upper bound is exceeded, collecting AtomPointer hashes.
        """
        refs: set[int] = set()
        try:
            if not (self.indexes and self.indexes.has(self.field_to_scan)):
                return frozenset()
            idx_dict = cast(Dictionary, self.indexes.get_at(self.field_to_scan))
            if idx_dict is None:
                return frozenset()

            from .dictionaries import DictionaryItem as _DI
            def _ok(v):
                return _DI._order_key(v)

            def _ref_of(rec) -> int:
                try:
                    if isinstance(rec, Atom) and getattr(rec, 'atom_pointer', None):
                        return rec.atom_pointer.hash()
                except Exception:
                    pass
                try:
                    return rec.hash()
                except Exception:
                    return hash(rec)

            count = idx_dict.content.count
            left, right = 0, count - 1
            lo_ok = _ok(self.lo)
            pos = 0
            while left <= right:
                center = (left + right) // 2
                item = cast(DictionaryItem, idx_dict.content.get_at(center))
                if item is None:
                    break
                iok = _ok(item.key)
                if iok >= lo_ok:
                    right = center - 1
                    pos = center
                else:
                    left = center + 1
            # Adjust exclusivity for lower bound
            if pos < count:
                it = cast(DictionaryItem, idx_dict.content.get_at(pos))
                if it is not None and not self.include_lower and it.key == self.lo:
                    pos += 1

            hi_ok = _ok(self.hi)
            i = pos
            while i < count:
                it = cast(DictionaryItem, idx_dict.content.get_at(i))
                i += 1
                if it is None:
                    continue
                key_ok = _ok(it.key)
                if key_ok > hi_ok or (key_ok == hi_ok and not self.include_upper and it.key == self.hi):
                    break
                if key_ok < lo_ok or (key_ok == lo_ok and not self.include_lower and it.key == self.lo):
                    continue
                value_set = cast(Set, it.value)
                for rec in value_set.as_iterable():
                    refs.add(_ref_of(rec))
            return frozenset(refs)
        except Exception:
            return frozenset()




class FromPlan(IndexedQueryPlan):
    """

    """
    alias: str

    def __init__(self,
                 alias: str,
                 indexes: dict[str, RepeatedKeysDictionary] = None,
                 based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        if alias and indexes:
            indexes = {
                f'{alias}.{field_name}': indexes
                for field_name, indexes in indexes.items()
            }
        super().__init__(indexes=indexes, based_on=based_on, transaction=transaction, atom_pointer=atom_pointer,
                         **kwargs)
        self.alias = alias

    def execute(self) -> list:
        for item in self.based_on.execute():
            result = DBObject(
                transaction=self.transaction
            )
            if self.alias:
                result = result._setattr(self.alias, item)
            else:
                for field_name, value in item.__dict__.items():
                    if not field_name.startswith('_') and not callable(value):
                        result = result._setattr(field_name, value)
            yield result

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        return FromPlan(
            alias=self.alias,
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )


class JoinPlan(QueryPlan):
    """
    Join two query plans with simple heuristic-based join semantics.

    Supported join_type values in tests:
      - 'inner': only matching pairs
      - 'left': all from left, matching from right when available
      - 'right': all from right, matching from left when available
      - 'external': cartesian product of both sides plus both sides individually
      - 'external_left': left-only plus cartesian product
      - 'external_right': right-only plus cartesian product
      - 'outer': only side-only elements (no combining)

    Matching heuristic for inner/left/right:
      If left has field "{right_alias}_id" and right has field "id":
        left.{right_alias}_id == right.id
      Else if left has field "id" and right has field "{left_alias}_id":
        left.id == right.{left_alias}_id
      Otherwise, no match.
    """

    def __init__(self,
                 join_query: QueryPlan,
                 join_type: str = 'inner',
                 based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.join_query = join_query
        self.join_type = (join_type or 'inner').lower()

    def _detect_alias(self, plan: QueryPlan, sample_record: DBObject | None) -> str | None:
        # Prefer alias from FromPlan
        try:
            from .queries import FromPlan as _FromPlan
            if isinstance(plan, _FromPlan):
                return plan.alias
        except Exception:
            pass
        # Otherwise, infer from record attributes if it has a single public attribute
        if sample_record is not None:
            pub = [k for k in sample_record.__dict__.keys() if not k.startswith('_')]
            if len(pub) == 1:
                return pub[0]
        return None

    def _get_side_object(self, rec: DBObject, alias: str | None):
        if alias and hasattr(rec, alias):
            return getattr(rec, alias)
        return rec

    def _match(self, lrec: DBObject, rrec: DBObject, la: str | None, ra: str | None) -> bool:
        left_obj = self._get_side_object(lrec, la)
        right_obj = self._get_side_object(rrec, ra)
        # Try left.{ra}_id == right.id
        key1 = f"{ra}_id" if ra else None
        try:
            if key1 and hasattr(left_obj, key1) and hasattr(right_obj, 'id'):
                return getattr(left_obj, key1) == getattr(right_obj, 'id')
        except Exception:
            pass
        # Try left.id == right.{la}_id
        key2 = f"{la}_id" if la else None
        try:
            if hasattr(left_obj, 'id') and key2 and hasattr(right_obj, key2):
                return getattr(left_obj, 'id') == getattr(right_obj, key2)
        except Exception:
            pass
        return False

    def _copy_public_attrs(self, target: DBObject, source: DBObject) -> DBObject:
        for k, v in source.__dict__.items():
            if not k.startswith('_') and not callable(v):
                target = target._setattr(k, v)
        return target

    def _combine(self, left: DBObject | None, right: DBObject | None) -> DBObject:
        res = DBObject(transaction=self.transaction)
        if left is not None:
            res = self._copy_public_attrs(res, left)
        if right is not None:
            res = self._copy_public_attrs(res, right)
        return res

    def execute(self):
        left_list = list(self.based_on.execute()) if self.based_on else []
        right_list = list(self.join_query.execute()) if self.join_query else []

        la = self._detect_alias(self.based_on, left_list[0] if left_list else None)
        ra = self._detect_alias(self.join_query, right_list[0] if right_list else None)

        jt = self.join_type
        if jt == 'outer':
            # Only side-only, no combined pairs
            for l in left_list:
                yield self._combine(l, None)
            for r in right_list:
                yield self._combine(None, r)
            return

        if jt.startswith('external'):
            include_left_only = jt in ('external', 'external_left')
            include_right_only = jt in ('external', 'external_right')
            # Cartesian combinations of both sides
            for l in left_list:
                for r in right_list:
                    yield self._combine(l, r)
            if include_left_only:
                for l in left_list:
                    yield self._combine(l, None)
            if include_right_only:
                for r in right_list:
                    yield self._combine(None, r)
            return

        # inner/left/right using match
        if jt == 'inner':
            for l in left_list:
                for r in right_list:
                    if self._match(l, r, la, ra):
                        yield self._combine(l, r)
            return

        if jt == 'left':
            for l in left_list:
                matched = False
                for r in right_list:
                    if self._match(l, r, la, ra):
                        yield self._combine(l, r)
                        matched = True
                if not matched:
                    yield self._combine(l, None)
            return

        if jt == 'right':
            for r in right_list:
                matched = False
                for l in left_list:
                    if self._match(l, r, la, ra):
                        yield self._combine(l, r)
                        matched = True
                if not matched:
                    yield self._combine(None, r)
            return

        # Fallback: treat as inner
        for l in left_list:
            for r in right_list:
                if self._match(l, r, la, ra):
                    yield self._combine(l, r)

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        return JoinPlan(
            join_query=self.join_query.optimize(full_plan) if self.join_query else None,
            join_type=self.join_type,
            based_on=self.based_on.optimize(full_plan) if self.based_on else None,
            transaction=self.transaction
        )


class WherePlan(QueryPlan):
    """
    Query plan for filtering records based on an expression.

    This plan evaluates each record from the underlying `based_on` query plan
    against a filtering expression (`filter`). Records that satisfy the expression
    are passed downstream.

    Attributes:
    - filter (Expression): An expression determining which records to retain.
    """

    filter: Expression

    def __init__(self,
                 filter_spec: list = None,
                 filter: Expression = None,
                 based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        if isinstance(filter_spec, list):
            self.filter = Expression.compile(filter_spec)
        else:
            self.filter = filter

    def execute(self) -> list:
        """
        Execute the filtering logic over the input records.

        Optimized path:
        - If the filter is an AndExpression and the underlying plan exposes indexes
          (IndexedQueryPlan), build candidate sets for indexable terms (==, in, contains,
          between, <, <=, >, >=) using the index, sort by selectivity (len), and intersect
          progressively. Then apply residual filtering on the reduced set only.
        - Otherwise, fallback to linear scan.

        :return: A generator yielding filtered records.
        :rtype: list
        """

        # Attempt index-aware evaluation for AND conditions
        from .queries import Between as _Between
        from .queries import Equal as _Equal, In as _In, Contains as _Contains
        from .queries import Greater as _Greater, GreaterOrEqual as _GreaterOrEqual
        from .queries import Lower as _Lower, LowerOrEqual as _LowerOrEqual
        base = self.based_on
        flt = self.filter

        def build_candidate_set(term: Term):
            # Only terms with target attribute present in indexes are eligible
            try:
                field = term.target_attribute
                # Ensure we have an IndexedQueryPlan and an index for the field
                from .queries import IndexedQueryPlan as _IQP
                # Ensure we have an IndexedQueryPlan and an index for the field
                from .queries import IndexedQueryPlan as _IQP
                if not isinstance(base, _IQP):
                    return None
                idxs = getattr(base, 'indexes', None)
                if not idxs or not getattr(idxs, 'has', None) or not idxs.has(field):
                    return None
                op = term.operation
                # Equality
                if isinstance(op, _Equal):
                    idx_dict = idxs.get_at(field)
                    item = idx_dict.get_at(str(term.value))
                    if not item:
                        return set()
                    # item is a Set; turn into Python set of records
                    return set(item.as_iterable())
                # IN operator: union of equalities
                if isinstance(op, _In):
                    result = set()
                    try:
                        for v in term.value:
                            idx_dict = idxs.get_at(field)
                            it = idx_dict.get_at(str(v))
                            if it:
                                result.update(it.as_iterable())
                    except Exception:
                        return None
                    return result
                # CONTAINS: treat as equality on elements if index was built for contained elements
                if isinstance(op, _Contains):
                    idx_dict = idxs.get_at(field)
                    it = idx_dict.get_at(str(term.value))
                    if it:
                        return set(it.as_iterable())
                    return set()
                # BETWEEN (range) — avoid materializing large range sets; leave as residual filter
                if isinstance(op, _Between):
                    return None
                # Greater/Less family as residual filters as well
                if isinstance(op, _Greater):
                    return None
                if isinstance(op, _GreaterOrEqual):
                    return None
                if isinstance(op, _Lower):
                    return None
                if isinstance(op, _LowerOrEqual):
                    return None
                return None
            except Exception:
                return None

        # Only attempt when filter is a conjunction of terms
        if isinstance(flt, AndExpression):
            candidate_sets: list[set] = []
            residual = []  # keep full filter for safety; residual list reserved for future split
            for t in flt.terms:
                if isinstance(t, Term):
                    cand = build_candidate_set(t)
                    if cand is None:
                        residual.append(t)
                    else:
                        candidate_sets.append(cand)
                else:
                    residual.append(t)

            if candidate_sets:
                # Order by selectivity (ascending size)
                candidate_sets.sort(key=lambda s: len(s))
                # Progressive intersection with early exit
                current = candidate_sets[0]
                for s in candidate_sets[1:]:
                    if not current:
                        break
                    current = current.intersection(s)
                if not current:
                    return  # empty generator
                # Apply only residual (non-indexed) terms on the reduced set
                def _matches_residual(rec):
                    if not residual:
                        return True
                    try:
                        for expr in residual:
                            if not expr.match(rec):
                                return False
                        return True
                    except Exception:
                        return False
                for rec in current:
                    if _matches_residual(rec):
                        yield rec
                return

        # Fallback: linear scan
        for record in base.execute():
            if flt.match(record):
                yield record

    def optimize(self, full_plan: 'QueryPlan') -> 'QueryPlan':
        """
        Optimizer rewrite to leverage indexes for:
        - Single Term expressions over an IndexedQueryPlan
        - AndExpression: intersect multiple indexable terms via AndMerge with residual filters
        - OrExpression: union multiple indexable terms via OrMerge when all terms are indexable
        Otherwise, fallback to keeping the original WherePlan (with optimized base).
        """
        optimized_based_on = self.based_on.optimize(full_plan)
        current_filter = self.filter

        # Preserve predicate pushdown when supported by the underlying plan
        if hasattr(optimized_based_on, 'accept_filter'):
            return optimized_based_on.accept_filter(current_filter)

        # Helper: construct an index-backed plan for a term if possible
        def plan_for_term(expr: Expression) -> QueryPlan | None:
            if not isinstance(optimized_based_on, IndexedQueryPlan):
                return None
            if not isinstance(expr, Term):
                return None
            idxs = getattr(optimized_based_on, 'indexes', None)
            if not idxs or not idxs.has(expr.target_attribute):
                return None
            field = expr.target_attribute
            op = expr.operation
            if isinstance(op, Equal):
                return IndexedSearchPlan(
                    field_to_scan=field,
                    operator=op,
                    value=expr.value,
                    indexes=idxs,
                    based_on=optimized_based_on,
                    transaction=self.transaction
                )
            if isinstance(op, Between):
                lo, hi = expr.value if isinstance(expr.value, tuple) else (None, None)
                if lo is None or hi is None:
                    return None
                return IndexedRangeSearchPlan(
                    field_to_scan=field,
                    lo=lo,
                    hi=hi,
                    include_lower=op.include_lower,
                    include_upper=op.include_upper,
                    indexes=idxs,
                    based_on=optimized_based_on,
                    transaction=self.transaction
                )
            if isinstance(op, Greater):
                return IndexedRangeSearchPlan(
                    field_to_scan=field,
                    lo=expr.value,
                    hi=float('inf'),
                    include_lower=False,
                    include_upper=True,
                    indexes=idxs,
                    based_on=optimized_based_on,
                    transaction=self.transaction
                )
            if isinstance(op, GreaterOrEqual):
                return IndexedRangeSearchPlan(
                    field_to_scan=field,
                    lo=expr.value,
                    hi=float('inf'),
                    include_lower=True,
                    include_upper=True,
                    indexes=idxs,
                    based_on=optimized_based_on,
                    transaction=self.transaction
                )
            if isinstance(op, Lower):
                return IndexedRangeSearchPlan(
                    field_to_scan=field,
                    lo=float('-inf'),
                    hi=expr.value,
                    include_lower=True,
                    include_upper=False,
                    indexes=idxs,
                    based_on=optimized_based_on,
                    transaction=self.transaction
                )
            if isinstance(op, LowerOrEqual):
                return IndexedRangeSearchPlan(
                    field_to_scan=field,
                    lo=float('-inf'),
                    hi=expr.value,
                    include_lower=True,
                    include_upper=True,
                    indexes=idxs,
                    based_on=optimized_based_on,
                    transaction=self.transaction
                )
            return None

        # 1) Single Term over an IndexedQueryPlan
        if isinstance(current_filter, Term):
            plan = plan_for_term(current_filter)
            if plan is not None:
                return plan

        # 2) AND expression: existing behavior with intersection
        if isinstance(current_filter, AndExpression) and isinstance(optimized_based_on, IndexedQueryPlan):
            idxs = getattr(optimized_based_on, 'indexes', None)
            if idxs:
                index_plans: list[QueryPlan] = []
                residuals: list[Expression] = []
                for expr in current_filter.terms:
                    p = plan_for_term(expr)
                    if p is not None:
                        index_plans.append(p)
                    else:
                        residuals.append(expr)
                if index_plans:
                    return AndMerge(and_queries=index_plans,
                                    based_on=optimized_based_on,
                                    transaction=self.transaction,
                                    residual_filters=residuals)

        # 3) OR expression: only if all sub-terms are indexable
        if isinstance(current_filter, OrExpression) and isinstance(optimized_based_on, IndexedQueryPlan):
            idx_plans: list[QueryPlan] = []
            all_indexable = True
            for expr in current_filter.terms:
                p = plan_for_term(expr)
                if p is None:
                    all_indexable = False
                    break
                idx_plans.append(p)
            if all_indexable and idx_plans:
                return OrMerge(or_queries=idx_plans, based_on=optimized_based_on, transaction=self.transaction)

        # Default: keep as WherePlan with optimized base
        self.based_on = optimized_based_on
        self.filter = current_filter
        return self

    def _reorder_and_expression(self, and_expression: 'AndExpression') -> 'AndExpression':
        """
        Sorts terms within an AndExpression based on a cost heuristic.

        The goal is to place less expensive and more selective terms at the
        beginning of the terms list. This allows the execution engine to
        reject non-matching records as early as possible.

        Args:
            and_expression: The AndExpression instance to reorder.

        Returns:
            A new AndExpression with the terms sorted according to the cost model.
        """

        def get_term_cost(term):
            """
            Calculates a cost for a filter term. A lower cost is better.
            This heuristic can be expanded to include index availability,
            cardinality estimates, etc.
            """
            # Simple heuristic: equality checks are generally cheaper and more
            # selective than other operations.
            if isinstance(term, Term) and term.operation in ['==', 'eq']:
                # A check on an indexed field would ideally have a cost of 0.
                return 1
            return 10  # Represents a higher cost for other operations.

        # Sort the terms, placing the lowest-cost terms first.
        sorted_terms = sorted(and_expression.terms, key=get_term_cost)
        return AndExpression(terms=sorted_terms)


class AgreggatorFunction(ABC):
    """
    Abstract base class for aggregator functions.

    Aggregator functions perform operations (e.g., sum, average) over fields
    or records grouped under a common criteria. Each subclass implements
    its own logic for computing aggregate results.
    """

    @abstractmethod
    def compute(self, values: list):
        """
        Compute the aggregation over a list of values.

        :param values: A list of values to aggregate.
        :type values: list
        :return: The result of the aggregation.
        """


class SumAgreggator(AgreggatorFunction):
    """
    Sum aggregator function for computing the sum of a list of numeric values.
    """

    def compute(self, values: list):
        """
        Calculate the total sum of the given numeric values.

        :param values: A list of numeric values.
        :type values: list[float|int]
        :return: The sum of all values in the list.
        :rtype: float
        """
        total_sum = 0.0
        for value in values:
            total_sum += value
        return total_sum


class AvgAggregator(AgreggatorFunction):
    def compute(self, values: list):
        sum = 0.0
        for value in values:
            sum += value

        if len(values) > 0:
            return sum / len(values)
        else:
            return 0.0


class CountAggregator(AgreggatorFunction):
    def compute(self, values: list):
        return len(values)


class MinAgreggator(AgreggatorFunction):
    def compute(self, values: list):
        minimun = None
        for value in values:
            if minimun is None or value < minimun:
                minimun = value
        return minimun


class MaxAgreggator(AgreggatorFunction):
    """
    An aggregator that finds the maximum value in a list of values.

    It initializes the maximum value to None and then iterates through the list,
    updating the maximum value whenever a larger value is found.
    It returns the overall maximum value. If the list is empty, it returns None.
    """

    def compute(self, values: list):
        """
        Computes the maximum value from a list of values.

        Args:
            values: The list of values to compute the maximum from.

        Returns:
            The maximum value in the list, or None if the list is empty.
        """
        max_value = None
        for value in values:
            if max_value is None or value > max_value:
                max_value = value

        return max_value


class AgreggatorSpec:
    def __init__(self, agreggator: AgreggatorFunction, source_field: str, target_field: str):
        self.agreggator = agreggator
        self.source_field = source_field
        self.target_field = target_field


class GroupByPlan(QueryPlan):
    def __init__(self,
                 group_fields: list[str],
                 agreggated_fields: dict[str, AgreggatorSpec],
                 based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.group_fields = group_fields or []
        self.agreggated_fields = agreggated_fields or {}

    def execute(self):
        groups: dict[tuple, list[DBObject]] = {}
        for rec in (self.based_on.execute() if self.based_on else []):
            key = tuple(rec.__dict__.get(f, None) for f in self.group_fields)
            groups.setdefault(key, []).append(rec)

        for key, records in groups.items():
            out = DBObject(transaction=self.transaction)
            # Set group fields
            for i, f in enumerate(self.group_fields):
                out = out._setattr(f, key[i])
            # Compute aggregations
            for name, spec in self.agreggated_fields.items():
                # Extract values; for sums/avgs, treat missing as 0; for min/max skip None
                values = []
                for r in records:
                    v = r.__dict__.get(spec.source_field, None)
                    values.append(0 if v is None and isinstance(spec.agreggator, (SumAgreggator, AvgAggregator)) else (v if v is not None else 0))
                # For avg/min/max, better to ignore None: build cleaned list when aggregator is not Count
                if isinstance(spec.agreggator, AvgAggregator):
                    clean = [v for v in values if v is not None]
                    result = spec.agreggator.compute(clean)
                elif isinstance(spec.agreggator, (MinAgreggator, MaxAgreggator)):
                    clean = [v for v in (r.__dict__.get(spec.source_field, None) for r in records) if v is not None]
                    result = spec.agreggator.compute(clean)
                elif isinstance(spec.agreggator, CountAggregator):
                    result = spec.agreggator.compute(records)
                else:
                    result = spec.agreggator.compute(values)
                out = out._setattr(spec.target_field, result)
            yield out

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        return GroupByPlan(
            group_fields=list(self.group_fields),
            agreggated_fields=dict(self.agreggated_fields),
            based_on=self.based_on.optimize(full_plan) if self.based_on else None,
            transaction=self.transaction
        )


class UnnestPlan(QueryPlan):
    """
    Plan that flattens a collection from each input record.
    - source_path: dotted path string or callable(record) -> iterable
    - element_alias: if provided, the element is attached to the original record under this key; otherwise the element replaces the record.
    """
    def __init__(self, source_path, element_alias: str | None = None, based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None, atom_pointer: AtomPointer = None, **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.source_path = source_path
        self.element_alias = element_alias

    def _resolve_path(self, record, path: str):
        parts = path.split('.') if isinstance(path, str) else [path]
        cur = record
        for p in parts:
            if cur is None:
                return None
            if isinstance(cur, dict):
                cur = cur.get(p)
            else:
                try:
                    cur = getattr(cur, p)
                except Exception:
                    return None
        return cur

    def execute(self):
        if not self.based_on:
            return
        for record in self.based_on.execute():
            # determine the collection
            collection = None
            if callable(self.source_path):
                try:
                    collection = self.source_path(record)
                except Exception:
                    collection = None
            elif isinstance(self.source_path, str):
                collection = self._resolve_path(record, self.source_path)
            # check iterability (avoid strings/bytes)
            if collection is None:
                continue
            if isinstance(collection, (str, bytes)):
                continue
            try:
                iterator = iter(collection)
            except TypeError:
                continue
            for elem in iterator:
                if self.element_alias:
                    if isinstance(record, dict):
                        out = dict(record)
                        out[self.element_alias] = elem
                        yield out
                    else:
                        # assume DBObject-like
                        try:
                            yield record._setattr(self.element_alias, elem)
                        except Exception:
                            yield {**(dict(record) if hasattr(record, 'items') else {}), self.element_alias: elem}
                else:
                    yield elem

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        if self.based_on:
            self.based_on = self.based_on.optimize(full_plan)
        return self


class CollectionFieldPlan(QueryPlan):
    """
    For each left record, executes a subplan built from it and attaches the collected results as a list under field_name.
    """
    def __init__(self, field_name: str, subplan_builder, based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None, atom_pointer: AtomPointer = None, **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.field_name = field_name
        self.subplan_builder = subplan_builder

    def execute(self):
        if not self.based_on:
            return
        for left in self.based_on.execute():
            subplan = self.subplan_builder(left)
            if subplan is None:
                results = []
            else:
                subplan = subplan.optimize(self)
                results = list(subplan.execute())
            if isinstance(left, dict):
                out = dict(left)
                out[self.field_name] = results
                yield out
            else:
                try:
                    yield left._setattr(self.field_name, results)
                except Exception:
                    out = {}
                    if hasattr(left, 'items'):
                        out.update(dict(left))
                    out[self.field_name] = results
                    yield out

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        if self.based_on:
            self.based_on = self.based_on.optimize(full_plan)
        return self


class SelectPlan(QueryPlan):
    """
    A query plan that selects and transforms fields from records produced by another query plan.

    This plan allows for both direct field mapping (renaming fields) and dynamic field
    generation through callable functions. It's useful for projecting and transforming
    data in a query pipeline.
    """

    def __init__(self, fields: dict, based_on: QueryPlan = None, transaction: 'ObjectTransaction' = None,
                 atom_pointer: AtomPointer = None, **kwargs):
        """
        Initialize a SelectPlan with field mappings and a base query plan.

        Args:
            fields: A dictionary mapping output field names to either:
                   - Source field names (strings) for direct mapping
                   - Callable functions that take a record and return a value
            based_on: The query plan that produces the records to transform
            transaction: The transaction context
            atom_pointer: Pointer to the atom in storage
            **kwargs: Additional arguments
        """
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.fields = fields

    def execute(self):
        """
        Execute the plan, transforming records from the base plan according to field mappings.

        For each record from the base plan, this method creates a new record with fields
        mapped according to the field specification provided at initialization.

        Yields:
            Transformed records with the specified field mappings applied.

        Raises:
            Any exception that might be raised by callable field mappings.
        """
        if not self.based_on:
            return

        for record in self.based_on.execute():
            result = {}

            for output_field, source_spec in self.fields.items():
                if callable(source_spec):
                    # Dynamic field generation through a callable
                    # Let any exceptions propagate up to the caller
                    result[output_field] = source_spec(record)
                else:
                    # Direct field mapping
                    # Only include the field if it exists in the source record
                    value = record.get(source_spec)
                    if value is not None:
                        result[output_field] = value

            yield result

    def optimize(self, full_plan: 'QueryPlan'):
        """
        Optimize this plan and its dependencies.

        This method delegates optimization to the underlying plan and then
        creates a new SelectPlan with the optimized base.

        Args:
            full_plan: The complete query plan for context

        Returns:
            An optimized version of this plan
        """
        if not self.based_on:
            return self

        optimized_base = self.based_on.optimize(full_plan)

        if optimized_base is self.based_on:
            return self

        return SelectPlan(
            fields=self.fields,
            based_on=optimized_base,
            transaction=self.transaction
        )


class CountPlan(QueryPlan):
    """
    A query plan that counts the results from a sub-plan.
    This plan is optimized to use index counts whenever possible,
    avoiding full data iteration.
    """

    def __init__(self, based_on: 'QueryPlan', transaction: 'ObjectTransaction'):
        """
        Initializes the CountPlan.

        Args:
            based_on: The underlying plan whose results will be counted.
            transaction: The active transaction.
        """
        super().__init__(based_on=based_on, transaction=transaction)
        self.alias = 'count'

    def execute(self) -> list[dict]:
        """
        Executes the count. If no optimization is possible,
        it iterates through the sub-plan's results and counts them.

        Returns:
            A list containing a single dictionary with the count, e.g., [{'count': 123}].
        """
        count = sum(1 for _ in self.based_on.execute())
        return [{'count': count}]

    def optimize(self, full_plan: 'QueryPlan') -> 'QueryPlan':
        """
        Optimizes the counting process.

        If the underlying plan can provide a count efficiently (e.g., it's an
        indexed search), this method will delegate the counting to it.
        Otherwise, it returns itself to perform a standard iteration count.

        Returns:
            A plan that can provide the count, potentially a new optimized plan
            or itself.
        """
        optimized_based_on = self.based_on.optimize(full_plan)

        # Duck-typing: Check if the optimized underlying plan has a fast `count` method.
        # Only delegate if the method is overridden (not the default QueryPlan.count).
        if hasattr(optimized_based_on, 'count'):
            try:
                # Special-case: don't delegate for plain ListPlan (no fast path expected)
                from .queries import ListPlan as _ListPlan
                if isinstance(optimized_based_on, _ListPlan):
                    raise Exception('Use default path for ListPlan')
                # Otherwise, delegate if a count method is present
                return CountResultPlan(count_value=optimized_based_on.count(), transaction=self.transaction)
            except Exception:
                # Fall through to default behavior
                pass

        self.based_on = optimized_based_on
        return self


class ArrayAgg(AgreggatorFunction):
    """
    Aggregator that collects values into a list (array aggregation).
    """
    def compute(self, values: list) -> list:
        return list(values)


class CountResultPlan(QueryPlan):
    """
    A terminal plan that simply holds and returns a pre-calculated count.
    This is the result of an optimized CountPlan.
    """

    def __init__(self, count_value: int, transaction: 'ObjectTransaction'):
        super().__init__(based_on=None, transaction=transaction)
        self.count_value = count_value
        self.alias = 'count'

    def execute(self) -> list[dict]:
        return [{'count': self.count_value}]

    def optimize(self, full_plan: 'QueryPlan') -> 'QueryPlan':
        return self
