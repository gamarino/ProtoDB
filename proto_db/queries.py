from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import cast

from .common import Atom, QueryPlan, AtomPointer, DBObject
from .db_access import ObjectTransaction
from .dictionaries import RepeatedKeysDictionary, Dictionary, DictionaryItem
from .exceptions import ProtoValidationException
from .hybrid_executor import HybridExecutor
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
        Compile a nested list of expressions into an `Expression` object.

        The method parses a structured list, where operators (`!`, `&`, and `|`)
        and terms are used to build a tree of expressions (e.g., AndExpression,
        OrExpression, etc.). These compiled objects can then be used to evaluate
        whether a record satisfies the given conditions.

        :param expression: A nested list representing logical operations and terms.
        :type expression: list
        :return: A fully compiled `Expression` instance.
        :rtype: Expression
        """
        index = 0

        def collect_expression(local_index: int) -> tuple[Expression, int]:
            if expression[local_index] == '!':
                local_index += 1
                following_expression, local_index = collect_expression(local_index)
                return NotExpression(following_expression), local_index
            elif expression[local_index] == '&':
                local_index += 1
                first_operand, local_index = collect_expression(local_index)
                second_operand, local_index = collect_expression(local_index)
                return AndExpression([first_operand, second_operand]), local_index
            elif expression[local_index] == '|':
                local_index += 1
                first_operand, local_index = collect_expression(local_index)
                second_operand, local_index = collect_expression(local_index)
                return OrExpression([first_operand, second_operand]), local_index
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
                else:
                    try:
                        value = expression[local_index + 2]
                    except Exception:
                        raise ProtoValidationException(message=f'Operand {op_token} expects binary term [attr, op, value]')
                    local_index += 3
                    return Term(attr, operand, value), local_index

        default_and_expression = list()
        while index < len(expression):
            new_expression, index = collect_expression(index)
            default_and_expression.append(new_expression)

        if len(default_and_expression) >= 2:
            return AndExpression(default_and_expression)
        else:
            return default_and_expression[0]

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
    indexes: Dictionary

    def __init__(
            self,
            indexes: Dictionary = None,
            based_on: QueryPlan = None,
            atom_pointer: AtomPointer = None,
            transaction: ObjectTransaction = None,
            **kwargs):
        super().__init__(based_on=based_on, atom_pointer=atom_pointer, transaction=transaction, **kwargs)
        self.indexes = indexes if indexes else Dictionary(transaction=self.transaction)

    def execute(self) -> list:
        return super().execute()

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
        new_index = RepeatedKeysDictionary(transaction=self.transaction)
        for record in self.execute():
            if record.has(field_name):
                new_index = new_index.set_at(record[field_name], record)

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
            index = cast(RepeatedKeysDictionary, index)
            if removed_record[field_name]:
                new_indexes.set_at(field_name, index.remove_record_at(removed_record[field_name], removed_record))

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
                new_indexes.set_at(field_name, index.set_at(added_record[field_name], added_record))

        return IndexedQueryPlan(
            indexes=new_indexes,
            based_on=self.based_on,
            transaction=self.transaction
        )

    def position_at(self, field_name: str, value) -> int:
        self._load()

        if field_name in self.indexes:
            index = cast(Dictionary, self.indexes[field_name])

            left = 0
            right = index.content.count - 1

            while left <= right:
                center = (left + right) // 2

                item = cast(DictionaryItem, index.content.get_at(center))
                if item and str(item.key) == value:
                    return center

                if str(item.key) > value:
                    right = center - 1
                else:
                    left = center + 1

            return left
        else:
            raise ProtoValidationException(
                message=f'No index on field {field_name}!'
            )

    def yield_from_index(self, field_name: str, index: int) -> list:
        while index < self.indexes[field_name].count:
            item = cast(DictionaryItem, self.indexes[field_name].get_at(index))
            index += 1
            value_set = cast(Set, item.value)
            for record in value_set.as_iterable():
                yield record

    def get_greater_than(self, field_name: str, value: object) -> list:
        index = self.position_at(field_name, value)
        item = cast(DictionaryItem, self.indexes[field_name].get_at(index))
        if item.key == value:
            item += 1
        return self.yield_from_index(field_name, index)

    def get_greater_or_equal_than(self, field_name: str, value: object) -> list:
        index = self.position_at(field_name, value)
        return self.yield_from_index(field_name, index)

    def get_equal_than(self, field_name: str, value: object) -> list:
        if field_name in self.indexes:
            index = cast(Dictionary, self.indexes[field_name])
            if index is None:
                return []

            item = cast(DictionaryItem, index.get_at(value))
            value_set = cast(Set, item.value)
            for record in value_set.as_iterable():
                yield record
        else:
            raise ProtoValidationException(
                message=f'No index on field {field_name}!'
            )

    def yield_up_to_index(self, field_name: str, index_up_to: int) -> list:
        index = 0
        while index < self.indexes[field_name].count and index < index_up_to:
            item = cast(DictionaryItem, self.indexes[field_name].get_at(index))
            index += 1
            value_set = cast(Set, item.value)
            for record in value_set.as_iterable():
                yield record

    def get_lower_than(self, field_name: str, value: object) -> list:
        index = self.position_at(field_name, value)
        item = cast(DictionaryItem, self.indexes[field_name].get_at(index))
        if item.key == value:
            item -= 1
        return self.yield_up_to_index(field_name, index)

    def get_lower_or_equal_than(self, field_name: str, value: object) -> list:
        index = self.position_at(field_name, value)
        return self.yield_up_to_index(field_name, index)


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
            if isinstance(self.operator, Equal) and self.field_to_scan in self.indexes:
                index = cast(Dictionary, self.indexes[self.field_to_scan])
                item = cast(DictionaryItem, index.get_at(self.value))
                if not item:
                    return 0
                value_set = cast(Set, item.value)
                return value_set.count
        except Exception:
            # On any unexpected shape, fallback to generic count
            pass
        return sum(1 for _ in self.execute())


class AndMerge(QueryPlan):
    and_queries: list[QueryPlan]

    def __init__(self,
                 and_queries: list[QueryPlan] = None,
                 based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.and_queries = and_queries

    def execute(self) -> list:
        result = set()

        accumulators = list()
        for i in range(0, len(self.and_queries)):
            accumulators.append(set([record for record in self.and_queries[i].execute()]))

        if len(self.and_queries) == 1:
            for record in accumulators[0]:
                yield record
        else:
            for record in accumulators[0]:
                found = True
                for index in range(1, len(accumulators)):
                    if record in accumulators[index]:
                        continue
                    else:
                        found = False
                        break
                if found:
                    result.add(record)

            for record in result:
                yield record

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


class OrMerge(QueryPlan):
    or_queries: list[QueryPlan]

    def __init__(self,
                 or_queries: list[QueryPlan] = None,
                 based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.or_queries = or_queries

    def execute(self) -> list:
        for query in self.or_queries:
            for record in query.execute():
                yield record

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        return OrMerge(
            or_queries=self.or_queries,
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )

    def count(self) -> int:
        """
        Calculates the count of the union of sub-queries.
        Prefer fast path with keys_iterator() when available; otherwise fall back to
        executing the plans and counting unique results.
        """
        if not self.or_queries:
            return 0

        optimized_queries = [q.optimize(self) for q in self.or_queries]

        all_ids = set()
        for q in optimized_queries:
            if hasattr(q, 'keys_iterator'):
                try:
                    for k in q.keys_iterator():
                        all_ids.add(k)
                    continue
                except Exception:
                    # Fallback to execute
                    pass
            for rec in q.execute():
                all_ids.add(rec)

        return len(all_ids)


class FromPlan(IndexedQueryPlan):
    """

    """
    alias: str

    def __init__(self,
                 alias: str,
                 indexes: dict[str, RepeatedKeysDictionary],
                 based_on: QueryPlan,
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

        Each record is checked against the `filter` expression. Only records
        that match the filter are yielded.

        :return: A generator yielding filtered records.
        :rtype: list
        """

        for record in self.based_on.execute():
            if self.filter.match(record):
                yield record

    def optimize(self, full_plan: 'QueryPlan') -> 'QueryPlan':
        """
        Optimizes the current execution plan node.

        This method is a core part of the query optimizer. It applies several
        strategies to transform the query plan into a more efficient equivalent.
        The primary optimizations include:
        1.  Filter Reordering: For AND expressions, reorder predicates to execute
            the most selective and least expensive ones first.
        2.  Predicate Pushdown: Attempt to move the filter logic ("predicate")
            further down the execution tree, closer to the data source. This
            reduces the volume of data processed in upstream operators (like Joins).
        3.  Index Utilization: If a predicate can be satisfied using an index,
            transform this plan node into an `IndexedSearchPlan`.

        Args:
            full_plan: The root of the entire query plan, providing context if needed.

        Returns:
            An optimized `QueryPlan`, which may be a different type of node
            (e.g., `IndexedSearchPlan`) or a reconfigured `WherePlan`.
        """
        # First, recursively optimize the source plan upon which this filter operates.
        optimized_based_on = self.based_on.optimize(full_plan)

        # 1. FILTER REORDERING
        # For composite AND expressions, reorder the terms based on a cost/selectivity
        # heuristic. This ensures that cheaper and more selective predicates are
        # evaluated first, potentially short-circuiting the evaluation early.
        current_filter = self.filter
        if isinstance(current_filter, AndExpression):
            current_filter = self._reorder_and_expression(current_filter)

        # 2. PREDICATE PUSHDOWN
        # Attempt to "push" this where clause down to the underlying plan node.
        # If the underlying node has an `accept_filter` method, it means it can
        # integrate the filter more efficiently (e.g., a JoinPlan applying it
        # pre-join, or a FromPlan applying it at the storage access level).
        if hasattr(optimized_based_on, 'accept_filter'):
            # The underlying plan will absorb the filter and return a new, optimized plan.
            # This `WherePlan` node can then be eliminated from the tree.
            return optimized_based_on.accept_filter(current_filter)

        # 3. INDEX UTILIZATION (the original optimization)
        # Check if the filter is a simple equality term that can be served by an index
        # on the underlying `IndexedQueryPlan`.
        if isinstance(current_filter, Term) and isinstance(current_filter.operation, Equal):
            if isinstance(optimized_based_on, IndexedQueryPlan):
                # If the target attribute is indexed, replace this `WherePlan`
                # with a more efficient `IndexedSearchPlan`.
                if current_filter.target_attribute in optimized_based_on.indexes:
                    return IndexedSearchPlan(
                        field_to_scan=current_filter.target_attribute,
                        operator=current_filter.operation,
                        value=current_filter.value,
                        indexes=optimized_based_on.indexes,
                        based_on=optimized_based_on,
                        transaction=self.transaction
                    )

        # If no specific optimization could be applied, return a `WherePlan`
        # with the optimized base and the (potentially reordered) filter.
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


class MaxAggregator(AgreggatorFunction):
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
