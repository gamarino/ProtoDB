from __future__ import annotations

import uuid
from selectors import SelectSelector
from typing import cast

from abc import ABC, abstractmethod
from collections import defaultdict

from . import DBCollections
from .exceptions import ProtoUnexpectedException, ProtoValidationException, ProtoCorruptionException
from .common import Atom, QueryPlan, AtomPointer, DBObject
from .db_access import ObjectTransaction
from .lists import List
from .dictionaries import RepeatedKeysDictionary, Dictionary, DictionaryItem
from .sets import Set
import os
import concurrent.futures


# Executor threads for async operations
max_workers = (os.cpu_count() or 1) * 5
executor_pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

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
                # It should be a plain term
                term_def = expression[local_index]
                local_index += 1
                if len(term_def) < 2:
                    raise ProtoValidationException(
                        message=f'Invalid term definition: {term_def}. It should contain at least two operands!'
                    )
                operand = Operator.get_operator(term_def[1])
                if len(term_def) != operand.parameter_count + 1:
                    raise ProtoValidationException(
                        message=f'Operand {operand} expect at list {operand.parameter_count} parameters!'
                    )
                return Term(term_def[0], operand, term_def[2]), local_index

        default_and_expression = list()
        while index < len(expression):
            new_expression, index = collect_expression(index)
            default_and_expression.append(new_expression)

        if len(default_and_expression) >= 2:
            return AndExpression(default_and_expression)
        else:
            return default_and_expression[0]

    def filter_by_alias(self, alias: str):
        if isinstance(self, Term):
            atribute_alias = self.target_attribute.split('.', maxsplit=1)[0]
            if atribute_alias == alias:
                return self
            else:
                return TrueTerm()
        elif isinstance(self, AndExpression):
            new_operands: list[Expression] = list()
            for operand in self.terms:
                new_operands.append(operand.filter_by_alias(alias))
            return AndExpression(new_operands)
        elif isinstance(self, OrExpression):
            return TrueTerm
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
        return not bool(value)


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
        return self.operation.match(
            getattr(record, self.target_attribute),
            self.value
        )


class TrueTerm(Expression):
    def match(self, record):
        return True


class FalseTerm(Expression):
    def match(self, record):
        return True


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
            item = cast(DictionaryItem, self.indexes.get_at(index))
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
            item = cast(DictionaryItem, self.indexes.get_at(index))
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
            accumulators[i] = set([record for record in self.and_queries])

        if len(self.queries) == 1:
            return accumulators[0]
        else:
            for record in accumulators[0]:
                found = True
                for index in range(1, len(accumulators) - 1):
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
            for record in query:
                yield record

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        return OrMerge(
            or_queries=self.or_queries,
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )


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
        super().__init__(indexes=indexes, based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)
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
                    if not field_name.starswith('_') and not callable(value):
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
                yield from record

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        based_on = self.based_on.optimize(full_plan)
        if isinstance(self.based_on, IndexedQueryPlan):
            # Base QueryPlan has indexes! try to use them
            indexed_base = cast(IndexedQueryPlan, self.based_on)
            indexed_fields = [field_name for field_name, index in indexed_base.indexes]
            used_fields = set()
            if isinstance(self.filter, AndExpression):
                and_filter = cast(AndExpression, self.filter)
                for and_term in and_filter.terms:
                    if isinstance(and_term, Term):
                        term = cast(Term, and_term)
                        used_fields.add(term)

                added_filters = []
                for term in used_fields:
                    target_attribute = term.target_attribute
                    operation = term.operation
                    value = cast(str, term.value)
                    if target_attribute in indexed_base.indexes:
                        added_filters.append(IndexedSearchPlan(
                            based_on=based_on,
                            field_to_scan=target_attribute,
                            operator=operation,
                            value=value,
                            transaction=self.transaction
                        ))

                if len(added_filters) == 1:
                    based_on = added_filters[0]
                elif len(added_filters) > 1:
                    based_on = AndMerge(added_filters)

            elif isinstance(self.filter, OrExpression):
                and_filter = cast(AndExpression, self.filter)
                for and_term in and_filter.terms:
                    if isinstance(and_term, Term):
                        term = cast(Term, and_term)
                        used_fields.add(term)

                added_filters = []
                for term in used_fields:
                    target_attribute = term.target_attribute
                    operation = term.operation
                    value = cast(str, term.value)
                    if target_attribute in indexed_base.indexes:
                        added_filters.append(IndexedSearchPlan(
                            based_on=based_on,
                            field_to_scan=target_attribute,
                            operator=operation,
                            value=value,
                            transaction=self.transaction
                        ))

                if len(added_filters) == 1:
                    based_on = added_filters[0]
                elif len(added_filters) > 1:
                    based_on = OrMerge(added_filters)

            return WherePlan(
            filter=self.filter,
            based_on=based_on,
            transaction=self.transaction
        )


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
    def compute(self, values: list):
        maximun = None
        for value in values:
            if maximun is None or value > maximun:
                maximun = value
        return maximun


class AgreggatorSpec:
    agreggator_function: AgreggatorFunction
    field_name: str
    alias: str

    def __init__(self, agreggator_function: AgreggatorFunction, field_name: str, alias: str):
        self.agreggator_function = agreggator_function
        self.field_name = field_name
        self.alias = alias

    def compute(self, values: list):
        return self.agreggator_function.compute(values)


class GroupByPlan(QueryPlan):
    """
    Query plan for grouping records and applying aggregate functions.

    This plan groups records based on the specified `group_fields`, then applies
    aggregate functions (`agreggated_fields`) to each group to compute summary
    statistics or values.

    Attributes:
    - group_fields (list): Fields used for grouping records.
    - agreggated_fields (dict[str, AgreggatorSpec]): Aggregators applied to grouped data.
    """

    group_fields: list
    agreggated_fields: dict[str, AgreggatorSpec] = dict()

    def __init__(self,
                 group_fields: list = None,
                 agreggated_fields: dict[str, AgreggatorSpec] = None,
                 based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        if not group_fields:
            raise ProtoValidationException(
                message=f"It's not possible to group by without a list of field names to use!"
            )
        self.group_fields = group_fields
        self.agreggated_fields = agreggated_fields

    def execute(self) -> list:
        """
        Group records by `group_fields` and compute aggregates.

        Records from the underlying `based_on` query are grouped in memory,
        and for each group, aggregate functions are applied as specified in
        `agreggated_fields`.

        :return: A generator yielding grouped and aggregated records.
        :rtype: list
        """
        grouped_data = defaultdict(list)

        # Agrupar directamente por campos
        for record in self.based_on.execute():
            key = tuple(record.get(field, None) for field in self.group_fields)
            grouped_data[key].append(record)

        # Procesar cada grupo
        for key, rows in grouped_data.items():
            result = dict(zip(self.group_fields, key))
            for alias, spec in self.agreggated_fields.items():
                values = [row.get(spec.field_name, 0) for row in rows]
                result[alias] = spec.compute(values)
            result_object = DBObject(transaction=self.transaction)
            for field_name, value in result.items():
                if not field_name.startswith('_') and not callable(value):
                    result_object = result_object._setattr(field_name, value)
            yield result_object

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        return GroupByPlan(
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )


class SelectPlan(QueryPlan):
    """
    Class that represents a plan for selecting specific fields from a query result.

    This class is used to generate a new set of records by applying field selection
    rules to an underlying `QueryPlan`. It allows specifying which fields to include
    and how they are derived from the original query results. Fields can be specified
    as direct attribute names from the original result or as callables to generate
    values dynamically.

    :ivar fields: Mapping of field names to values or callables defining how the
        field should be derived from the original query results.
    :type fields: dict[str, str | callable]
    """

    fields: dict[str, str | callable]

    def __init__(self,
                 fields: dict[str, str | callable] = None,
                 based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)

        if not fields:
            self.fields = dict()
        else:
            self.fields = fields

    def execute(self) -> list:
        """
        Executes a transformation process over records fetched from the underlying
        source, applies field mappings to each record, and outputs the transformed
        results as a generator.

        The function processes records provided by `self.based_on.execute()`, applies
        field transformations defined in `self.fields`, and constructs new objects
        with the resultant field values. Each record is either derived through string
        attribute access or callable functions provided in the field mapping.

        :param self: The class instance containing the input source `based_on` and
            mapping definitions `fields`. The `fields` attribute defines which fields
            are processed and how transformations are applied.

        :return: Generator producing transformed objects created from the input
            records, enriched with fields mapped as per `self.fields`.

        :rtype: Generator[object, None, None]
        """
        for record in self.based_on.execute():
            result = DBObject(transaction=self.transaction)
            for field_name, value in self.fields.items():
                if isinstance(value, str):
                    dotted_fields = value.split('.')
                    value = record
                    for path_component in dotted_fields:
                        value = value[path_component]
                    value = getattr(record, value)
                elif callable(value):
                    value = value(record)
                else:
                    continue
                result = DBObject._setattr(result, field_name, value)
            yield result

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        return SelectPlan(
            fields=self.fields,
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )


class OrderByPlan(QueryPlan):
    """
    Query plan for ordering records based on specified fields.

    This class consumes records from another query plan (`based_on`), sorts
    those records according to a given sorting specification (`sort_spec`),
    and optionally reverses the order.

    Attributes:
    - sort_spec (list): A list of field names that dictate the sorting priority.
    - reversed (bool): Whether to reverse the result (descending order).
    """

    sort_spec: list
    reversed: bool

    def __init__(self,
                 sort_spec: list,
                 reversed: bool = False,
                 based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.sort_spec = sort_spec
        self.reversed = reversed

    def execute(self) -> list:
        """
        Execute the order-by logic over the input records.

        Records are fetched from the `based_on` query plan, compared using the
        provided `sort_spec`, and sorted into ascending or descending order.

        The sorting uses a binary search to find the insertion point for new records,
        ensuring efficient insertion in sorted order.

        :return: A generator yielding sorted records.
        :rtype: list
        """
        ordered_output = List(transaction=self.transaction)

        def compare(a, b) -> int:
            """
            Compare two records based on the fields in `sort_spec`.

            Fields are evaluated in order of priority. If the two records are
            the same for a field, the next field in the list is used.

            :param a: The first record to compare.
            :param b: The second record to compare.
            :return: -1 if `a < b`, 1 if `a > b`, 0 if they are equal.
            :rtype: int
            """
            cmp = 0
            for field_name in self.sort_spec:
                cmp = 0 if a[field_name] == b[field_name] else \
                    1 if a[field_name] > b[field_name] else -1
                if cmp != 0:
                    break
            return cmp

        ordered_output = List(transaction=self.transaction)
        
        for record in self.based_on.execute():
            if ordered_output.count == 0:
                ordered_output = ordered_output.append(record)
            else:
                comparison = compare(record, ordered_output.get_at(-1))
                if comparison >= 0:
                    ordered_output = ordered_output.append(record)
                else:
                    # Find the right place to insert the new record
                    left = 0
                    right = ordered_output.count - 1
                    center = 0

                    while left <= right:
                        center = (left + right) // 2

                        item = ordered_output.get_at(center)
                        comparison = compare(record, item)
                        if comparison >= 0:
                            right = center - 1
                        else:
                            left = center + 1
                    
                    ordered_output = ordered_output.insert_at(center, record)
                    
        # At this point, all input has been ingested and ordered_output has an ascending order list
        if self.reversed:
            index = ordered_output.count
            while index > 0:
                index -= 1
                yield ordered_output.get_at(index)
        else:
            for item in ordered_output.as_iterable():
                yield item

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        return OrderByPlan(
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )


class LimitPlan(QueryPlan):
    """
    Represents a query plan that imposes a limit on the number of records to process.

    The LimitPlan class is used to define a query plan that enforces a constraint on
    the maximum number of records retrieved or processed from its associated query
    plan. This is achieved using a counter that tracks the number of records yielded
    and stops subsequent processing once the limit is reached. Additionally, the
    class provides optimization functionalities by enabling modifications of the
    underlying query plan while retaining the limit constraint. It is initialized
    with a required limit count and optional parameters to define its base plan and
    transaction context.

    :ivar limit_count: The maximum number of records to process.
    :type limit_count: int
    """

    limit_count: int

    def __init__(self,
                 limit_count: int = 0,
                 based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)

        if limit_count == 0:
            raise ProtoValidationException(
                message=f'Invalid limit count!'
            )
        self.limit_count = limit_count

    def execute(self) -> list:
        """
        Executes a generator function to fetch records from an underlying data source
        while imposing a limit on the number of records processed. This function
        iterates over a generator from the base execution and yields records until
        the specified limit is reached.

        :param self: Instance of the class containing execution logic.
        :return: A generator yielding records with a hard limit on the count, if
            applicable.
        :rtype: generator
        """
        count = 0
        for record in self.based_on.execute():
            count += 1
            if count > self.limit_count:
                break
            yield from record

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        return LimitPlan(
            limit_count=self.limit_count,
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )


class OffsetPlan(QueryPlan):
    """
    Manage query execution with an offset, bypassing a specified number of initial records.

    This class extends the QueryPlan and introduces an offset mechanism to skip the initial
    records while executing the query plan. It leverages the functionality of the parent
    QueryPlan class while adding additional capabilities related to offset-based query
    processing. This class is commonly used in cases where paginated or incremental data
    retrieval is necessary.

    :ivar offset: Number of records to skip in the result set.
    :type offset: int
    """
    offset: int

    def __init__(self,
                 offset: int = 0,
                 based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        if offset < 0:
            raise ProtoValidationException(
                message=f'Invalid offset count!'
            )
        self.offset = offset

    def execute(self) -> list:
        """
        Executes the underlying data retrieval process while skipping a specified number of
        initial results. This method is useful in scenarios where data pagination or result
        offsetting is required. The retrieved records will be lazily returned using a generator,
        allowing for memory-efficient processing of potentially large datasets.

        :raises AttributeError: If ``self.based_on`` does not have an `execute` method.
        :param self: The instance on which the method is invoked.
        :type self: Any
        :param self.offset: The number of initial records to skip before yielding results.
        :type self.offset: int

        :returns: A generator of records from the underlying data source, starting from the
            ``self.offset`` index.
        :rtype: Generator
        """
        current_count = 0
        for record in self.based_on.execute():
            current_count += 1
            if current_count > self.offset:
                yield from record

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        return OffsetPlan(
            offset=self.offset,
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )


class JoinPlan(QueryPlan):
    """
    JoinPlan class facilitates the execution and optimization of a structured join
    operation, supporting multiple join types including external, left, right,
    inner, and outer join types. It extends QueryPlan and is integral in handling
    complex join queries in a data processing pipeline. Validation is performed
    to ensure appropriate join query and join type are provided.

    The class's purpose is to encapsulate the logic for various join operations
    while allowing optimization of the underlying query plan. Usage entails
    instantiating the class with the necessary parameters and calling the execute
    or optimize methods for respective operations.

    :ivar join_query: The query plan to be used for the join operation.
    :type join_query: QueryPlan
    :ivar join_type: The type of join operation to perform. Valid types include
        'external', 'left', 'right', 'inner', 'external_left', 'external_right',
        and 'outer'.
    :type join_type: str
    """
    join_query: QueryPlan
    join_type: str

    def __init__(self,
                 join_query: QueryPlan = None,
                 join_type: str = None,
                 based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        if not join_query:
            raise ProtoValidationException(
                message=f'Missing join query!'
            )
        self.join_query = join_query

        if join_type and join_type in \
                ('external', 'left', 'right', 'inner',
                 'external_left', 'external_right', 'outer'):
            self.join_type = join_type
        else:
            raise ProtoValidationException(
                message=f'Invalid join type {join_type}!'
            )

    def execute(self) -> list:
        """
        Executes the join operation based on the specified join type and returns a list of
        results corresponding to the joined records.

        The joined operation type can be one of the following:
            - 'external'
            - 'external_left'
            - 'external_right'
            - 'left'
            - 'right'
            - 'outer'
            - 'inner'

        Records are iteratively processed and combined, depending on the join type provided.

        :param self: Instance of the class performing the join operation.
        :return: A list containing the combined records resulting from the join operation.
        :rtype: list
        """
        if self.join_type in ('external', 'external_left', 'left', 'outer'):
            for base_record in self.based_on.execute():
                yield from base_record

        if self.join_type in ('external', 'inner', 'right', 'left'):
            for base_record in self.based_on.execute():
                for join_record in self.join_query.execute():
                    result = DBObject(transaction=self.transaction)
                    for field_name, value in base_record.__dict__.items():
                        result = result._setattr(result, field_name, value)
                    for field_name, value in join_record.__dict__.items():
                        result = result._setattr(result, field_name, value)
                    yield result

        if self.join_type in ('external', 'external_right', 'right', 'outer'):
            for join_record in self.join_query.execute():
                yield from join_record

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        previous_query = full_plan
        while previous_query and previous_query.based_on != self:
            previous_query = previous_query.based_on

        based_on = self.based_on.optimize(full_plan)
        join_query = self.join_query.optimize(full_plan)
        if isinstance(previous_query, WherePlan):
            based_on = WherePlan(
                filter = previous_query.filter,
                based_on = self.based_on,
                transaction=self.transaction
            )
            join_query = WherePlan(
                filter = previous_query.filter,
                based_on = self.join_query,
                transaction=self.transaction
            )

        return JoinPlan(
            join_query=join_query,
            join_type=self.join_type,
            based_on=based_on,
            transaction=self.transaction
        )


class UnionPlan(QueryPlan):
    """
    Represents a plan that combines the results of two query plans using the union operation.

    This class facilitates combining the results of one query plan with those of
    another, effectively executing a union operation. It ensures that both query
    plans are valid and provides efficient execution and optimization mechanisms.
    The `UnionPlan` inherits from `QueryPlan` and extends its functionality
    to incorporate union-based operations.

    :ivar union_query: The query plan to be merged with the base query plan.
    :type union_query: QueryPlan
    """
    union_query: QueryPlan

    def __init__(self,
                 union_query: QueryPlan = None,
                 based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)

        if not union_query:
            raise ProtoValidationException(
                message=f'Missing union query!'
            )
        self.union_query = union_query

    def execute(self) -> list:
        for base_record in self.based_on.execute():
            yield from base_record

        for union_record in self.union_query.execute():
            yield from union_record

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        return UnionPlan(
            union_query=self.union_query.optimize(full_plan),
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )

