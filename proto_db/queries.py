from __future__ import annotations

import uuid
from selectors import SelectSelector
from typing import cast

from abc import ABC, abstractmethod
from collections import defaultdict

from . import DBCollections
from .exceptions import ProtoUnexpectedException, ProtoValidationException, ProtoCorruptionException
from .common import Atom, QueryPlan, AtomPointer
from .db_access import ObjectTransaction
from .lists import List
import os
import concurrent.futures


# Executor threads for async operations
max_workers = (os.cpu_count() or 1) * 5
executor_pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

class Expression:
    @staticmethod
    def compile(expression: list) -> Expression:
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

class AndExpression(Expression):
    terms: list[Expression]

    def __init__(self, terms: list[Expression]):
        self.terms = terms


class OrExpression(Expression):
    terms: list[Expression]

    def __init__(self, terms: list[Expression]):
        self.terms = terms


class NotExpression(Expression):
    negated_expression: Expression

    def __init__(self, negated_expression: Expression):
        self.negated_expression = negated_expression


class Operator:
    parameter_count: int = 2

    @staticmethod
    def get_operator(string: str):
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


class Equal(Operator):
    parameter_count: int = 2


class NotEqual(Operator):
    parameter_count: int = 2


class Greater(Operator):
    parameter_count: int = 2


class GreaterOrEqual(Operator):
    parameter_count: int = 2


class Lower(Operator):
    parameter_count: int = 2


class LowerOrEqual(Operator):
    parameter_count: int = 2


class Contains(Operator):
    parameter_count: int = 2


class In(Operator):
    parameter_count: int = 2


class IsTrue(Operator):
    parameter_count: int = 1


class NotTrue(Operator):
    parameter_count: int = 1


class IsNone(Operator):
    parameter_count: int = 1


class NotNone(Operator):
    parameter_count: int = 1


class Term(Expression):
    target_attribute: str
    operation: Operator
    value: object

    def __init__(self, target_attribute: str, operation: Operator, value: object):
        self.target_attribute = target_attribute
        self.operation = operation
        self.value = value

class TrueTerm(Expression):
    pass


class FalseTerm(Expression):
    pass


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


class FromPlan(QueryPlan):
    """

    """
    alias: str

    def __init__(self,
                 alias: str,
                 based_on: QueryPlan,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.alias = alias

    def execute(self) -> list:
        for item in self.based_on.execute():
            yield from item

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        return FromPlan(
            alias=self.alias,
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )


class WherePlan(QueryPlan):
    """

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
        if filter_spec:
            self.filter = Expression.compile(filter_spec)
        else:
            self.filter = filter

    def execute(self) -> list:
        # TODO
        pass

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        return WherePlan(
            filter=self.filter,
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )


class AgreggatorFunction(ABC):
    @abstractmethod
    def compute(self, values: list):
        """
        Compute the agreggotor over a set of records
        :param records: 
        :return: 
        """

class SumAgreggator(AgreggatorFunction):
    def compute(self, values: list):
        sum = 0.0
        for value in values:
            sum += value
        return sum


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
            yield result

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        return GroupByPlan(
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )


class SelectPlan(QueryPlan):
    """

    """
    # TODO

    def __init__(self,
                 based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)

    def execute(self) -> list:
        # TODO
        pass

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        # TODO
        return SelectPlan(
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )



class OrderByPlan(QueryPlan):
    """

    """
    sort_spec: list
    reversed: bool
    
    # TODO
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
        ordered_output = List(transaction=self.transaction)
        
        def compare(a, b) -> int:
            cmp = 0
            for field_name in self.sort_spec:
                cmp = 0 if a[field_name] == b[field_name] else \
                      1 if a[field_name] > b[field_name] else -1
                if cmp != 0:
                    break
            return cmp

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
        # TODO
        return OrderByPlan(
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )


class HavingPlan(QueryPlan):
    """

    """
    # TODO
    def __init__(self,
                 based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)

    def execute(self) -> list:
        # TODO
        pass

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        # TODO
        return HavingPlan(
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )


class LimitPlan(QueryPlan):
    """

    """
    # TODO

    def __init__(self,
                 based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)

    def execute(self) -> list:
        # TODO
        pass

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        # TODO
        return LimitPlan(
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )


class OffsetPlan(QueryPlan):
    """

    """
    # TODO

    def __init__(self,
                 based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)

    def execute(self) -> list:
        # TODO
        pass

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        # TODO
        return OffsetPlan(
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )


class ExternalJoinPlan(QueryPlan):
    """

    """
    # TODO

    def __init__(self,
                 based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)

    def execute(self) -> list:
        # TODO
        pass

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        # TODO
        return ExternalJoinPlan(
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )


class JoinPlan(QueryPlan):
    """

    """
    # TODO

    def __init__(self,
                 based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)

    def execute(self) -> list:
        # TODO
        pass

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        # TODO
        return JoinPlan(
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )



class LeftPlan(QueryPlan):
    """

    """
    # TODO

    def __init__(self,
                 based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)

    def execute(self) -> list:
        # TODO
        pass

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        # TODO
        return LeftPlan(
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )


class RightPlan(QueryPlan):
    """

    """
    # TODO

    def __init__(self,
                 based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)

    def execute(self) -> list:
        # TODO
        pass

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        # TODO
        return RightPlan(
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )


class UnionPlan(QueryPlan):
    """

    """
    # TODO

    def __init__(self,
                 based_on: QueryPlan = None,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(based_on=based_on, transaction=transaction, atom_pointer=atom_pointer, **kwargs)

    def execute(self) -> list:
        # TODO
        pass

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        # TODO
        return UnionPlan(
            based_on=self.based_on.optimize(full_plan),
            transaction=self.transaction
        )

