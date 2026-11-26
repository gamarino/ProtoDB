"""Runs the IndexedCollection contract over every registered collection factory."""
import pytest

from tests.contracts.collection_contract import (check_index_management, check_mutator,
                                                 check_intentions, check_mutator_sequence, factory_cases,
                                                 intention_cases, mutator_cases)


@pytest.mark.parametrize("factory, mutator_name, seed", mutator_cases())
def test_mutator_contract(factory, mutator_name, seed):
    check_mutator(factory, mutator_name, seed)


@pytest.mark.parametrize("factory, seed", factory_cases())
def test_index_management_contract(factory, seed):
    check_index_management(factory, seed)


@pytest.mark.parametrize("factory, seed", factory_cases())
def test_mutator_sequence_contract(factory, seed):
    check_mutator_sequence(factory, seed)


@pytest.mark.parametrize("factory, seed", intention_cases())
def test_intention_contract(factory, seed):
    check_intentions(factory, seed)
