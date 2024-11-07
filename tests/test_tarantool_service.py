import pytest

from src.tarantool.services.operations import OperationSelector
from tests.dev_data_source import data


@pytest.fixture
def operation_selector():
    return OperationSelector(prd=data.prd, fact=data.fact, contract=data.contract, hierarchy=data.hierarchy)


def test_select(operation_selector: OperationSelector):
    input_start = 33
    input_finish = 59
    return operation_selector.select(input_start, input_finish)


if __name__ == "__main__":
    selector = OperationSelector(prd=data.prd, fact=data.fact, contract=data.contract, hierarchy=data.hierarchy)
    print(test_select(selector))
