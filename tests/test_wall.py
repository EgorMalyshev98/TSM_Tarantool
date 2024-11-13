import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from src.tarantool.services.plan.operations import OperationSelector


@pytest.fixture()
def input_df():
    input_data = [
        [1, 1, 10, 40, 100],
        [2, 2, 10, 50, 100],
        [3, 3, 10, 15, 100],
        [3, 4, 15, 40, 100],
        [4, 5, 10, 20, 100],
        [4, 6, 20, 40, 100],
    ]

    input_cols = ["hierarchy", "id", "start_p", "finish_p", "volume_p"]
    return pd.DataFrame(input_data, columns=input_cols)


@pytest.fixture()
def expected_df():
    expected_data = [
        [0, 1, 1, 10, 40, 100],
        [1, 2, 2, 10, 40, 75],
        [2, 3, 3, 10, 15, 100],
        [3, 3, 4, 15, 40, 100],
        [4, 4, 5, 10, 20, 100],
        [5, 4, 6, 20, 40, 100],
        [6, 1, 2, 40, 50, 25],
    ]

    expected_cols = ["sort_key", "hierarchy", "id", "start_p", "finish_p", "volume_p"]
    return pd.DataFrame(expected_data, columns=expected_cols)


def test_sort_by_technology(input_df, expected_df):
    result_input_df = OperationSelector._sort_by_technology(input_df)  # noqa: SLF001

    assert_frame_equal(result_input_df, expected_df, check_dtype=False)
