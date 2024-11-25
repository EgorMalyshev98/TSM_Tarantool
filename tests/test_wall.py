from pprint import pprint

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from src.tarantool.services.plan.operations import WallBuilder

params = [
    # example 1
    (
        [
            [1, 1, 10, 40, 100, True, False],
            [2, 2, 10, 50, 100, True, False],
            [3, 3, 10, 15, 100, True, False],
            [3, 4, 15, 40, 100, True, False],
            [4, 5, 10, 20, 100, True, False],
            [4, 6, 20, 40, 100, True, False],
        ],
        [
            [0, 1, 1, 10, 40, 100, True, False],
            [1, 2, 2, 10, 40, 75, True, False],
            [2, 3, 3, 10, 15, 100, True, False],
            [3, 3, 4, 15, 40, 100, True, False],
            [4, 4, 5, 10, 20, 100, True, False],
            [5, 4, 6, 20, 40, 100, True, False],
            [6, 1, 2, 40, 50, 25, True, False],
        ],
    ),
    # example 2
    (
        [
            [2, 2, 10, 40, 100, True, False],
            [3, 3, 10, 15, 100, True, False],
            [3, 4, 15, 40, 100, True, False],
            [4, 5, 10, 15, 100, True, False],
            [4, 6, 15, 40, 100, True, False],
            [1, 1, 10, 40, 100, True, False],
        ],
        [
            [0, 1, 1, 10, 40, 100, True, False],
            [1, 2, 2, 10, 40, 100, True, False],
            [2, 3, 3, 10, 15, 100, True, False],
            [3, 4, 5, 10, 15, 100, True, False],
            [4, 3, 4, 15, 40, 100, True, False],
            [5, 4, 6, 15, 40, 100, True, False],
        ],
    ),
    # example 3
    (
        [
            [1, 1, 10, 15, 100, True, False],
            [2, 2, 10, 40, 100, True, False],
            [3, 3, 10, 15, 100, True, False],
            [3, 4, 15, 40, 100, True, False],
            [4, 5, 10, 15, 100, True, False],
            [4, 6, 15, 40, 100, True, False],
        ],
        [
            [0, 1, 1, 10, 15, 100, True, False],
            [1, 2, 2, 10, 15, 16.7, True, False],
            [2, 3, 3, 10, 15, 100, True, False],
            [3, 4, 5, 10, 15, 100, True, False],
            [4, 1, 2, 15, 40, 83.3, True, False],
            [5, 2, 4, 15, 40, 100, True, False],
            [6, 3, 6, 15, 40, 100, True, False],
        ],
    ),
    # example 4
    (
        [
            [2, 1, 10, 60, 100, True, False],
            [3, 2, 10, 15, 100, True, False],
            [3, 3, 40, 60, 100, True, False],
            [4, 4, 10, 60, 100, True, False],
        ],
        [
            [0, 1, 1, 10, 15, 10, True, False],
            [1, 2, 2, 10, 15, 100, True, False],
            [2, 3, 4, 10, 15, 10, True, False],
            [3, 1, 1, 15, 40, 50, True, False],
            [4, 2, 4, 15, 40, 50, True, False],
            [5, 1, 1, 40, 60, 40, True, False],
            [6, 2, 3, 40, 60, 100, True, False],
            [7, 3, 4, 40, 60, 40, True, False],
        ],
    ),
    # example 5
    (
        [
            [1, 1, 10, 50, 100, True, False],
            [2, 2, 10, 40, 100, True, False],
            [3, 3, 10, 30, 100, True, False],
            [4, 4, 10, 20, 100, True, False],
        ],
        [
            [0, 1, 1, 10, 20, 25, True, False],
            [1, 2, 2, 10, 20, 33.3, True, False],
            [2, 3, 3, 10, 20, 50, True, False],
            [3, 4, 4, 10, 20, 100, True, False],
            [4, 1, 1, 20, 30, 25, True, False],
            [5, 2, 2, 20, 30, 33.3, True, False],
            [6, 3, 3, 20, 30, 50, True, False],
            [7, 1, 1, 30, 40, 25, True, False],
            [8, 2, 2, 30, 40, 33.3, True, False],
            [9, 1, 1, 40, 50, 25, True, False],
        ],
    ),
    # example 6
    (
        [
            [1, 1, 10, 30, 100, True, False],
            [1, 2, 40, 60, 100, True, False],
            [2, 3, 10, 20, 100, True, False],
            [2, 4, 20, 33, 100, True, False],
            [2, 5, 33, 50, 100, True, False],
            [2, 6, 50, 60, 100, True, False],
            [3, 7, 10, 42, 100, True, False],
            [3, 8, 50, 60, 100, True, False],
        ],
        [
            [0, 1, 1, 10, 30, 100, True, False],
            [1, 2, 3, 10, 20, 100, True, False],
            [2, 2, 4, 20, 30, 76.9, True, False],
            [3, 3, 7, 10, 30, 62.5, True, False],
            [4, 1, 4, 30, 33, 23.1, True, False],
            [5, 1, 5, 33, 40, 41.2, True, False],
            [6, 2, 7, 30, 40, 31.2, True, False],
            [7, 1, 2, 40, 42, 10, True, False],
            [8, 2, 5, 40, 42, 11.8, True, False],
            [9, 3, 7, 40, 42, 6.2, True, False],
            [10, 1, 2, 42, 50, 40, True, False],
            [11, 2, 5, 42, 50, 47.1, True, False],
            [12, 1, 2, 50, 60, 50, True, False],
            [13, 2, 6, 50, 60, 100, True, False],
            [14, 3, 8, 50, 60, 100, True, False],
        ],
    ),
    # example 7
    (
        [
            [1, 1, 10, 40, 100, True, False],
            [2, 2, 10, 50, 100, True, False],
            [3, 3, 45, 45, 100, True, True],
            [4, 4, 45, 45, 100, True, True],
            [5, 5, 25, 47, 100, False, False],
            [5, 6, 10, 15, 100, True, False],
            [5, 7, 15, 50, 100, True, False],
            [6, 8, 10, 20, 100, True, False],
            [6, 9, 20, 40, 100, True, False],
        ],
        [
            [0, 1, 1, 10, 40, 100.0, True, False],
            [1, 2, 2, 10, 40, 75.0, True, False],
            [2, 3, 6, 10, 15, 100.0, True, False],
            [3, 3, 7, 15, 40, 71.4, True, False],
            [4, 3, 5, 25, 40, 68.2, False, False],
            [5, 4, 8, 10, 20, 100.0, True, False],
            [6, 4, 9, 20, 40, 100.0, True, False],
            [7, 1, 2, 40, 45, 12.5, True, False],
            [8, 4, 7, 40, 45, 14.3, True, False],
            [9, 4, 5, 40, 45, 22.7, False, False],
            [10, 2, 3, 45, 45, 100.0, True, True],
            [11, 3, 4, 45, 45, 100.0, True, True],
            [12, 1, 2, 45, 50, 12.5, True, False],
            [13, 2, 7, 45, 50, 14.3, True, False],
            [14, 2, 5, 45, 47, 9.1, False, False],
        ],
    ),
    # example 8
    (
        [
            [1, 1, 10, 50, 100, True, False],
            [2, 2, 10, 40, 100, True, False],
            [3, 3, 10, 70, 100, False, False],
            [3, 4, 10, 20, 100, True, False],
            [3, 5, 60, 70, 100, True, False],
            [4, 6, 60, 70, 100, True, False],
        ],
        [
            [0, 1, 1, 10, 20, 25, True, False],
            [1, 2, 2, 10, 20, 33.3, True, False],
            [2, 3, 4, 10, 20, 100, True, False],
            [3, 3, 3, 10, 20, 16.7, False, False],
            [4, 1, 1, 20, 40, 50, True, False],
            [5, 2, 2, 20, 40, 66.7, True, False],
            [6, 3, 3, 20, 40, 33.3, False, False],
            [7, 1, 1, 40, 50, 25, True, False],
            [8, 2, 3, 40, 50, 16.7, False, False],
            [9, 1, 3, 50, 60, 16.7, False, False],
            [10, 1, 5, 60, 70, 100, True, False],
            [11, 1, 3, 60, 70, 16.7, False, False],
            [12, 2, 6, 60, 70, 100, True, False],
        ],
    ),
    # example 9
    (
        [
            [1, 1, 10, 50, 100, True, False],
            [2, 2, 10, 40, 100, True, False],
            [3, 3, 10, 70, 100, False, False],
            [3, 4, 10, 20, 100, True, False],
        ],
        [
            [0, 1, 1, 10, 20, 25, True, False],
            [1, 2, 2, 10, 20, 33.3, True, False],
            [2, 3, 4, 10, 20, 100, True, False],
            [3, 3, 3, 10, 20, 16.7, False, False],
            [4, 1, 1, 20, 40, 50, True, False],
            [5, 2, 2, 20, 40, 66.7, True, False],
            [6, 3, 3, 20, 40, 33.3, False, False],
            [7, 1, 1, 40, 50, 25, True, False],
            [8, 2, 3, 40, 50, 16.7, False, False],
            [9, 1, 3, 50, 70, 33.3, False, False],
        ],
    ),
    # example 11
    (
        [
            [1, 1, 0, 60, 100, True, False],
            [2, 2, 0, 25, 100, False, False],
            [2, 3, 25, 70, 100, False, False],
            [2, 4, 70, 100, 100, False, False],
            [2, 5, 50, 100, 100, True, False],
            [2, 6, 100, 120, 100, False, False],
        ],
        [
            [0, 1, 1, 0, 50, 83.3, True, False],
            [1, 2, 2, 0, 25, 100, False, False],
            [2, 2, 3, 25, 50, 55.6, False, False],
            [3, 1, 1, 50, 60, 16.7, True, False],
            [4, 2, 5, 50, 60, 20, True, False],
            [5, 2, 3, 50, 60, 22.2, False, False],
            [6, 1, 5, 60, 100, 80, True, False],
            [7, 1, 3, 60, 70, 22.2, False, False],
            [8, 1, 4, 70, 100, 100, False, False],
            [9, 1, 6, 100, 120, 100, False, False],
        ],
    ),
]

DTYPES = {
    "finish_p": float,
    "id": int,
    "is_key_oper": bool,
    "is_point_object": bool,
    "level": int,
    "start_p": float,
    "volume_p": float,
}


@pytest.mark.parametrize("input_data, expected_data", params)
def test_sort_by_technology(input_data, expected_data):
    input_cols = ["level", "id", "start_p", "finish_p", "volume_p", "is_key_oper", "is_point_object"]
    input_df = pd.DataFrame(input_data, columns=input_cols).astype(DTYPES)

    result_input_df = WallBuilder.set_works_sequence(input_df)
    # округляем значения столбца volume_p до десятых
    result_input_df.loc[:, "volume_p"] = result_input_df.volume_p.round(1)

    expected_cols = ["sort_key", "level", "id", "start_p", "finish_p", "volume_p", "is_key_oper", "is_point_object"]
    expected_df = pd.DataFrame(expected_data, columns=expected_cols)

    assert_frame_equal(result_input_df, expected_df, check_dtype=False)


if __name__ == "__main__":
    input_df = [
        [1, 1, 10, 50, 100, True, False],
        [2, 2, 10, 40, 100, True, False],
        [3, 3, 10, 70, 100, False, False],
        [3, 4, 10, 20, 100, True, False],
    ]

    input_cols = ["level", "id", "start_p", "finish_p", "volume_p", "is_key_oper", "is_point_object"]
    in_df = pd.DataFrame(input_df, columns=input_cols)

    out_df = result_input_df = WallBuilder.set_works_sequence(in_df)
    # out_df.loc[:, "volume_p"] = out_df.volume_p.round(1)

    pprint(out_df.to_numpy().tolist())
