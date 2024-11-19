import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from src.tarantool.services.plan.operations import WallBuilder


# Вариант 1
@pytest.fixture()
def input_df_1():
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
def expected_df_1():
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


def test_sort_by_technology_1(input_df_1, expected_df_1):
    result_input_df = WallBuilder.set_works_sequence(input_df_1)
    result_input_df.volume_p = result_input_df.volume_p.round(1)

    assert_frame_equal(result_input_df, expected_df_1, check_dtype=False)


# Вариант 2
@pytest.fixture()
def input_df_2():
    input_data = [
        [1, 1, 10, 40, 100],
        [2, 2, 10, 40, 100],
        [3, 3, 10, 15, 100],
        [3, 4, 15, 40, 100],
        [4, 5, 10, 15, 100],
        [4, 6, 15, 40, 100],
    ]

    input_cols = ["hierarchy", "id", "start_p", "finish_p", "volume_p"]
    return pd.DataFrame(input_data, columns=input_cols)


@pytest.fixture()
def expected_df_2():
    expected_data = [
        [0, 1, 1, 10, 40, 100],
        [1, 2, 2, 10, 40, 100],
        [2, 3, 3, 10, 15, 100],
        [3, 4, 5, 10, 15, 100],
        [4, 3, 4, 15, 40, 100],
        [5, 4, 6, 15, 40, 100],
    ]

    expected_cols = ["sort_key", "hierarchy", "id", "start_p", "finish_p", "volume_p"]
    return pd.DataFrame(expected_data, columns=expected_cols)


def test_sort_by_technology_2(input_df_2, expected_df_2):
    result_input_df = WallBuilder.set_works_sequence(input_df_2)
    result_input_df.volume_p = result_input_df.volume_p.round(1)

    assert_frame_equal(result_input_df, expected_df_2, check_dtype=False)


# Вариант 3
@pytest.fixture()
def input_df_3():
    input_data = [
        [1, 1, 10, 15, 100],
        [2, 2, 10, 40, 100],
        [3, 3, 10, 15, 100],
        [3, 4, 15, 40, 100],
        [4, 5, 10, 15, 100],
        [4, 6, 15, 40, 100],
    ]

    input_cols = ["hierarchy", "id", "start_p", "finish_p", "volume_p"]
    return pd.DataFrame(input_data, columns=input_cols)


@pytest.fixture()
def expected_df_3():
    expected_data = [
        [0, 1, 1, 10, 15, 100],
        [1, 2, 2, 10, 15, 16.7],
        [2, 3, 3, 10, 15, 100],
        [3, 4, 5, 10, 15, 100],
        [4, 1, 2, 15, 40, 83.3],
        [5, 2, 4, 15, 40, 100],
        [6, 3, 6, 15, 40, 100],
    ]

    expected_cols = ["sort_key", "hierarchy", "id", "start_p", "finish_p", "volume_p"]
    return pd.DataFrame(expected_data, columns=expected_cols)


def test_sort_by_technology_3(input_df_3, expected_df_3):
    result_input_df = WallBuilder.set_works_sequence(input_df_3)
    result_input_df.volume_p = result_input_df.volume_p.round(1)

    assert_frame_equal(result_input_df, expected_df_3, check_dtype=False)


# Вариант 4
@pytest.fixture()
def input_df_4():
    input_data = [
        [2, 1, 10, 60, 100],
        [3, 2, 10, 15, 100],
        [3, 3, 40, 60, 100],
        [4, 4, 10, 60, 100],
    ]

    input_cols = ["hierarchy", "id", "start_p", "finish_p", "volume_p"]
    return pd.DataFrame(input_data, columns=input_cols)


@pytest.fixture()
def expected_df_4():
    expected_data = [
        [0, 1, 1, 10, 15, 10],
        [1, 2, 2, 10, 15, 100],
        [2, 3, 4, 10, 15, 10],
        [3, 1, 1, 15, 40, 50],
        [4, 2, 4, 15, 40, 50],
        [5, 1, 1, 40, 60, 40],
        [6, 2, 3, 40, 60, 100],
        [7, 3, 4, 40, 60, 40],
    ]

    expected_cols = ["sort_key", "hierarchy", "id", "start_p", "finish_p", "volume_p"]
    return pd.DataFrame(expected_data, columns=expected_cols)


def test_sort_by_technology_4(input_df_4, expected_df_4):
    result_input_df = WallBuilder.set_works_sequence(input_df_4)
    result_input_df.volume_p = result_input_df.volume_p.round(1)

    assert_frame_equal(result_input_df, expected_df_4, check_dtype=False)


# Вариант 5
@pytest.fixture()
def input_df_5():
    input_data = [
        [1, 1, 10, 50, 100],
        [2, 2, 10, 40, 100],
        [3, 3, 10, 30, 100],
        [4, 4, 10, 20, 100],
    ]

    input_cols = ["hierarchy", "id", "start_p", "finish_p", "volume_p"]
    return pd.DataFrame(input_data, columns=input_cols)


@pytest.fixture()
def expected_df_5():
    expected_data = [
        [0, 1, 1, 10, 20, 25],
        [1, 2, 2, 10, 20, 33.3],
        [2, 3, 3, 10, 20, 50],
        [3, 4, 4, 10, 20, 100],
        [4, 1, 1, 20, 30, 25],
        [5, 2, 2, 20, 30, 33.3],
        [6, 3, 3, 20, 30, 50],
        [7, 1, 1, 30, 40, 25],
        [8, 2, 2, 30, 40, 33.3],
        [9, 1, 1, 40, 50, 25],
    ]

    expected_cols = ["sort_key", "hierarchy", "id", "start_p", "finish_p", "volume_p"]
    return pd.DataFrame(expected_data, columns=expected_cols)


def test_sort_by_technology_5(input_df_5, expected_df_5):
    result_input_df = WallBuilder.set_works_sequence(input_df_5)
    result_input_df.volume_p = result_input_df.volume_p.round(1)

    assert_frame_equal(result_input_df, expected_df_5, check_dtype=False)


# Вариант 6
@pytest.fixture()
def input_df_6():
    input_data = [
        [1, 1, 10, 30, 100],
        [1, 2, 40, 60, 100],
        [2, 3, 10, 20, 100],
        [2, 4, 20, 33, 100],
        [2, 5, 33, 50, 100],
        [2, 6, 50, 60, 100],
        [3, 7, 10, 42, 100],
        [3, 8, 50, 60, 100],
    ]

    input_cols = ["hierarchy", "id", "start_p", "finish_p", "volume_p"]
    return pd.DataFrame(input_data, columns=input_cols)


@pytest.fixture()
def expected_df_6():
    expected_data = [
        [0, 1, 1, 10, 30, 100],
        [1, 2, 3, 10, 20, 100],
        [2, 2, 4, 20, 30, 76.9],
        [3, 3, 7, 10, 30, 62.5],
        [4, 1, 4, 30, 33, 23.1],
        [5, 1, 5, 33, 40, 41.2],
        [6, 2, 7, 30, 40, 31.2],
        [7, 1, 2, 40, 42, 10],
        [8, 2, 5, 40, 42, 11.8],
        [9, 3, 7, 40, 42, 6.2],
        [10, 1, 2, 42, 50, 40],
        [11, 2, 5, 42, 50, 47.1],
        [12, 1, 2, 50, 60, 50],
        [13, 2, 6, 50, 60, 100],
        [14, 3, 8, 50, 60, 100],
    ]

    expected_cols = ["sort_key", "hierarchy", "id", "start_p", "finish_p", "volume_p"]
    return pd.DataFrame(expected_data, columns=expected_cols)


def test_sort_by_technology_6(input_df_6, expected_df_6):
    result_input_df = WallBuilder.set_works_sequence(input_df_6)
    result_input_df.volume_p = result_input_df.volume_p.round(1)

    assert_frame_equal(result_input_df, expected_df_6, check_dtype=False)


if __name__ == "__main__":
    data = [
        [1, 1, 10, 40, 100, True, False],
        [2, 2, 10, 50, 100, True, False],
        [3, 3, 45, 45, 100, True, True],
        [4, 4, 45, 45, 100, True, True],
        [5, 5, 25, 47, 100, False, False],
        [5, 6, 10, 15, 100, True, False],
        [5, 7, 15, 50, 100, True, False],
        [6, 8, 10, 20, 100, True, False],
        [6, 9, 20, 40, 100, True, False],
    ]
    input_cols = ["level", "id", "start_p", "finish_p", "volume_p", "is_key_oper", "is_point_object"]
    df = pd.DataFrame(data, columns=input_cols)
    print(WallBuilder.set_works_sequence(df))
