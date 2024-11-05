import sys
import os
sys.path.append(os.getcwd()) # добавить в системные пути абсолютный путь к катологу, в котором лежит src

from src.sort_key import add_sort_key
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

@pytest.fixture()
def input_df():
    data = [
        ['snt_prs', 33.0, 40.0, 1],
        ['ust_nasp', 33.0, 90.0, 2],
        ['ust_kv', 33.0, 50.0, 2],
        ['ust_geotxtl', 33.0, 45.0, 3],
        ['ust_pps', 33.0, 45.0, 4],
        ['snt_prs', 40.0, 45.0, 1],
        ['snt_prs', 45.0, 59.0, 1],
        ['ust_geotxtl', 45.0, 59.0, 3],
        ['ust_pps', 45.0, 59.0, 4],
        ['ust_kv', 50.0, 59.0, 2],
        ['snt_prs', 60.0, 70.0, 1],
    ]

    cols = ["operation_type", "start_p", "finish_p", "hierarchy"]
    df = pd.DataFrame(data, columns=cols).sort_values(["start_p", "hierarchy"]).reset_index(drop=True)
    
    return df

@pytest.fixture()
def expected_df():

    expected_data = [
        ['snt_prs', 33.0, 40.0, 1, 0],
        ['ust_nasp', 33.0, 90.0, 2, -1],
        ['ust_kv', 33.0, 50.0, 2, 3],
        ['ust_geotxtl', 33.0, 45.0, 3, -1],
        ['ust_pps', 33.0, 45.0, 4, -1],
        ['snt_prs', 40.0, 45.0, 1, 1],
        ['snt_prs', 45.0, 59.0, 1, 2],
        ['ust_geotxtl', 45.0, 59.0, 3, -1],
        ['ust_pps', 45.0, 59.0, 4, -1],
        ['ust_kv', 50.0, 59.0, 2, 4],
        ['snt_prs', 60.0, 70.0, 1, 5],
    ]

    expected_cols = ["operation_type", "start_p", "finish_p", "hierarchy", "sort_key"]
    expected_df = pd.DataFrame(expected_data, columns=expected_cols)

    return expected_df

def test_sort_key(input_df, expected_df):
    
    input_to_func_df = input_df[["start_p", "finish_p", "hierarchy"]]
    
    input_df.loc[:, "sort_key"] = add_sort_key(input_to_func_df)


    
    assert_frame_equal(input_df, expected_df, check_dtype=False)