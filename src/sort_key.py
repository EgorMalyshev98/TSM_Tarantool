import numpy as np
import pandas as pd


def add_sort_key(works: pd.DataFrame) -> pd.Series:
    """Определение последовательности работ на пикетажных участках
    Args:
        works (pd.DataFrame):
            start_p (float): начало
            finish_p (float): окончание
            hierarchy (int): технологический порядок выполнения работ

    Returns:
        pd.Series: ключ сортировки
    """
    counter = 0
    sort_col = np.zeros(works.shape[0])
    works = works.to_numpy()

    for curr_inx, curr_row in enumerate(works):
        if sort_col[curr_inx]:
            continue

        if not curr_inx:
            sort_col[curr_inx] = curr_inx
            counter += 1
            continue

        prev_row = works[curr_inx - 1]

        p_finish, p_level = prev_row[1], prev_row[2]
        c_finish, c_level = curr_row[1], curr_row[2]

        if c_level > p_level and c_finish > p_finish:
            for n_inx, next_row in enumerate(works[curr_inx:]):
                n_start, n_finish, n_level = next_row[0], next_row[1], next_row[2]
                if n_level > c_level and n_start < c_finish and n_finish > c_finish:
                    sort_col[n_inx + curr_inx] = -1
                    continue
                if n_level == p_level and n_start < c_finish:
                    sort_col[n_inx + curr_inx] = counter
                    sort_col[curr_inx] = counter + 1
                    counter += 1
            counter += 1
            continue

        sort_col[curr_inx] = counter
        counter += 1

    return pd.Series(sort_col, dtype=int)


data = [
    ["snt_prs", 33.0, 40.0, 1],
    ["ust_nasp", 33.0, 50.0, 2],
    ["ust_kv", 33.0, 50.0, 2],
    ["ust_geotxtl", 33.0, 45.0, 3],
    ["ust_pps", 33.0, 45.0, 4],
    ["snt_prs", 40.0, 45.0, 1],
    ["snt_prs", 45.0, 59.0, 1],
    ["snt_prs", 60.0, 70.0, 1],
    ["ust_geotxtl", 45.0, 59.0, 3],
    ["ust_pps", 45.0, 59.0, 4],
    ["ust_kv", 50.0, 59.0, 2],
]


cols = ["operation_type", "start_p", "finish_p", "hierarchy"]
df = pd.DataFrame(data, columns=cols).sort_values(["start_p", "hierarchy"]).reset_index(drop=True)

df.loc[:, "sort_key"] = add_sort_key(df[["start_p", "finish_p", "hierarchy"]])

print(df)
