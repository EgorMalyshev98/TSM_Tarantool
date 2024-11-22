import numpy as np
import pandas as pd


def _insert_non_key_works(wall: pd.DataFrame, non_key_works: pd.DataFrame):
    cols = wall.columns
    key_oper_array = wall.to_numpy()
    non_key_oper_array = non_key_works.to_numpy()
    key_levels = wall["level"].drop_duplicates().to_list()  # список уровней ключевых ресурсов
    result = np.empty((0, 8))  # пустой массив numpy для заполнения
    if wall.shape[0] == 0:
        for row_non_key_work in non_key_oper_array:
            result = np.append(result, [row_non_key_work], axis=0)
    else:
        for row_key_work in key_oper_array:
            level, finish_p = row_key_work[1], row_key_work[4]
            result = np.append(result, [row_key_work], axis=0)
            for row_non_key_work in non_key_oper_array:
                level_n, finish_p_n = row_non_key_work[1], row_non_key_work[4]
                # уровень неключевой операции есть в списке ключевых
                if level == level_n and finish_p >= finish_p_n:
                    result = np.append(result, [row_non_key_work], axis=0)
                    non_key_oper_array = np.delete(non_key_oper_array, 0, 0)
                # уровня неключевой операции нет в списке ключевых
                if level == (level_n - 1) and finish_p >= finish_p_n and level_n not in key_levels:
                    result = np.append(result, [row_non_key_work], axis=0)
                    non_key_oper_array = np.delete(non_key_oper_array, 0, 0)
                # ключевая операция первая в датафрейме
                if level_n < min(key_levels):
                    result = np.append(result, [row_non_key_work], axis=0)
                    non_key_oper_array = np.delete(non_key_oper_array, 0, 0)

    return pd.DataFrame(result, columns=cols).drop(columns="sort_key").reset_index(names="sort_key")


cols = ["sort_key", "level", "id", "start_p", "finish_p", "volume_p", "is_key_oper", "is_point_object"]

area_1_k = [
    [0, 1, 1, 0, 50, 83.3, True, False],
]
area_1_n_k = [
    [1, 2, 2, 0, 25, 100, False, False],
    [2, 2, 3, 25, 50, 55.6, False, False],
]

area_2_k = [
    [3, 1, 1, 50, 60, 16.7, True, False],
    [4, 2, 5, 50, 60, 20, True, False],
]

area_2_n_k = [
    [5, 2, 3, 50, 60, 22.2, False, False],
]

area_3_k = [
    [6, 1, 5, 60, 100, 80, True, False],
]

area_3_n_k = [
    [7, 1, 3, 60, 70, 22.2, False, False],
    [8, 1, 4, 70, 100, 100, False, False],
]

area_4_k = []

area_4_n_k = [
    [9, 1, 6, 100, 120, 100, False, False],
]

key_oper1 = pd.DataFrame(area_1_k, columns=cols)
non_key_oper1 = pd.DataFrame(area_1_n_k, columns=cols)

print(_insert_non_key_works(key_oper1, non_key_oper1))

key_oper2 = pd.DataFrame(area_2_k, columns=cols)
non_key_oper2 = pd.DataFrame(area_2_n_k, columns=cols)

print(_insert_non_key_works(key_oper2, non_key_oper2))

key_oper3 = pd.DataFrame(area_3_k, columns=cols)
non_key_oper3 = pd.DataFrame(area_3_n_k, columns=cols)

print(_insert_non_key_works(key_oper3, non_key_oper3))


key_oper4 = pd.DataFrame(area_4_k, columns=cols)
non_key_oper4 = pd.DataFrame(area_4_n_k, columns=cols)

print(_insert_non_key_works(key_oper4, non_key_oper4))
