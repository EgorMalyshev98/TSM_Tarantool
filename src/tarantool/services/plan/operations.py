import numpy as np
import pandas as pd

from src.tarantool.models import PlanSources


class OperationSelector:
    def __init__(self, data: PlanSources):
        self.prd = data.prd
        self.fact_df = data.fact
        self.contract = data.contract
        self.hierarchy = data.hierarchy

    def _select_pikets(self, start: int, finish: int, pikets: pd.DataFrame) -> pd.DataFrame:
        """Выбор проектных объемов для заданного пикетажного участка.

        Args:
            start (int): пикет начала.
            finish (int): пикет окончания.
            pikets (pd.DataFrame): таблица с объемом и пикетажными участками.
                DataFrame fields:
                    'num_con' (object): №кв.
                    'operation_type' (object): тип операции.
                    'picket_start' (float): начало участка.
                    'picket_finish' (float): конец участка.
                    'vol_prd' (float): объем работ по проекту.

        Returns:
            pd.DataFrame: объем работ на заданном пикетажном участке.
        """

        pikets.loc[:, ["input_start", "input_fin"]] = start, finish

        vol_per_unit = pikets["vol_prd"] / (pikets["picket_finish"] - pikets["picket_start"])

        df = self._calculate_volume(
            pikets["picket_start"],
            pikets["picket_finish"],
            pikets["input_start"],
            pikets["input_fin"],
            vol_per_unit,
        )
        df = df.add_suffix("_p", axis=1)
        pikets = pd.concat([pikets, df], axis=1)
        mask = pikets["volume_p"] > 0
        pikets = pikets[mask][["num_con", "operation_type", "start_p", "finish_p", "volume_p"]]

        return pikets.reset_index(drop=True)

    def _calculate_volume(
        self,
        current_start: pd.Series,
        current_finish: pd.Series,
        input_start: pd.Series,
        input_finish: pd.Series,
        vol_per_unit: pd.Series,
    ) -> pd.Series:
        """Вычисление объемов на пересекающихся участках.

        Args:
            current_start (pd.Series): начало целевого участка
            current_finish (pd.Series): конец целевого участка
            input_start (pd.Series): начало пересекающего участка
            input_finish (pd.Series): конец пересекающего участка
            vol_per_unit (pd.Series): объем работ на единицу длины участка.

        Returns:
            pd.DataFrame:
                start (float): начало участка
                finish (float): конец участка
                volume (float): объем работ на пересеченном участке
        """
        start = np.maximum(current_start, input_start)
        finish = np.minimum(current_finish, input_finish)
        length = np.maximum(0, finish - start)

        volume = vol_per_unit * length

        return pd.DataFrame(
            {
                "start": start,
                "finish": finish,
                "volume": volume,
            },
        )

    def _add_fact(self, project: pd.DataFrame, fact: pd.DataFrame) -> pd.Series:
        """Добавление фактических объемов на выбранные пикетажные участки.

        Args:
            project (pd.DataFrame): пикетажные участки по проекту.
                start_p (float): начало
                finish_p (float): конец
                volume_p (float): объем
            fact (pd.DataFrame): фактические пикетажные участки.

        Returns:
            pd.Seies: поле с фактическими объемами работ на выбранных участках
        """

        project["index_p"] = project.index
        merged = project.merge(fact, how="left", on="operation_type")
        vol_per_unit = merged["vol_fact"] / (merged["picket_finish"] - merged["picket_start"])

        df = self._calculate_volume(
            merged["start_p"],
            merged["finish_p"],
            merged["picket_start"],
            merged["picket_finish"],
            vol_per_unit,
        )

        merged.loc[:, "merged_fact"] = df["volume"]
        grouped = merged[["index_p", "merged_fact"]].groupby("index_p").sum()
        return grouped["merged_fact"]

    def _add_cost(self, project: pd.DataFrame, contract: pd.DataFrame) -> pd.Series:
        """Добавление стоимости работ

        Args:
            project (pd.DataFrame):
                num_con (object): №КВ
                vol_remain (float): остаток работ к выполнению
            contract (pd.DataFrame):
                num_con (object): №КВ
                price (float): расценка
        Returns:
            pd.Series: поле стоимости работ
        """
        merged = project.merge(contract, how="left", on="num_con", validate="many_to_one")
        return merged["price"] * merged["vol_remain"]

    @staticmethod
    def _add_sort_key(works: pd.DataFrame) -> pd.Series:
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
                    n_start, n_level = next_row[0], next_row[2]
                    if n_level == p_level and n_start < c_finish:
                        sort_col[n_inx + curr_inx] = counter
                        sort_col[curr_inx] = counter + 1
                        counter += 1
                counter += 1
                continue

            sort_col[curr_inx] = counter
            counter += 1

        return pd.Series(sort_col, dtype=int)

    def select(self, input_start: float, input_fin: float) -> pd.DataFrame:
        """Выбор планируемых работ на пикетажных участках

        Args:
            input_start (float): начало участка
            input_fin (float): окончание участка

        Returns:
            pd.DataFrame: _description_
        """

        operations = self._select_pikets(input_start, input_fin, self.prd.copy())

        operations.loc[:, "volume_f"] = self._add_fact(operations.copy(), self.fact_df.copy())
        operations.loc[:, "vol_remain"] = operations["volume_p"] - operations["volume_f"]
        operations = (
            operations[operations["vol_remain"] > 0]
            .merge(self.hierarchy, how="left", on="operation_type")
            .sort_values(["start_p", "hierarchy"])
            .reset_index(drop=True)
        )

        operations.loc[:, "cost_remain"] = self._add_cost(operations[["num_con", "vol_remain"]], self.contract.copy())

        operations.loc[:, "sort_key"] = self._add_sort_key(operations[["start_p", "finish_p", "hierarchy"]])
        return operations.sort_values("sort_key")
