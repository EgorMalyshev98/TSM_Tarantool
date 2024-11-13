from typing import Dict, List

import numpy as np
import pandas as pd

from src.log import logger
from src.tarantool.models import PlanSources


class Wall:
    def __init__(self, blocks: Dict[int, List[List[int]]], slice_=2):
        """Расчет последовательности выполнения работ на типовом участке

        Args:
            blocks (Dict[int, List[List[int]]]): _description_
            slice_ (int, optional): _description_. Defaults to 2.
        """
        self.values = []
        self.merged_blocks_per_layer: Dict[int, List[List]] = {}
        self.spaces: Dict[int, List[List]] = {}
        self.blocks = blocks
        self.slice_ = slice_

    def _merge_block(self, start: int, finish: int, lvl: int):
        layer = self.merged_blocks_per_layer.get(lvl)

        if not layer:
            self.merged_blocks_per_layer[lvl] = [[start, finish]]
            return

        _, prev_finish = self.merged_blocks_per_layer[lvl][-1]

        if prev_finish == start:
            self.merged_blocks_per_layer[lvl][-1][1] = finish
            return

        self.merged_blocks_per_layer[lvl].append([start, finish])
        logger.debug(self.merged_blocks_per_layer)
        self.spaces.setdefault(lvl, []).append([prev_finish, start])

    def _add_block(self, block: list, lvl: int):
        level, id_, start_p, finish_p, volume_p = block
        self._merge_block(start_p, finish_p, lvl)
        self.values.append(block)

    def _is_valid(self, start: int, finish: int, lvl: int):
        if lvl == 1:
            return True

        layer = self.merged_blocks_per_layer[lvl - 1]

        for prev_block in layer:
            prev_start, prev_fin = prev_block

            if prev_fin >= finish:
                return True
            # if start >= prev_start and finish <= prev_fin:
            #     return True

        return False

    def _is_over_space(self, block: list, lvl: int):
        layer = self.spaces.get(lvl - 1)
        if not layer:
            return False

        start, finish = block[: self.slice_]

        for space in layer:
            space_start, space_finish = space
            if space_start < finish and start < space_finish:
                self.spaces.setdefault(lvl, []).append([start, finish])
                return True

        return False

    def build(self) -> List:
        levels = sorted(self.blocks.keys())
        lvl = levels[0]
        max_lvl = levels[-1]

        while any(block for levels in self.blocks.values() for block in levels):
            if lvl not in levels:
                return self.values

            if not self.blocks[lvl]:
                if lvl == levels[0]:
                    return self.values
                lvl -= 1
                continue

            block = self.blocks[lvl].pop()
            logger.debug(block)
            level, id_, start_p, finish_p, volume_p = block

            is_valid = self._is_valid(start_p, finish_p, lvl)

            if is_valid:
                self._add_block(block, lvl)
                if lvl + 1 <= max_lvl:
                    lvl += 1
                continue

            is_over_space = self._is_over_space(block, lvl)
            if is_over_space:
                logger.warning("Пробелы в рабочей документации!")
                continue

            self.blocks[lvl].append(block)
            lvl -= 1

        return self.values


class BlockSeparator:
    @staticmethod
    def _find_split_points(works: pd.DataFrame):
        """находит точки в которых изменяется конструкция"""

        works = (
            works.sort_values(["hierarchy", "start_p"])
            .assign(prev_fin=lambda x: x.groupby("hierarchy")["finish_p"].shift(1).fillna(x.start_p))
            .assign(last_block=lambda x: x.reset_index().groupby("hierarchy")["index"].transform("max"))
        )

        spaces_filter = works["start_p"] > works["prev_fin"]
        error_filter = works["start_p"] < works["prev_fin"]
        last_block_filter = works.index == works["last_block"]

        err_df = works[error_filter]

        if not err_df.empty:
            raise ValueError(f'элементы одного уровня пересакаются на пикетах {err_df[["start_p", "prev_fin"]]}')

        spaced_points = works[spaces_filter][["start_p", "prev_fin"]].to_numpy().flatten().tolist()
        last_block_points = works[last_block_filter]["finish_p"].to_numpy().flatten().tolist()

        return sorted({*spaced_points, *last_block_points})

    @staticmethod  # TODO numba
    def _numba_split_blocks(works: np.ndarray, split_points: list):
        splitted_blocks = []

        for inx in range(works.shape[0]):
            row = works[inx]
            level, id_, start_p, finish_p, vol, vol_per_unit = row
            for point in split_points:
                if finish_p <= point:
                    splitted_blocks.append([point, level, id_, start_p, finish_p, vol])
                    break
                if start_p < point <= finish_p:
                    left_vol = (point - start_p) * vol_per_unit
                    splitted_blocks.append([point, level, id_, start_p, point, left_vol])
                    start_p = point
                    vol -= left_vol

        return splitted_blocks

    @classmethod
    def split_by_construct(cls, works: pd.DataFrame):
        """Разделяет пикетажные участки рабочей документации при изменении конструктива
        Args:
            works (pd.Series):
                hierarchy: уровень операции
                id_: id строки
                start_p: начало участка
                finish_p: окончание участка
                volume_p: проектный объем
        Returns:
            List[pd.Dataframe]
        """

        split_points = cls._find_split_points(works)
        out_cols = ["split_point", *works.columns]
        works = works.assign(vol_per_unit=lambda x: (x.volume_p / (x.finish_p - x.start_p))).to_numpy()

        splitted_blocks = cls._numba_split_blocks(works, split_points)

        blocks_df = (
            pd.DataFrame(splitted_blocks, columns=out_cols)
            .sort_values(["split_point", "hierarchy", "start_p"], ascending=[True, True, False])
            .reset_index(drop=True)
            .assign(hierarchy=lambda df: df.groupby("split_point")["hierarchy"].transform("rank", method="dense"))
        )

        return [df.drop(columns="split_point") for _, df in blocks_df.groupby("split_point")]


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
    def _sort_by_technology(works: pd.DataFrame) -> pd.DataFrame:
        """Определение последовательности работ на пикетажных участках
        Args:
            works (pd.DataFrame):

        Returns:
            pd.Series: ключ сортировки
        """

        wall = []
        cols = works.columns
        blocks_by_construct = BlockSeparator.split_by_construct(works)
        print(*blocks_by_construct, sep="\n")

        for contruct_block in blocks_by_construct:
            blocks: Dict[int, list] = (
                contruct_block.sort_values(["hierarchy", "start_p"], ascending=[True, False])
                .reset_index(drop=True)
                .groupby("hierarchy")[cols]
                .apply(lambda x: x.to_numpy().tolist())
                .to_dict()
            )
            wall.extend(Wall(blocks).build())

        return pd.DataFrame(wall, columns=cols).reset_index(names="sort_key")

    def select(self, input_start: float, input_fin: float) -> pd.DataFrame:
        """Выбор планируемых работ на пикетажных участках

        Args:
            input_start (float): начало участка
            input_fin (float): окончание участка

        Returns:
            pd.DataFrame: _description_
        """
        used_cols = ["hierarchy", "id", "start_p", "finish_p", "volume_p"]

        dop_cols = ["id", "num_con", "operation_type", "work_name", "unit"]

        operations = self._select_pikets(input_start, input_fin, self.prd.copy()).merge(
            self.hierarchy, how="left", on="operation_type"
        )

        dop_cols_df = operations[dop_cols]

        operations = self._sort_by_technology(operations[used_cols]).merge(dop_cols_df, how="left", on="id")

        operations.loc[:, "volume_f"] = self._add_fact(operations.copy(), self.fact_df.copy())
        operations.loc[:, "vol_remain"] = operations["volume_p"] - operations["volume_f"]
        operations.loc[:, "cost_remain"] = self._add_cost(operations[["num_con", "vol_remain"]], self.contract.copy())
        print(operations)

        return operations
