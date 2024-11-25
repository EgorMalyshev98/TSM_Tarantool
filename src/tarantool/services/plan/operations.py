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
        self.spaces.setdefault(lvl, []).append([prev_finish, start])

    def _add_block(self, block: list, lvl: int):
        level, id_, start_p, finish_p, volume_p, is_key_oper, is_point_object = block
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
            level, id_, start_p, finish_p, volume_p, is_key_oper, is_point_object = block

            if not is_key_oper:
                self.values.append(block)
                if lvl + 1 <= max_lvl:
                    lvl += 1
                continue

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
            works.sort_values(["level", "start_p"])
            .assign(prev_fin=lambda x: x.groupby("level")["finish_p"].shift(1).fillna(x.start_p))
            .assign(last_block=lambda x: x.reset_index().groupby(["is_key_oper", "level"])["index"].transform("max"))
            .assign(first_block=lambda x: x.reset_index().groupby(["is_key_oper", "level"])["index"].transform("min"))
        )

        spaces_filter = works["start_p"] > works["prev_fin"]
        spaced_points = works[spaces_filter][["start_p", "prev_fin"]].to_numpy().flatten().tolist()

        last_block_points = works[works["is_key_oper"]].groupby("level")["finish_p"].max().tolist()
        first_block_points = works[works["is_key_oper"]].groupby("level")["start_p"].min().tolist()
        last_point = float(works["finish_p"].max())

        return sorted({*spaced_points, *last_block_points, *first_block_points, last_point})

    @staticmethod  # TODO numba
    def _numba_split_blocks(works: np.ndarray, split_points: list):
        splitted_blocks = []

        for inx in range(works.shape[0]):
            row = works[inx]
            level, id_, start_p, finish_p, vol, is_key_oper, is_point_object, vol_per_unit = row
            for point in split_points:
                if finish_p <= point:
                    splitted_blocks.append([point, level, id_, start_p, finish_p, vol, is_key_oper, is_point_object])
                    break
                if start_p < point <= finish_p:
                    left_vol = (point - start_p) * vol_per_unit
                    splitted_blocks.append([point, level, id_, start_p, point, left_vol, is_key_oper, is_point_object])
                    start_p = point
                    vol -= left_vol

        return splitted_blocks

    @classmethod
    def split_by_construct(cls, works: pd.DataFrame):
        """Разделяет пикетажные участки рабочей документации при изменении конструктива
        Args:
            works (pd.Series):
                level: уровень операции
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
            .sort_values(["split_point", "level", "start_p"], ascending=[True, True, False])
            .reset_index(drop=True)
            .assign(level=lambda df: df.groupby("split_point")["level"].transform("rank", method="dense"))
        )

        return [df.drop(columns="split_point") for _, df in blocks_df.groupby("split_point")]


class WallBuilder:
    @staticmethod
    def _df_blocks_to_dict(construct_block: pd.DataFrame) -> Dict[int, list]:
        cols = construct_block.columns
        return (
            construct_block.sort_values(["level", "start_p", "is_key_oper"], ascending=[True, False, True])
            .reset_index(drop=True)
            .groupby("level")[cols]
            .apply(lambda x: x.to_numpy().tolist())
            .to_dict()
        )

    @staticmethod
    def _insert_non_key_works(wall: pd.DataFrame, non_key_works: pd.DataFrame):
        if non_key_works.empty:
            return wall
        cols = wall.columns
        # ['level', 'id', 'start_p', 'finish_p', 'volume_p', 'is_key_oper', 'is_point_object']
        types = wall.dtypes.to_dict()
        key_w = wall.to_numpy().tolist()
        n_key_w = non_key_works.sort_values(["level", "start_p"]).to_numpy().tolist()

        result = []

        k_inx, n_inx = 0, 0

        while k_inx < len(key_w) and n_inx < len(n_key_w):
            k_row = key_w[k_inx]
            n_row = n_key_w[n_inx]

            lvl_n, _, start_n, fin_n, _, _, _ = n_row

            if not k_inx:
                result.append(k_row)
                k_inx += 1
                continue

            prev_lvl_k, _, prev_start_k, prev_fin_k, _, _, _ = key_w[k_inx - 1]

            if prev_lvl_k == lvl_n and prev_fin_k >= fin_n:
                result.append(n_row)
                n_inx += 1
                continue

            result.append(k_row)
            k_inx += 1

        remain_k = key_w[k_inx:]
        remain_n = n_key_w[n_inx:]

        if remain_k:
            result.extend(remain_k)
        elif remain_n:
            result.extend(remain_n)

        return pd.DataFrame(result, columns=cols).astype(types)

    @staticmethod
    def _insert_point_objects(wall: pd.DataFrame, point_objects: pd.DataFrame):
        if point_objects.empty:
            return wall

        concated = []

        for start, obj in point_objects.groupby("start_p"):
            inx = wall[wall["start_p"] == start].index.min()
            top = wall.iloc[inx:]
            bottom = wall.iloc[:inx]

            concated.extend([bottom, obj, top])

        return pd.concat(concated)

    @classmethod
    def set_works_sequence(cls, works: pd.DataFrame) -> pd.DataFrame:
        """Определение последовательности работ на пикетажных участках
        Args:
            works (pd.DataFrame):

        Returns:
            pd.Series: ключ сортировки
        """

        cols = works.columns
        types = works.dtypes.to_dict()
        wall = pd.DataFrame(columns=cols).astype(types)
        local_constructs = BlockSeparator.split_by_construct(works)
        point_object_blocks = []

        for _inx, local_construct in enumerate(local_constructs):
            point_obj_filter = local_construct["is_point_object"]
            key_work_filter = local_construct["is_key_oper"]

            point_obj = local_construct[point_obj_filter]
            non_key_works = local_construct[~key_work_filter]
            lvl_df = local_construct[["id", "level"]]

            if not point_obj.empty:
                point_object_blocks.append(point_obj.sort_values(["start_p", "level"]))

            key_works = (
                local_construct[(~point_obj_filter) & (key_work_filter)]
                .reset_index(drop=True)
                .assign(level=lambda df: df.level.rank(method="dense"))
            )

            blocks = cls._df_blocks_to_dict(key_works)

            local_wall = (
                pd.DataFrame(Wall(blocks).build(), columns=cols)
                .astype(types)
                .merge(lvl_df, how="inner", on="id", suffixes=(None, "_old"))
                .assign(level=lambda df: df.level_old)
                .drop(columns="level_old")
                .pipe(cls._insert_non_key_works, non_key_works.sort_values(["level", "start_p"]))
                .dropna(how="all", axis=1)
            )
            wall = pd.concat([wall if not wall.empty else None, local_wall], ignore_index=True)

        if point_object_blocks:
            point_objects = pd.concat(point_object_blocks)
            wall = cls._insert_point_objects(wall, point_objects.sort_values(["start_p", "level"]))
        return wall.reset_index(drop=True).reset_index(names="sort_key")


class OperationSelector:
    def __init__(self, data: PlanSources, builder: WallBuilder = WallBuilder):
        self.prd = data.prd
        self.fact_df = data.fact
        self.contract = data.contract
        self.technology = data.technology
        self.builder = builder

    def _select_pikets(self, start: int, finish: int, pikets: pd.DataFrame) -> pd.DataFrame:
        """Выбор проектных объемов для заданного пикетажного участка.

        Args:
            start (int): пикет начала.
            finish (int): пикет окончания.
            pikets (pd.DataFrame): таблица с объемом и пикетажными участками.
                DataFrame fields:
                    'num_con' (object): №кв.
                    'operation_type' (object): тип операции.
                    'unit' (object): ед. объема
                    'picket_start' (float): начало участка.
                    'picket_finish' (float): конец участка.
                    'vol_prd' (float): объем работ по проекту.
                    'is_point_object' bool: точечный объект

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
        pikets.loc[pikets.is_point_object, "volume_p"] = pikets[pikets.is_point_object]["vol_prd"]
        mask = pikets["volume_p"] > 0
        pikets = pikets[mask]

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

    def select(self, input_start: float, input_fin: float) -> pd.DataFrame:
        """Выбор планируемых работ на пикетажных участках

        Args:
            input_start (float): начало участка
            input_fin (float): окончание участка

        Returns:
            pd.DataFrame: _description_
        """
        used_cols = ["level", "id", "start_p", "finish_p", "volume_p", "is_key_oper", "is_point_object"]

        dop_cols = [
            "id",
            "num_con",
            "operation_type",
            "work_name",
            "unit",
            "construct_type",
            "construct_name",
        ]

        operations = self.prd.merge(self.technology, how="left", on="operation_type")

        operations = self._select_pikets(input_start, input_fin, operations)

        dop_cols_df = operations[dop_cols]

        operations = self.builder.set_works_sequence(operations[used_cols]).merge(dop_cols_df, how="left", on="id")

        operations.loc[:, "volume_f"] = self._add_fact(operations.copy(), self.fact_df.copy())
        operations.loc[:, "vol_remain"] = operations["volume_p"] - operations["volume_f"]
        operations.loc[:, "cost_remain"] = self._add_cost(operations[["num_con", "vol_remain"]], self.contract.copy())

        return operations
