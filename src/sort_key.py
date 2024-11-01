from collections import deque
from typing import Dict, List

import pandas as pd

data = [
    [1, 10, 20],
    [1, 20, 30],
    [1, 30, 40],
    [2, 10, 25],
    [2, 28, 37],
    [3, 10, 30],
    [3, 30, 37],
    [4, 10, 29],
    [4, 30, 37],
]


cols = ["hierarchy", "start_p", "finish_p"]
df = pd.DataFrame(data, columns=cols).sort_values(["hierarchy", "start_p"]).reset_index(drop=True)
blocks: Dict[int, deque] = (
    df.groupby("hierarchy")[["start_p", "finish_p"]].apply(lambda x: deque(x.to_numpy().tolist())).to_dict()
)

# {1: deque([[10, 20], [20, 30], [30, 40]]),
#  2: deque([[10, 25], [28, 37]]),
#  3: deque([[10, 30], [30, 37]]),
#  4: deque([[10, 29], [30, 37]])}


class Wall:
    def __init__(self):
        self.wall = []
        self.merged_blocks_per_layer: Dict[int, List[List]] = {}
        self.counter = 1
        self.added_blocks = set()

    def _merge_block(self, block: list, lvl: int):
        if not self.merged_blocks_per_layer.get(lvl):
            self.merged_blocks_per_layer[lvl] = [block]
            return

        prev_block = self.merged_blocks_per_layer[lvl][-1]
        start, finish = block
        _, prev_finish = prev_block

        if prev_finish == start:
            self.merged_blocks_per_layer[lvl][-1][1] = finish
            return

        self.merged_blocks_per_layer[lvl].append(block)

    def add_block(self, block: list, lvl: int):
        if (lvl, *block) in self.added_blocks:
            return
        self.added_blocks.add((lvl, *block))
        self._merge_block(block, lvl)
        self.wall.append([self.counter, lvl, *block])
        self.counter += 1

    def is_valid(self, block: list, lvl: int):
        if lvl == 1:
            return True

        layer = wall.merged_blocks_per_layer[lvl - 1]
        start, finish = block

        for prev_block in layer:
            prev_start, prev_fin = prev_block
            if start >= prev_start and finish <= prev_fin:
                return True

        return False

    def __str__(self):
        return str(self.wall)


wall = Wall()


def backtracking(lvl=1):
    if lvl not in blocks:
        return

    lvl_blocks = blocks[lvl]

    if not lvl_blocks:
        return

    block = lvl_blocks.popleft()

    is_valid = wall.is_valid(block, lvl)

    if is_valid:
        wall.add_block(block, lvl)
        backtracking(lvl + 1)

    backtracking(lvl)
    lvl_blocks.appendleft(block)


backtracking()

print(wall)
