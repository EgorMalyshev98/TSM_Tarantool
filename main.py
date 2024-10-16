import pandas as pd
from dev_data_source import P_RD, FACT
import numpy as np


def calculate_volume(current_start: pd.Series, 
                     current_finish: pd.Series, 
                     input_start: pd.Series, 
                     input_finish: pd.Series, 
                     vol_per_unit: pd.Series) -> pd.Series:
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
    
    return pd.DataFrame({
        'start': start,
        'finish': finish,
        'volume': volume
    })


def select_pikets(start: int, finish: int, pikets: pd.DataFrame) -> pd.DataFrame:
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

    pikets.loc[:, ['input_start', 'input_fin']] = start, finish
    
    vol_per_unit = pikets['vol_prd'] / (pikets['picket_finish'] - pikets['picket_start'])
    
    df = calculate_volume(
        pikets['picket_start'], pikets['picket_finish'], 
        pikets['input_start'], pikets['input_fin'], 
        vol_per_unit
    )
    df = df.add_suffix('_p', axis=1)
    pikets = pd.concat([pikets, df], axis=1)
    mask = pikets['volume_p'] > 0
    pikets = pikets[mask][['num_con', 'operation_type', 'start_p', 'finish_p', 'volume_p']]
    
    return pikets.reset_index(drop=True)



def add_fact(project: pd.DataFrame, fact: pd.DataFrame) -> pd.DataFrame:
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
    
    project['index_p'] = project.index
    merged = project.merge(fact, how='left', on='operation_type')
    
    vol_per_unit = merged['vol_fact'] / (merged['picket_finish'] - merged['picket_start'])
    
    df = calculate_volume(
        merged['start_p'], merged['finish_p'], 
        merged['picket_start'], merged['picket_finish'], 
        vol_per_unit
    )
    
    merged.loc[:, 'merged_fact'] = df['volume']
    
    grouped = merged[['index_p', 'merged_fact']].groupby('index_p').sum()
   
    return grouped['merged_fact']
    
    
def main():
    input_cost = 1000000
    input_start = 33
    input_fin = 59
    
    prd_cols = ['num_con', 'operation_type', 'picket_start', 'picket_finish', 'vol_prd']
    fact_cols = ['operation_type', 'picket_start', 'picket_finish', 'vol_fact']
    
    project_volume = select_pikets(input_start, input_fin, P_RD[prd_cols])
    
    project_volume.loc[:, 'volume_f'] = add_fact(project_volume.copy(), FACT[fact_cols])
    project_volume.loc[:, 'vol_balance'] = project_volume['volume_p'] - project_volume['volume_f'] 


pd.options.mode.copy_on_write = True
    
main()

