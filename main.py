import array
from calendar import c
from re import S
import timeit
from typing import Literal
import pandas as pd
from dev_data_source import P_RD, FACT
import numpy as np

def interpolation(pikets: pd.DataFrame, piket: int, is_start: bool, vol_col: str) -> pd.DataFrame:
    """Интерполяция объемов на выбранном участке

    Args:
        pikets (pd.DataFrame): пикетажные участки с объемами для интерполяции
        piket (int): целевой пикет
        is_start (bool): флаг начала участка
        vol_col (str): поле объема

    Returns:
        pd.DataFrame: интерполированные объемы с измененной протяженностью
    """
    
    extra_col, picket_col = ['picket_finish', 'picket_start'] if is_start else ['picket_start', 'picket_finish']
    
    length = abs(piket - pikets[extra_col])
    pikets[vol_col] = (pikets[vol_col] / pikets['length']) * length
    pikets[picket_col] = piket
    pikets['length'] = length

    return pikets


def volume_section(start: int, finish: int, data: pd.DataFrame, vol_col: str) -> pd.DataFrame:
    """Расчет объемов для пикетажного участка

    Args:
        start (int): пикет начала
        finish (int): пикет окончания
        data (pd.DataFrame): любая таблица, включающая объемы и пикетажные участки
        vol_col (str): название поля с объемом
        
    Returns:
        pd.DataFrame: объем работ на заданном пикетажном участке
    """

    full_sections_filter = (data['picket_start'] >= start) & (data['picket_finish'] <= finish)
    start_section_filter = (data['picket_start'] < start) & (data['picket_finish'] > start)
    fin_section_filter = (data['picket_start'] < finish) & (data['picket_finish'] > finish)
    #TO DO: 4 вариант
    
    full_sections = data[full_sections_filter]
    
    start_section = data[start_section_filter]
    fin_section = data[fin_section_filter]
    
    start_section = interpolation(start_section, start, True, vol_col)
    fin_section = interpolation(fin_section, finish, False, vol_col)
    concated = pd.concat([start_section, full_sections, fin_section])

    return concated
    


def map_fact(project: pd.DataFrame, fact: pd.DataFrame) -> pd.DataFrame:
    """Подсчет фактических объемов на выбраннных пикетажных участках
        proj_vol (pd.DataFrame): проектны
        fact_vol (pd.DataFrame): объем выполненных работ

    Returns:
        pd.Series: поле с фактом работ на пикетажном участке
    """
    
    fact = fact.sort_values(by=['work_name', 'picket_start', 'picket_finish'])
    project = project.sort_values(by=['work_name', 'picket_start', 'picket_finish'])
    
    proj_values = project[['work_name', 'picket_start', 'picket_finish', 'vol_prd']].values
    fact_values = fact[['work_name', 'picket_start', 'picket_finish', 'vol_fact']].values
    
    mapped_fact = np.zeros(proj_values.shape[0])

    j = 0
    for i in range(proj_values.shape[0]):
        p_work, p_start, p_fin, p_vol = proj_values[i]
        f_work, f_start, f_fin, f_vol = fact_values[j]

        if p_start >= f_fin or p_fin <= f_start:
            continue
        
        vol_per_unit = f_vol / (f_fin - f_start)
        
        start = max(p_start, f_start)
        finish = min(p_fin, f_fin)
        length = finish - start
        
        mapped_fact[i] += length * vol_per_unit
        
        
            
            
def main():
    input_cost = 1000000
    input_start = 33
    input_fin = 59

    project_volume = volume_section(input_start, input_fin, P_RD, 'vol_prd')
    fact_volume = volume_section(input_start, input_fin, FACT, 'vol_fact')
    
    full_volume = map_fact(project_volume, fact_volume)
    

pd.options.mode.copy_on_write = True
    
main()

