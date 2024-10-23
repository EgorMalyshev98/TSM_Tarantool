from os import name
import pandas as pd


class Tech:
    def __init__(self, resources: pd.DataFrame, norms: pd.DataFrame, num_days: int):
        self.resources = self._workload_limit(resources, num_days)
        self.norms = norms
    
        
    @staticmethod
    def _workload_limit(resourсes: pd.DataFrame, num_days: int):\
        return (resourсes
                 .copy()
                 .assign(workload_limit=lambda df: df['quantity'] * 10 * df['shift_work'] * num_days)
                 .drop(columns=['quantity', 'shift_work'])
                 )
        
        
    def require_workload(self, operations: pd.DataFrame) -> pd.Series:
        """Определение требуемой трудоемкости

        Args:
            operations (pd.DataFrame):
                operation_type (object): тип работы
                vol_remain (float): объем работ
                
        Returns:
            pd.DataFrame: _description_
        """
        return (operations
            .merge(self.norms, how='left', on='operation_type')
            .merge(self.resources, how='left', on='technique_type')
            .assign(require_workload=lambda df: df['vol_remain'] * df['workload_1000_units'] * df['num_of_tech'] / 1000)
            .drop(columns=['workload_1000_units', 'num_of_tech'])
        )
        
    
    
if __name__ == '__main__':
    from dev_data_source import data
    
    plan_opers = \
    [[1.1, 'snt_prs', 33.0, 40.0, 700.0, 229.06403940886702, 470.935960591133, 1, 117733.99014778325, 0],
    [1.3, 'ust_nasp', 33.0, 40.0, 700.0, 525.0, 175.0, 2, 96250.0, 1],
    [1.4, 'ust_kv', 33.0, 40.0, 140.0, 70.0, 70.0, 2, 14000.0, 2],
    [2.1, 'ust_geotxtl', 33.0, 40.0, 1088.8888888888887, 466.6666666666667, 622.222222222222, 3, 118222.22222222218, 3],
    [2.2, 'ust_pps', 33.0, 40.0, 466.6666666666667, 233.33333333333334, 233.33333333333334, 4, 182000.0, 4],
    [1.1, 'snt_prs', 40.0, 59.0, 1540.5405405405406, 609.090909090909, 931.4496314496316, 1, 232862.4078624079, 0],
    [1.3, 'ust_nasp', 40.0, 50.0, 1000.0, 750.0, 250.0, 2, 137500.0, 1],
    [1.4, 'ust_kv', 40.0, 50.0, 200.0, 100.0, 100.0, 2, 20000.0, 2],
    [2.1, 'ust_geotxtl', 40.0, 45.0, 777.7777777777777, 333.33333333333337, 444.44444444444434, 3, 84444.44444444442, 3],
    [2.2, 'ust_pps', 40.0, 45.0, 333.33333333333337, 166.66666666666669, 166.66666666666669, 4, 130000.00000000001, 4],
    [2.1, 'ust_geotxtl', 45.0, 59.0, 2800.0, 400.0, 2400.0, 3, 456000.0, 5],
    [2.2, 'ust_pps', 45.0, 59.0, 1200.0, 600.0, 600.0, 4, 468000.0, 6],
    [1.4, 'ust_kv', 50.0, 59.0, 180.0, 126.0, 54.0, 2, 10800.0, 7]]
    
    cols = ['num_con', 'operation_type', 'start_p', 'finish_p', 'volume_p', 'volume_f', 'vol_remain', 'hierarchy', 'cost_remain', 'sort_key']
    
    df = pd.DataFrame(plan_opers, columns=cols)[['operation_type', 'vol_remain']]
    
    num_days = 1
    
    tech = Tech(data.resources, data.norms, num_days=1)
    
    require_workload = tech.require_workload(df)
    
    print(require_workload)
    
    
    
        
        