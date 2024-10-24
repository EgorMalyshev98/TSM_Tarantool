import pandas as pd


class TechRequire:
    def __init__(self, resources: pd.DataFrame, norms: pd.DataFrame):
        self.resources = resources
        self.norms = norms
    
    def workload_constrain(self, require_workload: pd.DataFrame, num_days: int):
        """

        Args:
            require_workload (pd.DataFrame): _description_
            num_days (int): _description_

        Returns:
            _type_: _description_
        """
        res = (self.resources
                 .copy()
                 .assign(workload_limit=lambda df: df['quantity'] * 10 * df['shift_work'] * num_days)
                 .drop(columns=['quantity', 'shift_work'])
                 )
        
        cum_workload = (require_workload
         .merge(res, how='left', on='technique_type')
         .assign(cum_workload=lambda df: df.groupby('technique_type')['require_workload'].cumsum()))
        
        last_oper = cum_workload[cum_workload['workload_limit'] < cum_workload['cum_workload']]['sort_key'].min()
        
        return last_oper
        
        
    def require_workload(self, operations: pd.DataFrame) -> pd.DataFrame:
        """Определение требуемой трудоемкости

        Args:
            operations (pd.DataFrame):
                operation_type (object): тип работы
                vol_remain (float): объем работ
                
        Returns:
            pd.DataFrame: _description_
        """
        return (operations
            .merge(self.norms[['operation_type', 'technique_type', 'workload_1000_units', 'num_of_tech']], how='left', on='operation_type')
            .assign(require_workload=lambda df: df['vol_remain'] * df['workload_1000_units'] * df['num_of_tech'] / 1000)
            .drop(columns=['workload_1000_units', 'num_of_tech']))
    
    
    
        
        