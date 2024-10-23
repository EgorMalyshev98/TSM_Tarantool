import pandas as pd
from dataclasses import dataclass


CONTRACT = pd.read_csv('data_samples/contract.csv', sep='#')
P_RD = pd.read_csv('data_samples/prd.csv', sep='#')
FACT = pd.read_csv('data_samples/fact.csv', sep='#')
TECH_RES = pd.read_csv('data_samples/avail_res.csv', sep='#')
NORMS = pd.read_csv('data_samples/norm.csv', sep='#') 
TECHNOLOGY = pd.read_csv('data_samples/technology.csv', sep='#')


@dataclass(frozen=True)
class DataSources:
    prd: pd.DataFrame
    fact: pd.DataFrame
    contract: pd.DataFrame
    norms: pd.DataFrame
    hierarchy: pd.DataFrame
    resources: pd.DataFrame
    
prd_cols = ['num_con', 'operation_type', 'picket_start', 'picket_finish', 'vol_prd']
fact_cols = ['operation_type', 'picket_start', 'picket_finish', 'vol_fact']
    
        
data = DataSources(
        prd = P_RD[prd_cols],
        fact = FACT[fact_cols],
        contract = CONTRACT[['num_con', 'price']],
        norms = NORMS[['operation_type', 'technique_type', 'num_of_tech', 'workload_1000_units']],
        hierarchy = TECHNOLOGY[['operation_type', 'hierarchy']],
        resources = TECH_RES[['technique_type', 'quantity', 'shift_work']]
    )