# -*- coding: utf-8 -*-
"""
Created on Fri Jun 14 14:16:18 2019

@author: michaelek
"""
import os
import pandas as pd
from pdsql import mssql
import yaml

############################################
### Parameters

base_dir = os.path.realpath(os.path.dirname(__file__))

with open(os.path.join(base_dir, 'parameters.yml')) as param:
        param = yaml.safe_load(param)


############################################
### Misc functions

## Log
def log(run_time_start, data_from_time, data_to_time, database_name, table_name, run_result, comment):
    """

    """
    run_time_end = pd.Timestamp.today()
    log1 = pd.DataFrame([[run_time_start, run_time_end, data_from_time, data_to_time, database_name[:99], table_name[:99], run_result[:9], comment[:299]]], columns=['RunTimeStart', 'RunTimeEnd', 'DataFromTime', 'DataToTime', 'DatabaseName', 'TableName', 'RunResult', 'Comment'])
    mssql.to_mssql(log1, param['Output']['log_server'], param['Output']['log_database'], param['Output']['log_table'])

    return log1














