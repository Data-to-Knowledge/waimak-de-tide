# -*- coding: utf-8 -*-
"""
Created on Wed Oct  9 08:54:51 2019

@author: michaelek
"""
import os
import pandas as pd
from pyhydrotel import get_ts_data, get_sites_mtypes
import yaml
from pdsql import mssql

pd.set_option('display.max_columns', 10)
pd.set_option('display.max_rows', 30)
run_time_start = pd.Timestamp.today()

###########################################
### Parameters

base_dir = os.path.realpath(os.path.dirname(__file__))

with open(os.path.join(base_dir, 'parameters.yml')) as param:
        param = yaml.safe_load(param)

input_server = 'sql2012prod04'
output_server = 'sql2012dev01'

sites_mtypes = {'66401': ['flow', 'flow detided', 'flow detided + browns rock'], '66450': ['flow']}

from_date = '2019-10-08'
to_date = '2019-10-11'


##########################################
### Extract and load

for k, v in sites_mtypes.items():
    print(k, v)
    for m in v:
        print(m)

        ## Get the data out
        data1 = get_ts_data(input_server, 'Hydrotel', m, k, from_date, to_date, resample_code=None).reset_index()
        ids = get_sites_mtypes(output_server, 'Hydrotel', m, k).iloc[0]

        ## Prepare the data
        data2 = data1.drop(['ExtSiteID', 'MType'], axis=1).rename(columns={'DateTime': 'DT', 'Value': 'SampleValue'})

        data2['Point'] = ids['Point']
        data2['Quality'] = param['Output']['quality_code']

        ## Save the data
        new_data = mssql.update_from_difference(data2, output_server, 'Hydrotel', 'Samples', on=['Point', 'DT'])





































