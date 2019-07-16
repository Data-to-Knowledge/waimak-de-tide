# -*- coding: utf-8 -*-
"""
Created on Sun Sep 30 12:33:58 2018

@author: michaelek
"""
import os
import pandas as pd
from pyhydrotel import get_ts_data, get_sites_mtypes
from pdsql import mssql
import detidelevel as dtl
import yaml

pd.options.display.max_columns = 10
run_time_start = pd.Timestamp.today()

######################################
### Parameters

base_dir = os.path.realpath(os.path.dirname(__file__))

with open(os.path.join(base_dir, 'parameters.yml')) as param:
        param = yaml.safe_load(param)

to_date = run_time_start.floor('H')
from_date = (to_date - pd.DateOffset(days=7)).round('D')

######################################
### Determine last saved values


######################################
### Get data

tsdata = get_ts_data(param['Input']['hydrotel_server'], param['Input']['hydrotel_database'], param['Input']['mtype'], str(param['Input']['site']), str(from_date), str(to_date), None)

tsdata1 = dtl.util.tsreg(tsdata.unstack(1).reset_index().drop(['ExtSiteID'], axis=1).set_index('DateTime')).interpolate('time')

roll1 = tsdata1[[param['Input']['mtype']]].rolling(12, center=True).mean().dropna()
roll1.columns = ['smoothed original']

######################################
### Run detide

#det1 = dtl.detide(roll1, float(param.quantile))

det2 = dtl.plot.plot_detide(roll1, float(param['Input']['quantile']), output_path=output_path)









######################################
###

#ts1 = mssql.rd_sql('edwprod01', 'hydro', 'TSDataNumericDaily', ['DateTime', 'Value'], where_in={'DatasetTypeID': [5], 'ExtSiteID': ['66403']})
#
#ts1['DateTime'] = pd.to_datetime(ts1['DateTime'])
#ts1.set_index('DateTime', inplace=True)
#ts1.loc['2019-01-01':'2019-01-15'].plot()





