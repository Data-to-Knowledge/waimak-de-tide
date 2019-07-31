# -*- coding: utf-8 -*-
"""
Created on Sun Sep 30 12:33:58 2018

@author: michaelek
"""
import os
import pandas as pd
from pyhydrotel import get_ts_data, get_sites_mtypes, get_mtypes
from pdsql import mssql
import detidelevel as dtl
import yaml

pd.options.display.max_columns = 10

######################################
### Parameters

output_path = r'E:\ecan\shared\projects\de-tide\de-tide_2019-07-04.html'
from_date = '2019-07-13'
to_date = '2019-07-22'

## Hydrotel test
server = 'sql2012dev01'
db = 'hydrotel'
ts_table = 'Samples'
point_val = 7229

base_dir = os.path.realpath(os.path.dirname(__file__))

with open(os.path.join(base_dir, 'parameters.yml')) as param:
        param = yaml.safe_load(param)

######################################
### Determine last saved values


######################################
### Get data

tsdata = get_ts_data(param['Input']['server'], param['Input']['database'], param['Input']['mtype'], str(param['Input']['site']), str(from_date), str(to_date), None)

tsdata1 = dtl.util.tsreg(tsdata.unstack(1).reset_index().drop(['ExtSiteID'], axis=1).set_index('DateTime')).interpolate('time')

roll1 = tsdata1[[param['Input']['mtype']]].rolling(12, center=True).mean().dropna()
roll1.columns = ['smoothed original']

######################################
### Run detide

#det1 = dtl.detide(roll1, float(param.quantile))

#det2 = dtl.plot.plot_detide(roll1, float(param['Input']['quantile']), output_path=output_path)





######################################
### Check hydrotel data

new_tsdata1 = mssql.rd_sql(server, db, ts_table, ['DT', 'SampleValue'], where_in={'Point': [point_val]}, from_date=from_date, to_date=to_date, date_col='DT', rename_cols=['DateTime', 'de-tided']).set_index('DateTime')

combo1 = pd.concat([roll1, new_tsdata1], axis=1)
combo1.plot()

######################################
###

ts1 = mssql.rd_sql('edwprod01', 'hydro', 'TSDataNumericDaily', ['DateTime', 'Value'], where_in={'DatasetTypeID': [5], 'ExtSiteID': ['66403']})
#
#ts1['DateTime'] = pd.to_datetime(ts1['DateTime'])
#ts1.set_index('DateTime', inplace=True)
#ts1.loc['2019-01-01':'2019-01-15'].plot()

server = 'sql2012prod05'
database = 'hydrotel'
mtypes = ['Hmax', 'Hs', 'Tz', 'Dir']
sites = ['3373011', '3373012']

mtypes1 = get_mtypes(server, database)

sites_mtypes = get_sites_mtypes(server, database, mtypes)

data1 = get_ts_data(server, database, mtypes, sites).reset_index()

data1.groupby(['ExtSiteID', 'MType'])['DateTime'].describe()















