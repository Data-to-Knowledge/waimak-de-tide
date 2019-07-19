# -*- coding: utf-8 -*-
"""
Created on Sun Sep 30 12:33:58 2018

@author: michaelek
"""
import pandas as pd
from pyhydrotel import get_ts_data, get_sites_mtypes
from pdsql import mssql
import detidelevel as dtl

pd.options.display.max_columns = 10

######################################
### Parameters

output_path = r'E:\ecan\shared\projects\de-tide\de-tide_2019-07-04.html'
from_date = '2019-07-01'
to_date = '2019-07-14 19:00'

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

det2 = dtl.plot.plot_detide(roll1, float(param['Input']['quantile']), output_path=output_path)









######################################
###

ts1 = mssql.rd_sql('edwprod01', 'hydro', 'TSDataNumericDaily', ['DateTime', 'Value'], where_in={'DatasetTypeID': [5], 'ExtSiteID': ['66403']})
#
#ts1['DateTime'] = pd.to_datetime(ts1['DateTime'])
#ts1.set_index('DateTime', inplace=True)
#ts1.loc['2019-01-01':'2019-01-15'].plot()





