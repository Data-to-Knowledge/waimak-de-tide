# -*- coding: utf-8 -*-
"""
Created on Sun Sep 30 12:33:58 2018

@author: michaelek
"""
import pandas as pd
from pyhydrotel import get_ts_data
import detidelevel as dtl
import parameters as param

pd.options.display.max_columns = 10

######################################
### Parameters

output_path = r'E:\ecan\shared\projects\de-tide\de-tide_2018-10-16.html'


######################################
### Determine last saved values


######################################
### Get data

tsdata = get_ts_data(param.hydrotel_server, param.hydrotel_database, param.mtype, param.site, from_date, None, None)

tsdata1 = dtl.util.tsreg(tsdata.unstack(1).reset_index().drop(['ExtSiteID'], axis=1).set_index('DateTime')).interpolate('time')

roll1 = tsdata1[['water level']].rolling(12, center=True).mean().dropna()
roll1.columns = ['smoothed original']

######################################
### Run detide

det1 = dtl.detide(roll1, param.quantile)

det2 = dtl.plot.plot_detide(roll1, param.quantile, output_path=output_path)



















