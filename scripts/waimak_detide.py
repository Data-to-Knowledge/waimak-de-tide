# -*- coding: utf-8 -*-
"""
Created on Sun Sep 30 12:33:58 2018

@author: michaelek
"""
import os
import pandas as pd
from pyhydrotel import get_ts_data
from pdsql import mssql
import detidelevel as dtl
import yaml
import util

pd.options.display.max_columns = 10
run_time_start = pd.Timestamp.today()

######################################
### Parameters

base_dir = os.path.realpath(os.path.dirname(__file__))

with open(os.path.join(base_dir, 'parameters.yml')) as param:
        param = yaml.safe_load(param)

to_date = run_time_start.floor('H')
from_date = (to_date - pd.DateOffset(days=7)).round('D')

try:
    ######################################
    ### Get data

    tsdata = get_ts_data(param['Input']['hydrotel_server'], 'hydrotel', param['Input']['mtype'], str(param['Input']['site']), str(from_date), str(to_date), None)

    tsdata1 = dtl.util.tsreg(tsdata.unstack(1).reset_index().drop(['ExtSiteID'], axis=1).set_index('DateTime')).interpolate('time')

    roll1 = tsdata1[[param['Input']['mtype']]].rolling(12, center=True).mean().dropna()
    roll1.columns = ['smoothed original']

    ######################################
    ### Run detide

    det1 = dtl.detide(roll1, float(param['Input']['quantile'])).round(3).reset_index()

#    det2 = dtl.plot.plot_detide(roll1, float(param['Input']['quantile']))

    #####################################
    ### Clip data to last value in Hydrotel

    last_val1 = mssql.rd_sql(param['Output']['hydrotel_server'], 'hydrotel', stmt='select max(DT) from Samples where Point = {point}'.format(point=param['Output']['detided_point'])).iloc[0][0]

    if isinstance(last_val1, pd.Timestamp):
        det1 = det1[det1.DateTime > last_val1].copy()

    #####################################
    ### Save to Hydrotel and log result

    if not det1.empty:
        det1['Point'] = param['Output']['detided_point']
        det1['Quality'] = param['Output']['quality_code']
        det1.rename(columns={'DateTime': 'DT', 'de-tided': 'SampleValue'}, inplace=True)

        mssql.to_mssql(det1, param['Output']['hydrotel_server'], 'hydrotel', 'Samples')

        util.log(run_time_start, from_date, det1.DT.max(), 'Hydrotel', 'Samples', 'pass', '{det} data points added to {mtype} (Point {point})'.format(det=len(det1), mtype=param['Input']['detided_mtype'], point=param['Output']['detided_point']))

    else:
        util.log(run_time_start, to_date, to_date, 'Hydrotel', 'Samples', 'pass', 'No data needed to be added')


except Exception as err:
    err1 = err
    print(err1)
    util.log(run_time_start, from_date, to_date, 'Hydrotel', 'Samples', 'fail', str(err1))


