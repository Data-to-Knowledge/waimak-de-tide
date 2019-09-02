# -*- coding: utf-8 -*-
"""
Created on Sun Sep 30 12:33:58 2018

@author: michaelek
"""
import os
import pandas as pd
from hilltoppy import web_service as ws
from hilltoppy.util import convert_site_names
from pyhydrotel import get_ts_data, get_sites_mtypes
from flownat import FlowNat
from pdsql import mssql
import yaml
import util

pd.set_option('display.max_columns', 10)
pd.set_option('display.max_rows', 30)
run_time_start = pd.Timestamp.today()

######################################
### Parameters

base_dir = os.path.realpath(os.path.dirname(__file__))

with open(os.path.join(base_dir, 'parameters.yml')) as param:
        param = yaml.safe_load(param)

if run_time_start.hour < 12:
    to_date = run_time_start.floor('D') - pd.DateOffset(hours=12)
else:
    to_date = run_time_start.floor('D') + pd.DateOffset(hours=12)

from_date = (to_date - pd.DateOffset(days=3)).floor('D')

#from_date = pd.Timestamp('2019-02-01')
#to_date = pd.Timestamp('2019-02-03')


try:

    ######################################
    ### Get data

    ## Detided data
    tsdata = get_ts_data(param['Output']['hydrotel_server'], 'hydrotel', param['Input']['detided_mtype'], str(param['Input']['site']), str(from_date), str(to_date), None).droplevel([0, 1])
    tsdata.name = 'detided'

    ## Determine the Wap usage ratios
    fn1 = FlowNat(from_date=from_date, to_date=to_date, rec_data_code='RAW', input_sites=str(param['Input']['site']))

    up_takes1 = fn1.upstream_takes()
    up_takes2 = up_takes1[up_takes1.AllocatedRate > 0].copy()
    up_takes2['AllocatedRateSum'] = up_takes2.groupby('Wap')['AllocatedRate'].transform('sum')
    up_takes2['AllocatedRateRatio'] = up_takes2['AllocatedRate']/up_takes2['AllocatedRateSum']

    wap_ratios = up_takes2[up_takes2.HydroFeature == 'Surface Water'].groupby('Wap')['AllocatedRateRatio'].sum()

    ## Pull out the usage data
    # Hilltop
    ht_sites = ws.site_list(param['Input']['hilltop_base_url'], param['Input']['hilltop_hts'])
    ht_sites['Wap'] = convert_site_names(ht_sites.SiteName)

    ht_sites1 = ht_sites[ht_sites['Wap'].isin(wap_ratios.index) & ~ht_sites['Wap'].isin(param['Input']['browns_rock_waps'])].copy()
    ht_sites1.rename(columns={'SiteName': 'Site'}, inplace=True)

    mtype_list = []
    for site in ht_sites1.Site:
        m1 = ws.measurement_list(param['Input']['hilltop_base_url'], param['Input']['hilltop_hts'], site)
        mtype_list.append(m1)
    mtypes = pd.concat(mtype_list).reset_index()

    mtypes1 = mtypes[mtypes.To >= from_date]
    mtypes2 = mtypes1[~mtypes1.Measurement.str.contains('regularity', case=False)].sort_values('To').drop_duplicates('Site', keep='last')

    tsdata_list = []
    for i, row in mtypes2.iterrows():
        t1 = ws.get_data(param['Input']['hilltop_base_url'], param['Input']['hilltop_hts'], row['Site'], row['Measurement'], str(from_date), str(row['To']))
        tsdata_list.append(t1)
    tsdata1 = pd.concat(tsdata_list)

    tsdata2 = util.proc_ht_use_data_ws(tsdata1)

    tsdata3 = tsdata2.unstack(0)[:to_date].droplevel(0, axis=1)
    other_ts = tsdata3.ffill().sum(axis=1)/15/60
    other_ts.name = 'other'

    # Hydrotel
    br_ts = get_ts_data(param['Input']['hydrotel_server'], 'hydrotel', sites=param['Input']['browns_rock_site'], mtypes=param['Input']['browns_rock_mtype'], from_date=str(from_date), to_date=str(to_date), resample_code=None).droplevel([0, 1])
    br_ts.name = 'br'

    ## Combine all datasets
    combo1 = pd.concat([tsdata, other_ts, br_ts], axis=1).interpolate()

    ## Add datasets to de-tided flow
    combo1['nat_flow'] = combo1['detided'] + combo1['other'] + combo1['br']
    combo2 = combo1['nat_flow'].round(3).reset_index().copy()

    #####################################
    ### Clip data to last value in Hydrotel

    last_val1 = mssql.rd_sql(param['Output']['hydrotel_server'], 'hydrotel', stmt='select max(DT) from Samples where Point = {point}'.format(point=param['Output']['unmod_point'])).iloc[0][0]

    if isinstance(last_val1, pd.Timestamp):
        combo2 = combo2[combo2.DateTime > last_val1].copy()

    #####################################
    ### Save to Hydrotel and log result

    if not combo2.empty:
        combo2['Point'] = param['Output']['unmod_point']
        combo2['Quality'] = param['Output']['quality_code']
        combo2.rename(columns={'DateTime': 'DT', 'nat_flow': 'SampleValue'}, inplace=True)

        mssql.to_mssql(combo2, param['Output']['hydrotel_server'], 'hydrotel', 'Samples')

        util.log(run_time_start, from_date, combo2.DT.max(), 'Hydrotel', 'Samples', 'pass', '{det} data points added to {mtype} (Point {point})'.format(det=len(combo2), mtype=param['Input']['unmod_mtype'], point=param['Output']['unmod_point']))

    else:
        util.log(run_time_start, to_date, to_date, 'Hydrotel', 'Samples', 'pass', 'No data needed to be added')


except Exception as err:
    err1 = err
    print(err1)
    util.log(run_time_start, from_date, to_date, 'Hydrotel', 'Samples', 'fail', str(err1))


