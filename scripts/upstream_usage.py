# -*- coding: utf-8 -*-
"""
Created on Sun Sep 30 12:33:58 2018

@author: michaelek
"""
import os
import pandas as pd
from hilltoppy import web_service as ws
from hilltoppy.util import convert_site_names
from pyhydrotel import get_ts_data
from flownat import FlowNat
from pdsql import mssql
import yaml
import util

pd.options.display.max_columns = 10
run_time_start = pd.Timestamp.today()

######################################
### Parameters

base_dir = os.path.realpath(os.path.dirname(__file__))

with open(os.path.join(base_dir, 'parameters.yml')) as param:
        param = yaml.safe_load(param)

to_date = run_time_start.floor('D')
from_date = (to_date - pd.DateOffset(days=3)).round('D')

try:
    ######################################
    ### Determine last generated unmod flow date
    last_val1 = mssql.rd_sql(param['Output']['hydrotel_server'], 'hydrotel', stmt='select max(DT) from Samples where Point = {point}'.format(point=param['Output']['unmod_point'])).iloc[0][0]

    if last_val1 is None:
        last_val1 = pd.Timestamp('1900-01-01')

    ######################################
    ### Get data

    ## Detided data
    tsdata = get_ts_data(param['Output']['hydrotel_server'], 'hydrotel', param['Input']['detided_mtype'], str(param['Input']['site']), str(last_val1), None, None)[1:]

    ## Determine the Wap usage ratios
    fn1 = FlowNat(from_date=from_date, to_date=to_date, rec_data_code='RAW', input_sites=str(param['Input']['site']))

    up_takes1 = fn1.upstream_takes()
    up_takes2 = up_takes1[up_takes1.AllocatedRate > 0].copy()
    up_takes2['AllocatedRateSum'] = up_takes2.groupby('Wap')['AllocatedRate'].transform('sum')
    up_takes2['AllocatedRateRatio'] = up_takes2['AllocatedRate']/up_takes2['AllocatedRateSum']

    wap_ratios = up_takes2[up_takes2.HydroFeature == 'Surface Water'].groupby('Wap')['AllocatedRateRatio'].sum()

    ## Pull out the usage data
    ht_sites = ws.site_list(param['Input']['hilltop_base_url'], param['Input']['hilltop_hts'])
    ht_sites['Wap'] = convert_site_names(ht_sites.SiteName)

    ht_sites1 = ht_sites[ht_sites['Wap'].isin(wap_ratios.index)].copy()
    ht_sites1.rename(columns={'SiteName': 'Site'}, inplace=True)

    mtype_list = []
    for site in ht_sites1.Site:
        m1 = ws.measurement_list(param['Input']['hilltop_base_url'], param['Input']['hilltop_hts'], site)
        mtype_list.append(m1)
    mtypes = pd.concat(mtype_list).reset_index()

    mtypes1 = mtypes[mtypes.To >= from_date]
    mtypes2 = mtypes1[~mtypes1.Measurement.str.contains('regularity', case=False)].sort_values('To').drop_duplicates('Site', keep='last')






    ######################################
    ### Run detide

    det1 = dtl.detide(roll1, float(param['Input']['quantile'])).round(3).reset_index()

#    det2 = dtl.plot.plot_detide(roll1, float(param['Input']['quantile']))

    mtypes3 = pd.merge(ht_sites1, mtypes2.drop(['DataType', 'Units'], axis=1), on='Site')
    takes1 = pd.merge(up_takes2[['RecordNumber', 'HydroFeature', 'AllocationBlock', 'Wap', 'FromDate', 'ToDate', 'FromMonth', 'ToMonth', 'AllocatedRate', 'AllocatedAnnualVolume', 'WaterUse', 'ConsentStatus']], mtypes3, on='Wap', how='left').sort_values('AllocatedRate', ascending=False)
    takes1.to_csv(os.path.join(base_dir, 'waimak_consents_2019-07-24.csv'), index=False)

    #####################################
    ### Clip data to last value in Hydrotel

    last_val1 = mssql.rd_sql(param['Output']['hydrotel_server'], 'hydrotel', stmt='select max(DT) from Samples where Point = {point}'.format(point=param['Output']['new_point'])).iloc[0][0]

    if isinstance(last_val1, pd.Timestamp):
        det1 = det1[det1.DateTime > last_val1].copy()

    #####################################
    ### Save to Hydrotel and log result

    if not det1.empty:
        det1['Point'] = param['Output']['new_point']
        det1['Quality'] = param['Output']['quality_code']
        det1.rename(columns={'DateTime': 'DT', 'de-tided': 'SampleValue'}, inplace=True)

        mssql.to_mssql(det1, param['Output']['server'], param['Input']['database'], 'Samples')
        util.log(run_time_start, from_date, det1.DT.max(), 'Hydrotel', 'Samples', 'pass', '{det} data points added to {mtype} (Point {point})'.format(det=len(det1), mtype=param['Input']['new_mtype'], point=param['Output']['new_point']))

    else:
        util.log(run_time_start, to_date, to_date, 'Hydrotel', 'Samples', 'pass', 'No data needed to be added')


except Exception as err:
    err1 = err
    print(err1)
    util.log(run_time_start, from_date, to_date, 'Hydrotel', 'Samples', 'fail', str(err1))


