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
from pdsql import mssql
from time import sleep
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

allo_csv = 'above_66401_allo_2020-08-12.csv'
#from_date = pd.Timestamp('2019-07-01 00:30:00')
#to_date = pd.Timestamp('2019-02-03')


try:

    ######################################
    ### Get detided data

    tsdata = get_ts_data(param['Output']['hydrotel_server'], 'hydrotel', param['Input']['detided_mtype'], str(param['Input']['site']), str(from_date), str(to_date), None).droplevel([0, 1])
    tsdata.name = 'detided'

    to_date = tsdata.index.max()

    try:

        #####################################
        ### Determine the Wap usage ratios
        up_takes1 = pd.read_csv(os.path.join(base_dir, allo_csv))
        up_takes2 = up_takes1[up_takes1.AllocatedRate > 0].copy()
        up_takes2['AllocatedRateSum'] = up_takes2.groupby('Wap')['AllocatedRate'].transform('sum')
        up_takes2['AllocatedRateRatio'] = up_takes2['AllocatedRate']/up_takes2['AllocatedRateSum']

        wap_ratios = up_takes2[up_takes2.HydroGroup == 'Surface Water'].groupby('Wap')['AllocatedRateRatio'].sum()
        wap_ratios.index.name = 'ExtSiteID'

        ####################################
        ### Pull out the Hilltop usage data

        ## Determine the sites available in Hilltop
        ht_sites = ws.site_list(param['Input']['hilltop_base_url'], param['Input']['hilltop_hts'])
        ht_sites['Wap'] = convert_site_names(ht_sites.SiteName)

        ht_sites1 = ht_sites[ht_sites['Wap'].isin(wap_ratios.index) & ~ht_sites['Wap'].isin(param['Input']['browns_rock_waps'])].copy()
        ht_sites1.rename(columns={'SiteName': 'Site'}, inplace=True)

        mtype_list = []
        for site in ht_sites1.Site:
            timer = 10
            while timer > 0:
                try:
                    m1 = ws.measurement_list(param['Input']['hilltop_base_url'], param['Input']['hilltop_hts'], site)
                    break
                except Exception as err:
                    err1 = err
                    timer = timer - 1
                    if timer == 0:
                        raise ValueError(err1)
                    else:
                        print(err1)
                        sleep(3)

            mtype_list.append(m1)
        mtypes = pd.concat(mtype_list).reset_index()

        mtypes1 = mtypes[mtypes.To >= from_date]
        mtypes2 = mtypes1[~mtypes1.Measurement.str.contains('regularity', case=False)].sort_values('To').drop_duplicates('Site', keep='last')

        ## Pull out the usage data and process
        tsdata_list = []
        for i, row in mtypes2.iterrows():
            timer = 10
            while timer > 0:
                try:
                    t1 = ws.get_data(param['Input']['hilltop_base_url'], param['Input']['hilltop_hts'], row['Site'], row['Measurement'], str(from_date), str(row['To']))
                    break
                except Exception as err:
                    err1 = err
                    timer = timer - 1
                    if timer == 0:
                        raise ValueError(err1)
                    else:
                        print(err1)
                        sleep(3)

            tsdata_list.append(t1)
        tsdata1 = pd.concat(tsdata_list)

        tsdata2 = util.proc_ht_use_data_ws(tsdata1)

        ## Apply WAP ratios
        tsdata2a = pd.merge(tsdata2.reset_index(), wap_ratios.reset_index(), on='ExtSiteID')
        tsdata2a['Rate'] = tsdata2a['AllocatedRateRatio'] * tsdata2a['Value']
        tsdata2b = tsdata2a.set_index(['ExtSiteID', 'DateTime'])[['Rate']]

        ## Reformat and aggregate to sngle time series
        tsdata3 = tsdata2b.unstack(0)[:to_date].droplevel(0, axis=1)
        other_ts = tsdata3.ffill().sum(axis=1)/15/60
        other_ts.name = 'other'

    except Exception as err:
        print('*Extraction of water usage data failed')
        print(err)
        alt_dates = pd.date_range(from_date, to_date, freq='15T')
        other_ts = pd.Series(0, index=alt_dates, name='other')
        other_ts.index.name = 'DateTime'

    #############################################
    ### Browns Rock data
    br_ts = get_ts_data(param['Input']['hydrotel_server'], 'hydrotel', sites=param['Input']['browns_rock_site'], mtypes=param['Input']['browns_rock_mtype'], from_date=str(from_date), to_date=str(to_date), resample_code=None).droplevel([0, 1])
    br_ts.name = 'br'

    #############################################
    ### Combine all datasets
    combo1 = pd.concat([tsdata, other_ts, br_ts], axis=1).interpolate()

    ## Add datasets to de-tided flow
    combo1['nat_flow'] = combo1['detided'] + combo1['other'] + combo1['br']
    combo2 = combo1.round(3).reset_index().copy()

    #####################################
    ### Clip data to last value in Hydrotel

    last_val1 = mssql.rd_sql(param['Output']['hydrotel_server'], 'hydrotel', stmt='select max(DT) from Samples where Point = {point}'.format(point=param['Output']['unmod_point'])).iloc[0][0]

    if isinstance(last_val1, pd.Timestamp):
        combo2 = combo2[combo2.DateTime > last_val1].copy()

    #####################################
    ### Save to Hydrotel and log result

    if not combo2.empty:

        combo2.rename(columns={'DateTime': 'DT'}, inplace=True)

        ## Nat flow
        nat_flow = combo2[['DT', 'nat_flow']].copy()

        nat_flow['Point'] = param['Output']['unmod_point']
        nat_flow['Quality'] = param['Output']['quality_code']
        nat_flow['BypassValidation'] = 0
        nat_flow['BypassAlarms'] = 0
        nat_flow['BypassScaling'] = 0
        nat_flow['BypassTimeOffset'] = 0
        nat_flow['Priority'] = 3
        nat_flow.rename(columns={'nat_flow': 'SampleValue'}, inplace=True)

        mssql.to_mssql(nat_flow, param['Output']['hydrotel_server'], 'hydrotel', 'SampleBuf')

        str1 = '{det} data points added to {mtype} (Point {point})'.format(det=len(combo2), mtype=param['Input']['unmod_mtype'], point=param['Output']['unmod_point'])

        print(str1)

#        util.log(run_time_start, from_date, combo2.DT.max(), 'Hydrotel', 'SampleBuf', 'pass', '{det} data points added to {mtype} (Point {point})'.format(det=len(combo2), mtype=param['Input']['unmod_mtype'], point=param['Output']['unmod_point']))

        ## Other flow
        other_flow = combo2[['DT', 'other']].copy()

        other_flow['Point'] = param['Output']['other_point']
        other_flow['Quality'] = param['Output']['quality_code']
        other_flow['BypassValidation'] = 0
        other_flow['BypassAlarms'] = 0
        other_flow['BypassScaling'] = 0
        other_flow['BypassTimeOffset'] = 0
        other_flow['Priority'] = 3
        other_flow.rename(columns={'other': 'SampleValue'}, inplace=True)

        mssql.to_mssql(other_flow, param['Output']['hydrotel_server'], 'hydrotel', 'SampleBuf')

        str1 = '{det} data points added to {mtype} (Point {point})'.format(det=len(combo2), mtype=param['Input']['other_mtype'], point=param['Output']['other_point'])

        print(str1)

#        util.log(run_time_start, from_date, combo2.DT.max(), 'Hydrotel', 'SampleBuf', 'pass', '{det} data points added to {mtype} (Point {point})'.format(det=len(combo2), mtype=param['Input']['other_mtype'], point=param['Output']['other_point']))

    else:
        print('No data needed to be added')
#        util.log(run_time_start, to_date, to_date, 'Hydrotel', 'SampleBuf', 'pass', 'No data needed to be added')


except Exception as err:
    err1 = err
    print(err1)
    util.log(run_time_start, from_date, to_date, 'Hydrotel', 'SampleBuf', 'fail', str(err1))
