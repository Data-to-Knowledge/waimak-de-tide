# -*- coding: utf-8 -*-
"""
Created on Fri Jun 14 14:16:18 2019

@author: michaelek
"""
import os
import pandas as pd
from pdsql import mssql
import yaml
import numpy as np
from hilltoppy.util import convert_site_names

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


def grp_ts_agg(df, grp_col, ts_col, freq_code):
    """
    Simple function to aggregate time series with dataframes with a single column of sites and a column of times.

    Parameters
    ----------
    df : DataFrame
        Dataframe with a datetime column.
    grp_col : str or list of str
        Column name that contains the sites.
    ts_col : str
        The column name of the datetime column.
    freq_code : str
        The pandas frequency code for the aggregation (e.g. 'M', 'A-JUN').

    Returns
    -------
    Pandas resample object
    """

    df1 = df.copy()
    if type(df[ts_col].iloc[0]) is pd.Timestamp:
        df1.set_index(ts_col, inplace=True)
        if type(grp_col) is list:
            grp_col.extend([pd.Grouper(freq=freq_code)])
        else:
            grp_col = [grp_col, pd.Grouper(freq=freq_code)]
        df_grp = df1.groupby(grp_col)
        return (df_grp)
    else:
        print('Make one column a timeseries!')


def proc_ht_use_data_ws(ht_data):
    """
    Function to process the water usage data at 15 min resolution.
    """

    ### Groupby mtypes and sites
    grp = ht_data.groupby(level=['Measurement', 'Site'])

    res1 = []
    for index, data1 in grp:
        data = data1.copy()
        mtype, site = index
#        units = sites[(sites.site == site) & (sites.mtype == mtype)].unit

        ### Select the process sequence based on the mtype and convert to period volume

        data[data < 0] = np.nan

        if mtype == 'Water Meter':
            ## Check to determine whether it is cumulative or period volume
            count1 = float(data.count())
            diff1 = data.diff()
            neg_index = diff1 < 0
            neg_ratio = sum(neg_index.values)/count1
            if neg_ratio > 0.1:
                vol = data
            else:
                # Replace the negative values with zero and the very large values
                diff1[diff1 < 0] = data[diff1 < 0]
                vol = diff1
        elif mtype in ['Compliance Volume', 'Volume']:
            vol = data
        elif mtype == 'Flow':
#            vol = (data * 60*60*24).fillna(method='ffill').round(4)
            vol = (data * 60*15)
        else:
            continue

        res1.append(vol)

    ### Convert to dataframe
    df1 = pd.concat(res1).reset_index()

    ### Drop the mtypes level and uppercase the sites
    df2 = df1.drop('Measurement', axis=1)
    df2.loc[:, 'Site'] = df2.loc[:, 'Site'].str.upper()

    ### Remove duplicate WAPs
    df3 = df2.groupby(['Site', 'DateTime']).Value.last().reset_index()

    df3.loc[:, 'Site'] = convert_site_names(df3.Site)
    ht4 = df3[df3.Site.notnull()].copy()
    ht4.rename(columns={'Site': 'ExtSiteID'}, inplace=True)

    ht5 = grp_ts_agg(ht4, 'ExtSiteID', 'DateTime', '15T').sum().round()

    return ht5











