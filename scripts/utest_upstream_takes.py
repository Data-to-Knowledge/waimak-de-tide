# -*- coding: utf-8 -*-
"""
Created on Wed Apr 24 09:32:35 2019

@author: michaelek
"""
import os
import pandas as pd
from gistools import rec, vector
from ecandbparams import sql_arg
from allotools import AlloUsage
from pdsql import mssql

pd.options.display.max_columns = 10
today1 = str(pd.Timestamp.today().date())

#######################################
### Parameters

py_path = os.path.realpath(os.path.dirname(__file__))
project_path = os.path.split(py_path)[0]

site = '66401'

server = 'edwprod01'
database = 'hydro'
sites_table = 'ExternalSite'
crc_wap_table = 'CrcWapAllo'

rec_rivers_sql = 'rec_rivers_gis'
rec_catch_sql = 'rec_catch_gis'

from_date = '2019-04-01'
to_date = '2019-06-30'

results_path = os.path.join(project_path, 'results')

catch_del_shp = 'catch_del_66401_{}.shp'.format(today1)
allo_csv = 'allo_66401_{}.csv'.format(today1)
allo_wap_csv = 'allo_wap_66401_{}.csv'.format(today1)
wap_shp = 'wap_66401_{}.shp'.format(today1)

######################################
### Read in data

sites1 = mssql.rd_sql(server, database, sites_table, ['ExtSiteID', 'NZTMX', 'NZTMY'])

sites0 = sites1[sites1.ExtSiteID == site].copy()

sites2 = vector.xy_to_gpd('ExtSiteID', 'NZTMX', 'NZTMY', sites0)

sql1 = sql_arg()

rec_rivers_dict = sql1.get_dict(rec_rivers_sql)
rec_catch_dict = sql1.get_dict(rec_catch_sql)


###################################
### Catchment delineation and WAPs

catch1 = rec.catch_delineate(sites2, rec_rivers_dict, rec_catch_dict)
catch1.to_file(os.path.join(results_path, catch_del_shp))

wap1 = mssql.rd_sql(server, database, crc_wap_table, ['wap']).wap.unique()

sites3 = sites1[sites1.ExtSiteID.isin(wap1)].copy()

sites4 = vector.xy_to_gpd('ExtSiteID', 'NZTMX', 'NZTMY', sites3)

sites5 = vector.sel_sites_poly(sites4, catch1)
sites5.to_file(os.path.join(results_path, wap_shp))

##################################
### Get crc data

allo1 = AlloUsage(crc_wap_filter={'wap': sites5.ExtSiteID.tolist()}, from_date=from_date, to_date=to_date)

#allo1.allo[allo1.allo.crc_status == 'Terminated - Replaced']

allo1.allo_wap.to_csv(os.path.join(results_path, allo_wap_csv))
allo1.allo.to_csv(os.path.join(results_path, allo_csv))




























