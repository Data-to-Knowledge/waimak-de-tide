# -*- coding: utf-8 -*-
"""
Created on Tue Jul 16 13:58:23 2019

@author: michaelek
"""
import os
import pandas as pd
from pyhydrotel import create_site_mtype
import yaml

pd.options.display.max_columns = 10
run_time_start = pd.Timestamp.today()

######################################
### Parameters

base_dir = os.path.realpath(os.path.dirname(__file__))

with open(os.path.join(base_dir, 'parameters.yml')) as param:
        param = yaml.safe_load(param)


new_mtype = create_site_mtype(param['Output']['server'], param['Input']['database'], param['Input']['site'], param['Input']['ref_point'], param['Input']['new_mtype'])

