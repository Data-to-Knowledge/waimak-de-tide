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


detided_mtype = create_site_mtype(param['Output']['hydrotel_server'], 'hydrotel', param['Input']['site'], param['Input']['ref_point'], param['Input']['detided_mtype'])

unmod_mtype = create_site_mtype(param['Output']['hydrotel_server'], 'hydrotel', param['Input']['site'], param['Input']['ref_point'], param['Input']['unmod_mtype'])

other_abstr = create_site_mtype(param['Output']['hydrotel_server'], 'hydrotel', param['Input']['site'], param['Input']['ref_point'], param['Input']['other_mtype'])


