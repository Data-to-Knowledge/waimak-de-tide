# -*- coding: utf-8 -*-
"""
Created on Tue Apr 24 09:29:52 2018

@author: michaelek
"""
import os
import pandas as pd
from configparser import ConfigParser


#####################################
### Parameters

## Generic
print('load parameters')

py_dir = os.path.realpath(os.path.dirname(__file__))

ini1 = ConfigParser()
ini1.read([os.path.join(py_dir, os.path.splitext(__file__)[0] + '.ini')])

hydrotel_server = str(ini1.get('Input', 'hydrotel_server'))
hydrotel_database = str(ini1.get('Input', 'hydrotel_database'))
mtype = str(ini1.get('Input', 'mtype'))
quantile = str(ini1.get('Input', 'quantile'))
site = str(ini1.get('Input', 'site'))
