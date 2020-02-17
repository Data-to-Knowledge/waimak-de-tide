# -*- coding: utf-8 -*-
"""
Created on Wed Apr 25 07:49:41 2018

@author: michaelek

This module runs through the sequence of other python modules for updating the Waimak unmod flow in Hydrotel
"""
import time

print('Remove the tides!')
import waimak_detide

time.sleep(10)

print('Create naturalised flows')
import waimak_unmod_flow
