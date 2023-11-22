# -*- coding: utf-8 -*-
"""
Created on Mon Oct 25 09:13:51 2021

@author: chris.kerklaan
"""
import os
import sys
from pathlib import Path

NOTEBOOK_DIRECTORY = str(Path(__file__).parent.absolute())
PATHS = {file: NOTEBOOK_DIRECTORY + "/" + file for file in os.listdir(NOTEBOOK_DIRECTORY)}
