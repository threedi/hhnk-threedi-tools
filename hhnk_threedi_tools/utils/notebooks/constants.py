# -*- coding: utf-8 -*-
"""
Created on Mon Oct 25 09:13:51 2021

@author: chris.kerklaan
"""
import os
import pathlib
import sys

NOTEBOOK_DIRECTORY = str(pathlib.Path(__file__).parent.absolute())
PATHS = {
    file: NOTEBOOK_DIRECTORY + "/" + file for file in os.listdir(NOTEBOOK_DIRECTORY)
}
