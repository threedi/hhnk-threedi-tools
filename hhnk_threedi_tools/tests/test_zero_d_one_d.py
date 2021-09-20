# -*- coding: utf-8 -*-
"""
Created on Mon Aug 23 10:46:05 2021

@author: chris.kerklaan

Functional testing for zeroDoneD object

"""
# First-party imports
import pathlib

# Local imports
from hhnk_threedi_tools.core.zero_d_one_d import ZeroDOneDTest

# Globals
# __file__ = "C:/Users/chris.kerklaan/Documents/Github/hhnk-threedi-tests/hhnk_threedi_tests/tests/test_zero_d_one_d.py"
TEST_MODEL = str(pathlib.Path(__file__).parent.absolute()) + "/data/model_test/"


def test_run_zero_d_one_d_test():
    """test of de 0d1d test werkt"""
    zdod_test = ZeroDOneDTest.from_path(TEST_MODEL)
    zdod_test.run()

    assert zdod_test.results["lvl_end"].count() == 157


def test_run_hydraulic_test():
    """test of de hydraulische testen werken"""
    zdod_test = ZeroDOneDTest.from_path(TEST_MODEL)
    zdod_test.run_hydraulic()
    assert zdod_test.hydraulic_results["channels"]["code"].count() == 134
