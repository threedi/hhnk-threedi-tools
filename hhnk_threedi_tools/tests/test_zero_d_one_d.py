# %%
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 23 10:46:05 2021

@author: chris.kerklaan

Functional testing for zeroDoneD object
"""
if __name__ == "__main__":
    import set_local_paths  # add local git repos.

# First-party imports
import pathlib

# Local imports
from hhnk_threedi_tools.core.checks.zero_d_one_d import ZeroDOneDTest

#from hhnk_threedi_tools.core.folders import Folders

# Globals
# __file__ = "C:/Users/chris.kerklaan/Documents/Github/hhnk-threedi-tests/hhnk_threedi_tests/tests/test_zero_d_one_d.py"

#folder = str(pathlib.Path(__file__).parent.absolute()) + "/data/model_test/"
#TEST_MODEL = folder.threedi_results.zero_d_one_d[0].grid
TEST_MODEL = str(pathlib.Path(__file__).parent.absolute()) + "/data/model_test/"


def test_run_zero_d_one_d_test():
    """test of de 0d1d test werkt"""
    test_0d1d = ZeroDOneDTest.from_path(TEST_MODEL)
    test_0d1d.run()

    assert test_0d1d.results["lvl_end"].count() == 157


def test_run_hydraulic_test():
    """test of de hydraulische testen werken"""
    test_0d1d = ZeroDOneDTest.from_path(TEST_MODEL)
    test_0d1d.run_hydraulic()
    assert test_0d1d.hydraulic_results["channels"]["code"].count() == 134


# %%
if __name__ == "__main__":
    test_run_zero_d_one_d_test()
    test_run_hydraulic_test()

# %%
