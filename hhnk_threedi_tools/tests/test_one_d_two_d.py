# %%
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 23 10:46:05 2021

@author: chris.kerklaan

Functional testing for oneDtwoD object

"""
# First-party imports
import os
import pathlib


# Local imports
from hhnk_threedi_tools.core.checks.one_d_two_d import OneDTwoDTest

# Globals
# __file__ = "C:/Users/chris.kerklaan/Documents/Github/hhnk-threedi-tests/hhnk_threedi_tools/tests/test_one_d_two_d.py"
TEST_MODEL = str(pathlib.Path(__file__).parent.absolute()) + "/data/model_test/"


def test_run_depth_at_timesteps_test():
    """test of de 0d1d test werkt"""
    od1d_test = OneDTwoDTest.from_path(TEST_MODEL)
    output = od1d_test.run_levels_depths_at_timesteps()

    assert len(output) > 0
    assert output[0] == 1
    assert "waterdiepte_T15.tif" in od1d_test.fenv.output.one_d_two_d[0].content


def test_run_flowline_stats():
    """test of de hydraulische testen werken"""
    odtd_test = OneDTwoDTest.from_path(TEST_MODEL)
    output = odtd_test.run_flowline_stats()

    assert output["pump_capacity_m3_s"][1094] == 0.00116666666666667


def test_run_node_stats():
    odtd_test = OneDTwoDTest.from_path(TEST_MODEL)
    output = odtd_test.run_node_stats()

    assert round(output["minimal_dem"][1], 3) == 1.54

# %%
if __name__ == '__main__':
    test_run_depth_at_timesteps_test()
    test_run_flowline_stats()
    test_run_node_stats()