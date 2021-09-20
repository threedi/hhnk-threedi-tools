# -*- coding: utf-8 -*-
"""
Created on Tue Aug 24 16:17:00 2021

@author: chris.kerklaan
"""
# First-party imports
import os
import pathlib

# Local imports
from hhnk_threedi_tests.core.sqlite import SqliteTest
from hhnk_threedi_tests.core.folders import Folders

# Globals
# __file__ = "C:/Users/chris.kerklaan/Documents/Github/hhnk-threedi-tests/hhnk_threedi_tests/tests/test_sqlite.py"
TEST_MODEL = str(pathlib.Path(__file__).parent.absolute()) + "/data/model_test/"


def test_run_controlled_structures():
    sqlite_test = SqliteTest.from_path(TEST_MODEL)
    output = sqlite_test.run_controlled_structures()
    assert output["hdb_kruin_max"][0] == -0.25


def test_run_dem_max_value():
    sqlite_test = SqliteTest.from_path(TEST_MODEL)
    output = sqlite_test.run_dem_max_value()
    assert "voldoet aan de norm" in output


def test_run_dewatering_depth():
    sqlite_test = SqliteTest.from_path(TEST_MODEL)
    output = sqlite_test.run_dewatering_depth()
    assert os.path.exists(output)


def test_run_model_checks():
    sqlite_test = SqliteTest.from_path(TEST_MODEL)
    output = sqlite_test.run_model_checks()
    assert "node without initial waterlevel" in output["error"][482]


def test_run_geometry():
    """still incorrect"""
    sqlite_test = SqliteTest.from_path(TEST_MODEL)
    output = sqlite_test.run_geometry_checks()
    assert False


def test_run_imp_surface_area():
    sqlite_test = SqliteTest.from_path(TEST_MODEL)
    output = sqlite_test.run_imp_surface_area()
    assert "61 ha" in output


def test_run_isolated_channels():
    sqlite_test = SqliteTest.from_path(TEST_MODEL)
    output = sqlite_test.run_isolated_channels()
    assert output[0]["length_in_meters"][10] == 168.45


def test_run_used_profiles():
    sqlite_test = SqliteTest.from_path(TEST_MODEL)
    output = sqlite_test.run_used_profiles()

    assert output["width_at_wlvl"][0] == 2


def test_run_struct_channel_bed_level():
    """empty"""
    sqlite_test = SqliteTest.from_path(TEST_MODEL)
    output = sqlite_test.run_struct_channel_bed_level()
    assert False


def test_run_watersurface_area():
    sqlite_test = SqliteTest.from_path(TEST_MODEL)
    output = sqlite_test.run_watersurface_area()
    assert output[0]["area_diff"][0] == -20


def test_run_weir_flood_level():
    sqlite_test = SqliteTest.from_path(TEST_MODEL)
    output = sqlite_test.run_weir_floor_level()
    assert output[0]["proposed_reference_level"][1] == -1.26
