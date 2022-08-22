# %%
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 24 16:17:00 2021

@author: chris.kerklaan
"""
if __name__ == "__main__":
    import set_local_paths  # add local git repos.

# First-party imports
import os
import pathlib

# Local imports
from hhnk_threedi_tools.core.checks.sqlite import SqliteTest
from hhnk_threedi_tools.core.folders import Folders


# %%
# Globals
# __file__ = "C:/Users/chris.kerklaan/Documents/Github/hhnk-threedi-tests/hhnk_threedi_tools/tests/test_sqlite.py"
TEST_MODEL = str(pathlib.Path(__file__).parent.absolute()) + "/data/model_test/"


def test_run_controlled_structures():
    folder = Folders(TEST_MODEL)
    sqlite_test = SqliteTest(folder=folder)
    output = sqlite_test.run_controlled_structures()
    assert output["hdb_kruin_max"][0] == -0.25


def test_run_dem_max_value():
    folder = Folders(TEST_MODEL)
    sqlite_test = SqliteTest(folder=folder)
    output = sqlite_test.run_dem_max_value()
    assert "voldoet aan de norm" in output


def test_run_dewatering_depth():
    folder = Folders(TEST_MODEL)
    sqlite_test = SqliteTest(folder=folder)
    output = sqlite_test.run_dewatering_depth(
        output_file=folder.output.sqlite_tests.drooglegging.path
    )
    assert os.path.exists(output)


def test_run_model_checks():
    folder = Folders(TEST_MODEL)
    sqlite_test = SqliteTest(folder=folder)
    output = sqlite_test.run_model_checks()
    assert "node without initial waterlevel" in output.set_index("id").loc[482, "error"]


def test_run_geometry():
    """#TODO empty check"""
    folder = Folders(TEST_MODEL)
    sqlite_test = SqliteTest(folder=folder)
    output = sqlite_test.run_geometry_checks()
    assert output.empty


def test_run_imp_surface_area():
    folder = Folders(TEST_MODEL)
    sqlite_test = SqliteTest(folder=folder)
    output = sqlite_test.run_imp_surface_area()
    assert "61 ha" in output


def test_run_isolated_channels():
    folder = Folders(TEST_MODEL)
    sqlite_test = SqliteTest(folder=folder)
    output = sqlite_test.run_isolated_channels()
    assert output[0]["length_in_meters"][10] == 168.45


def test_run_used_profiles():
    folder = Folders(TEST_MODEL)
    sqlite_test = SqliteTest(folder=folder)
    output = sqlite_test.run_used_profiles()

    assert output["width_at_wlvl"][0] == 2


def test_run_struct_channel_bed_level():
    """#TODO empty check"""
    folder = Folders(TEST_MODEL)
    sqlite_test = SqliteTest(folder=folder)
    output = sqlite_test.run_struct_channel_bed_level()
    assert output.empty


def test_run_watersurface_area():
    folder = Folders(TEST_MODEL)
    sqlite_test = SqliteTest(folder=folder)
    output = sqlite_test.run_watersurface_area()
    assert output[0]["area_diff"][0] == -20


def test_run_weir_flood_level():
    folder = Folders(TEST_MODEL)
    sqlite_test = SqliteTest(folder=folder)
    output = sqlite_test.run_weir_floor_level()
    assert output[0]["proposed_reference_level"][1] == -1.26


# %%
if __name__ == "__main__":
    test_run_dem_max_value()
    test_run_dewatering_depth()
    test_run_model_checks()
    test_run_geometry()
    test_run_imp_surface_area()
    test_run_isolated_channels()
    test_run_used_profiles()
    test_run_struct_channel_bed_level()
    test_run_watersurface_area()
    test_run_weir_flood_level()


# %%
