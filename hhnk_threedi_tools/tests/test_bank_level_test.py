# %%
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 17 10:01:23 2021

@author: chris.kerklaan

Note: the curent tests are only ran to check if the functions work.
They still must be checked qualitatively

"""
if __name__ == "__main__":
    import set_local_paths  # add local git repos.

# First-party imports
import pathlib

# Local imports
from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.core.checks.bank_levels import BankLevelTest

# Globals
# __file__ = "C:/Users/chris.kerklaan/Documents/Github/hhnk-threedi-tools/hhnk_threedi_tools/tests/test_bank_level_test.py"
TEST_MODEL = str(pathlib.Path(__file__).parent.absolute()) + "/data/model_test/"


def test_import_information_object():
    """tests if the import of information works, if the correct amount is imported"""
    bl_test = BankLevelTest(Folders(TEST_MODEL))

    bl_test.import_data()

    # look at counts
    assert all(bl_test.imports["manholes"].count() == 0)  # no manholes
    assert bl_test.imports["fixeddrainage"].count()["id"] == 32
    assert bl_test.imports["fixeddrainage_lines"].count()["id"] == 35
    assert bl_test.imports["lines_1d2d"].count()["node_id"] == 104
    assert bl_test.imports["conn_nodes"].count()["conn_node_id"] == 72
    assert bl_test.imports["channels"].count()["channel_id"] == 49
    assert bl_test.imports["cross_loc"].count()["cross_loc_id"] == 96
    assert bl_test.imports["levee_lines"].count()["levee_id"] == 54

    # look at random info
    assert bl_test.imports["fixeddrainage"]["peil_id"][0] == "46442"
    assert (
        bl_test.imports["fixeddrainage_lines"]["streefpeil_bwn2"][1].values[0] == -0.85
    )
    assert bl_test.imports["lines_1d2d"]["storage_area"][423] == 20.5620565088836
    assert bl_test.imports["conn_nodes"]["conn_node_id"][15] == 15
    assert bl_test.imports["channels"]["initial_waterlevel"][487] == -0.55
    assert bl_test.imports["cross_loc"]["reference_level"][282] == -0.94
    assert bl_test.imports["levee_lines"]["levee_height"][8970] == 0.159


def test_levee_intersections():
    """tests if levee intersections can be done"""
    bl_test = BankLevelTest(Folders(TEST_MODEL))
    bl_test.import_data()
    bl_test.line_intersections()
    assert bl_test.line_intersects["levee_id"][425] == 1154.0


def test_divergent_waterlevel_nodes():
    bl_test = BankLevelTest(Folders(TEST_MODEL))
    bl_test.import_data()
    bl_test.divergent_waterlevel_nodes()

    assert bl_test.diverging_wl_nodes["type"][0] == "node_in_wrong_fixeddrainage_area"


def test_manhole_information():

    bl_test = BankLevelTest(Folders(TEST_MODEL))
    bl_test.import_data()
    bl_test.divergent_waterlevel_nodes()
    bl_test.line_intersections()
    bl_test.manhole_information()

    assert bl_test.manholes_info["type"][9] == "node_in_wrong_fixeddrainage_area"


def test_flowlines_1d2d():

    bl_test = BankLevelTest(Folders(TEST_MODEL))
    bl_test.import_data()
    bl_test.line_intersections()
    bl_test.flowlines_1d2d()

    assert bl_test.all_1d2d_flowlines["type"][99] == "1d2d_crosses_levee"


def test_manholes_to_add_to_model():

    bl_test = BankLevelTest(Folders(TEST_MODEL))
    bl_test.import_data()
    bl_test.divergent_waterlevel_nodes()
    bl_test.line_intersections()
    bl_test.manhole_information()
    bl_test.manholes_to_add_to_model()

    assert bl_test.new_manholes_df["connection_node_id"][0] == 44


def test_generate_cross_section_locations():
    bl_test = BankLevelTest(Folders(TEST_MODEL))
    bl_test.import_data()
    bl_test.line_intersections()
    bl_test.generate_cross_section_locations()

    assert bl_test.cross_loc_new_filtered["bank_level_source"][0] == "initial+10cm"
    assert bl_test.cross_loc_new["bank_level_diff"][82] == -1.66


def test_generate_channels():

    bl_test = BankLevelTest(Folders(TEST_MODEL))
    bl_test.import_data()
    bl_test.line_intersections()
    bl_test.flowlines_1d2d()
    bl_test.generate_cross_section_locations()
    bl_test.generate_channels()

    assert bl_test.new_channels["initial_waterlevel"][48] == -0.85


def test_run():
    bl_test = BankLevelTest(Folders(TEST_MODEL))
    bl_test.import_data()
    bl_test.run()


def test_results():
    bl_test = BankLevelTest(Folders(TEST_MODEL))
    bl_test.import_data()
    bl_test.run()
    results = bl_test.results

    assert results["line_intersects"].count()["node_id"] == 9


# %%
if __name__ == "__main__":
    test_import_information_object()
    test_levee_intersections()
    test_divergent_waterlevel_nodes()
    test_manhole_information()
    test_flowlines_1d2d()
    test_manholes_to_add_to_model()
    test_generate_cross_section_locations()
    test_generate_channels()
    test_run()
    test_results()

# %%

self = BankLevelTest(Folders(TEST_MODEL))
from hhnk_research_tools.threedi.grid import Grid
