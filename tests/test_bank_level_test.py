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
from pathlib import Path

# Local imports
from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.core.checks.bank_levels import BankLevelTest

import pandas as pd
pd.options.mode.chained_assignment = 'raise' #catch SettingWithCopyWarning

from tests.config import FOLDER_TEST


class TestBankLevel:


    bl_test = BankLevelTest(FOLDER_TEST)

    bl_test.import_data()


    def test_import_information_object(self):
        """tests if the import of information works, if the correct amount is imported"""

        # look at counts
        assert all(self.bl_test.imports["manholes"].count() == 0)  # no manholes
        assert self.bl_test.imports["fixeddrainage"].count()["id"] == 32
        assert self.bl_test.imports["fixeddrainage_lines"].count()["id"] == 35
        assert self.bl_test.imports["lines_1d2d"].count()["node_id"] == 104
        assert self.bl_test.imports["conn_nodes"].count()["conn_node_id"] == 72
        assert self.bl_test.imports["channels"].count()["channel_id"] == 49
        assert self.bl_test.imports["cross_loc"].count()["cross_loc_id"] == 96
        assert self.bl_test.imports["levee_lines"].count()["levee_id"] == 54

        # look at random info
        assert self.bl_test.imports["fixeddrainage"]["peil_id"][0] == "46442"
        assert (
            self.bl_test.imports["fixeddrainage_lines"]["streefpeil_bwn2"][1].values[0] == -0.85
        )
        assert self.bl_test.imports["lines_1d2d"]["storage_area"][423] == 20.5620565088836
        assert self.bl_test.imports["conn_nodes"]["conn_node_id"][15] == 15
        assert self.bl_test.imports["channels"]["initial_waterlevel"][487] == -0.55
        assert self.bl_test.imports["cross_loc"]["reference_level"][282] == -0.94
        assert self.bl_test.imports["levee_lines"]["levee_height"][54] == 0.159


    def test_levee_intersections(self):
        """tests if levee intersections can be done"""
        self.bl_test.line_intersections()
        assert self.bl_test.line_intersects["levee_id"][425] == 16


    def test_divergent_waterlevel_nodes(self):
        self.bl_test.divergent_waterlevel_nodes()

        assert self.bl_test.diverging_wl_nodes["type"][0] == "node_in_wrong_fixeddrainage_area"


    def test_manhole_information(self):

        self.bl_test.divergent_waterlevel_nodes()
        self.bl_test.line_intersections()
        self.bl_test.manhole_information()

        assert self.bl_test.manholes_info["type"][9] == "node_in_wrong_fixeddrainage_area"


    def test_flowlines_1d2d(self):

        self.bl_test.line_intersections()
        self.bl_test.flowlines_1d2d()

        assert self.bl_test.all_1d2d_flowlines["type"][99] == "1d2d_crosses_levee"


    def test_manholes_to_add_to_model(self):
        self.bl_test.divergent_waterlevel_nodes()
        self.bl_test.line_intersections()
        self.bl_test.manhole_information()
        self.bl_test.manholes_to_add_to_model()

        assert self.bl_test.new_manholes_df["connection_node_id"][0] == 44


    def test_generate_cross_section_locations(self):
        self.bl_test.line_intersections()
        self.bl_test.generate_cross_section_locations()

        assert self.bl_test.cross_loc_new_filtered["bank_level_source"][0] == "initial+10cm"
        assert self.bl_test.cross_loc_new["bank_level_diff"][82] == -1.66


    def test_generate_channels(self):
        self.bl_test.line_intersections()
        self.bl_test.flowlines_1d2d()
        self.bl_test.generate_cross_section_locations()
        self.bl_test.generate_channels()

        assert self.bl_test.new_channels["initial_waterlevel"][48] == -0.85


    def test_run(self):
        self.bl_test.run()


    def test_results(self):
        self.bl_test.run()
        results = self.bl_test.results

        assert results["line_intersects"].count()["node_id"] == 9


# %%
if __name__ == "__main__":
    import inspect
    selftest = TestBankLevel()
    self = selftest.bl_test
    #Run all testfunctions
    for i in dir(selftest):
        if i.startswith('test_') and hasattr(inspect.getattr_static(selftest,i), '__call__'):
            print(i)
            getattr(selftest, i)()    
# %%
