# %% 
# Test Sqlite test
if __name__ == "__main__":
    import set_local_paths  # add local git repos.

# First-party imports
import os
from pathlib import Path
import inspect

# Local imports
from hhnk_threedi_tools.core.checks.sqlite.sqlite_main import SqliteCheck

from tests.config import FOLDER_TEST

# %%
# Globals
import csv

class TestSqlite:
    FOLDER_TEST.output.sqlite_tests.unlink_contents()

    sqlite_check = SqliteCheck(folder=FOLDER_TEST)

    # open the file in the write mode
    with open(r'E:\02.modellen\model_test_v2\t2.txt', 'w') as f:
        # create the csv writer
        writer = csv.writer(f)

        # write a row to the csv file
        writer.writerow([f"{sqlite_check.fenv.source_data}"])
        writer.writerow([f"{sqlite_check.fenv.source_data.datachecker}"])
        writer.writerow([f"{sqlite_check.fenv.source_data.datachecker.layers.culvert.parent}"])

    def test_run_controlled_structures(self):       
        # open the file in the write mode
        with open(r'E:\02.modellen\model_test_v2\t3.txt', 'w') as f:
            # create the csv writer
            writer = csv.writer(f)

            # write a row to the csv file
            writer.writerow([f"{self.sqlite_check.fenv.source_data}"])
            writer.writerow([f"{self.sqlite_check.fenv.source_data.datachecker}"])
            writer.writerow([f"{self.sqlite_check.fenv.source_data.datachecker.layers.culvert.parent}"])

        self.sqlite_check.run_controlled_structures()

        output_file = self.sqlite_check.output_fd.gestuurde_kunstwerken
        assert output_file.exists

        output_df = output_file.load()
        assert output_df["hdb_kruin_max"][0] == -0.25


    def test_run_dem_max_value(self):
        output = self.sqlite_check.run_dem_max_value()
        assert "voldoet aan de norm" in output


    def test_run_dewatering_depth(self):           
        self.sqlite_check.run_dewatering_depth()
        assert self.sqlite_check.output_fd.drooglegging.pl.exists

        assert self.sqlite_check.output_fd.drooglegging.statistics(approve_ok=False
                    ) == {'min': -0.76, 
                          'max': 10004.290039, 
                          'mean': 2.167882, 
                          'std': 91.391436}


    def test_run_model_checks(self):
        output = self.sqlite_check.run_model_checks()
        assert "node without initial waterlevel" in output.set_index("id").loc[482, "error"]


    def test_run_geometry(self):
        """TODO empty check"""
        output = self.sqlite_check.run_geometry_checks()
        assert output.empty


    def test_run_imp_surface_area(self):
        output = self.sqlite_check.run_imp_surface_area()
        assert "61 ha" in output


    def test_run_isolated_channels(self):
        output = self.sqlite_check.run_isolated_channels()
        assert output[0]["length_in_meters"][10] == 168.45


    def test_run_used_profiles(self):
        output = self.sqlite_check.run_used_profiles()
        assert output["width_at_wlvl"][0] == 2


    def test_run_cross_section(self):
        output = self.sqlite_check.run_cross_section()
        assert output.empty


    def test_run_cross_section_vertex(self):
        output = self.sqlite_check.run_cross_section_vertex()
        assert output.empty


    def test_run_struct_channel_bed_level(self):
        """TODO empty check"""
        output = self.sqlite_check.run_struct_channel_bed_level()
        assert output.empty


    def test_run_watersurface_area(self):
        output = self.sqlite_check.run_watersurface_area()
        assert output[0]["area_diff"][0] == -20


    def test_run_weir_flood_level(self):
        output = self.sqlite_check.run_weir_floor_level()
        assert output[0]["proposed_reference_level"][1] == -1.26



# %%
if __name__ == "__main__":
    import hhnk_research_tools as hrt
    import geopandas as gpd
    import numpy as np

    selftest = TestSqlite()
    # self=selftest
    self = selftest.sqlite_test

    # Run all testfunctions
    for i in dir(selftest):
        if i.startswith('test_') and hasattr(inspect.getattr_static(selftest,i), '__call__'):
            print(i)
            getattr(selftest, i)()

# %%
