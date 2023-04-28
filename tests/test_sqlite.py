# %%
if __name__ == "__main__":
    import set_local_paths  # add local git repos.

# First-party imports
import os
import pathlib
import inspect

# Local imports
from hhnk_threedi_tools.core.checks.sqlite import SqliteTest
from hhnk_threedi_tools.core.folders import Folders


# Globals
TEST_MODEL = str(pathlib.Path(__file__).parent.absolute()) + "/data/model_test/"


class TestSqlite:
    folder = Folders(TEST_MODEL)
    sqlite_test = SqliteTest(folder=folder)


    def test_run_controlled_structures(self):
        output = self.sqlite_test.run_controlled_structures()
        assert output["hdb_kruin_max"][0] == -0.25


    def test_run_dem_max_value(self):
        output = self.sqlite_test.run_dem_max_value()
        assert "voldoet aan de norm" in output


    def test_run_dewatering_depth(self):
        self.folder.output.sqlite_tests.drooglegging.unlink_if_exists()
            
        output = self.sqlite_test.run_dewatering_depth(
            output_file=self.folder.output.sqlite_tests.drooglegging.path
        )
        assert os.path.exists(output)


    def test_run_model_checks(self):
        output = self.sqlite_test.run_model_checks()
        assert "node without initial waterlevel" in output.set_index("id").loc[482, "error"]


    def test_run_geometry(self):
        """TODO empty check"""
        output = self.sqlite_test.run_geometry_checks()
        assert output.empty


    def test_run_imp_surface_area(self):
        output = self.sqlite_test.run_imp_surface_area()
        assert "61 ha" in output


    def test_run_isolated_channels(self):
        output = self.sqlite_test.run_isolated_channels()
        assert output[0]["length_in_meters"][10] == 168.45


    def test_run_used_profiles(self):
        output = self.sqlite_test.run_used_profiles()
        assert output["width_at_wlvl"][0] == 2


    def test_run_cross_section(self):
        output = self.sqlite_test.run_cross_section()
        assert output.empty


    def test_run_cross_section_vertex(self):
        output = self.sqlite_test.run_cross_section_vertex()
        assert output.empty


    def test_run_struct_channel_bed_level(self):
        """TODO empty check"""
        output = self.sqlite_test.run_struct_channel_bed_level()
        assert output.empty


    def test_run_watersurface_area(self):
        output = self.sqlite_test.run_watersurface_area()
        assert output[0]["area_diff"][0] == -20


    def test_run_weir_flood_level(self):
        output = self.sqlite_test.run_weir_floor_level()
        assert output[0]["proposed_reference_level"][1] == -1.26


# %%
if __name__ == "__main__":
    self = TestSqlite()

    #Run all testfunctions
    for i in dir(self):
        if i.startswith('test_') and hasattr(inspect.getattr_static(self,i), '__call__'):
            print(i)
            getattr(self, i)()