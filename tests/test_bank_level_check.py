# %%
import os
import sys

import pytest

from hhnk_threedi_tools.core.checks.bank_levels import BankLevelCheck
from tests.config import FOLDER_TEST

os.environ["GDAL_DATA"] = r"C:\git\hhnk-threedi-tools\.pixi\envs\default\Library\share\gdal"
os.environ["PROJ_LIB"] = r"C:\git\hhnk-threedi-tools\.pixi\envs\default\Library\share"

spatialite_path = r"C:\git\hhnk-threedi-tools\.pixi\envs\default\Library\bin"
os.environ["PATH"] = spatialite_path + ";" + os.environ["PATH"]
# %%

bl_check = BankLevelCheck(FOLDER_TEST)
bl_check.prepare_data()


# %%
@pytest.mark.skipif(sys.version_info < (3, 12), reason="# TODO bank level check moet nog bijgewerkt worden.")
def test_import_data(bl_check: BankLevelCheck):
    """Test if the import of data works, if the correct amount is imported"""

    assert bl_check.fixeddrainage_gdf.count()["peil_id"] == 32
    assert bl_check.fixeddrainage_boundary_gdf.count()["peil_id"] == 35
    assert bl_check.lines_1d2d.count()["id"] == 105
    assert bl_check.channel_gdf.count()["code"] == 49
    assert bl_check.connection_node_gdf.count()["code"] == 72
    assert bl_check.cross_section_gdf.count()["code"] == 96
    assert bl_check.obstacle_gdf.count()["code"] == 54
    assert bl_check.obstacle_gdf["crest_level"][54] == 0.159


def test_intersections(bl_check: BankLevelCheck):
    """Test if obstacle and fdla with 1d2d lines intersections can be done"""
    result = bl_check.line_intersections()
    assert result["crest_level"][165] == 0.510
    assert result.count()["type"] == 10


def test_divergent_waterlevel_nodes(bl_check: BankLevelCheck):
    result = bl_check.divergent_waterlevel_nodes()

    assert result["type"][0] == "node_in_wrong_fixeddrainage_area"

    # %%%%

    def test_manhole_information(self, bl_check):
        bl_check.divergent_waterlevel_nodes()
        bl_check.line_intersections()
        bl_check.manhole_information()

        assert bl_check.manholes_info["type"][9] == "node_in_wrong_fixeddrainage_area"

    def test_flowlines_1d2d(self, bl_check):
        bl_check.line_intersections()
        bl_check.flowlines_1d2d()

        assert bl_check.all_1d2d_flowlines["type"][99] == "1d2d_crosses_levee"

    def test_manholes_to_add_to_model(self, bl_check):
        bl_check.divergent_waterlevel_nodes()
        bl_check.line_intersections()
        bl_check.manhole_information()
        bl_check.manholes_to_add_to_model()

        assert bl_check.new_manholes_df["connection_node_id"][0] == 44

    def test_generate_cross_section_locations(self, bl_check):
        bl_check.line_intersections()
        bl_check.generate_cross_section_locations()

        assert bl_check.cross_loc_new_filtered["bank_level_source"][0] == "initial+10cm"
        assert bl_check.cross_loc_new["bank_level_diff"][82] == -1.66

    def test_generate_channels(self, bl_check):
        bl_check.line_intersections()
        bl_check.flowlines_1d2d()
        bl_check.generate_cross_section_locations()
        bl_check.generate_channels()

        assert bl_check.new_channels["initial_waterlevel"][48] == -0.85

    @pytest.mark.skipif(sys.version_info < (3, 12), reason="# TODO bank level check moet nog bijgewerkt worden.")
    def test_results(self, bl_check):
        bl_check.run()
        results = bl_check.results

        assert results["line_intersects"].count()["node_id"] == 9


# %%
if __name__ == "__main__":
    import inspect

    selftest = TestBankLevel()
    bl_check = selftest.bl_check()
    # Run all testfunctions
    for i in dir(selftest):
        if i.startswith("test_") and hasattr(inspect.getattr_static(selftest, i), "__call__"):
            print(i)
            getattr(selftest, i)(bl_check)

# %%
