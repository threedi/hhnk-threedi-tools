# %%
"""
Note: the curent tests are only ran to check if the functions work.
They still must be checked qualitatively

"""

import sys

import pytest

from hhnk_threedi_tools.core.checks.bank_levels import BankLevelCheck
from tests.config import FOLDER_TEST

# pd.options.mode.chained_assignment = 'raise' #catch SettingWithCopyWarning


class TestBankLevel:
    @pytest.fixture(scope="class")
    def bl_check(self):
        bl = BankLevelCheck(FOLDER_TEST)
        bl.import_data()
        return bl

    @pytest.mark.skipif(sys.version_info < (3, 12), reason="# TODO bank level check moet nog bijgewerkt worden.")
    def test_import_information_object(self, bl_check):
        """Test if the import of information works, if the correct amount is imported"""

        # look at counts
        assert all(bl_check.imports["manholes"].count() == 0)  # no manholes
        assert bl_check.imports["fixeddrainage"].count()["id"] == 32
        assert bl_check.imports["fixeddrainage_lines"].count()["id"] == 35
        assert bl_check.imports["lines_1d2d"].count()["node_id"] == 104
        assert bl_check.imports["conn_nodes"].count()["conn_node_id"] == 72
        assert bl_check.imports["channels"].count()["channel_id"] == 49
        assert bl_check.imports["cross_loc"].count()["cross_loc_id"] == 96
        assert bl_check.imports["levee_lines"].count()["levee_id"] == 54

        # look at random info
        assert bl_check.imports["fixeddrainage"]["peil_id"][0] == "46442"
        assert bl_check.imports["fixeddrainage_lines"]["streefpeil_bwn2"][1].values[0] == -0.85
        assert bl_check.imports["lines_1d2d"]["storage_area"][423] == 20.5620565088836
        assert bl_check.imports["conn_nodes"]["conn_node_id"][15] == 15
        assert bl_check.imports["channels"]["initial_waterlevel"][487] == -0.55
        assert bl_check.imports["cross_loc"]["reference_level"][282] == -0.94
        assert bl_check.imports["levee_lines"]["levee_height"][54] == 0.159

    @pytest.mark.skipif(sys.version_info < (3, 12), reason="# TODO bank level check moet nog bijgewerkt worden.")
    def test_levee_intersections(self, bl_check):
        """Test if levee intersections can be done"""
        bl_check.line_intersections()
        assert bl_check.line_intersects["levee_id"][425] == 16

    @pytest.mark.skipif(sys.version_info < (3, 12), reason="# TODO bank level check moet nog bijgewerkt worden.")
    def test_divergent_waterlevel_nodes(self, bl_check):
        bl_check.divergent_waterlevel_nodes()

        assert bl_check.diverging_wl_nodes["type"][0] == "node_in_wrong_fixeddrainage_area"

    @pytest.mark.skipif(sys.version_info < (3, 12), reason="# TODO bank level check moet nog bijgewerkt worden.")
    def test_manhole_information(self, bl_check):
        bl_check.divergent_waterlevel_nodes()
        bl_check.line_intersections()
        bl_check.manhole_information()

        assert bl_check.manholes_info["type"][9] == "node_in_wrong_fixeddrainage_area"

    @pytest.mark.skipif(sys.version_info < (3, 12), reason="# TODO bank level check moet nog bijgewerkt worden.")
    def test_flowlines_1d2d(self, bl_check):
        bl_check.line_intersections()
        bl_check.flowlines_1d2d()

        assert bl_check.all_1d2d_flowlines["type"][99] == "1d2d_crosses_levee"

    @pytest.mark.skipif(sys.version_info < (3, 12), reason="# TODO bank level check moet nog bijgewerkt worden.")
    def test_manholes_to_add_to_model(self, bl_check):
        bl_check.divergent_waterlevel_nodes()
        bl_check.line_intersections()
        bl_check.manhole_information()
        bl_check.manholes_to_add_to_model()

        assert bl_check.new_manholes_df["connection_node_id"][0] == 44

    @pytest.mark.skipif(sys.version_info < (3, 12), reason="# TODO bank level check moet nog bijgewerkt worden.")
    def test_generate_cross_section_locations(self, bl_check):
        bl_check.line_intersections()
        bl_check.generate_cross_section_locations()

        assert bl_check.cross_loc_new_filtered["bank_level_source"][0] == "initial+10cm"
        assert bl_check.cross_loc_new["bank_level_diff"][82] == -1.66

    @pytest.mark.skipif(sys.version_info < (3, 12), reason="# TODO bank level check moet nog bijgewerkt worden.")
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
