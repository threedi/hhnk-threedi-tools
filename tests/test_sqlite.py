# %%
# First-party imports
import inspect
import shutil
from pathlib import Path

import hhnk_research_tools as hrt
import pytest

# Local imports
from hhnk_threedi_tools.core.checks.sqlite.sqlite_main import SqliteCheck
from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.core.schematisation.model_splitter import ModelSchematisations
from tests.config import FOLDER_TEST, PATH_NEW_FOLDER


class TestSqlite:
    FOLDER_TEST.output.sqlite_tests.unlink_contents()

    sqlite_check = SqliteCheck(folder=FOLDER_TEST)
    sqlite_check.output_fd.create(parents=True)

    @pytest.fixture(scope="class")
    def folder_new(self):
        """Copy folder structure and sqlite and then run splitter so we
        get the correct sqlite (with errors) to run tests on.
        """
        FOLDER_NEW = Folders(PATH_NEW_FOLDER, create=True)
        shutil.copytree(FOLDER_TEST.model.schema_base.path, FOLDER_NEW.model.schema_base.path, dirs_exist_ok=True)
        shutil.copy(FOLDER_TEST.model.settings.path, FOLDER_NEW.model.settings.path)
        shutil.copy(FOLDER_TEST.model.settings_default.path, FOLDER_NEW.model.settings_default.path)
        shutil.copy(FOLDER_TEST.model.model_sql.path, FOLDER_NEW.model.model_sql.path)
        # self.folder=FOLDER_TEST
        spl = ModelSchematisations(folder=FOLDER_NEW)
        spl.create_schematisation(name="basis_errors")

        return FOLDER_NEW

    def test_run_controlled_structures(self):
        self.sqlite_check.run_controlled_structures()

        output_file = self.sqlite_check.output_fd.gestuurde_kunstwerken
        assert output_file.exists()

        output_df = output_file.load()
        assert output_df["hdb_kruin_max"][0] == -0.25

    def test_run_dem_max_value(self):
        output = self.sqlite_check.run_dem_max_value()
        assert "voldoet aan de norm" in output

    def test_run_dewatering_depth(self):
        self.sqlite_check.run_dewatering_depth()
        assert self.sqlite_check.output_fd.drooglegging.exists()

        assert self.sqlite_check.output_fd.drooglegging.statistics(approx_ok=False) == {
            "min": -0.76,
            "max": 10004.290039,
            "mean": 2.167882,
            "std": 91.391436,
        }

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

    def test_run_cross_section_duplicates(self, folder_new):
        database = folder_new.model.schema_basis_errors.database
        output = self.sqlite_check.run_cross_section_duplicates(database=database)
        assert output["cross_loc_id"].to_list() == [282, 99999]

    def test_run_cross_section_no_vertex(self, folder_new):
        database = folder_new.model.schema_basis_errors.database
        output = self.sqlite_check.run_cross_section_no_vertex(database=database)
        assert output["cross_loc_id"].to_list() == [320]
        assert output["distance_to_vertex"].to_list() == [1.0]

    def test_create_grid_from_sqlite(self, folder_new):
        self.sqlite_check.create_grid_from_sqlite(output_folder=folder_new.output.sqlite_tests.path)
        assert folder_new.output.sqlite_tests.full_path("cells.gpkg").exists()

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
    import geopandas as gpd
    import hhnk_research_tools as hrt
    import numpy as np

    self = TestSqlite()
    # self = selftest.sqlite_check

    # # Run all testfunctions
    # for i in dir(selftest):
    #     if i.startswith('test_') and hasattr(inspect.getattr_static(selftest,i), '__call__'):
    #         print(i)
    #         getattr(selftest, i)()
    folder_new = self.folder_new()
    # self.test_run_cross_section_duplicates(folder_new=folder_new)
    # self.test_run_cross_section_no_vertex(folder_new=folder_new)
    self.test_run_geometry()
