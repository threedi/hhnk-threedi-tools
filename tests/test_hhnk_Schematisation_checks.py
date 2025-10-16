# %%
# First-party imports
import shutil

import pytest

# Local imports
from hhnk_threedi_tools.core.checks.schematisation.schematisation_checks_main import HhnkSchematisationChecks
from hhnk_threedi_tools.core.folders import Folders

# from hhnk_threedi_tools.core.schematisation.model_splitter import ModelSchematisations
from tests.config import FOLDER_TEST, PATH_NEW_FOLDER


class TestSchematisation:
    FOLDER_TEST.output.hhnk_schematisation_checks.unlink_contents()

    def __init__(self):
        self.hhnk_schematisation_checks = HhnkSchematisationChecks(folder=FOLDER_TEST, results={})
        self.hhnk_schematisation_checks.output_fd.mkdir(parents=True)

    @pytest.fixture(scope="class")
    def folder_new(self):
        """Copy folder structure and model database and then run splitter so we
        get the correct model database (with errors) to run tests on.
        """
        folder_new = Folders(PATH_NEW_FOLDER, create=True)
        shutil.copytree(FOLDER_TEST.model.schema_base.path, folder_new.model.schema_base.path, dirs_exist_ok=True)
        shutil.copy(FOLDER_TEST.model.settings.path, folder_new.model.settings.path)
        shutil.copy(FOLDER_TEST.model.settings_default.path, folder_new.model.settings_default.path)
        shutil.copy(FOLDER_TEST.model.model_sql.path, folder_new.model.model_sql.path)
        # self.folder = FOLDER_TEST
        # spl = ModelSchematisations(folder=folder_new) #TODO WVE zet weer aan als modelsplitter is bijgewerkt
        # spl.create_schematisation(name="basis_errors")

        return folder_new

    @pytest.mark.skipif(True, reason="ControlStructure Clss moet helemaal overhoop")
    def test_run_controlled_structures(self):
        self.hhnk_schematisation_checks.run_controlled_structures()

        output_file = self.hhnk_schematisation_checks.output_fd.gestuurde_kunstwerken
        assert output_file.exists()

        output_df = output_file.load()
        assert output_df["hdb_kruin_max"][0] == -0.25

    def test_run_dem_max_value(self):
        output = self.hhnk_schematisation_checks.run_dem_max_value()
        assert "voldoet aan de norm" in output

    def test_run_dewatering_depth(self):
        self.hhnk_schematisation_checks.run_dewatering_depth()
        assert self.hhnk_schematisation_checks.output_fd.drooglegging.exists()

        assert self.hhnk_schematisation_checks.output_fd.drooglegging.statistics() == {
            "min": -0.76,
            "max": 9.401,
            "mean": 1.332812,
            "std": 1.19355,
        }

    def test_run_model_checks(self):
        output = self.hhnk_schematisation_checks.run_model_checks()
        assert "node without initial waterlevel" in output.set_index("id").loc[482, "error"]

    def test_run_geometry(self):
        """TODO empty check"""
        output = self.hhnk_schematisation_checks.run_geometry_checks()
        assert output.empty

    def test_run_imp_surface_area(self):
        output = self.hhnk_schematisation_checks.run_imp_surface_area()
        assert "61 ha" in output

    def test_run_isolated_channels(self):
        output, some_text = self.hhnk_schematisation_checks.run_isolated_channels()
        assert output["length_in_meters"].iloc[0] == 168.45

    def test_run_used_profiles(self):
        output = self.hhnk_schematisation_checks.run_used_profiles()
        assert output["width_at_wlvl_mean"].iloc[0] == 2

    @pytest.mark.skipif(True, reason="Splitter heeft heel veel aanpassingen nodig voordat dit werkt")
    def test_run_cross_section_duplicates(self, folder_new: Folders):  # FIXME WVE
        database = folder_new.model.schema_basis_errors.database
        output = self.hhnk_schematisation_checks.run_cross_section_duplicates(database=database)
        assert output["cross_loc_id"].to_list() == [282, 99999]

    @pytest.mark.skipif(True, reason="Splitter heeft heel veel aanpassingen nodig voordat dit werkt")
    def test_run_cross_section_no_vertex(self, folder_new: Folders):  # FIXME WVE
        database = folder_new.model.schema_basis_errors.database
        output = self.hhnk_schematisation_checks.run_cross_section_no_vertex(database=database)
        assert output["cross_loc_id"].to_list() == [320]
        assert output["distance_to_vertex"].to_list() == [1.0]

    def test_create_grid_from_schematisation(self, folder_new: Folders):
        self.hhnk_schematisation_checks.create_grid_from_schematisation(
            output_folder=folder_new.output.hhnk_schematisation_checks.path
        )
        output_path = folder_new.output.hhnk_schematisation_checks.full_path("grid.gpkg")
        assert output_path.exists()
        assert not output_path.load(layer="cells").empty

    def test_run_struct_channel_bed_level(self):
        # TODO improve test with better result from function
        output = self.hhnk_schematisation_checks.run_struct_channel_bed_level()
        assert output.iloc[0]["beneden_has_assumption"]

    def test_run_watersurface_area(self):
        output = self.hhnk_schematisation_checks.run_watersurface_area()
        assert output[0]["area_diff"][0] == -20

    def test_run_weir_flood_level(self):
        output = self.hhnk_schematisation_checks.run_weir_floor_level()
        assert output[0]["proposed_reference_level"].iloc[0] == -1.25


# %%

if __name__ == "__main__":
    import inspect

    self = TestSchematisation()
    # folder_new = self.folder_new()

    # Run all testfunctions
    for i in dir(self):
        if i.startswith("test_"):
            # break
            # Find out if function needs folder_new as input
            params = inspect.signature(getattr(self, i)).parameters
            if "folder_new" in params:
                continue
                # folder_new = self.folder_new()
                getattr(self, i)(folder_new=self.folder_new)

            elif i.startswith("test_") and hasattr(inspect.getattr_static(self, i), "__call__"):
                print(i)
                getattr(self, i)()
    # self.test_run_cross_section_duplicates(folder_new=folder_new)
    # self.test_run_cross_section_no_vertex(folder_new=folder_new)
    self.test_run_geometry()

# %%
