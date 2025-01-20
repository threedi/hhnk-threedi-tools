# %%
import shutil
from pathlib import Path

from hhnk_threedi_tools.core.folders import Folders
from tests.config import FOLDER_NEW, FOLDER_TEST, PATH_NEW_FOLDER

SUB_FOLDERS = ["01_source_data", "02_schematisation", "03_3di_results", "04_test_results"]


class TestFolder:
    def test_create_project(self):
        """Test if a new project folders are created"""
        # create a project without creating sub-dirs
        new_folder = Folders(PATH_NEW_FOLDER, create=True)

        # check if SUB_FOLDERS exist and contain a readme.txt
        for i in SUB_FOLDERS:
            assert PATH_NEW_FOLDER.joinpath(i).exists(), f"No folder: {i}"
            assert PATH_NEW_FOLDER.joinpath(i, "read_me.txt").exists(), f"No readme in {i}"

    def test_to_file_dict(self):
        """Test if a base path dictionary can be generated"""
        files_dict = FOLDER_NEW.to_file_dict()
        assert files_dict["0d1d_results_dir"] == str(PATH_NEW_FOLDER / "03_3di_results" / "0d1d_results")

    def test_create_revision(self):
        """Test if a new revision folder can be made"""
        if FOLDER_NEW.threedi_results.zero_d_one_d["new"].exists():
            shutil.rmtree(FOLDER_NEW.threedi_results.zero_d_one_d["new"].base)

        FOLDER_NEW.threedi_results.zero_d_one_d["new"].mkdir()

        assert FOLDER_NEW.threedi_results.zero_d_one_d["new"].exists() is True

    def test_find_dem(self):
        dem = FOLDER_TEST.full_path(r"02_schematisation/00_basis/rasters/dem_hoekje.tif")
        assert Path(FOLDER_TEST.model.schema_base.rasters.dem.path) == Path(dem.path)

    def test_find_threediresult(self):
        assert (
            FOLDER_TEST.threedi_results.zero_d_one_d[0].base
            == FOLDER_TEST.threedi_results.zero_d_one_d["BWN bwn_test #7 0d1d_test"].base
        )

    # TODO .find() is weg.
    # def test_find_threedi_sources(self):
    #     results_path = TEST_DIRECTORY / r"model_test/03_3di_results/0d1d_results/BWN bwn_test #7 0d1d_test"
    #     folder = Folders(MODEL_FOLDER)
    #     results = folder.threedi_results.find(revision_path=results_path)
    #     assert Path(results["nc_file"]) == Path(
    #         results_path / "results_3di.nc"
    #     )

    def test_hasatrr_batch_fd(self):
        batch_fd = FOLDER_NEW.threedi_results.batch["test"]
        assert hasattr(batch_fd.downloads, "blok_ghg_T1000")


# %%
if __name__ == "__main__":
    import inspect

    selftest = TestFolder()
    # Run all testfunctions
    for i in dir(selftest):
        if i.startswith("test_") and hasattr(inspect.getattr_static(selftest, i), "__call__"):
            print(i)
            getattr(selftest, i)()
# %%
# FOLDER_NEW.to_file_dict()
