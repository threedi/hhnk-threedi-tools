# %%
import shutil
from pathlib import Path
from hhnk_threedi_tools.core.folders import Folders



TEST_DIRECTORY = Path(__file__).parent.absolute() / "data"

TEST_MODEL = TEST_DIRECTORY / "model_test"
NEW_FOLDER = TEST_DIRECTORY / "new_project"

SUB_FOLDERS = ["01_source_data", "02_schematisation", "03_3di_results", "04_test_results"]


class TestFolder:
    folder = Folders(TEST_MODEL)

    if NEW_FOLDER.exists():
        shutil.rmtree(NEW_FOLDER)


    def test_create_project(self):
        """tests if a new project folders are created"""
        # create a project without creating sub-dirs
        new_folder = Folders(NEW_FOLDER, create=False)
        assert not Path(NEW_FOLDER).exists()    # check sub-dirs empty
        new_folder = Folders(NEW_FOLDER, create=True)

        # check if SUB_FOLDERS exist and contain a readme.txt
        for i in SUB_FOLDERS:
            assert NEW_FOLDER.joinpath(i).exists(), f"No folder: {i}"
            assert NEW_FOLDER.joinpath(i, "read_me.txt").exists(), f"No readme in {i}"


    def test_to_file_dict(self):
        """tests if a base path dictionary can be generated"""
        folder = Folders(NEW_FOLDER)
        files_dict = folder.to_file_dict()
        assert files_dict["hdb_sturing_3di_layer"] == "Sturing_3Di"
        assert files_dict["0d1d_results_dir"] == str(NEW_FOLDER / "03_3di_results" / "0d1d_results")


    def test_create_revision(self):
        """tests if a new revision folder can be made"""
        folder = Folders(NEW_FOLDER)

        if folder.threedi_results.zero_d_one_d["new"].exists:
            shutil.rmtree(folder.threedi_results.zero_d_one_d["new"].path)

        folder.threedi_results.zero_d_one_d["new"].create()

        assert folder.threedi_results.zero_d_one_d["new"].exists is True

        
    def test_find_dem(self):
        dem_path = TEST_MODEL / r"02_schematisation/00_basis/rasters/dem_hoekje.tif"
        assert Path(self.folder.model.schema_base.rasters.dem.path) == Path(
            dem_path
        )

    # TODO .find() is weg.
    # def test_find_threedi_sources(self):
    #     results_path = TEST_DIRECTORY / r"model_test/03_3di_results/0d1d_results/BWN bwn_test #7 0d1d_test"
    #     folder = Folders(MODEL_FOLDER)
    #     results = folder.threedi_results.find(revision_path=results_path)
    #     assert Path(results["nc_file"]) == Path(
    #         results_path / "results_3di.nc"
    #     )




# %%
if __name__ == "__main__":
    import inspect
    selftest = TestFolder()
    self = selftest.folder
    #Run all testfunctions
    for i in dir(selftest):
        if i.startswith('test_') and hasattr(inspect.getattr_static(selftest,i), '__call__'):
            print(i)
            getattr(selftest, i)()    
# %%
