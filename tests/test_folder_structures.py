# %%
import shutil
from pathlib import Path
from hhnk_threedi_tools.core.folders import Folders

TEST_DIRECTORY = Path(__file__).parent.absolute() / "data"
FOLDER = TEST_DIRECTORY / "new_project"
MODEL_FOLDER = TEST_DIRECTORY / "model_test"
SUB_FOLDERS = ["01_source_data", "02_schematisation", "03_3di_results", "04_test_results"]

# def is_empty_dir(dir_path):
#     empty = next((dir_path.iterdir()), False)
#     return not empty

def test_create_project():
    """tests if a new project folders are created"""
    if FOLDER.exists():
        shutil.rmtree(FOLDER)
    # create a project without creating sub-dirs
    folder = Folders(FOLDER, create=False)
    assert not Path(FOLDER).exists()    # check sub-dirs empty
    folder = Folders(FOLDER, create=True)

    # check if SUB_FOLDERS exist and contain a readme.txt
    for i in SUB_FOLDERS:
        assert FOLDER.joinpath(i).exists()
        assert FOLDER.joinpath(i, "read_me.txt").exists(), f"No readme in {i}"


def test_to_file_dict():
    """tests if a base path dictionary can be generated"""
    folder = Folders(FOLDER)
    files_dict = folder.to_file_dict()
    assert files_dict["hdb_sturing_3di_layer"] == "Sturing_3Di"
    assert files_dict["0d1d_results_dir"] == str(FOLDER / "03_3di_results" / "0d1d_results")


def test_create_revision():
    """tests if a new revision folder can be made"""
    folder = Folders(FOLDER)

    if folder.threedi_results.zero_d_one_d["new"].exists:
        shutil.rmtree(folder.threedi_results.zero_d_one_d["new"].path)

    folder.threedi_results.zero_d_one_d["new"].create()

    assert folder.threedi_results.zero_d_one_d["new"].exists is True

    
def test_find_dem():
    folder = Folders(MODEL_FOLDER)
    dem_path = TEST_DIRECTORY / r"model_test/02_schematisation/00_basis/rasters/dem_hoekje.tif"
    assert Path(folder.model.schema_base.rasters.dem.path) == Path(
        dem_path
    )


def test_find_threedi_sources():
    results_path = TEST_DIRECTORY / r"model_test/03_3di_results/0d1d_results/BWN bwn_test #7 0d1d_test"
    folder = Folders(MODEL_FOLDER)
    results = folder.threedi_results.find(revision_path=results_path)
    assert Path(results["nc_file"]) == Path(
        results_path / "results_3di.nc"
    )




# %%
if __name__ == "__main__":
    test_create_project()
    test_to_file_dict()
    test_create_revision()
    test_find_dem()
    test_find_threedi_sources()
# %%
