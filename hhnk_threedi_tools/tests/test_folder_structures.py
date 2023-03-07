# %%
# # -*- coding: utf-8 -*-
"""
Created on Fri Aug 13 14:55:32 2021

@author: chris.kerklaan


TODO tests
    test_working_paths
    

"""
# First-party imports
import os
import shutil
import pathlib

if __name__ == "__main__":
    import set_local_paths  # add local git repos.

# Local imports
from hhnk_threedi_tools.core.folders import Folders

# Globals
# __file__ = "C:/Users/chris.kerklaan/Documents/Github/hhnk-threedi-tools/hhnk_threedi_tools/tests/test_folder_structures.py"
TEST_DIRECTORY = str(pathlib.Path(__file__).parent.absolute()) + "/data"
FOLDER = TEST_DIRECTORY + "/new_project"
MODEL_FOLDER = TEST_DIRECTORY + "/model_test"


def test_create_project():
    """tests if a new project folders are created"""
    if os.path.exists(FOLDER):
        shutil.rmtree(FOLDER)
    folder = Folders(FOLDER)
    folder.create_project()
    assert os.path.exists(FOLDER + "/01_Source_data")
    assert os.path.exists(FOLDER + "/02_schematisation")
    assert os.path.exists(FOLDER + "/02_schematisation/00_basis")
    assert os.path.exists(FOLDER + "/03_3di_resultaten")
    assert os.path.exists(FOLDER + "/Output")

    assert os.path.exists(FOLDER + "/01_Source_data/read_me.txt")
    assert os.path.exists(FOLDER + "/02_schematisation/read_me.txt")
    assert os.path.exists(FOLDER + "/03_3di_resultaten/read_me.txt")
    assert os.path.exists(FOLDER + "/Output/read_me.txt")


def test_to_dict():
    """tests if object can be made into a folder dict"""
    folder = Folders(TEST_DIRECTORY + "/new_project")
    folder_dict = folder.to_dict()

    assert "model_folder" in folder_dict.keys()
    assert "threedi_results_folder" in folder_dict.keys()
    assert "threedi_0d1d_results_folder" in folder_dict.keys()
    assert "threedi_climate_results_folder" in folder_dict.keys()
    assert "source_data_folder" in folder_dict.keys()

    assert folder_dict["model_folder"] is not None
    assert folder_dict["threedi_results_folder"] is not None
    assert folder_dict["threedi_0d1d_results_folder"] is not None
    assert folder_dict["threedi_climate_results_folder"] is not None
    assert folder_dict["source_data_folder"] is not None


def test_to_file_dict():
    """tests if a base path dictionary can be generated"""
    folder = Folders(FOLDER)
    files_dict = folder.to_file_dict()
    assert files_dict["hdb_sturing_3di_layer"] == "Sturing_3Di"


def test_to_test_file_dict():
    """tests if a test file dictionary can be made"""

    folder = Folders(FOLDER)
    test_files_dict = folder.to_test_file_dict(1)
    assert test_files_dict["water_surface_filename"] == "oppervlaktewater"

    test_files_dict = folder.to_test_file_dict(2)
    assert (
        test_files_dict["hyd_test_structs_filename"] == "hydraulische_toets_kunstwerken"
    )

    test_files_dict = folder.to_test_file_dict(3)
    assert test_files_dict["flow_1d2d_manholes_filename"] == "stroming_1d2d_putten"

    test_files_dict = folder.to_test_file_dict(4)
    assert test_files_dict["grid_nodes_2d_filename"] == "grid_nodes_2d"


def test_find_dem():
    folder = Folders(MODEL_FOLDER)
    dem_path = TEST_DIRECTORY + "/model_test/02_schematisation/00_basis/rasters/dem_hoekje.tif"
    assert pathlib.Path(folder.model.schema_base.rasters.dem.path) == pathlib.Path(
        dem_path
    )


def test_find_threedi_sources():
    results_path = (
        TEST_DIRECTORY
        + "/model_test/03_3di_resultaten/0d1d_results/BWN bwn_test #5 0d1d_test"
    )
    folder = Folders(MODEL_FOLDER)
    results = folder.threedi_results.find(revision_path=results_path)
    assert pathlib.Path(results["nc_file"]) == pathlib.Path(
        results_path + "/results_3di.nc"
    )


def test_create_revision():
    """tests if a new revision folder can be made"""
    folder = Folders(FOLDER)

    if folder.threedi_results.zero_d_one_d["new"].exists:
        shutil.rmtree(folder.threedi_results.zero_d_one_d["new"].path)

    folder.threedi_results.zero_d_one_d["new"].create()

    assert folder.threedi_results.zero_d_one_d["new"].exists is True


# def test_working_paths():
#     results_path = TEST_DIRECTORY + "/model_test/03_3di_resultaten/0d1d_results/BWN bwn_test #5 0d1d_test"
#     folder = Folders(MODEL_FOLDER)
#     folder.working_paths(2,
#                          active_paths=None, # do not know what this does
#                          threedi_results_path=results_path

#                          )

# %%
if __name__ == "__main__":
    test_create_project()
    test_to_dict()
    test_to_file_dict()
    test_to_test_file_dict()
    test_find_dem()
    test_find_threedi_sources()
    test_create_revision()


# %%
