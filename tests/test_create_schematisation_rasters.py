# %%
import importlib
import shutil

import pytest

from hhnk_threedi_tools.core import folders_modelbuilder
from hhnk_threedi_tools.core.folders import Folders

# from hhnk_threedi_tools.core.modelbuilder.create_schematisation_rasters import (
#     ModelbuilderRasters,
# )
from tests.config import FOLDER_TEST, PATH_NEW_FOLDER, TEMP_DIR

importlib.reload(folders_modelbuilder)


FOLDER_NEW = Folders(PATH_NEW_FOLDER, create=True)
shutil.copytree(FOLDER_TEST.source_data.path, FOLDER_NEW.source_data.path, dirs_exist_ok=True)
shutil.copy(FOLDER_TEST.model.calculation_rasters.polder.path, FOLDER_NEW.model.calculation_rasters.polder.path)
shutil.copy(FOLDER_TEST.model.calculation_rasters.waterdeel.path, FOLDER_NEW.model.calculation_rasters.waterdeel.path)


def test_create_schematisation_rasters():
    """Test creation of model rasters"""
    # %%

    rasters_dir = FOLDER_TEST.model.schema_base.rasters
    source_paths = folders_modelbuilder.SourcePaths(
        dem_path=rasters_dir.dem,
        glg_path=rasters_dir.full_path("storage_glg_hoekje.tif"),
        ggg_path=rasters_dir.full_path("storage_ggg_hoekje.tif"),
        ghg_path=rasters_dir.full_path("storage_ghg_hoekje.tif"),
        infiltration_path=rasters_dir.full_path("infiltration_hoekje.tif"),
        friction_path=rasters_dir.full_path("friction_hoekje.tif"),
        polder_path=FOLDER_TEST.source_data.polder_polygon,
        waterdeel_path=FOLDER_TEST.source_data.modelbuilder.channel_from_profiles,
    )

    folder_mb = folders_modelbuilder.FoldersModelbuilder(folder=FOLDER_NEW, source_paths=source_paths)

    assert folder_mb.dst.dem.exists() is False

    resolution = 0.5
    nodata = -9999
    overwrite = False
    verbose = True
    chunksize = None
    self = model_rasters = ModelbuilderRasters(
        folder=folder, resolution=resolution, nodata=nodata, overwrite=overwrite, verbose=verbose
    )

    model_rasters.run()

    assert folder.dst.dem.exists() is True
    assert folder.dst.dem.shape == [6962, 7686]


# %%
if __name__ == "__main__":
    test_create_schematisation_rasters()
