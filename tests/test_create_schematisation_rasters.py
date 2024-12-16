# %%
import importlib

from hhnk_threedi_tools.core import folders_modelbuilder
from hhnk_threedi_tools.core.modelbuilder.create_schematisation_rasters import (
    ModelbuilderRasters,
)
from tests.config import FOLDER_TEST, TEMP_DIR

importlib.reload(folders_modelbuilder)


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
        watervlakken_path=FOLDER_TEST.source_data.modelbuilder.channel_from_profiles,
    )

    folder = folders_modelbuilder.FoldersModelbuilder(dst_path=TEMP_DIR, source_paths=source_paths)

    assert folder.dst.dem.exists() is False

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
