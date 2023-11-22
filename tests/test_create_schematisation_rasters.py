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
    source_paths = folders_modelbuilder.SourcePaths(
        dem_path=FOLDER_TEST.model.schema_base.rasters.dem,
        glg_path=FOLDER_TEST.model.schema_base.rasters.full_path("storage_glg_hoekje.tif"),
        ggg_path=FOLDER_TEST.model.schema_base.rasters.full_path("storage_ggg_hoekje.tif"),
        ghg_path=FOLDER_TEST.model.schema_base.rasters.full_path("storage_ghg_hoekje.tif"),
        infiltration_path=FOLDER_TEST.model.schema_base.rasters.full_path("infiltration_hoekje.tif"),
        friction_path=FOLDER_TEST.model.schema_base.rasters.full_path("friction_hoekje.tif"),
        polder_path=FOLDER_TEST.source_data.polder_polygon,
        watervlakken_path=FOLDER_TEST.source_data.modelbuilder.channel_from_profiles,
    )

    folder_rasters = folders_modelbuilder.FoldersModelbuilder(dst_path=TEMP_DIR, source_paths=source_paths)

    assert folder_rasters.dst.dem.exists() is False

    model_rasters = ModelbuilderRasters(
        folder=folder_rasters,
        resolution=0.5,
        nodata=-9999,
        overwrite=False,
    )

    model_rasters.run()

    assert folder_rasters.dst.dem.exists() is True
    assert folder_rasters.dst.dem.shape == [6962, 7686]


# %%
if __name__ == "__main__":
    test_create_schematisation_rasters()
