# %%

import hhnk_research_tools as hrt

import hhnk_threedi_tools.core.raster_creation.storage_raster as storage_raster
from tests.config import FOLDER_TEST, TEMP_DIR


def test_create_storage_raster_rxr():
    """Test create storage raster with rxr"""
    # %%
    folder_schema = FOLDER_TEST

    raster_out = hrt.Folder(TEMP_DIR).full_path(f"storage_glg_{hrt.get_uuid()}.tif")

    rootzone_thickness_cm = 20  # cm

    folder_schema = FOLDER_TEST

    overwrite = True
    nodata = -9999
    chunksize = None

    raster_paths_dict = {
        "gwlvl": folder_schema.model.schema_base.rasters.gwlvl_glg,
        "dem": folder_schema.model.schema_base.rasters.dem,
        "soil": folder_schema.model.schema_base.rasters.soil,
    }
    metadata_key = "soil"
    verbose = False
    tempdir = None

    self = storage_raster.StorageRaster(
        raster_out=raster_out,
        raster_paths_dict=raster_paths_dict,
        metadata_key=metadata_key,
        verbose=verbose,
        tempdir=tempdir,
    )

    output_raster = self.run(rootzone_thickness_cm=rootzone_thickness_cm)

    statistics = output_raster.statistics()

    assert statistics == {
        "min": 0.0,
        "max": 0.14029,
        "mean": 0.052926,
        "std": 0.026567,
    }


# %%
if __name__ == "__main__":
    test_create_storage_raster()
