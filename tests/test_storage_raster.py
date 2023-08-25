# %%

import hhnk_research_tools as hrt
import hhnk_threedi_tools as htt
from tests.config import FOLDER_TEST, TEMP_DIR
import hhnk_threedi_tools.core.raster_creation.storage_lookup as storage_lookup
import hhnk_threedi_tools.core.raster_creation.storage_raster as storage_raster

def test_create_storage_raster():
 
    #Create/load Storage lookup df
    storage_lookup_df = storage_lookup.create_storage_lookup(rootzone_thickness_cm=20,
                                                                storage_unsa_sim_path=None)

    folder_schema =FOLDER_TEST

    output_raster = hrt.Folder(TEMP_DIR).full_path(f"storage_glg_{hrt.get_uuid()}.tif")
    overwrite=True
    nodata = -9999

    groundwlvl_raster = folder_schema.model.schema_base.rasters.gwlvl_glg
    dem_raster = folder_schema.model.schema_base.rasters.dem
    soil_raster = folder_schema.model.schema_base.rasters.soil
    meta_raster = dem_raster


    storage_raster.calculate_storage_raster(output_raster = output_raster, 
                                meta_raster = meta_raster,
                                groundwlvl_raster = groundwlvl_raster,
                                dem_raster = dem_raster,
                                soil_raster = soil_raster,
                                storage_lookup_df = storage_lookup_df,
                                nodata = nodata,
                                overwrite = overwrite)


    assert output_raster.statistics(approve_ok=False) == {'min': 0.0, 'max': 0.14029, 'mean': 0.024702, 'std': 0.031965}


# %%
if __name__ == "__main__":
    test_create_storage_raster()
    