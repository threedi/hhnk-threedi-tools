# %%
"""For storage raster creation we need """


from threedi_scenario_downloader import downloader as dl
import hhnk_research_tools as hrt
import os
import hhnk_threedi_tools as htt
import hhnk_threedi_tools.core.api.download_gui_class as download_gui_class
from tests.config import FOLDER_TEST, TEMP_DIR

api_keys_path = fr"{os.getenv('APPDATA')}\3Di\QGIS3\profiles\default\python\plugins\hhnk_threedi_plugin\api_key.txt"
api_keys = hrt.read_api_file(api_keys_path)

dl.set_api_key(api_key=api_keys["lizard"])

dem = FOLDER_TEST.model.schema_base.rasters.dem

#Download rasters for area
uuids= {}
uuids['soil'] = '9e3534b7-b5d4-46ab-be35-4a0990379f76'
# uuids['building'] = '98b5155d-dbc4-4a0c-a407-a9620741d308'

# Download rasters that are not on system yet.

folder = hrt.Folder(TEMP_DIR)

# %%


dl_raster_settings = download_gui_class.dlRasterSettingsV4()
r = download_gui_class.dlRaster(scenario_uuid = uuids['soil'],
                                raster_code = "",
                                resolution = dem.metadata.pixel_width,
                                output_path = folder.full_path("soil.tif").base,
                                is_threedi_scenario = False,
                                bbox=dem.metadata.bbox)
dl_raster_settings.add_raster(r)


dl.download_raster(
    scenario=dl_raster_settings.scenario_uuid_list,
    raster_code=dl_raster_settings.raster_code_list,
    projection=dl_raster_settings.projection_list,
    resolution=dl_raster_settings.resolution_list,
    bbox=dl_raster_settings.bbox_list,
    time=dl_raster_settings.time_list,
    pathname=dl_raster_settings.pathname_list,
    is_threedi_scenario=dl_raster_settings.is_threedi_scenario_list,
    export_task_csv=folder.full_path(f"download_log_{hrt.get_uuid()}.csv").path,
)


# %% create dummy gwlvl rasters
# diff with dem: glg=-1, ggg=-0.7, ghg=-0.4
folder_test = FOLDER_TEST

for idx, gxg in enumerate(["glg", "ggg", "ghg"]):
    gwlvl_raster = getattr(folder_test.model.schema_base.rasters, f"gwlvl_{gxg}")

    if not gwlvl_raster.exists():
        dem_array = dem._read_array()

        gwlvl_array = dem_array - 1 + 0.3*idx #glg=-1, ggg=-0.7, ghg=-0.4
        mask = dem_array == dem.nodata

        gwlvl_array[mask] = dem.nodata
        hrt.save_raster_array_to_tiff(
            output_file = gwlvl_raster,
            raster_array = gwlvl_array,
            nodata = dem.nodata,
            metadata = dem.metadata,
            overwrite=True
        )