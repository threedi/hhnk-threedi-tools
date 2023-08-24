# %%
"""For storage raster creation we need """


from threedi_scenario_downloader import downloader as dl
import hhnk_research_tools as hrt
import os
import hhnk_threedi_tools as htt

from tests.config import FOLDER_TEST, TEMP_DIR

api_keys_path = fr"{os.getenv('APPDATA')}\3Di\QGIS3\profiles\default\python\plugins\hhnk_threedi_plugin\api_key.txt"}
api_keys = hrt.read_api_file(api_keys_path)

dl.set_api_key(api_key=api_keys["lizard"])

FOLDER_TEST.model.schema_base.rasters.dem

#Download rasters for area
uuids= {}
uuids['soil'] = '9e3534b7-b5d4-46ab-be35-4a0990379f76'
# uuids['building'] = '98b5155d-dbc4-4a0c-a407-a9620741d308'

# Download rasters that are not on system yet.



dl.download_raster(scenario=uuids['soil'],
                            raster_code = "",
                            target_srs  = "EPSG:28992",
                            resolution  = meta.pixel_width,
                            bounds      = meta.bounds_dl,
                            bounds_srs  = "EPSG:28992",
                            pathname    = folder.input.soil.path,
                            is_threedi_scenario = False,
                            export_task_csv=folder.input.downloader_log.path)

# functions_bodemberging.download_lizard_rasters(uuid=uuids["soil"],
#                         output_path=folder.input.soil.path, 
#                         resolution=meta.pixel_width, 
#                         bounds=meta.bounds_dl)