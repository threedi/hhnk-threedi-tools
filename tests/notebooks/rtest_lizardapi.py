# %%
"""
Check if the lizard v4 downloader pruduces the desired results.
"""

import hhnk_research_tools as hrt
from tests.config import FOLDER_TEST, TEMP_DIR, TEST_DIRECTORY


from hhnk_threedi_tools.utils.notebooks.notebook_setup import setup_notebook
notebook_data = setup_notebook()

from threedi_scenario_downloader import downloader as dl_new
from hhnk_threedi_tools.core.api import downloader as dl_old

dl_old.LIZARD_URL = "https://hhnk.lizard.net/api/v3/"
dl_new.LIZARD_URL = "https://hhnk.lizard.net/api/v4/"

api_keys = hrt.read_api_file(notebook_data["api_keys_path"])
dl_old.set_api_key(api_keys["lizard"])
dl_new.set_api_key(api_keys["lizard"])


uuid="ef71c256-9987-4b75-9aad-b006a12b9ba0"
resolution=10
v3_raster = hrt.Raster(TEST_DIRECTORY/f"dl/maxdepth_old_res{resolution}.tif")
v4_raster = hrt.Raster(TEST_DIRECTORY/f"dl/maxdepth_new_res{resolution}.tif")

# %%

dem = hrt.Raster(FOLDER_TEST.model.schema_base.rasters.dem)
dem.metadata.bounds_dl


# %%

if not v3_raster.pl.exists():
    dl_old.download_maximum_waterdepth_raster(scenario_uuid=uuid, 
        target_srs="EPSG:28992", 
        resolution=resolution, 
        bounds=None, 
        bounds_srs=None, 
        pathname=v3_raster.path)

if not v4_raster.pl.exists():
    dl_new.download_maximum_waterdepth_raster(scenario_uuid=uuid, 
        projection=None, 
        resolution=resolution, 
        bbox=None, 
        pathname=v4_raster.path,
    )


for v,r in zip(["v3", "v4"], [v3_raster, v4_raster]):
    print(f"""
    {v}
    requested resolution: {resolution}
    sum: {r.sum()}
    stats: {r.statistics()}
    bounds: {r.metadata.bounds_dl}
    pixelsize: {r.metadata.pixel_width}
    """)
# %%
