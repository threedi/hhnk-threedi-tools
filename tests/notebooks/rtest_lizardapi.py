# %%
"""
Check if the lizard v4 downloader pruduces the desired results.
"""

import hhnk_research_tools as hrt
import numpy as np

from hhnk_threedi_tools.utils.notebooks.notebook_setup import setup_notebook
from tests.config import FOLDER_TEST, TEMP_DIR, TEST_DIRECTORY

notebook_data = setup_notebook()

from threedi_scenario_downloader import downloader as dl_new

from deprecated.core.api import downloader as dl_old

dl_old.LIZARD_URL = "https://hhnk.lizard.net/api/v3/"
dl_new.LIZARD_URL = "https://hhnk.lizard.net/api/v4/"

api_keys = hrt.read_api_file(notebook_data["api_keys_path"])
dl_old.set_api_key(api_keys["lizard"])
dl_new.set_api_key(api_keys["lizard"])


uuid = "ef71c256-9987-4b75-9aad-b006a12b9ba0"
resolution = 25

rasters = {}

for calctype in ["maxdepth", "maxwlvl"]:
    rasters[calctype] = {}
    for v in ["v3", "v4"]:
        rasters[calctype][v] = hrt.Raster(TEST_DIRECTORY / f"dl/{calctype}_{v}_res{resolution}.tif")


dem = hrt.Raster(FOLDER_TEST.model.schema_base.rasters.dem)


# %%
for v in ["v4"]:
    calctype = "maxdepth"
    if v == "v3":
        if not rasters[calctype][v].pl.exists():
            dl_old.download_maximum_waterdepth_raster(
                scenario_uuid=uuid,
                target_srs="EPSG:28992",
                resolution=resolution,
                bounds=None,
                bounds_srs=None,
                pathname=rasters[calctype][v].path,
            )
    if v == "v4":
        if not rasters[calctype][v].pl.exists():
            dl_new.download_maximum_waterdepth_raster(
                scenario_uuid=uuid,
                projection="EPSG:28992",
                resolution=resolution,
                bbox="109750,542000,110000,543010",
                # bbox=None,
                pathname=rasters[calctype][v].path,
            )
    calctype = "maxwlvl"
    if v == "v3":
        if not rasters[calctype][v].pl.exists():
            dl_old.download_maximum_waterlevel_raster(
                scenario_uuid=uuid,
                target_srs="EPSG:28992",
                resolution=resolution,
                bounds=None,
                bounds_srs=None,
                pathname=rasters[calctype][v].path,
            )
    if v == "v4":
        if not rasters[calctype][v].pl.exists():
            dl_new.download_maximum_waterlevel_raster(
                scenario_uuid=uuid,
                projection=None,
                resolution=resolution,
                bbox=dem.metadata.bbox,
                pathname=rasters[calctype][v].path,
            )

# %%
for calctype in ["maxdepth", "maxwlvl"]:
    for v in ["v3", "v4"]:
        r = rasters[calctype][v]
        if r.pl.exists():
            print(f"""
            {calctype} {v}
            requested resolution: {resolution}
            projection: {r.metadata.projection}
            sum: {r.sum()}
            stats: {r.statistics(approve_ok=False)}
            bbox: {r.metadata.bbox}
            georef: {r.metadata.georef}
            pixelsize (x): {r.metadata.pixel_width}
            """)
    # %%
r = dem
print(f"""
{calctype} {v}
requested resolution: {resolution}
projection: {r.metadata.projection}
sum: {r.sum()}
stats: {r.statistics(approve_ok=False)}
bbox: {r.metadata.bbox}
georef: {r.metadata.georef}
pixelsize (x): {r.metadata.pixel_width}
""")
