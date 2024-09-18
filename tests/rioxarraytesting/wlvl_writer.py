# %%
import os
import time
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np

from hhnk_threedi_tools import GridToWaterDepth, GridToWaterLevel
from hhnk_threedi_tools.core.result_rasters.grid_to_raster import get_interpolator

SETUP = "clip_medium"
PLAYGROUND_DIR = Path(os.environ["3DI_PLAYGROUND_DIR"])
grid_gdf = gpd.read_file(PLAYGROUND_DIR / SETUP / "grid_corr.gpkg")
dem_path = PLAYGROUND_DIR / SETUP / "dem.tif"
NO_DATA_VALUE = -9999.0
wlvl_column = "wlvl_max_replaced"

now = time.time()

interpolator = get_interpolator(grid_gdf, wlvl_column, NO_DATA_VALUE)

grid_to_level = GridToWaterLevel(dem_path, grid_gdf=grid_gdf, wlvl_column=wlvl_column)
level_file = grid_to_level.run(output_file=dem_path.with_name("level.tif"), overwrite=True)

grid_to_depth = GridToWaterDepth(dem_path, wlvl_path=level_file.path)
depth_file = grid_to_depth.run(output_file=dem_path.with_name("depth.tif"), overwrite=True)

# %%
