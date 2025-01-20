# %%

import time
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
from geocube.api.core import make_geocube

PLAYGROUND_DIR = Path(r"d:\projecten\D2402.HHNK.Ondersteuning_Python\03.playground")
PLAYGROUND_DIR = Path(r"C:\Users\Wietse\Documents\HHNK\playground")

SETUP = "clip_small"


# dem = FOLDER_TEST.model.schema_base.rasters.dem FIXME: Exception: No connection or database path provided
dem = hrt.Raster(PLAYGROUND_DIR / SETUP / "dem.tif")
grid_gpkg = PLAYGROUND_DIR / SETUP / "grid_corr.gpkg"
grid_tif_geocube = PLAYGROUND_DIR / SETUP / "grid_geocube.tif"
grid_tif_hrt = PLAYGROUND_DIR / SETUP / "grid_hrt.tif"

grid_gdf = gpd.read_file(grid_gpkg)


# %%

now = time.time()

out_grid = make_geocube(vector_data=grid_gdf[["wlvl_max_replaced", "geometry"]], resolution=(-0.5, 0.5), fill=-9999)
# out_grid["wlvl_max_replaced"].rio.to_raster(grid_tif)


# out_grid["wlvl_max_replaced"].rio.set_nodata(-9999, inplace=True)

raster_out = hrt.Raster(grid_tif_geocube)

# This is just taken from the new hrt.Raster. So we can do the writing in the exact same way.
dtype = "float32"

if "float" in dtype:
    compress = "LERC_DEFLATE"
else:
    compress = "ZSTD"

out_grid["wlvl_max_replaced"].rio.to_raster(
    raster_out.base,
    # chunks={"x": CHUNKSIZE, "y": CHUNKSIZE},
    chunks=True,
    # lock=threading.Lock(), #Use dask multithread
    lock=True,
    tiled=True,
    windowed=True,  # If lock is False, window doesnt work.
    COMPRESS=compress,  # gdal options
    PREDICTOR=2,  # gdal options
    ZSTD_LEVEL=1,  # gdal options
    MAX_Z_ERROR=0.001,  # gdal options
    NUM_THREADS="ALL_CPUS",  # gdal options
    dtype=dtype,
)

print(time.time() - now)


# %% hrt gdf to raster

now = time.time()

hrt.gdf_to_raster(
    gdf=grid_gdf,
    value_field="wlvl_max_replaced",
    raster_out=grid_tif_hrt,
    nodata=-9999,
    metadata=hrt.Raster(grid_tif_geocube).metadata,
    read_array=False,
    overwrite=True,
)

print(time.time() - now)
