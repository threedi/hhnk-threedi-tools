# %%

# import cupy_xarray
import threading

# from dask.utils import SerializableLock
import time
from pathlib import Path

import hhnk_research_tools as hrt
import rioxarray as rxr

from tests.config import FOLDER_TEST, TEMP_DIR

now = time.time()


dem = FOLDER_TEST.model.schema_base.rasters.dem
wlvl = dem
depth = hrt.Raster("raster_out.tif")

# path_out = Path(
#     r"C:/Users/Wietse/Documents/HHNK/Poldermodellen/callantsoog/03_3di_results/batch_results/callantsoog_small/01_downloads"
# )
# dem = hrt.Raster(
#     r"c:/Users/Wietse/Documents/HHNK/Poldermodellen/callantsoog/02_schematisation/00_basis/rasters/dem_callantsoog.tif"
# )
# wlvl = hrt.Raster(
#     r"C:/Users/Wietse/Documents/HHNK/Poldermodellen/callantsoog/03_3di_results/batch_results/callantsoog_small/01_downloads/wlvl_max_piek_glg_T10.tif"
# )

# depth = hrt.Raster(path_out.joinpath("depth.tif"))

CHUNKSIZE = 64
# Load your rasters (replace 'path_to_raster1.tif' and 'path_to_raster2.tif' with actual file paths)
raster1 = rxr.open_rasterio(wlvl.base, chunks={"x": CHUNKSIZE, "y": CHUNKSIZE})
raster2 = rxr.open_rasterio(dem.base, chunks={"x": CHUNKSIZE, "y": CHUNKSIZE})


# Set NoData values (if needed, adjust to match your actual NoData value)
raster1 = raster1.where(raster1 != raster1.rio.nodata)
raster2 = raster2.where(raster2 != raster2.rio.nodata)


# raster1_chunk = raster1.chunk({'x': CHUNKSIZE, 'y': CHUNKSIZE})
# raster2_chunk = raster2.chunk({'x': CHUNKSIZE, 'y': CHUNKSIZE})
# result = raster1_chunk - raster2_chunk


# %% calc option without function

now = time.time()

# Subtract the rasters, excluding NoData values
result = raster1 - raster2

# Assign values less than 0 to NoData

result *= 1000  # scale_factor so we can save ints

result = result.where(result >= 0, -9999)

# Export the result directly to the output raster in chunks
result.rio.set_nodata(raster1.rio.nodata)
dir(result.rio)
# result.rio.scale_factor = 0.001
result.rio.to_raster(
    depth.base,
    chunks={"x": CHUNKSIZE, "y": CHUNKSIZE},
    # lock=True, #Use dask multithread
    compress="ZSTD",
    tiled=True,
    PREDICTOR=2,  # from hrt, does it still work?
    ZSTD_LEVEL=1,  # from hrt, does it still work?
    scale=0.01,
    dtype="int16",
)

# Settings the scale_factor does not work with rioxarray. T
gdal_source = depth.open_gdal_source_write()
b = gdal_source.GetRasterBand(1)
b.SetScale(0.001)
gdal_source = None

print(time.time() - now)

# %%
# %%timeit
import xarray as xr

CHUNKSIZE = 64

now = time.time()


result = xr.zeros_like(raster1)


def calc(da):
    if da.x.data[0] == 109581:
        return xr.zeros_like(da)

    else:
        da2 = da + 2

        # print(da)
        # print(da.x.data[0])
        # print(da.y.data[0])
        return da2


# result += 2
result = result.map_blocks(func=calc, template=result)


result.rio.to_raster(
    depth.base,
    chunks={"x": CHUNKSIZE, "y": CHUNKSIZE},
    # lock=True, #Use dask multithread
    compress="ZSTD",
    tiled=True,
    PREDICTOR=2,  # from hrt, does it still work?
    ZSTD_LEVEL=1,  # from hrt, does it still work?
    scale=0.01,
    dtype="int16",
)


print(time.time() - now)

depth.statistics(approve_ok=False)

# %%
import matplotlib.pyplot as plt
import numpy as np

# result.where(result==-9999.0, -9999)
# result +=1
# r = result.compute()
plt.imshow(result.values[0, :, :])

print(np.unique(r.values))
# %%
