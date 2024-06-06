# %%



# import cupy_xarray
import threading

# from dask.utils import SerializableLock
import time
from pathlib import Path
import shutil
import hhnk_research_tools as hrt
import rioxarray as rxr

from tests.config import FOLDER_TEST, TEMP_DIR



dem = FOLDER_TEST.model.schema_base.rasters.dem
wlvl = hrt.Raster(TEMP_DIR.joinpath(f"wlvl_{hrt.current_time(date=True)}.tif"))
depth = hrt.Raster(TEMP_DIR.joinpath(f"raster_out_{hrt.current_time(date=True)}.tif"))

if not wlvl.exists():
    shutil.copy(dem.base, wlvl.base)

# %%
now = time.time()

def get_rasters(chunksize=64):
    # Load your rasters (replace 'path_to_raster1.tif' and 'path_to_raster2.tif' with actual file paths)
    raster1 = rxr.open_rasterio(dem.base, chunks={"x": chunksize, "y": chunksize})
    raster2 = rxr.open_rasterio(wlvl.base, chunks={"x": chunksize, "y": chunksize})


    # Set NoData values (if needed, adjust to match your actual NoData value)
    raster1 = raster1.where(raster1 != raster1.rio.nodata)
    raster2 = raster2.where(raster2 != raster2.rio.nodata)
    return raster1, raster2


# raster1_chunk = raster1.chunk({'x': CHUNKSIZE, 'y': CHUNKSIZE})
# raster2_chunk = raster2.chunk({'x': CHUNKSIZE, 'y': CHUNKSIZE})
# result = raster1_chunk - raster2_chunk


# %% calc option without function


def rxr_calc_scaled_raster():
    now = time.time()

    raster1, raster2 = get_rasters()

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


%timeit rxr_calc_scaled_raster()
# %%
# %%timeit
import xarray as xr

CHUNKSIZE = 64

now = time.time()

raster1, raster2 = get_rasters()

result = xr.zeros_like(raster1)


def calc(da):
    if da.x.data[0] == 109581:
        return xr.zeros_like(da)

    else:
        da2 = da+1

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
# %% ufunc
import xarray as xr

# now = time.time()

def rxr_apply_ufunc():

    raster1, raster2 = get_rasters(chunksize=1024)

    result = xr.zeros_like(raster1)


    def calc(da1, da2):
        # if da.x.data[0] == 109581:
        #     return xr.zeros_like(da)
        # print(da2)
        # else:
        r = da1 - da2
        # print(da)
        # print(da.x.data[0])
        # print(da.y.data[0])

        r = r.where(r >= 0, -9999)
        r *= 1000  # scale_factor so we can save ints

        return r


    # result += 2
    result = xr.map_blocks(calc, obj=raster1, args=[raster2], template = result)


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

%timeit rxr_apply_ufunc()

# print(time.time() - now)

depth.statistics(approve_ok=False)
# %%
