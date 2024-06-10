# %%


# import cupy_xarray
import shutil
import threading

# from dask.utils import SerializableLock
import time
from pathlib import Path

import hhnk_research_tools as hrt
import numpy as np
import rioxarray as rxr
import xarray as xr

from tests.config import FOLDER_TEST, TEMP_DIR

CHUNKSIZE = 1024


dem = FOLDER_TEST.model.schema_base.rasters.dem
dem = hrt.Raster(
    rf"C:\Users\wiets\Documents\HHNK\07.Poldermodellen\LangeWeerenToekomstHHNK_1d2d_ghg\work in progress\schematisation\rasters\dem_ontsluitingsroute_ahn4_lw_v1.tif"
)

wlvl = hrt.Raster(TEMP_DIR.joinpath(f"wlvl_{hrt.current_time(date=True)}.tif"))
depth = hrt.Raster(TEMP_DIR.joinpath(f"raster_out_{hrt.current_time(date=True)}.tif"))

if not wlvl.exists():
    shutil.copy(dem.base, wlvl.base)

# %%


def get_rasters(chunksize=CHUNKSIZE):
    # Load your rasters (replace 'path_to_raster1.tif' and 'path_to_raster2.tif' with actual file paths)
    raster1 = rxr.open_rasterio(dem.base, chunks={"x": chunksize, "y": chunksize})
    raster2 = rxr.open_rasterio(wlvl.base, chunks={"x": chunksize, "y": chunksize})

    # Set NoData values (if needed, adjust to match your actual NoData value)
    raster1 = raster1.where(raster1 != raster1.rio.nodata, np.nan)
    raster2 = raster2.where(raster2 != raster2.rio.nodata, np.nan)
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


rxr_calc_scaled_raster()

assert depth.statistics(approve_ok=False) == {"min": 0.0, "max": 0.0, "mean": 0.0, "std": 0.0}


# %%
# import matplotlib.pyplot as plt
# import numpy as np

# # result.where(result==-9999.0, -9999)
# # result +=1
# # r = result.compute()
# plt.imshow(raster2.values[0, :, :])


# print(np.unique(r.values))
# %% ufunc
def rxr_map_blocks():
    now = time.time()

    raster1, raster2 = get_rasters(chunksize=CHUNKSIZE)

    result = xr.zeros_like(raster1)

    def calc(da1, da2):
        # if da1.x.data[0] != 10958122:
        #     return xr.zeros_like(da1)

        # if da1.x.data[0] != 109581:
        #     return xr.zeros_like(da1)
        # print(da2)
        # else:
        r = da1 - da2

        # print(da)
        # print(da.x.data[0])
        # print(da.y.data[0])

        r *= 1000  # scale_factor so we can save ints
        r = r.where(r >= 0, -9999)

        return r

    # result += 2
    result = xr.map_blocks(calc, obj=raster1, args=[raster2], template=result)

    result.rio.set_nodata(raster1.rio.nodata)

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


rxr_map_blocks()

# print(time.time() - now)

assert depth.statistics(approve_ok=False) == {"min": 0.0, "max": 0.0, "mean": 0.0, "std": 0.0}
# %%


def hrt_rastercalculator():
    now = time.time()

    def run_rtype_window(block):
        """Custom calc function on blocks in hrt.RasterCalculatorV2"""
        block_out = block.blocks["wlvl"] - block.blocks["dem"]
        block_out *= 1000

        # Nodatamasks toepassen
        block_out[block.masks_all] = dem.nodata
        block_out[block_out < 0] = dem.nodata
        return block_out

    # Calculate drooglegging raster
    raster_calc = hrt.RasterCalculatorV2(
        raster_out=depth,
        raster_paths_dict={
            "dem": dem,
            "wlvl": wlvl,
        },
        nodata_keys=["dem"],
        mask_keys=["wlvl"],
        metadata_key="dem",
        custom_run_window_function=run_rtype_window,
        output_nodata=dem.nodata,
        min_block_size=CHUNKSIZE,
        verbose=False,
        tempdir="",
    )
    # Run calculation of output raster
    raster_calc.run(overwrite=True)

    print(time.time() - now)


hrt_rastercalculator()

assert depth.statistics(approve_ok=False) == {"min": 0.0, "max": 0.0, "mean": 0.0, "std": 0.0}


# %%
