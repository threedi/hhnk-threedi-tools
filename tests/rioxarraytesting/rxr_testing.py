# %%
import os
import shutil
import threading

# from dask.utils import SerializableLock
import time
import timeit
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
import rasterio as rio
import rioxarray as rxr
import xarray as xr

from hhnk_threedi_tools.core.result_rasters.calculate_raster import BaseCalculatorGPKG
from tests.config import FOLDER_TEST, TEMP_DIR

CHUNKSIZE = 4096
# CHUNKSIZE = 8192
# CHUNKSIZE = 16384
# CHUNKSIZE = 1024

SETUP = "clip_small"


SETUPS = {
    "clip_small": {"statistics": {"min": -32767.0, "max": 32765.0, "mean": 670.35384, "std": 2858.262956}},
    "clip_medium": {"statistics": {"min": -32767.0, "max": 32765.0, "mean": 664.976118, "std": 2841.971657}},
    "full": {"statistics": {"min": -32760.0, "max": 32765.0, "mean": 699.288424, "std": 2826.983867}},
}

# When using chunksize 16384, to make wlvl rater, stats come out a bit different.
SETUPS = {
    "clip_small": {"statistics": {"min": -32767.0, "max": 32765.0, "mean": 670.353979, "std": 2858.263808}},
    "clip_medium": {"statistics": {"min": -32767.0, "max": 32765.0, "mean": 664.976254, "std": 2841.972502}},
    "full": {"statistics": {"min": -32767.0, "max": 32765.0, "mean": 664.976254, "std": 2841.972502}},
}


SETUPS={
    "clip_small": {"statistics": {'min': 0.0, 'max': 2792.0, 'mean': 1584.604598, 'std': 633.447248}},
    "clip_medium": {"statistics": {'min': 0.0, 'max': 2792.0, 'mean': 1584.399304, 'std': 633.472572}},
}

PLAYGROUND_DIR = Path(r"d:\projecten\D2402.HHNK.Ondersteuning_Python\03.playground")
PLAYGROUND_DIR = Path(r"C:\Users\Wietse\Documents\HHNK\playground")
# dem = FOLDER_TEST.model.schema_base.rasters.dem FIXME: Exception: No connection or database path provided
dem = hrt.Raster(PLAYGROUND_DIR / SETUP / "dem.tif")
grid_gdf = gpd.read_file(PLAYGROUND_DIR / SETUP / "grid_corr.gpkg")
wlvl = hrt.Raster(PLAYGROUND_DIR / SETUP / "wlvl.tif")

# Performance
perf = pd.DataFrame()
perf["wvg_home_small_1024"] = [28.03, 1.87, 1.95, 4.44]
perf["wvg_home_small_4096"] = [28.66, 2.36, 1.83, 3.42]
perf["wvg_home_small_16384"] = [28.45, 2.65, 1.9, 3.32]
perf["wvg_home_medium_1024"] = [0, 9.2, 10.2, 18.2]
perf["wvg_home_medium_4096"] = [97.36, 6.0, 7.1, 11.7]
perf["wvg_home_medium_16384"] = [104.1, 8.39, 8.03, 12.0]
perf["wvg_home_full_4096"] = [343.7, 22.5, 26.8, 56.9]
perf["wvg_home_full_16384"] = [0, 21.4, 30.17, 47.1]
perf = perf.T
perf.columns = ["wlvl", "rxr", "rxr_block", "hrt"]
perf

# %% water level

# waterlevel raster maken
if not wlvl.exists():
    now = time.time()
    calculator_kwargs = {
        "dem_path": dem.base,
        "grid_gdf": grid_gdf,
        "wlvl_column": "wlvl_max_replaced",
    }
    with BaseCalculatorGPKG(**calculator_kwargs) as basecalc:
        basecalc.run(output_file=wlvl.path, mode="MODE_WLVL", overwrite=True, min_block_size=CHUNKSIZE)
    print(time.time() - now)

depth_rxr = hrt.Raster(PLAYGROUND_DIR / SETUP / "depth_rxr.tif")
depth_rxr_block = hrt.Raster(PLAYGROUND_DIR / SETUP / "depth_rxr_blocks.tif")
depth_hrt = hrt.Raster(PLAYGROUND_DIR / SETUP / "depth_hrt.tif")


def get_rasters(chunksize=CHUNKSIZE):
    # Load your rasters (replace 'path_to_raster1.tif' and 'path_to_raster2.tif' with actual file paths)
    raster1 = rxr.open_rasterio(dem.base, chunks={"x": chunksize, "y": chunksize})
    raster2 = rxr.open_rasterio(wlvl.base, chunks={"x": chunksize, "y": chunksize})

    # Set NoData values (if needed, adjust to match your actual NoData value)
    raster1 = raster1.where(raster1 != raster1.rio.nodata, np.nan)
    raster2 = raster2.where(raster2 != raster2.rio.nodata, np.nan)
    return raster1, raster2


def write_to_raster(raster_out, result, scale_factor):
    dtype = "int16"
    if "float" in dtype:
        compress = "LERC_DEFLATE"
    else:
        compress = "ZSTD"

    result.rio.to_raster(
        raster_out.base,
        # chunks={"x": CHUNKSIZE, "y": CHUNKSIZE},
        chunks=True,
        # lock=threading.Lock(), #Use dask multithread
        lock=True,
        tiled=True,
        windowed=True,
        COMPRESS=compress,  # gdal options
        PREDICTOR=2,  # gdal options
        ZSTD_LEVEL=1,  # gdal options
        NUM_THREADS="ALL_CPUS",  # gdal options
        dtype=dtype,
        MAX_Z_ERROR=0.001,
    )

    # Settings the scale_factor does not work with rioxarray. T
    with rio.open(raster_out.base, "r+") as src:
        src.scales = (scale_factor,)


# raster1_chunk = raster1.chunk({'x': CHUNKSIZE, 'y': CHUNKSIZE})
# raster2_chunk = raster2.chunk({'x': CHUNKSIZE, 'y': CHUNKSIZE})
# result = raster1_chunk - raster2_chunk


# %% calc option without function

# rio-xarray


# {'min': 0.0, 'max': 637.0, 'mean': 209.638603, 'std': 162.312035}
def rxr_calc_scaled_raster():
    now = time.time()

    raster1, raster2 = get_rasters()

    # Subtract the rasters, excluding NoData values
    result = raster2 - raster1

    # Assign values less than 0 to NoData

    result *= 1000  # scale_factor so we can save ints

    result = result.where(result >= 0, -9999)

    # Export the result directly to the output raster in chunks
    result.rio.set_nodata(raster1.rio.nodata)
    write_to_raster(depth_rxr, result, scale_factor=0.001)

    print(time.time() - now)


%timeit rxr_calc_scaled_raster()

assert depth_rxr.statistics(approve_ok=False) == SETUPS[SETUP]["statistics"]


# %%
# xarray map_blocks
def rxr_map_blocks():
    now = time.time()

    raster1, raster2 = get_rasters(chunksize=CHUNKSIZE)

    result = xr.full_like(raster1, raster1.rio.nodata)

    def calc(ra1, da1, da2):
        # if da1.x.data[0] != 10958122:
        # return ra1

        # if da1.x.data[0] != 109581:
        #     return xr.zeros_like(da1)
        # print(da2)
        # else:
        r = da2 - da1

        # print(da)
        # print(da.x.data[0])
        # print(da.y.data[0])

        r *= 1000  # scale_factor so we can save ints
        r = r.where(r >= 0, -9999)

        return r

    # result += 2
    result = xr.map_blocks(calc, obj=result, args=[raster1, raster2], template=result)

    result.rio.set_nodata(raster1.rio.nodata)

    write_to_raster(depth_rxr_block, result, scale_factor=0.001)

    print(time.time() - now)


%timeit rxr_map_blocks()

assert depth_rxr_block.statistics(approve_ok=False) == SETUPS[SETUP]["statistics"]
# %%


# hrt rastercalculator
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
        raster_out=depth_hrt,
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

assert depth_hrt.statistics(approve_ok=False) == SETUPS[SETUP]["statistics"]

# %%
