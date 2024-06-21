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
import rasterio as rio
import rioxarray as rxr
import xarray as xr

from hhnk_threedi_tools.core.result_rasters.calculate_raster import BaseCalculatorGPKG
from tests.config import FOLDER_TEST, TEMP_DIR

CHUNKSIZE = 4096
SETUP = "full"
SETUPS = {
    "clip_small": {"statistics": {"min": -32767.0, "max": 32765.0, "mean": 670.35384, "std": 2858.262956}},
    "clip_medium": {"statistics": {"min": -32767.0, "max": 32765.0, "mean": 664.976118, "std": 2841.971657}},
    "full": {"statistics": {"min": -32760.0, "max": 32765.0, "mean": 699.288424, "std": 2826.983867}},
}


PLAYGROUND_DIR = Path(r"d:\projecten\D2402.HHNK.Ondersteuning_Python\03.playground")
# dem = FOLDER_TEST.model.schema_base.rasters.dem FIXME: Exception: No connection or database path provided
dem = hrt.Raster(PLAYGROUND_DIR / SETUP / "dem.tif")
grid_gdf = gpd.read_file(PLAYGROUND_DIR / SETUP / "grid_corr.gpkg")
wlvl = hrt.Raster(PLAYGROUND_DIR / SETUP / "wlvl.tif")


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
        basecalc.run(output_file=wlvl.path, mode="MODE_WLVL", overwrite=True)
    print(time.time() - now)

depth = hrt.Raster(PLAYGROUND_DIR / SETUP / "depth.tif")


def get_rasters(chunksize=CHUNKSIZE):
    # Load your rasters (replace 'path_to_raster1.tif' and 'path_to_raster2.tif' with actual file paths)
    raster1 = rxr.open_rasterio(dem.base, chunks={"x": chunksize, "y": chunksize})
    raster2 = rxr.open_rasterio(wlvl.base, chunks={"x": chunksize, "y": chunksize})

    # Set NoData values (if needed, adjust to match your actual NoData value)
    raster1 = raster1.where(raster1 != raster1.rio.nodata, np.nan)
    raster2 = raster2.where(raster2 != raster2.rio.nodata, np.nan)
    return raster1, raster2


def write_to_raster(result, scale_factor):
    result.rio.to_raster(
        depth.base,
        chunks={"x": CHUNKSIZE, "y": CHUNKSIZE},
        # lock=True, #Use dask multithread
        compress="ZSTD",
        tiled=True,
        PREDICTOR=2,
        ZSTD_LEVEL=1,
        dtype="int16",
    )

    # Settings the scale_factor does not work with rioxarray. T
    with rio.open(depth.base, "r+") as src:
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
    result = raster1 - raster2

    # Assign values less than 0 to NoData

    result *= 1000  # scale_factor so we can save ints

    result = result.where(result >= 0, -9999)

    # Export the result directly to the output raster in chunks
    result.rio.set_nodata(raster1.rio.nodata)
    write_to_raster(result, scale_factor=0.001)

    print(time.time() - now)


rxr_calc_scaled_raster()

assert depth.statistics(approve_ok=False) == SETUPS[SETUP]["statistics"]


# %%
# xarray map_blocks
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

    write_to_raster(result, scale_factor=0.001)

    print(time.time() - now)


rxr_map_blocks()

assert depth.statistics(approve_ok=False) == SETUPS[SETUP]["statistics"]
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

assert depth.statistics(approve_ok=False) == SETUPS[SETUP]["statistics"]

# %%
