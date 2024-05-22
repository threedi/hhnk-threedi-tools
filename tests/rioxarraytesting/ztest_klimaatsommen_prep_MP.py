# %%
# First-party imports
import datetime
import multiprocessing as mp
import shutil

import hhnk_research_tools as hrt
import pandas as pd
import pytest

# Local imports
from hhnk_threedi_tools.core.climate_scenarios.klimaatsommen_prep import (
    KlimaatsommenPrep,
)
from hhnk_threedi_tools.core.folder_helpers import ClimateResult
from hhnk_threedi_tools.core.folders import Folders
from tests.config import FOLDER_TEST, TEMP_DIR, TEST_DIRECTORY

# def test_klimaatsommenprep_verify():
#     """Raises because not all 18 scenarios downloaded"""
#     with pytest.raises(Exception):
#         klimaatsommenprep = KlimaatsommenPrep(
#             folder=FOLDER_TEST,
#             batch_name="batch_test",
#             cfg_file="cfg_lizard.cfg",
#             landuse_file=FOLDER_TEST.model.schema_base.rasters.landuse,
#             verify=True,
#         )

start_time = datetime.datetime.now()
folder = Folders(r"C:\Users\Wietse\Documents\HHNK\Poldermodellen\callantsoog")
batch_name = "callantsoog_small"


self = klimaatsommenprep = KlimaatsommenPrep(
    folder=folder,
    batch_name=batch_name,
    cfg_file="cfg_lizard.cfg",
    landuse_file=FOLDER_TEST.model.schema_base.rasters.landuse,
    min_block_size=2**12,
    verify=False,
)

klimaatsommenprep.dem = klimaatsommenprep.get_dem()

import importlib

import test_mp_func

importlib.reload(test_mp_func)

scenarios = [self.get_scenario(name=name) for name in self.batch_fd.downloads.names]
scenarios = [i for i in scenarios if i.netcdf.exists()]


# %%
import time

import dask

# print(*dask.compute(squares))


@dask.delayed
def create_depth(scenario):
    start_time = datetime.datetime.now()
    print(scenario.depth_max.name)
    threedi_result = scenario.netcdf

    klimaatsommenprep = KlimaatsommenPrep(
        folder=folder,
        batch_name=batch_name,
        cfg_file="cfg_lizard.cfg",
        landuse_file=folder.model.schema_base.rasters.landuse,
        min_block_size=2**12,
        verify=False,
    )

    klimaatsommenprep.dem = klimaatsommenprep.get_dem()

    # klimaatsommenprep.calculate_depth(
    #     scenario=scenario,
    #     threedi_result=threedi_result,
    #     grid_filename="grid_wlvl.gpkg",
    #     overwrite=True,
    # )

    klimaatsommenprep.calculate_damage(scenario=scenario, overwrite=True)

    duration = hrt.time_delta(start_time)
    print(duration)
    return duration


dasktasks = []
for scenario in scenarios[:6]:
    # dasktasks.append(create_depth(scenario))
    dasktasks.append(create_depth(scenario))

# dask.compute(dasktasks)


# # %%
# if __name__ == "__main__":
#     test_klimaatsommenprep()
# %%

from dask.distributed import Client

client = Client(processes=False)
client
# %%
dasktasks[0].visualize()
# %%

# Cell in Jupyter Notebook
from audioop import mul
from multiprocessing import Pool

import pandas as pd

# %%
# numbers = [1, 2, 3, 4, 5]
# with Pool() as pool:
#     squares = pool.map(worker, numbers)
# print(squares)
from hhnk_research_tools.processes import multiprocess
from test_mp_func import worker

df = pd.DataFrame([1, 2, 3, 4, 5])

# multiprocess(df, worker)

# %%
import time

import dask


@dask.delayed
def square(x):
    time.sleep(5)
    # return x**2


numbers = [1, 2, 3, 4, 5]

squares = []

for number in numbers:
    s = square(number)
    squares.append(s)

dask.compute(squares)

# print(*dask.compute(squares))


# %% TESTING WITH xarray, rasterio and rioxarray


r_path = r"C:\Users\Wietse\Documents\HHNK\Poldermodellen\callantsoog\03_3di_results\batch_results\callantsoog_speedtest\01_downloads\damage_total_blok_ghg_T1000.tif"
import rioxarray as rxr
import xarray as xr

# da = xr.open_dataarray(r_path, chunks='auto')
xds = rxr.open_rasterio(r_path, masked=False, chunks=True)
xds

# %%
import numpy as np
import xarray as xr
from xrspatial.zonal import apply

zones_val = np.array([[1, 1, 0, 2], [0, 2, 1, 2]])

zones = xr.DataArray(zones_val)

values_val = np.array([[2, -1, 5, 3], [3, np.nan, 20, 10]])

agg = xr.DataArray(values_val)

func = lambda x: 0


def func(x):
    return np.unique(x, return_counts=True)


apply(zones, agg, func, nodata=-1)

agg


# %%

# import cupy_xarray
import threading

# from dask.utils import SerializableLock
import time
from pathlib import Path

import hhnk_research_tools as hrt
import rioxarray as rxr

now = time.time()

path_out = Path(
    r"C:/Users/Wietse/Documents/HHNK/Poldermodellen/callantsoog/03_3di_results/batch_results/callantsoog_small/01_downloads"
)
dem = hrt.Raster(
    r"c:/Users/Wietse/Documents/HHNK/Poldermodellen/callantsoog/02_schematisation/00_basis/rasters/dem_callantsoog.tif"
)
wlvl = hrt.Raster(
    r"C:/Users/Wietse/Documents/HHNK/Poldermodellen/callantsoog/03_3di_results/batch_results/callantsoog_small/01_downloads/wlvl_max_piek_glg_T10.tif"
)

depth = hrt.Raster(path_out.joinpath("depth.tif"))

CHUNKSIZE = 4096
# Load your rasters (replace 'path_to_raster1.tif' and 'path_to_raster2.tif' with actual file paths)
raster1 = rxr.open_rasterio(wlvl.base, chunks={"x": CHUNKSIZE, "y": CHUNKSIZE})
raster2 = rxr.open_rasterio(dem.base, chunks={"x": CHUNKSIZE, "y": CHUNKSIZE})


# Set NoData values (if needed, adjust to match your actual NoData value)
raster1 = raster1.where(raster1 != raster1.rio.nodata)
raster2 = raster2.where(raster2 != raster2.rio.nodata)


# raster1_chunk = raster1.chunk({'x': CHUNKSIZE, 'y': CHUNKSIZE})
# raster2_chunk = raster2.chunk({'x': CHUNKSIZE, 'y': CHUNKSIZE})
# result = raster1_chunk - raster2_chunk


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
with depth.open_gdal_source_write() as gdal_source:
    b = gdal_source.GetRasterBand(1)
    b.SetScale(0.001)

print(time.time() - now)

# %%

d = rxr.open_rasterio(depth.base, mask_and_scale=False)
print(d)
d.close()

import numpy as np

np.unique(d.data)

# %%
import time

now = time.time()
dem = hrt.Raster(
    r"C:/Users/Wietse/Documents/HHNK/Poldermodellen/callantsoog/02_schematisation/00_basis/rasters/dem_callantsoog.tif"
)
wlvl = hrt.Raster(
    r"C:/Users/Wietse/Documents/HHNK/Poldermodellen/callantsoog/03_3di_results/batch_results/callantsoog_small/01_downloads/wlvl_max_piek_glg_T10.tif"
)
depth = hrt.Raster(path_out.joinpath("depth2.tif"))


def run_rtype_window(block):
    """Custom calc function on blocks in hrt.RasterCalculatorV2"""
    block_out = block.blocks["wlvl"] - block.blocks["dem"]

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
    min_block_size=1024,
    verbose=True,
    tempdir="",
)
# Run calculation of output raster
raster_calc.run(overwrite=True)

print(time.time() - now)
