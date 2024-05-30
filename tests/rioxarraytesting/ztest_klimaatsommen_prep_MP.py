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

a = time.time()
@dask.delayed
def square(x):
    now = time.time() - a
    print(f"{round(now,1)} {x} start")
    time.sleep(x)
    return x**2

numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
squares = []
for number in numbers:
    s = square(number)
    squares.append(s)

dask.compute(squares)
print(time.time() - a)
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
