# %% [markdown]
# ## Create grid nodes and lines from sqlite

# %%
# Add qgis plugin deps to syspath and load notebook_data
from notebook_setup import setup_notebook

notebook_data = setup_notebook()

import importlib.resources as pkg_resources  # Load resource from package
import json
import os
import sys

import geopandas as gpd
import ipywidgets as widgets
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from osgeo import gdal

from hhnk_threedi_tools import Folders, SqliteCheck

# %%
folder_dir = notebook_data["polder_folder"]

folder = Folders(folder_dir)
sqlite_test = SqliteCheck(folder)

sqlite_test.create_grid_from_sqlite(
    sqlite_path=folder.model.sqlite_paths[0],
    dem_path=folder.model.rasters.dem.path,
    output_folder=folder.output.sqlite_tests.path,
)
