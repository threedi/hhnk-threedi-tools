# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.6
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# ## Create grid nodes and lines from sqlite

# %%
# Add qgis plugin deps to syspath and load notebook_data
from notebook_setup import setup_notebook

notebook_data = setup_notebook()

import os
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from osgeo import gdal
import sys
import importlib.resources as pkg_resources  # Load resource from package
import ipywidgets as widgets
import json


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
