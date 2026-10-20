# %% [markdown]

## Open the notebook

# Open the `.py` file as a notebook in JupyterLab:

# 1. Right-click the `.py` file.
# 2. Select **Open With** → **Jupytext Notebook**.

# ---
# ## Overview

# This notebook calculates the water balance for a **3Di/RANA model** using downloaded simulation results and a user-defined polygon. The water balance is computed for all available timesteps and exported to a new folder within the batch directory.

# ---
# ## Requirements

# Before running this notebook, make sure you have:

# 1. Downloaded the aggregated 3Di results.
#    - You can use the download tool provided in **Notebook 02**.

# 2. Created a polygon defining the area for which the water balance should be calculated.
#    - The polygon must be stored as a **GeoPackage (`.gpkg`)** file.

# ---
# ## Inputs

# The following inputs are required:

# - **Model folder** (`modelfolder`)
# - **Storage polygon GeoPackage** (`.gpkg`)

# The GeoPackage containing the polygon should be placed in:

# ```text
# 01_source_data
# ```

# ---
# ## Important

# The path to the **GeoPackage** is not detected automatically and must add the geopackge **NAME** manually to the **path_dictionary** before running the notebook.

# ---
# ## Output

# The tool will:

# - Calculate the water balance for all timesteps.
# - Generate water balance tables and results.
# - Save the output in a new folder within the batch directo
try:
    from hhnk_threedi_tools.utils.notebooks.notebook_setup import setup_notebook
except:
    from notebook_setup import setup_notebook  # in case hhnk-threedi-tools is not part of python installation


notebook_data = setup_notebook()
import os

import geopandas as gpd
import hhnk_research_tools as hrt
from IPython.display import HTML, display

from hhnk_threedi_tools import Folders
from hhnk_threedi_tools.core.waterbalance_calculation import plot_waterbalance
from hhnk_threedi_tools.core.waterbalance_calculation.waterbalance import WaterBalance

# %matplotlib inline


folder = Folders(notebook_data["polder_folder"])

# paths = {
#     r"H:\02.modellen\bergen_noord_huidig_situatie_JA": "water_berging_JA.gpkg",
#     r"H:\02.modellen\bergen_noord_variant_1_JA": "waterberging_v1.shp",
#     r"H:\02.modellen\bergen_noord_variant_2_JA": "waterberging_v2.shp",
#     r"H:\02.modellen\bergen_noord_variant_3_JA": "waterberging_v3.shp",
# }


def get_waterbalance(paths, batch_folder_name):
    for path, waterbergin_polygon in paths.items():
        folder = Folders(path)
        polygon_gdf = gpd.read_file(folder.source_data.path / waterbergin_polygon)
        batch_path = folder.threedi_results.batch.path
        batch_folders = os.listdir(batch_path)
        if batch_folder_name:
            batch_folders = batch_folder_name
        for results in batch_folders:
            downloads_path = batch_path / results / "01_downloads"
            output_raster_path = batch_path / results / "02_output_rasters"
            downloads = os.listdir(downloads_path)
            for download in downloads:
                scenario_result_path = downloads_path / download
                output_path = output_raster_path / download / f"waterbalance_{download}"
                if not os.path.isdir(scenario_result_path):
                    continue
                if os.path.exists(output_path):
                    continue
                self = WaterBalance(threedi_result=hrt.ThreediResult(scenario_result_path), polygon_gdf=polygon_gdf)
                self.export(output_path)
                print(f"scenario {folder.name} / {download} done")


# %% [markdown]
## Inputs **get_waterbalance()*
# %%

paths = {folder.path: "water_berging_JA.gpkg"}
batch_folder_name = ["huidig_blok"]
get_waterbalance(paths, batch_folder_name)

# %% [markdown]
## Water Balance Plot
# Create water balance plots from exported 3Di water balance CSV files.


def get_waterbalance_plot(batch_path, batch_folder_name):

    batch_folders = os.listdir(batch_path)
    if batch_folder_name:
        batch_folders = batch_folder_name
    for results in batch_folders:
        output_raster_path = batch_path / results / "02_output_rasters"
        downloads = os.listdir(output_raster_path)
        for download in downloads:
            plot_water_balance_folder = output_raster_path / download / f"waterbalance_{download}"
            csv_water_balance_timeseries = plot_water_balance_folder / "water_balance_timeseries.csv"
            csv_water_balance_volumen = plot_water_balance_folder / "water_balance_volumes.csv"
            output_path_timeseries = plot_water_balance_folder / "water_balance_timeseries.png"
            output_path_net = plot_water_balance_folder / "water_balance_volume.png"
            # if os.path.exists(output_path_timeseries):
            #     continue
            plot_waterbalance.plot_water_balance(csv_water_balance_timeseries, output_path_timeseries)
            plot_waterbalance.plot_water_balance_volumes(csv_water_balance_volumen, output_path_net)
            print(f"Water balance plots created successfully.\nOutput folder: {plot_water_balance_folder}")


# %% [markdown]
## Inputs **get_waterbalance_plot()**
# %%

batch_path = folder.threedi_results.batch.path
batch_folder_name = ["huidig_blok"]
get_waterbalance_plot(batch_path, batch_folder_name)
