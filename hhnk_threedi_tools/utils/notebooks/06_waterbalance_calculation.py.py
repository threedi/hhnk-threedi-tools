# %%
# Open in jupyterlab as a notebook; right click .py -> Open With -> Jupytext Notebook
# # Steps:
# 1. Load imports
# 2. Enter variables
# 3. Run start_calculation tab

try:
    from hhnk_threedi_tools.utils.notebooks.notebook_setup import setup_notebook
except:
    from notebook_setup import (
        setup_notebook,
    )  # in case hhnk-threedi-tools is not part of python installation

notebook_data = setup_notebook()


# ipython imports
from IPython.display import HTML, display

display(HTML("<style>.container {width:90% !important;}</style>"))

from hhnk_threedi_tools.core.vergelijkingstool.gui.vergelijkingstool_gui import (
    VergelijkingstoolGUI,
)

self = VergelijkingstoolGUI()

display(self)

# %%
# %% [markdown]
# Open in jupyterlab as a notebook; right click .py -> Open With -> Jupytext Notebook
# # 06. Waterbalance berekenen
# To use the waterbalacne tool calculate the waterbalance for a 3Di/RANA model, you need to have the model results downloaded.
# You can use the download tool in notebook 02 to download to get the aggregated 3Di results. As a second requierement,
# you will need to add a polygon where you would like the waterbalance tool to be used.
# The waterbalance tool will calculate the waterbalance for all time steps and export the results to a new folder in the batch folder.

import os

import geopandas as gpd
import hhnk_research_tools as hrt
from IPython.display import HTML, display

from hhnk_threedi_tools import Folders
from hhnk_threedi_tools.core.waterbalance_calculation.waterbalance import WaterBalance

# %%
# Add qgis plugin deps to syspath and load notebook_data
try:
    from hhnk_threedi_tools.utils.notebooks.notebook_setup import setup_notebook
except:
    from notebook_setup import setup_notebook  # in case hhnk-threedi-tools is not part of python installation


notebook_data = setup_notebook()
# %%

display(HTML("<style>.container {width:90% !important;}</style>"))
# %matplotlib inline


folder = Folders(notebook_data["polder_folder"])

# paths = {
#     r"H:\02.modellen\bergen_noord_huidig_situatie_JA": "water_berging_JA.gpkg",
#     r"H:\02.modellen\bergen_noord_variant_1_JA": "waterberging_v1.shp",
#     r"H:\02.modellen\bergen_noord_variant_2_JA": "waterberging_v2.shp",
#     r"H:\02.modellen\bergen_noord_variant_3_JA": "waterberging_v3.shp",
# }

paths = {folder.path: "water_berging_JA.gpkg"}

for path, waterbergin_polygon in paths.items():
    folder = Folders(path)
    polygon_gdf = gpd.read_file(folder.source_data.path / waterbergin_polygon)
    batch_path = folder.threedi_results.batch.path
    batch_folders = os.listdir(batch_path)
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
# %%
