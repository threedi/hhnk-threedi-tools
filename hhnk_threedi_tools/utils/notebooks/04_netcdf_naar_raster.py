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
# # Naverwerking waterdiepte
#
# De waterdiepte kaarten worden geinterpoleerd tussen verschillende waterstand punten. Bij HHNK worden echter watervlakken opgehoogd in het 2D om te voorkomen dat er dubbele berging ontstaat. Als dit vlak binnen een rekencel valt kan het zijn dat er water op dit vlak komt te staan. Dit willen we niet.
#
# Dit is opgelost door zelf de rasters te maken op basis van een gpkg. De werkstappen zijn;
#
# 1. NetCDF omzetten in GPKG met max wlvl
# 2. Cellen selecteren waar:
#     - DEM minder dan 50% van opp cel
#     - meer dan 95% water
#     - meer dan 99% pand
# 2. Max wlvl van geselecteerde cellen vervangen.
# 3. Wlvl/wdepth raster maken op basis van gpkg/kolom.
#

# %%
# Add qgis plugin deps to syspath and load notebook_data
from notebook_setup import setup_notebook

notebook_data = setup_notebook()


import xarray

# import threedi_raster_edits as tre
from hhnk_threedi_tools import Folders
import hhnk_research_tools as hrt
import geopandas as gpd

import hhnk_threedi_tools as htt
from hhnk_threedi_tools.core.result_rasters.calculate_raster import GridToWaterLevel, GridToWaterDepthDepth


# User input
folder_path = r"E:\02.modellen\model_test_v2"
scenario = "katvoed #1 piek_ghg_T1000"  # mapnaam


folder = Folders(folder_path)

dem_path = folder.model.schema_base.rasters.dem.base
# dem_path = r'E:\\02.modellen\\23_Katvoed\\02_schematisation\\00_basis\\rasters/dem_katvoed_ahn3.tif'

threedi_result = folder.threedi_results.one_d_two_d[scenario]

# %% [markdown]
# 1. Klaarzetten grid

# %%
# Select result
netcdf_gpkg = htt.NetcdfToGPKG.from_folder(
    folder=folder,
    threedi_result=threedi_result,
)

# Convert netcdf to grid gpkg
netcdf_gpkg.run()

# %% [markdown]
# 2. Berekenen rasters

# %%
OVERWRITE = False

grid_gdf = threedi_result.full_path("grid_wlvl.gpkg").load()

# %%
calculator_kwargs = {"dem_path": dem_path, "grid_gdf": grid_gdf, "wlvl_column": "wlvl_max_replaced"}

# Init calculator
with GridToWaterLevel(**calculator_kwargs) as self:
    wlvl_raster = self.run(output_file=threedi_result.full_path("wlvl_corr.tif"), overwrite=OVERWRITE)
with GridToWaterDepthDepth(calculator_kwargs["dem_path"], wlvl_path=wlvl_raster.path) as self:
    self.run(output_file=threedi_result.full_path("wdepth_corr.tif"), overwrite=OVERWRITE)
    print("Done.")

# %%
calculator_kwargs = {"dem_path": dem_path, "grid_gdf": grid_gdf, "wlvl_column": "wlvl_max_orig"}

with GridToWaterLevel(**calculator_kwargs) as self:
    wlvl_raster = self.run(output_file=threedi_result.full_path("wlvl_orig.tif"), overwrite=OVERWRITE)
with GridToWaterDepthDepth(calculator_kwargs["dem_path"], wlvl_path=wlvl_raster.path) as self:
    self.run(output_file=threedi_result.full_path("wdepth_orig.tif"), overwrite=OVERWRITE)

# %% [markdown]
# 3. Berekenen Schaderaster

# %%
# Schadeschatter heeft wat extra voorbereiding nodig.
from pathlib import Path

# Variables
cfg_file = hrt.get_pkg_resource_path(package_resource=hrt.waterschadeschatter.resources, name="cfg_lizard.cfg")
landuse_file = r"E:\01.basisgegevens\rasters\landgebruik\landuse2019_3di_tiles\landuse2019_3di_tiles.vrt"

depth_file = threedi_result.full_path("wdepth_corr.tif")
output_file = threedi_result.full_path("damage_corr_lizard.tif")


wss_settings = {
    "inundation_period": 48,  # uren
    "herstelperiode": "10 dagen",
    "maand": "sep",
    "cfg_file": cfg_file,
    "dmg_type": "gem",
}

# Calculation
self = hrt.Waterschadeschatter(depth_file=depth_file.path, landuse_file=landuse_file.path, wss_settings=wss_settings)

# Berkenen schaderaster
self.run(
    output_raster=hrt.Raster(output_file),
    calculation_type="sum",
    verbose=False,
    overwrite=False,
)
