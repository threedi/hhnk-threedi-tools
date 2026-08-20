# %%
import os
from pathlib import Path

import fiona
import geopandas as gpd
import hhnk_research_tools as hrt
import pandas as pd

from hhnk_threedi_tools import Folders
from hhnk_threedi_tools.breaches.breaches import Breaches
from hhnk_threedi_tools.core.result_rasters.netcdf_to_gridgpkg import NetcdfToGPKG

model_folder = r"H:\02.modelrepos\00_DPRA_stresstest_1d2d_modellen"
results = r"H:\03.resultaten\DPRA stedelijk gebied\DPRA70mm2uur\results_per_polder"
modelen = os.listdir(model_folder)
# %%
for model in modelen[21:]:
    folder = Folders(os.path.join(model_folder, model))
    name_results = model + "_1d2d_dpra_70"
    resutls_model = os.path.join(results, name_results)
    if not os.path.exists(resutls_model):
        continue
    breach = Breaches(resutls_model)
    output_scenario_wss = breach.wss.path
    netcdf_folder = breach.netcdf.path
    print(f"start calculation for scenario {output_scenario_wss.parent.name}")

    # Define names of the outputfiles
    output_file = os.path.join(output_scenario_wss, "grid_nodes_correct_juan.gpkg")
    mask_flood = os.path.join(output_scenario_wss, "mask_flood.gpkg")
    dem_clip_output = os.path.join(output_scenario_wss, "dem_clip.vrt")
    output_file_depth = Path(os.path.join(output_scenario_wss, "max_wdepth_orig.tif"))
    output_waterlevel_raster = Path(os.path.join(output_scenario_wss, "max_waterlevel_orig.tif"))
    panden_path = folder.source_data.path / "panden.gpkg"

    # start the convertion from netcdf to gpkg
    if os.path.exists(output_file):
        print(f"Creating grid_raw for {breach.name}")
        NetcdfToGPKG(
            hrt.ThreediResult(netcdf_folder),
            waterdeel_path=folder.source_data.damo.path,
            waterdeel_layer=folder.source_data.damo.layers.waterdeel.name,
            panden_path=panden_path,
            panden_layer="panden",
            use_aggregate=False,
        ).run(
            output_file=output_file,
            timesteps_seconds=["max"],
            wlvl_correction=True,
            wlvl_correct_1d=True,
            overwrite=True,
        )
