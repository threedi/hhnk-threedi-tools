# %%
import os
import sys
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import matplotlib.pyplot as plt
import pandas as pd
from osgeo import gdal

from hhnk_threedi_tools.breaches.breaches import Breaches
from hhnk_threedi_tools.core.result_rasters.grid_to_raster import (
    GridToWaterDepth,
    GridToWaterLevel,
)
from hhnk_threedi_tools.core.result_rasters.netcdf_to_gridgpkg import NetcdfToGPKG

schadeschatter_path = Path(r"E:\01.basisgegevens\hhnk_schadeschatter")
if str(schadeschatter_path) not in sys.path:
    sys.path.append(str(schadeschatter_path))
import faulthandler

faulthandler.enable()


# %%
# This function clips a DEM raster to a shapefile and saves it in VRT.
def clip_DEM(inputraster, outputraster, projection, shapefile, resolution, raster_format):
    if not os.path.exists(outputraster):
        options = gdal.WarpOptions(
            cutlineDSName=shapefile,
            cropToCutline=(True),
            format= raster_format,
            dstSRS=projection,
            xRes=resolution,
            yRes=resolution,
        )
        outimage = gdal.Warp(srcDSOrSrcDSTab=inputraster, destNameOrDestDS=outputraster, options=options)
    else:
        print(f"The raster {outputraster} already exists")


# This function select from the grid.gpkg the data that has water
def grid_selection(grid_gdf, output_scenario_wss):
    # select grids to be used to rasteriezed water depth

    #ROR
    # active_cells = grid_gdf[grid_gdf["vol_max"] > 1.5]

    #IPO due to different versions
    if "vol_netcdf_m3" in grid_gdf.columns: 
                vol_max = "vol_netcdf_m3"
    else: 
        vol_max = "vol_max"

    active_cells = grid_gdf[grid_gdf[f'{vol_max}'] > 1.5]
    if active_cells.empty:
        print(f"There is no inundation for de polder {output_scenario_wss.parent.name}")
        return None
    else:
        # buffer active cells
        active_cells_buffer = gpd.GeoDataFrame.buffer(active_cells, 1)
        # add geomtery to active cells.
        cells_buffer_gdf = gpd.GeoDataFrame(geometry=active_cells_buffer)

        # Add a column to disolve grids and create a single shape  explode the shape file in case there are many.
        cells_buffer_gdf["disolve"] = 1
        cells_disolve = cells_buffer_gdf.dissolve(by="disolve").explode()

        # clip grid_gdf witht  the previous shapefile to select the grid to be used for rasterized
        grid_selection = grid_gdf.clip(cells_disolve)

        # Selct the id from selection from clipped grid
        filter_id = grid_selection["id"]
        grid_gdf_new = grid_gdf[grid_gdf["id"].isin(filter_id)]

        # Save the new grid to a file
        new_grid_output = os.path.join(output_scenario_wss, "new_grid.gpkg")
        grid_gdf_new.to_file(new_grid_output, driver="GPKG", engine="pyogrio")

        # disolve grid selection
        grid_gdf_new["disolve"] = 1
        mask = grid_gdf_new.dissolve(by="disolve")

        # save file
        mask_path = os.path.join(output_scenario_wss, "mask_flood.gpkg")
        mask.to_file(mask_path, driver="GPKG")
        return grid_gdf


# Get the paths of all scenarios
def get_paths(base_folder, scenario_name: list = None, specific_scenario=False, skip: list = None):
    # Get all the folders in the base folder
    items = os.listdir(base_folder)
    models = []
    region_paths = []
    # Get all folders from the output basefolder
    for item in items:
        path = Path(os.path.join(base_folder, item))
        # select only the folders that are directories
        if os.path.isdir(path):
            models.append(path)
        else:
            continue
    # If scenario_name is provided, filter the models otherwise use all models
    for model in models:
        if specific_scenario:
            model_scenarios = scenario_name
        else:
            model_scenarios = os.listdir(model)
        for model_scenario in model_scenarios:
            if model_scenario in skip:
                continue
            # Check if the path exists and append into the list region_paths
            path = Path(os.path.join(model, model_scenario))
            if path.exists():
                region_paths.append(path)
        else:
            continue

    return region_paths



def calculate_depth_raster(region_paths, dem_path, OVERWRITE, EPSG, spatialResolution):
    # Set variables for depth raster
    DEM_location = dem_path

    # loop over the pregion_paths
    for region_path in region_paths:
        # Define output folder and result file
        breach = Breaches(region_path)
        output_scenario_wss = breach.wss.path
        netcdf_folder = breach.netcdf.path
        print(f"start calculation for scenario {output_scenario_wss.parent.name}")

        # Define names of the outputfiles
        output_file = os.path.join(output_scenario_wss, "grid_raw.gpkg")
        mask_flood = os.path.join(output_scenario_wss, "mask_flood.gpkg")
        dem_clip_output = os.path.join(output_scenario_wss, "dem_clip.vrt")
        output_file_depth = Path(os.path.join(output_scenario_wss, "max_wdepth_orig.tif"))
        print(output_file_depth)
        output_waterlevel_raster = Path(os.path.join(output_scenario_wss, "max_waterlevel_orig.tif"))


        # start the convertion from netcdf to gpkg
        if not os.path.exists(output_file):

            print(f"Creating grid_raw for {breach.name}")
            NetcdfToGPKG(hrt.ThreediResult(netcdf_folder)).run(
                output_file=output_file,
                timesteps_seconds=["max"],
                wlvl_correction=False,
                overwrite=OVERWRITE,
            )

            # read the grid result
            grid_gdf = gpd.read_file(output_file, driver="GPKG", engine="pyogrio")

            # select the grid with water
            new_grid_gdf = grid_selection(grid_gdf, output_scenario_wss)

            if new_grid_gdf is None:
                continue

        else:
            # read the grid in case it exists
            grid_gdf = gpd.read_file(output_file, driver="GPKG", engine="pyogrio")
            if "vol_netcdf_m3" in grid_gdf.columns: 
                vol_max = "vol_netcdf_m3"
            else: 
                vol_max = "vol_max"

            if grid_gdf[grid_gdf[f"{vol_max}"] > 1.5].empty:
                continue
            print(f"The grid for scenario {breach.name} already exists")

        # Check if the output depth raster exists
        if not os.path.exists(output_file_depth):
            print(os.path.exists(output_file_depth))
            # clip the DEM to be used to calculate the depth raster
            clip_DEM(DEM_location, dem_clip_output, EPSG, mask_flood, spatialResolution, raster_format='GTiff')

            new_grid_gdf = gpd.read_file(
                os.path.join(output_scenario_wss, "new_grid.gpkg"), driver="GPKG", engine="pyogrio"
            )

            print(f"Calculating max level raster for scenario {netcdf_folder.parent.name} has started")
            # Set the parameters for the calculator
            calculator_kwargs = {
                "dem_path": dem_clip_output,
                "grid_gdf": new_grid_gdf,
                "wlvl_column": "wlvl_max",
                # "wlvl_column": "wlvl_max_orig"
            }

            # Initialize the GridToWaterLevel class and run the calculation
            
            faulthandler.enable()
            with GridToWaterLevel(**calculator_kwargs) as self:
                self.run(output_file=output_waterlevel_raster, overwrite=True)
            
            calculator_kwargs = {
                "dem_path": dem_clip_output,
                "wlvl_path": output_waterlevel_raster,
            }
            # Initialize the GridToWaterDepth class and run the calculation
            with GridToWaterDepth(**calculator_kwargs) as self:
                print(f"Calculating max water depth raster for scenario {netcdf_folder.parent.name} has started")
                self.run(output_file=output_file_depth, overwrite=True)

        else:
            print(f"the {output_file_depth.stem} already exists, check in case to need replacement")
            continue


# This function calculates the indirect and direct damage based on the calculation type: "sum" (total), "direct", "indirect"
def indirect_direct(folder_output, calculation_type, self_wss):
    # Calculate total damagae if calculation type is sum
    if calculation_type == "sum":
        name = "damage_orig_lizard.tif"
        damage_output_file = os.path.join((folder_output), name)
        print(f"Calculating {name}")
    # Calculate direct and indirect damage
    else:
        name = calculation_type + "_cost_damage.tif"
        damage_output_file = os.path.join((folder_output), name)
        print(f"Calculating {name}")
    # Check if the output file already exists and run the calculation
    if not os.path.exists(damage_output_file):
        self_wss.run(
            output_raster=hrt.Raster(damage_output_file),
            calculation_type=calculation_type,
            verbose=True,
            overwrite=False,
        )
    else:
        print(f"the output file {damage_output_file} already exists")


# This function collect all the inputs to be use in the indirect_direct function
def calculate_damage_raster(region_paths, landuse_file, cfg_file, EPSG="EPSG:28992"):
    # Set variable for damage
    wss_settings = {
        "inundation_period": 48,  # uren
        "herstelperiode": "10 dagen",
        "maand": "sep",
        "cfg_file": cfg_file,
        "dmg_type": "gem",
    }

    # define calculation types
    calculation_types = ["sum", "direct", "indirect"]

    # loop over the pregion_paths
    for region_path in region_paths:
        # Define output folder and results file
        breach = Breaches(region_path)
        output_scenario_wss = breach.wss.path
        mask_flood = os.path.join(output_scenario_wss, "mask_flood.gpkg")
        out_landuse = os.path.join(output_scenario_wss, "landuse_2021_clip.tif")
        depth_file = os.path.join(output_scenario_wss, "max_wdepth_orig.tif")
        depth_file_out = os.path.join(output_scenario_wss, "max_wdepth_orig_bounds_fix.tif")

        open_depth_raster = gdal.Open(depth_file)
        spatialResolution = open_depth_raster.GetGeoTransform()[1]
        open_depth_raster = None

        # clip landuse
        clip_DEM(landuse_file, out_landuse, EPSG, mask_flood, spatialResolution, raster_format='GTiff')
        # clip depth
        clip_DEM(depth_file, depth_file_out, EPSG, mask_flood, spatialResolution,  raster_format='GTiff')

        # set the file to run damage raster calculation
        self_wss = hrt.Waterschadeschatter(
            depth_file=Path(depth_file_out),
            landuse_file=out_landuse,
            wss_settings=wss_settings,
        )

        for calculation_type in calculation_types:
            indirect_direct(output_scenario_wss, calculation_type, self_wss)
            print(calculation_type)


# This function sum values per pixel in the damage rasters and returns the total cost
def sum_total_raster(calculation_type, region_path):
    # loop over the pregion_paths

    # Define output folder and result file
    breach = Breaches(region_path)
    output_scenario_wss = breach.wss.path
    # Define the raster path based on the calculation type
    if calculation_type == "sum":
        name = "damage_orig_lizard.tif"
        raster_path = os.path.join(output_scenario_wss, name)
        print(f"Calculating {name} for {breach.name}")
    else:
        name = calculation_type + "_cost_damage.tif"
        raster_path = os.path.join(output_scenario_wss, name)
        print(f"Calculating {name}")
    # Open Damage Raster, get pixel width, calculate area and sum values
    raster_resolution = gdal.Open(raster_path, gdal.GA_ReadOnly)
    raster = hrt.Raster(raster_path)
    area_pixel = pow(hrt.RasterMetadata(raster_resolution).pixel_width, 2)
    sum_values = raster.sum()
    total_cost = sum_values * area_pixel

    return total_cost


# Save the total_cost from the sum_total_raster in a CSV file.
def save_damage_csv(region_paths):
    # loop over the pregion_paths and set output paths
    for region_path in region_paths:
        breach = Breaches(region_path)
        breach_name = breach.name
        output_scenario_wss = breach.wss.path
        csv_path = os.path.join(output_scenario_wss, "total_costs_area.csv")
        if not os.path.exists(csv_path):
            print(f"Creating csv file for {breach_name}")
            name = "damage_orig_lizard.tif"
            damage_output_file = output_scenario_wss / name

            # Check if the damage output file exists and set the columns of the csv file
            if damage_output_file.exists():
                calculation_types = ["sum", "direct", "indirect"]
                # Initialize values
                results = {
                    "scenario": breach_name,
                    "direct": 0,
                    "indirect": 0,
                    "total_damage_raster": 0,
                }
                # Loop through calculation types and calculate total costs
                for calculation_type in calculation_types:
                    value = sum_total_raster(calculation_type, region_path)
                    if calculation_type == "direct":
                        results["direct"] = value
                    elif calculation_type == "indirect":
                        results["indirect"] = value
                    elif calculation_type == "sum":
                        results["total_damage_raster"] = value

                    # Calculate totals and differences
                    results["total_sum"] = results["direct"] + results["indirect"]
                    results["difference"] = results["total_damage_raster"] - results["total_sum"]
                    
                    #TODO fix indirect raster calculation and delete this line.
                    results["indirect_fix"] = results["total_damage_raster"] - results["direct"]
                # Append to DataFrame
                final_result = pd.DataFrame(results, index=[0])
                # Save the DataFrame to a CSV file
                final_result.to_csv(csv_path)
        else:
            print(f"The csv output file {csv_path} exists, check in case to need replacement")
            continue


# Create BAR graph for damage and save it as png
def create_pgn_dagame(region_paths):
    for region_path in region_paths:
        # Define output folder and result file
        breach = Breaches(region_path)
        output_scenario_wss = breach.wss.path
        csv_path = output_scenario_wss / "total_costs_area.csv"
        breach_name = breach.name
        jpeg_output = breach.jpeg.path
        png_name = breach_name + "_bar_graph.png"
        # Check if the csv file exists and create the bar graph
        if csv_path.exists():
            # Read the CSV file and prepare data for plotting, drop unnecessary columns
            df_data = pd.read_csv(csv_path)
            df_no_unamaed = df_data.drop(["Unnamed: 0"], axis=1)
            df_no_total = df_no_unamaed.drop(["total_sum"], axis=1)
            df_no_diference = df_no_total.drop(["difference"], axis=1)
            df_indirect_fix = df_no_diference.drop(["indirect"], axis=1)

            #TODO fix indirect raster calculation and delete this two lines.
            df_indirect_fix = df_indirect_fix.iloc[:,[0,1,3,2]]
            df_indirect_fix.rename(columns={"indirect_fix": 'indirect'})

            # Transpose the DataFrame and select the first three rows for plotting
            df_data = df_indirect_fix.T
            df_selection = df_data[1:4]
            color = ["orange", "yellow", "red"]

            # Add column  color column into the dataframe
            df_selection["color"] = color
            y = df_selection[0]
            
            plt.style.use('seaborn-v0_8-whitegrid')
            plt.figure(figsize=(9,7)) # Width: 10 inches, Height: 6 inches
            # Plot bar column with the selected values
            plt.bar(x=df_selection.index, height=y, color=color, width=0.5)

            plt.xticks(color="black", rotation="horizontal")

            # Set the title of the graph
            scenario = df_data.loc['scenario'].values[0]
            title = f"Direct, Indirect en Totaal Kost\nbij de Scenario {scenario}"
            plt.title(title, fontweight="semibold", size =  11, loc='center')


            # Add labels to the axes
            plt.xlabel("Cost Type", fontweight="semibold", size =  10)
            plt.ylabel("Euros €", fontweight="semibold", size =  10)
            # Optional: reduce space around the bars
            
            # Set path to save the graph
            path_png = os.path.join(jpeg_output, png_name)

            # Set values in the columns
            for i, v in enumerate(y):
                
                formatted_label = f"€ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                plt.text(i, v + v*0.0070, formatted_label, ha="center", va="bottom", fontsize=9.5)

            print(f"graph done for {breach_name}")

            # plt.tight_layout(pad=1)
            # plt.subplots_adjust(left=0, right=1, top=1, bottom=0)
            plt.subplots_adjust(left=0.05, right=0.98, top=0.92, bottom=0.08)
            # Save the graph as a PNG file
            plt.savefig(path_png)
            # plt.show()
            # Clear the current figure to avoid overlap in future plots
            plt.clf()


# %%
if __name__ == "__main__":
    # Set the paths for the DEM, landuse file, base folder and configuration file
    dem_path = r"E:\02.modellen\RegionalFloodModel\work in progress\schematisation\rasters\dem_1_met_amstelmeer.tif"
    landuse_file = r"E:\01.basisgegevens\rasters\landgebruik\landuse2021_tiles\combined_rasters.vrt"
    base_folder = r"E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\output"
    cfg_file = schadeschatter_path / "01_data/cfg/cfg_lizard.cfg"
    # ipo_paths_path = r"E:\03.resultaten\Normering Regionale Keringen\output\ipo_scenarios_paths.csv"
    region_paths = [
        
            r'E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\output\ROR_PRI-dijktrajecten_12-1_12-2_13-6_13-7_Deel_Zuid\ROR-PRI-OOSTERDIJK_VAN_DRECHTERLAND_0.5-T1000',
    ]
    # Set the parameters for the calculation
    OVERWRITE = False
    EPSG = "EPSG:28992"

    #This scenarios need to be recalculated
    # r'E:\03.resultaten\Normering Regionale Keringen\output\IPO_SBLZ_JA_WIP_DONE\IPO_SBLZ_1097_JA', 
    # r'E:\03.resultaten\Normering Regionale Keringen\output\IPO_VRNKWE_WIP_DONE\IPO_VRNK_WEST_355_WE'
    # r"E:\03.resultaten\Normering Regionale Keringen\output\IPO_SBLZ_JA_WIP_DONE\IPO_SBLZ_908_JA",
    # r"E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\output\ROR_PRI-dijktrajecten_12-1_12-2_13-6_13-7_Deel_Zuid\ROR-PRI-OOSTERDIJK_VAN_DRECHTERLAND_0.5-T10",
    # r"E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\output\ROR_PRI-dijktrajecten_12-1_12-2_13-6_13-7_Deel_Zuid\ROR-PRI-OOSTERDIJK_VAN_DRECHTERLAND_0.5-T100",
    spatialResolution = 0.5

    # Define scenarios to skip

    skip = []
    # I have to structure better this code, the idea is that it finish everything in one go.
    # So frist: calculate damage, second csv, and the create pgn. This process needs to be done by scenario
    #TODO IPO SBLZ 908, ipo_vrnk_west_355
    specefic_scenario = False
    # region_paths = get_paths(base_folder, scenario_name=None, specific_scenario=specefic_scenario, skip=skip)

    # calculate_depth_raster(region_paths, dem_path, OVERWRITE, EPSG, spatialResolution)
    calculate_damage_raster(region_paths, landuse_file, cfg_file, EPSG)
    save_damage_csv(region_paths)
    create_pgn_dagame(region_paths)
# %%
