# %%
# -*- coding: utf-8 -*-
"""
Created on Thu Feb  3 11:01:55 2022

@author: chris.kerklaan

-- Selecteer rekencellen die volledig binnen watervlakken liggen.
-- 1. Interpoleer de naast gelegen cellen naar dit vlak
-- 2. Vind 1D/2D connectie en plaats de 1D waterstand op de 2D cel.
-- Bereken het volume verschil per cel.
 

Structuur:
    Classes:
        - GridEdit - Past de netcdf aan op basis van grid ids
    Functions:
        - Selecteer nodes op basis van een vector
        - Creeer waterdiepte op basis van een tijdstap
    
"""

# system appends
import sys

# sys.path.append(r"E:\github\wvangerwen\hhnk-threedi-tools")
# sys.path.append(r"C:\Users\chris.kerklaan\Documents\Github\hhnk-research-tools")

# First-party imports
import shutil
import numpy as np
from osgeo import ogr

# Third-party imports
import xarray as xr

# import rasterio
from shapely.geometry import mapping
# from rasterio.mask import mask

import pandas as pd
from shapely import wkt
import geopandas as gpd
from rasterstats import zonal_stats
# from threedidepth.calculate import calculate_waterdepth
import matplotlib.pyplot as plt

import calculate_patched as threedidepth_calculate

from threedigrid.admin.gridresultadmin import GridH5ResultAdmin

# Local imports
#TODO use logging? Logging breaks when import threeditools.
from hhnk_threedi_tools import Folders

# from hhnk_research_tools.threedi.gridedit import GridEdit
from hhnk_research_tools.gis.raster import Raster
import hhnk_research_tools as hrt




class GridEdit:
    def __init__(self, netcdf_path):
        self.ds = xr.open_dataset(netcdf_path)
        self.edit = self.ds.load()

    def set_wl_timeseries(self, node_id, levels):
        assert len(levels) == len(self.edit["Mesh2D_s1"])

        for timestep in range(len(self.edit["Mesh2D_s1"])):
            self.edit["Mesh2D_s1"][timestep][node_id - 1] = levels[timestep]

    def write(self, output_path):
        self.edit.to_netcdf(output_path)


# Helper functions
def get_geometry_grid_cells(grid: GridH5ResultAdmin, use_ogr=True):
    """returns an ogr Datasource with the cells of a threedigrid"""
    grid.cells._to_ogr("ESRI Shapefile", "/vsimem/cells.shp")
    if use_ogr:
        ds = ogr.Open("/vsimem/cells.shp")
        layer = ds.GetLayer()
        return ds, layer
    else:
        return gpd.read_file("/vsimem/cells.shp")


def get_neighbor_cells(grid: GridH5ResultAdmin, node_id: int):
    """returns the neighboring cells of a grid cell"""
    ds, cells = get_geometry_grid_cells(grid)
    cell = cells.GetFeature(node_id - 1)
    cells.SetSpatialFilter(cell.GetGeometryRef())
    return [c["nod_id"] for c in cells if c["nod_id"] != node_id]


# main functions
def threedi_nodes_within_vector(
    vector: gpd.geodataframe.GeoDataFrame, grid: GridH5ResultAdmin
):

    """
    returns node ids of 2D and 1D nodes which are present within
    the shape of the Vector. The vectors are the watersurface polygons.
    params:
        vector: Geopandas geodataframewith polygons
        grid: GridH5ResultAdmin with 1D/2D

    """
    # Get cells
    ds, cells = get_geometry_grid_cells(grid)
    # Get dissolved geometry
    # shape = ogr.CreateGeometryFromWkt(vector.dissolve()["geometry"].to_wkt().iloc[0])
    shape = ogr.CreateGeometryFromWkt(vector.geometry.unary_union.wkt)


    # Spatial filter
    cells.SetSpatialFilter(shape)

    # Return only things within the shape
    output = []
    for i, c in enumerate(cells):
        geometry = wkt.loads(c.GetGeometryRef().ExportToWkt())
        if vector.geometry.contains(geometry).any():
            output.append(c["nod_id"])
    return output


def average_neighboring_levels(grid: GridH5ResultAdmin, node_id: int, timesteps: list):
    """
    returns the average levels of neighboring cells
    params:
        grid: GridH5ResultAdmin with 2D results
        node_id: Grid node id of which the neighbors are taken
        timesteps: List of timesteps
    """
    # Grab them neighbors
    neighbors = get_neighbor_cells(grid, node_id)
    print(f"Neighbors of {node_id}:", neighbors)

    # spatial filter
    nodes = grid.nodes.filter(id__in=neighbors)

    # temporal filter
    nodes = nodes.timeseries(indexes=timesteps).s1

    # return level per timesteps
    levels = []
    for n in nodes:
        n[n == -9999] = np.nan
        levels.append(np.nanmean(n))
    return levels


def replacement_1d2d(grid: GridH5ResultAdmin, node_2d_id: int, timesteps: list):
    """
    returns the first connected 1D waterlevel of a 2D node.
    Multiple 1D connection to a single 2D node are possible.
    Here, the first is taken.
    params:
        grid:  GridH5ResultAdmin with 1D/2D results.
        node_2d_id: 2D node id
        timesteps: waterlevels are returned of this timestep
    """
    # retrieve all 1d connections
    line_nodes = grid.lines.subset("1d2d").line_nodes
    connections = []
    for line_node in line_nodes:
        if node_2d_id in line_node:
            connections.append(int(line_node[line_node != node_2d_id]))

    if len(connections) == 0:
        print("No 1D2D connecton for this 2D node")
        return []

    # get the first
    connection = connections[0]

    print(f"Found 1d replacement for 2d node {node_2d_id}:", connection)

    # get the data
    levels = grid.nodes.filter(id=connection).timeseries(indexes=timesteps).s1

    return list(levels.flatten())


# def edit_nodes_in_grid(
#     folder_path: str, scenario: str, method: str, output_folder: str
# ):
#     """
#     writes a new netcdf with an edited version based on the chosen methodology

#     This functions edits a threediresult and rewrites 2D waterlevels
#     based a the location and a methodology.
#     Methodology can be the average of neighboring cells or a 1D replacement.

#     params:
#         grid:  GridH5ResultAdmin with 1D/2D results.
#         vector:gpd.geodataframe.GeoDataFrame
#         method:
#             - interpolated
#             - 1d_replacement

#         node_2d_id: 2D node id
#         timesteps: waterlevels are returned of this timestep


#     """

#     # 0. Open variables
#     folder = Folders(folder_path)
#     vector = gpd.read_file(
#         str(folder.source_data.damo), driver="FileGDB", layer="Waterdeel"
#     )
#     grid = folder.threedi_results.one_d_two_d[scenario].grid
#     netcdf_path = str(folder.threedi_results.one_d_two_d[scenario].grid_path)
#     edit = GridEdit(netcdf_path)

#     # 1. retrieve the nodes where the cells are fully within the geometry
#     node_ids = threedi_nodes_within_vector(vector, grid)
#     print(f"Retrieved {len(node_ids)} nodes:", node_ids)

#     # 2. get timesteps
#     timesteps = list(range(0, len(grid.nodes.timestamps)))

#     # 3. find the appropriate waterlevels per id
#     levels_per_node = {}
#     for node_id in node_ids:
#         if method == "interpolated":
#             levels = average_neighboring_levels(grid, node_id, timesteps)
#         elif method == "1d_replacement":
#             levels = replacement_1d2d(grid, node_id, timesteps)
#         else:
#             print(
#                 "Method incorrect, should be either 'interpolated' or '1d_replacement'"
#             )

#         if len(levels) == 0:
#             print(f"No levels found, skipping node id {node_id}")
#             continue

#         levels_per_node[node_id] = levels

#     # 4. edit the data
#     for node_id, levels in levels_per_node.items():
#         edit.set_wl_timeseries(node_id, levels)

#     # write
#     if not os.path.exists(output_folder):
#         os.mkdir(output_folder)
#     edit.write(output_folder/"results_3di.nc")
#     shutil.copy(
#         str(folder.threedi_results.one_d_two_d[scenario].admin_path),
#         output_folder/"gridadmin.h5",
#     )
#     return levels_per_node


def calculate_depth(
    folder_path: str, results_type: str, revision: str, calculation_step: int
):
    """Calculates the waterdepth based on a threedi results
    params:
        folder: A HHNK Folder object
        result_type: "0d1d_result", "1d2d_results" or "batch"
    """
    if "0d1d" in results_type:
        raise ValueError("No 0d1d result please...")

    folder = Folders(folder_path)
    result = folder.threedi_results[results_type][revision]
    dem_path = str(folder.model.schema_base.rasters.dem)

    folder.output[results_type][revision].create()
    output_path = str(folder.output[results_type][revision].pl/f"depth_{calculation_step}.tif")


    if calculation_step!="MAX":
        calculation_step=[int(calculation_step)]

    threedidepth_calculate.create_raster_from_netcdf(
        gridadmin_path = str(result.admin_path),
        results_3di_path = str(result.grid_path),
        dem_path = dem_path,
        output_path = output_path,
        calculation_steps=calculation_step,
        netcdf=True
    )
    print("Output at", output_path)


def volume_difference(grid, raster_path, _type="max"):
    cells = get_geometry_grid_cells(grid, use_ogr=False)

    geoms = cells.geometry.values  # list of shapely geometries
    geoms = [mapping(geoms[0])]

    source = rasterio.open(raster_path)
    out_image, out_transform = mask(source, geoms[0], crop=True)

    # derive the sum of all cells
    cells["sum"] = pd.DataFrame(
        zonal_stats(vectors=cells["geometry"], raster=raster_path, stats="sum")
    )["sum"]

    for cell in cells:
        cell.id
        vol = grid.nodes.filter(cell.id)

        # get aster


def raster_volume(waterdepth_raster_path: str):
    """returns the volume of the waterdepth raster"""
    raster = Raster(waterdepth_raster_path)
    total = 0
    for index, array in raster:
        array[array == raster.nodata] = np.nan
        total += np.nansum(array)
    return total * raster.pixelarea


def threedi_result_volume(
    folder: Folders, results_type: str, revision: str, calculation_step: str
):
    """
    Extracts the total volume of a certain timestep of the gridadmin
    """

    folder = Folders(folder_path)
    grid = folder.threedi_results[results_type][revision].grid

    vol = grid.nodes.timeseries(indexes=[calculation_step]).vol

    if calculation_step == "max":
        grid.nodes.vol

    # spatial filter
    # geometries = get_geometry_grid_cells(grid)
    # for geometry in geometries:
    #    get_geometry_grid_cells(grid)

    vol = grid.nodes.timeseries(indexes=[calculation_step]).vol
    return np.nansum(vol)


# %%
if __name__ == "__main__":
    import os

    # os.chdir(r"E:\02.modellen\23_Katvoed")

    ## input
    folder_path = r"E:\02.modellen\23_Katvoed"
    folder = Folders(folder_path)

    threedi_result = folder.threedi_results.one_d_two_d['ghg_blok_t1000_interp']

    # # original
    folder_path=folder.path
    scenario='ghg_blok_t1000'
    method="interpolated"
    output_folder = threedi_result.pl.with_stem("ghg_blok_t1000_interp")
    # %%
    # data = edit_nodes_in_grid(folder_path=folder.path, scenario='ghg_blok_t1000', method="interpolated", output_folder=output_folder)

    # %%
    calculate_depth(folder_path=folder_path,
                         results_type="1d2d_results",
                         revision="ghg_blok_t1000",
                         calculation_step="MAX",
                        )
    # %%
    # edit = GridEdit(netcdf_path)
    # output_path = r"C:\Users\chris.kerklaan\Documents\Projecten\hhnk_detachering\hhnk_scripting\bwn_beemster/03_3di_resultaten\1d2d_results/replacement/results_3di.nc"
    # edit_nodes_in_grid(folder_path, "nieuw", "1d_replacement", output_path)
    # calculate_depth(folder_path, "1d2d_results", "test", 576)


# %% dataframe met max depth.


gr = threedi_result.grid

# threedi_result.grid.lines()
grid_gdf = get_geometry_grid_cells(gr, use_ogr=False)
s1_all = gr.nodes.subset("2D_open_water").timeseries(indexes=slice(0, -1)).s1
vol_all = gr.nodes.subset("2D_open_water").timeseries(indexes=slice(0, -1)).vol

s1_max_ind = s1_all.argmax(axis=0)
grid_gdf["wlvl_max_orig"] = np.round([row[s1_max_ind[enum]] for enum, row in enumerate(s1_all.T)], 5)
grid_gdf["vol_netcdf_m3"] = np.round([row[s1_max_ind[enum]] for enum, row in enumerate(vol_all.T)], 5)
# grid_gdf["wlvl_max_corrected"] = np.round([row[s1_max_ind[enum]] for enum, row in enumerate(s1_all.T)], 5)


from shapely.geometry import Point

# %%
from shapely.geometry import box

class ThreediGrid:
    def __init__(self, folder, threedi_result,):
        """threedi_result : htt.core.folders.ThreediResult instance"""

        self.folder=folder
        self.threedi_result = threedi_result

        self.gpkg_raw_path = self.threedi_result.pl/"grid_raw.gpkg"
        self.gpkg_corr_path = self.gpkg_raw_path.with_stem("grid_corr")


    @property
    def grid(self):
        return self.threedi_result.grid


    def netcdf_to_grid_gpkg(self, replace_dem_below_perc=50, replace_water_above_perc=95, replace_pand_above_perc=99):
        """
        ignore_dem_perc : if cell has no dem above this value waterlevels will be replaced
        ignore_water_perc : if cell has water surface area above this value waterlevels will be replaced

        create gpkg of grid with maximum wlvl
        """
        grid_gdf = gpd.GeoDataFrame()

        s1_all = self.grid.nodes.subset("2D_open_water").timeseries(indexes=slice(0, -1)).s1
        vol_all = self.grid.nodes.subset("2D_open_water").timeseries(indexes=slice(0, -1)).vol

        #Find index of max wlvl value in timeseries
        s1_max_ind = s1_all.argmax(axis=0)

        # * inputs every element from row as a new function argument.
        grid_gdf["geometry"] = [box(*row) for row in self.grid.nodes.subset("2D_ALL").cell_coords.T]
        grid_gdf.crs = "EPSG:28992"
        # nodes_2d["geometry"] = [Point(*row) for row in gr.nodes.subset("2D_ALL").coordinates.T] #centerpoints.


        grid_gdf["id"] = self.grid.cells.subset("2D_open_water").id

        #Retrieve values when wlvl is max
        grid_gdf["wlvl_max_orig"] = np.round([row[s1_max_ind[enum]] for enum, row in enumerate(s1_all.T)], 5)
        grid_gdf["vol_netcdf_m3"] = np.round([row[s1_max_ind[enum]] for enum, row in enumerate(vol_all.T)], 5)


        #Percentage of dem in a calculation cell
        #so we can make a selection of cells on model edge that need to be ignored
        grid_gdf["dem_area"] = self.grid.cells.subset("2D_open_water").sumax
        #Percentage dem in calculation cell
        grid_gdf["dem_perc"] = grid_gdf["dem_area"]  / grid_gdf.area *100 



        #Check water surface area in a cell.
        water_gdf = self.folder.source_data.damo.load(layer="Waterdeel")

        water_gdf["water"] = 1
        water_cell = gpd.overlay(grid_gdf[["id", "geometry"]], water_gdf[["water", "geometry"]], how="union")
        #Select only areas with the merged feature.
        water_cell = water_cell[water_cell["water"]==1]

        #Calculate sum of area per cell
        water_cell["water_area"] = water_cell.area
        water_cell_area = water_cell.groupby("id").agg("sum")

        grid_gdf = pd.merge(grid_gdf, water_cell_area["water_area"], left_on="id", right_on="id", how="left")
        grid_gdf["water_perc"] = grid_gdf["water_area"]  / grid_gdf.area *100


        #Check building area in a cell
        pand_gdf = self.folder.source_data.panden.load(layer="panden")

        pand_gdf["pand"] = 1
        pand_cell = gpd.overlay(grid_gdf[["id", "geometry"]], pand_gdf[["pand", "geometry"]], how="union")
        #Select only areas with the merged feature.
        pand_cell = pand_cell[pand_cell["pand"]==1]

        #Calculate sum of area per cell
        pand_cell["pand_area"] = pand_cell.area
        pand_cell_area = pand_cell.groupby("id").agg("sum")

        grid_gdf = pd.merge(grid_gdf, pand_cell_area["pand_area"], left_on="id", right_on="id", how="left")
        grid_gdf["pand_perc"] = grid_gdf["pand_area"]  / grid_gdf.area *100



        #Select cells that need replacing of wlvl
        grid_gdf["replace_dem"] = grid_gdf["dem_perc"] < replace_dem_below_perc
        grid_gdf["replace_water"] = grid_gdf["water_perc"] > replace_water_above_perc
        grid_gdf["replace_pand"] = grid_gdf["pand_perc"] > replace_pand_above_perc


        grid_gdf["replace_all"] = 0
        grid_gdf.loc[grid_gdf["replace_dem"]==True, "replace_all"] = "dem"
        grid_gdf.loc[grid_gdf["replace_water"]==True, "replace_all"] = "water"
        grid_gdf.loc[grid_gdf["replace_pand"]==True, "replace_all"] = "pand"


        #grid_gdf["replace_all"] = grid_gdf["replace_dem"] | grid_gdf["replace_water"] | grid_gdf["replace_pand"]

        grid_gdf=gpd.GeoDataFrame(grid_gdf, geometry="geometry")


        #Save to file
        grid_gdf.to_file(self.gpkg_raw_path, driver="GPKG")



    def waterlevel_correction(self, 
        grid_gpkg: str, method: str, output_path: str
    ):
        """
        This functions edits a threediresult and rewrites 2D waterlevels
        based a the location and a methodology.
        Methodology can be the average of neighboring cells or a 1D replacement.

        params:
            grid:  GridH5ResultAdmin with 1D/2D results.
            vector:gpd.geodataframe.GeoDataFrame
            method:
                - interpolated
                - 1d_replacement

            node_2d_id: 2D node id
            timesteps: waterlevels are returned of this timestep


        """

        # 1. retrieve the nodes where the cells are fully within the geometry
        print(f"Retrieved {len(node_ids)} nodes:", node_ids)

        # 2. get timesteps
        node_ids

        # 3. find the appropriate waterlevels per id
        levels_per_node = {}
        for node_id in node_ids:
            if method == "interpolated":
                levels = average_neighboring_levels(grid, node_id, timesteps)
            elif method == "1d_replacement":
                levels = replacement_1d2d(grid, node_id, timesteps)
            else:
                print(
                    "Method incorrect, should be either 'interpolated' or '1d_replacement'"
                )

            if len(levels) == 0:
                print(f"No levels found, skipping node id {node_id}")
                continue

            levels_per_node[node_id] = levels

        # 4. edit the data
        for node_id, levels in levels_per_node.items():
            edit.set_wl_timeseries(node_id, levels)


        return levels_per_node



## input
folder_path = r"E:\02.modellen\23_Katvoed"
folder = Folders(folder_path)

threedi_result = folder.threedi_results.one_d_two_d['ghg_blok_t1000']


self = ThreediGrid(folder=folder, threedi_result=threedi_result)

#Convert netcdf to grid gpkg
# self.netcdf_to_grid_gpkg()

grid_gdf = gpd.read_file(self.gpkg_raw_path, driver="GPKG")

#Slow.
# zonal_stats(vectors=grid_gdf["geometry"], raster=folder.model.schema_base.rasters.dem.path, stats=["sum", "std", "mean"])

# Opgehoogde panden shapefile?

# %% TEST INTERPOLATION
DEBUG=True
NO_DATA_VALUE = -9999.0
dem_path = str(folder.model.schema_base.rasters.dem)
nodeid_raster_path = threedi_result.pl/"nodeid.tif"

i=0
output_path = threedi_result.pl/f"depth_test_{i}.tif"
# while output_path.exists():
#     i+=1
#     try: 
#         output_path.unlink()
#     except:
#         output_path = threedi_result.pl/f"depth_test_{i}.tif"
print(output_path)
# %%
from scipy.spatial import qhull
from threedidepth import morton
from shapely.geometry import Point
from osgeo import gdal


metadata = hrt.Raster(dem_path).metadata

#Create raster of nodeids with same res as dem.
if not nodeid_raster_path.exists():
    hrt.gdf_to_raster(gdf=grid_gdf,
        value_field="id",
        raster_out=str(nodeid_raster_path),
        nodata=NO_DATA_VALUE,
        metadata=metadata,
        datatype=gdal.GDT_Int32,
        read_array=False)

nodeid_raster = hrt.Raster(nodeid_raster_path)
output_raster = hrt.Raster(output_path)


#Create empty raster.
output_raster.create(metadata=metadata, nodata=NO_DATA_VALUE)




# for window, block, idx in output_raster:
#     # points = _get_points_mesh(window)
    
#     break

output_raster.min_block_size = 256
output_raster.generate_blocks_geometry()
for idx, block_row in output_raster.blocks.iterrows():

    if idx==500:
        window=block_row['window_readarray']
        block = output_raster._read_array(window=window)

        break



metadata = hrt.create_meta_from_gdf(gdf=gpd.GeoDataFrame(block_row).T, res=0.5)
block_nodeid = nodeid_raster._read_array(window=window)

if DEBUG:
    hrt.save_raster_array_to_tiff(output_file=threedi_result.pl/"nodeid_block.tif", 
            raster_array=block_nodeid,
            nodata=NO_DATA_VALUE,
            metadata=metadata,
            datatype=gdal.GDT_Int32)




# _get_points
# %%



class test:
    def __init__(self):
        self.gr = threedi_result.grid

self=test()
SUBSET_2D_OPEN_WATER = "2D_open_water"


# %%
if DEBUG:
    if False:
        points_mesh_gdf = gpd.GeoDataFrame(geometry=[Point(i) for i in points_mesh])
        points_mesh_gdf.crs= "EPSG:28992"

        points_mesh_gdf.to_file(threedi_result.pl/"debug"/"mesh_block.gpkg", driver="GPKG")



# %%
hrt.save_raster_array_to_tiff(output_file=threedi_result.pl/"debug"/"delauney_grid.tif", 
        raster_array=mesh_grid_interp,
        nodata=NO_DATA_VALUE,
        metadata=metadata,
        datatype=gdal.GDT_Float32)


# plt.imshow(a.reshape(block.shape))

# %% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
converter_kwargs = {"source_path": dem_path,
            "target_path":str(output_path)}



with threedidepth_calculate.GeoTIFFConverter(**converter_kwargs) as converter:
    
    calculator_kwargs = {"gridadmin_path" : threedi_result.admin_path.path,
                "results_3di_path" : threedi_result.grid_path.path,
                "dem_geo_transform" : converter.geo_transform,
                "dem_shape" : (converter.raster_y_size, converter.raster_x_size),
                "calculation_step" : "MAX"}

    with threedidepth_calculate.LinearLevelCalculator(**calculator_kwargs) as calculator:
        indices, values, no_data_value = converter.convert_using(calculator=calculator, band=0)



converter = threedidepth_calculate.GeoTIFFConverter(**converter_kwargs)
converter.__enter__()
converter.geo_transform
# %%
calculator_kwargs = {"gridadmin_path" : threedi_result.admin_path.path,
                "results_3di_path" : threedi_result.grid_path.path,
                "dem_geo_transform" : converter.geo_transform,
                "dem_shape" : (converter.raster_y_size, converter.raster_x_size),
                "calculation_step" : "MAX"}

calculator = threedidepth_calculate.LinearLevelCalculator(**calculator_kwargs)
calculator.__enter__()
        # calculator(indices, values, no_data_value)
