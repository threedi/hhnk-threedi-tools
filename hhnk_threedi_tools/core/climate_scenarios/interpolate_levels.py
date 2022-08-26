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

sys.path.append(r"C:\Users\chris.kerklaan\Documents\Github\hhnk-threedi-tools")
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

# from rasterstats import zonal_stats
from threedidepth.calculate import calculate_waterdepth
from threedigrid.admin.gridresultadmin import GridH5ResultAdmin

# Local imports
from hhnk_threedi_tools import Folders

# from hhnk_research_tools.threedi.gridedit import GridEdit
from hhnk_research_tools.gis.raster import Raster


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
    the shape of the Vector.
    params:
        vector: Geopandas geodataframewith polygons
        grid: GridH5ResultAdmin with 1D/2D

    """
    # Get cells
    ds, cells = get_geometry_grid_cells(grid)
    # Get dissolved geometry
    shape = ogr.CreateGeometryFromWkt(vector.dissolve()["geometry"].to_wkt().iloc[0])

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


def edit_nodes_in_grid(
    folder_path: str, scenario: str, method: str, output_folder: str
):
    """
    writes a new netcdf with an edited version based on the chosen methodology

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

    # 0. Open variables
    folder = Folders(folder_path)
    vector = gpd.read_file(
        str(folder.source_data.damo), driver="FileGDB", layer="Waterdeel"
    )
    grid = folder.threedi_results.one_d_two_d[scenario].grid
    netcdf_path = str(folder.threedi_results.one_d_two_d[scenario].grid_path)
    edit = GridEdit(netcdf_path)

    # 1. retrieve the nodes where the cells are fully wthing the geometry
    node_ids = threedi_nodes_within_vector(vector, grid)
    print(f"Retrieved {len(node_ids)} nodes:", node_ids)

    # 2. get timesteps
    timesteps = list(range(0, len(grid.nodes.timestamps)))

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

    # write
    if not os.path.exists(output_folder):
        os.mkdir(output_folder)
    edit.write(output_folder + "/results_3di.nc")
    shutil.copy(
        str(folder.threedi_results.one_d_two_d[scenario].admin_path),
        output_folder + "/gridadmin.h5",
    )
    return levels_per_node


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
    dem_path = str(folder.model.rasters.dem)

    folder.output[results_type][revision].create()
    output_path = (
        str(folder.output[results_type][revision]) + f"/depth_{calculation_step}.tif"
    )
    calculate_waterdepth(
        str(result.admin_path),
        str(result.grid_path),
        dem_path,
        output_path,
        calculation_steps=[int(calculation_step)],
    )
    print("Output at", output_path)


# def volume_difference(grid, raster_path, _type="max"):
#     #FIXME not used?
#     cells = get_geometry_grid_cells(grid, use_ogr=False)

#     geoms = cells.geometry.values # list of shapely geometries
#     geoms = [mapping(geoms[0])]

#     # source = rasterio.open(raster_path)
#     # out_image, out_transform = mask(source,geoms[0],crop=True)

#     # derive the sum of all cells
#     cells['sum'] = pd.DataFrame(
#                 zonal_stats(
#                     vectors=cells['geometry'],
#                     raster=raster_path,
#                 stats='sum'
#                 )
#             )['sum']

#     for cell in cells:
#         cell.id
#         vol = grid.nodes.filter(cell.id)

#         # get aster

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


if __name__ == "__main__":
    pass
    import os

    os.chdir(
        r"C:\Users\chris.kerklaan\Documents\Projecten\hhnk_detachering\hhnk_scripting\bwn_beemster"
    )

    ## input
    folder_path = r"C:\Users\chris.kerklaan\Documents\Projecten\hhnk_detachering\hhnk_scripting\bwn_beemster"
    results_type = (
        "results_1d2d"  # can be "0d1d_results", "1d2d_results" or "batch_results"
    )
    revision = "nieuw"
    calculation_step = 1

    vector_path = r"C:\Users\chris.kerklaan\Documents\Projecten\hhnk_detachering\hhnk_scripting\bwn_beemster/01_Source_data/DAMO.gdb"
    layer_name = "Waterdeel"
    netcdf_path = r"C:\Users\chris.kerklaan\Documents\Projecten\hhnk_detachering\hhnk_scripting\bwn_beemster/03_3di_resultaten\1d2d_results\nieuw/results_3di.nc"
    depth_path = r"C:\Users\chris.kerklaan\Documents\Projecten\hhnk_detachering\hhnk_scripting\bwn_beemster\Output\1d2d_tests\nieuw\Layers\depth_576.tif"

    # original
    # calculate_depth(folder_path, "1d2d_results", "nieuw", 576)
    scenario = "nieuw"
    method = "interpolated"

    output_folder = r"C:\Users\chris.kerklaan\Documents\Projecten\hhnk_detachering\hhnk_scripting\bwn_beemster/03_3di_resultaten\1d2d_results/test_interpolated"
    data = edit_nodes_in_grid(folder_path, "nieuw", "interpolated", output_folder)

    calculate_depth(folder_path, "1d2d_results", "test_interpolated", 576)

    edit = GridEdit(netcdf_path)
    output_path = r"C:\Users\chris.kerklaan\Documents\Projecten\hhnk_detachering\hhnk_scripting\bwn_beemster/03_3di_resultaten\1d2d_results/replacement/results_3di.nc"
    edit_nodes_in_grid(folder_path, "nieuw", "1d_replacement", output_path)
    calculate_depth(folder_path, "1d2d_results", "test", 576)
