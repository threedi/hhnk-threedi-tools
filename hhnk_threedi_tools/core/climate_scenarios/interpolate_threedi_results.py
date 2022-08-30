# -*- coding: utf-8 -*-
"""
Created on Sat May 14 09:28:04 2022

@author: chris.kerklaan

TODO:
    1. geometry grid moeten niet op de directe cellen worden aangesloten,
    maar op de eerste volgende cellen met een waterstand.
"""
# First-party imports
import os
import shutil
import pathlib
import numpy as np

# Third-party imports
import sys

sys.path.append(r"C:\Users\chris.kerklaan\Documents\Github\threedi-raster-edits")
import threedi_raster_edits as tre
import xarray as xr

from threedidepth.calculate import calculate_waterdepth
from threedigrid.admin.gridresultadmin import GridH5ResultAdmin

class GridEdit:
    def __init__(self, netcdf_path):
        self.ds = xr.open_dataset(netcdf_path)
        self.edit = self.ds.load()

    def set_wl_timeseries(self, node_id, levels):
        assert len(levels) == len(self.edit["Mesh2D_s1"])

        for timestep in range(len(self.edit["Mesh2D_s1"])):
            self.edit["Mesh2D_s1"][timestep][node_id - 1] = levels[timestep]

    def set_wl_timestep(self, node_id, timestep, level):
        self.edit["Mesh2D_s1"][timestep][node_id - 1] = level

    def write(self, output_path):
        self.edit.to_netcdf(output_path)


# Helper functions
def _grid_geometry(grid: GridH5ResultAdmin, use_ogr=True):
    """returns an ogr Datasource with the cells of a threedigrid"""
    grid.cells.to_shape("/vsimem/cells.shp")
    vector = tre.Vector("/vsimem/cells.shp")
    return vector


def get_neighbor_cells(grid: GridH5ResultAdmin, node_id: int):
    """returns the neighboring cells of a grid cell"""
    cells = _grid_geometry(grid)
    cell = cells[node_id - 1]
    nodes = cells.spatial_filter(cell.geometry)
    return [n["nod_id"] for n in nodes if n["nod_id"] != node_id]


def threedi_nodes_within_vector(vector: tre.Vector, grid: GridH5ResultAdmin):
    """
    returns node ids of 2D and 1D nodes which are present within
    the shape of the Vector.
    params:
        vector: Geopandas geodataframewith polygons
        grid: GridH5ResultAdmin with 1D/2D

    """
    # Get cells
    cells = _grid_geometry(grid)
    within_cells = cells.spatial_filter(vector, method="Within", return_vector=True)
    return within_cells


def edit_nodes_in_grid(
    waterdeel: tre.Vector,
    buildings: tre.Vector,
    threedi_results_folder: str,
    output_folder: str,
    calculation_steps: list,
    dem_height,
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
    minimum_grid = 10
    search_distance = 10
    cell_search_distance = 2

    netcdf_path = threedi_results_folder + "/results_3di.nc"
    gridadmin_path = threedi_results_folder + "/gridadmin.h5"
    grid = GridH5ResultAdmin(gridadmin_path, netcdf_path)
    edit = GridEdit(netcdf_path)

    # check timestep
    timestep = int(grid.nodes.timestamps[1])
    if timestep not in list(range(300, 340)):
        print("We got a different timestep than 300, we found:", timestep)
        return

    # create folder

    output_folder = pathlib.Path(output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)

    # retrieve the gridcells and clip to the surrounding extent
    water_cell_path = output_folder.parents[0] / "water_cells.shp"
    cell_path = output_folder.parents[0] / "extent_cells.shp"
    if water_cell_path.exists() and cell_path.exists():
        filtered_water_cells = tre.Vector(str(water_cell_path))
        cells = tre.Vector(str(cell_path))
    else:
        grid_cells = _grid_geometry(grid)
        fixed_cells = grid_cells.fix(quiet=False)
        water_cells = fixed_cells.spatial_filter(
            waterdeel.buffer(cell_search_distance), quiet=False, use_ogr=False
        )

        # TODO
        # We have double cells: my_dict = {i:node_ids.count(i) for i in node_ids}

        node_ids = water_cells.table["nod_id"]
        nodes = grid.nodes.filter(id__in=node_ids).timeseries(indexes=calculation_steps)
        node_levels = nodes.s1
        node_ids = nodes.id

        filtered_water_cells = water_cells.copy(shell=True)
        for i in tre.Progress(
            range(0, len(node_ids)), "Filtering water cells on 10 meter levels"
        ):
            node_id = node_ids[i]
            if node_levels[0][i] > dem_height: #or node_levels[1][i] > dem_height:
                water_cell = water_cells.filter(nod_id=node_id)[0].copy()
                filtered_water_cells.add(water_cell)

        building_cells = fixed_cells.spatial_filter(
            buildings, method="Within", quiet=False, use_ogr=False
        )
        for building_cell in tre.Progress(building_cells, "adding within cells of buildings"):
            filtered_water_cells.add(building_cell)

        cells = fixed_cells.spatial_filter(
            filtered_water_cells.buffer(search_distance * 2),
            method="Extent",
            quiet=False,
            use_ogr=False,
        )

        filtered_water_cells.write(str(water_cell_path))
        cells.write(str(cell_path))

    water_grid = tre.Raster(
        filtered_water_cells.rasterize(resolution=minimum_grid, extent=cells.extent)
    )

    water_index = water_grid.array == 1
    levels = {c.id: [] for c in filtered_water_cells}
    for timestep in calculation_steps:
        spatial_timeseries = cells.copy()
        spatial_timeseries.add_field("s1", float)
        timeseries = grid.nodes.timeseries(indexes=[timestep]).s1[0]

        for spatial_timeserie in spatial_timeseries:
            spatial_timeserie["s1"] = float(timeseries[spatial_timeserie["nod_id"]])

        ts_raster = tre.Raster(
            spatial_timeseries.rasterize(resolution=minimum_grid, field="s1")
        )

        # Set all nodata to 0, set all water to np.nan.
        ts_array = ts_raster.array
        ts_array[np.isnan(ts_array)] = 0
        ts_array[water_index] = np.nan
        ts_raster.array = ts_array

        filled = ts_raster.fill_nodata(search_distance, quiet=False)

        ts_raster.write(str(output_folder / f"ts_{timestep}.tif"))
        filled.write(str(output_folder / f"filled_{timestep}.tif"))

        for cell in tre.Progress(
            filtered_water_cells, f"Writing to Netcdf, ts is {timestep}"
        ):
            level = np.nanmean(filled.read(cell.geometry))
            edit.set_wl_timestep(cell["nod_id"], timestep, level)
            levels[cell.id].append(level)

    # write

    interpolated_folder = output_folder / "interpolated"
    interpolated_folder.mkdir(parents=True, exist_ok=True)
    edit.write(str((interpolated_folder / "results_3di.nc")))
    shutil.copy(gridadmin_path, str(output_folder / "interpolated" / "gridadmin.h5"))

    return levels


def calculate_depth(
    threedi_results_folder, dem_path, output_path, calculation_steps: list
):
    """Calculates the waterdepth based on a threedi results
    params:
        folder: A HHNK Folder object
        result_type: "0d1d_result", "1d2d_results" or "batch"

    """

    netcdf_path = threedi_results_folder + "/results_3di.nc"
    gridadmin_path = threedi_results_folder + "/gridadmin.h5"

    for step in calculation_steps:
        path = f"{output_path}/depths_{step}.tif"
        if not os.path.exists(path):
            progress = tre.Progress(total=100, message=f"Creating depths for {step}")

            calculate_waterdepth(
                gridadmin_path,
                netcdf_path,
                dem_path,
                path,
                calculation_steps=[step],
                progress_func=progress.gdal,
                mode='lizard'
            )

