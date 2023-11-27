# %%

"""Functional testing for oneDtwoD object"""
import os
import pathlib

import pytest

from hhnk_threedi_tools.core.checks.one_d_two_d import OneDTwoDTest
from hhnk_threedi_tools.core.result_rasters import netcdf_to_gridgpkg
from tests.config import FOLDER_NEW, FOLDER_TEST

if __name__ == "__main__":
    import importlib

    importlib.reload(netcdf_to_gridgpkg)


def test_netcdf_to_gridgpkg():
    pass


if __name__ == "__main__":
    test_netcdf_to_gridgpkg()

# %%
self = netcdf_gpkg = netcdf_to_gridgpkg.NetcdfToGPKG.from_folder(
    folder=FOLDER_TEST, threedi_result=FOLDER_TEST.threedi_results.batch["batch_test"].downloads.piek_glg_T10.netcdf
)


import geopandas as gpd
import numpy as np
from shapely import box

layer_path = netcdf_gpkg.waterdeel_path
layer_name = netcdf_gpkg.waterdeel_layer
column_base_name = "water"



# %%
if True:
            grid_gdf = gpd.GeoDataFrame()

            # Waterlevel and volume at all timesteps
            s1_all = self.grid.nodes.subset("2D_open_water").timeseries(indexes=slice(0, -1)).s1
            vol_all = self.grid.nodes.subset("2D_open_water").timeseries(indexes=slice(0, -1)).vol

            # * inputs every element from row as a new function argument.
            grid_gdf["geometry"] = [box(*row) for row in self.grid.nodes.subset("2D_ALL").cell_coords.T]
            grid_gdf.crs = "EPSG:28992"
            # nodes_2d["geometry"] = [Point(*row) for row in gr.nodes.subset("2D_ALL").coordinates.T] #centerpoints.

            grid_gdf["id"] = self.grid.cells.subset("2D_open_water").id

            # Retrieve values when wlvl is max
            s1_max_index = s1_all.argmax(axis=0)
            grid_gdf["wlvl_max_orig"] = np.round([row[s1_max_index[enum]] for enum, row in enumerate(s1_all.T)], 5)
            grid_gdf["vol_m3_max_orig"] = np.round([row[s1_max_index[enum]] for enum, row in enumerate(vol_all.T)], 5)

            # Percentage of dem in a calculation cell
            # so we can make a selection of cells on model edge that need to be ignored
            grid_gdf["dem_area"] = self.grid.cells.subset("2D_open_water").sumax
            # Percentage dem in calculation cell
            grid_gdf["dem_perc"] = grid_gdf["dem_area"] / grid_gdf.area * 100

            grid_gdf["water_area", "water_perc"] = self.calculate_layer_area_per_cell(
                grid_gdf=grid_gdf,
                layer_path=self.waterdeel_path,
                layer_name=self.waterdeel_layer,
            )
# %%

if True:
    # Load layer as gdf

