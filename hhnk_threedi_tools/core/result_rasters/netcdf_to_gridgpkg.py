# %%
from dataclasses import dataclass

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
from shapely.geometry import box

from hhnk_threedi_tools import Folders


class ThreediGrid:
    """TODO deprecated, remove in later release."""

    def __init__(self, **kwargs):
        raise DeprecationWarning(
            "The ThreediGrid class has been replaced by \
htt.NetcdfToGPKG since v2023.5. Please rewrite your code."
        )


class NetcdfToGPKG:
    """Transform netcdf into a gpkg. Can also correct waterlevels
    based on conditions. These are passed when running the function
    .netcdf_to_grid_gpkg.

    Input layers can be passed directly, or when using the htt.Folders
    structure, use the .from_folder classmethod.

    """

    def __init__(
        self,
        threedi_result: hrt.ThreediResult,
        waterdeel_path: str = None,
        waterdeel_layer: str = None,
        panden_path: str = None,
        panden_layer: str = None,
        grid_raw_filename="grid_raw.gpkg",
        grid_corr_filename="grid_corr.gpkg",
    ):
        """_summary_

        Parameters
        ----------
        threedi_result : hrt.ThreediResult
            _description_
        folder : Folders, optional
            _description_, by default None
        waterdeel_path : str, optional
            _description_, by default None
        waterdeel_layer : str, optional
            _description_, by default "Waterdeel"
        panden_path : str, optional
            _description_, by default None
        panden_layer : str, optional
            _description_, by default "panden"
        grid_raw_filename : str, optional, by default "grid_raw.gpkg"
            _description_, by default "grid_raw.gpkg"
        grid_corr_filename : str, optional, by default "grid_corr.gpkg"



        grid creation requires:
            folder.source_data.damo
            folder.source_data.panden

        Otherwise use waterdeel_path with waterdeel_layer

        """
        self.threedi_result = threedi_result
        self.waterdeel_path = waterdeel_path
        self.waterdeel_layer = waterdeel_layer
        self.panden_path = panden_path
        self.panden_layer = panden_layer

        self.grid_path = self.threedi_result.full_path(grid_raw_filename)
        self.grid_corr_path = self.threedi_result.full_path(grid_corr_filename)

    @classmethod
    def from_folder(cls, threedi_result: hrt.ThreediResult, folder: Folders, **kwargs):
        """Load from folder structure. Pand and waterdeel is taken from
        the folder structure.
        """
        waterdeel_path = folder.source_data.damo
        waterdeel_layer = "Waterdeel"

        panden_path = folder.source_data.panden
        panden_layer = "panden"

        return NetcdfToGPKG(
            threedi_result=threedi_result,
            waterdeel_path=waterdeel_path,
            waterdeel_layer=waterdeel_layer,
            panden_path=panden_path,
            panden_layer=panden_layer,
        )

    @property
    def grid(self):
        """Instance of threedigrid.admin.gridresultadmin.GridH5ResultAdmin"""
        return self.threedi_result.grid

    def _load_layer(self, layer_path, layer_name: str = None) -> gpd.GeoDataFrame:
        """Load layer as gdf, pand or waterdeel"""
        gdf = None
        if (layer_path is not None) and (layer_path.exists()):
            gdf = gpd.read_file(layer_path, layer=layer_name)
        else:
            print(f"Couldn't load {layer_path.name}. Ignoring it in correction.")
        return gdf

    def netcdf_to_grid_gpkg(
        self,
        replace_dem_below_perc=50,
        replace_water_above_perc=95,
        replace_pand_above_perc=99,
        overwrite=False,
    ):
        """
        Create gpkg of grid with maximum wlvl

        replace_dem_below_perc
            if cell area has no dem (isna) above this value waterlevels will be replaced
        replace_water_above_perc
            if cell has water surface area above this value waterlevels will be replaced
        replace_pand_above_perc
            if cell has pand surface area above this value waterlevels will be replaced

        """

        create = hrt.check_create_new_file(output_file=self.grid_path, overwrite=overwrite)
        if create:
            grid_gdf = gpd.GeoDataFrame()

            s1_all = self.grid.nodes.subset("2D_open_water").timeseries(indexes=slice(0, -1)).s1
            vol_all = self.grid.nodes.subset("2D_open_water").timeseries(indexes=slice(0, -1)).vol

            # Find index of max wlvl value in timeseries
            s1_max_ind = s1_all.argmax(axis=0)

            # * inputs every element from row as a new function argument.
            grid_gdf["geometry"] = [box(*row) for row in self.grid.nodes.subset("2D_ALL").cell_coords.T]
            grid_gdf.crs = "EPSG:28992"
            # nodes_2d["geometry"] = [Point(*row) for row in gr.nodes.subset("2D_ALL").coordinates.T] #centerpoints.

            grid_gdf["id"] = self.grid.cells.subset("2D_open_water").id

            # Retrieve values when wlvl is max
            grid_gdf["wlvl_max_orig"] = np.round([row[s1_max_ind[enum]] for enum, row in enumerate(s1_all.T)], 5)
            grid_gdf["vol_m3_max_orig"] = np.round([row[s1_max_ind[enum]] for enum, row in enumerate(vol_all.T)], 5)

            # Percentage of dem in a calculation cell
            # so we can make a selection of cells on model edge that need to be ignored
            grid_gdf["dem_area"] = self.grid.cells.subset("2D_open_water").sumax
            # Percentage dem in calculation cell
            grid_gdf["dem_perc"] = grid_gdf["dem_area"] / grid_gdf.area * 100

            # Check water surface area in a cell.
            water_gdf = self._load_layer(layer_path=self.waterdeel_path, layer_name=self.waterdeel_layer)
            if water_gdf is not None:
                water_gdf["water"] = 1
                water_cell = gpd.overlay(grid_gdf[["id", "geometry"]], water_gdf[["water", "geometry"]], how="union")
                # Select only areas with the merged feature.
                water_cell = water_cell[water_cell["water"] == 1]

                # Calculate sum of area per cell
                water_cell["water_area"] = water_cell.area
                water_cell_area = water_cell.groupby("id").agg("sum")

                grid_gdf = grid_gdf.merge(water_cell_area["water_area"], left_on="id", right_on="id", how="left")
                grid_gdf["water_perc"] = grid_gdf["water_area"] / grid_gdf.area * 100
            else:
                grid_gdf["water_perc"] = None

            # Check building area in a cell
            pand_gdf = self._load_layer(layer_path=self.panden_path, layer_name=self.panden_layer)
            if pand_gdf is not None:
                pand_gdf["pand"] = 1
                pand_cell = gpd.overlay(grid_gdf[["id", "geometry"]], pand_gdf[["pand", "geometry"]], how="union")
                # Select only areas with the merged feature.
                pand_cell = pand_cell[pand_cell["pand"] == 1]

                # Calculate sum of area per cell
                pand_cell["pand_area"] = pand_cell.area
                pand_cell_area = pand_cell.groupby("id").agg("sum")

                grid_gdf = pd.merge(grid_gdf, pand_cell_area["pand_area"], left_on="id", right_on="id", how="left")
                grid_gdf["pand_perc"] = grid_gdf["pand_area"] / grid_gdf.area * 100
            else:
                grid_gdf["pand_perc"] = None

            # Select cells that need replacing of wlvl
            grid_gdf["replace_dem"] = grid_gdf["dem_perc"] < replace_dem_below_perc
            grid_gdf["replace_water"] = grid_gdf["water_perc"] > replace_water_above_perc
            grid_gdf["replace_pand"] = grid_gdf["pand_perc"] > replace_pand_above_perc

            # Write reason of replacing
            grid_gdf["replace_all"] = 0
            grid_gdf.loc[grid_gdf["replace_dem"] == True, "replace_all"] = "dem"
            grid_gdf.loc[grid_gdf["replace_water"] == True, "replace_all"] = "water"
            grid_gdf.loc[grid_gdf["replace_pand"] == True, "replace_all"] = "pand"

            # grid_gdf["replace_all"] = grid_gdf["replace_dem"] | grid_gdf["replace_water"] | grid_gdf["replace_pand"]

            grid_gdf = gpd.GeoDataFrame(grid_gdf, geometry="geometry")

            # Save to file
            grid_gdf.to_file(self.grid_path.base, driver="GPKG")

    def waterlevel_correction(self, orig_col: str, corrected_col: str = None, overwrite: bool = False):
        """Correct the waterlevel for the input columns. Results are only corrected
        for cells where the 'replace_all' value is not False.

        ----------
        orig_col : str
            Column with waterlevels that need to be replaced.
        corrected_col : str, optional, by default None
            Output column that will hold the corrected waterlevels. If this value is not passed
            the name will be based on the original col, adding _corr at the end. If the original
            column has _orig at the end it will replace that.
        overwrite : bool, optional, by default False
        """
        create = hrt.check_create_new_file(output_file=self.grid_corr_path, overwrite=overwrite)

        # TODO correction needs to happen seperately from writing. Also neighbours
        # only need to be calculated once for the whole gpkg.
        if create:
            if corrected_col is None:
                # Create output column name if it was not passed
                if orig_col.endswith("_orig"):
                    corrected_col = orig_col.replace("_orig", "_corr")
                else:
                    corrected_col = f"{orig_col}_corr"

            grid_gdf = self.grid_path.load()

            grid_gdf[corrected_col] = grid_gdf[orig_col]
            replace_idx = grid_gdf["replace_all"] != "0"
            # set values to none so they are not used in calculation of new values.
            grid_gdf.loc[replace_idx, corrected_col] = None

            # Loop cells that need replacing.
            for idx, row in grid_gdf.loc[replace_idx].iterrows():
                # Find neighbour cells
                neighbours_idx = grid_gdf[grid_gdf.geometry.touches(row.geometry)].index.tolist()
                neighbours_id = [
                    grid_gdf.loc[neighbour_idx].id for neighbour_idx in neighbours_idx if idx != neighbour_idx
                ]
                grid_gdf.loc[idx, "neighbours"] = str(neighbours_id)

                # Calculate avg wlvl of neighbours
                neighbour_avg_wlvl = np.round(grid_gdf.loc[neighbours_idx][corrected_col].mean(), 5)
                grid_gdf.loc[idx, corrected_col] = neighbour_avg_wlvl

            grid_gdf["diff"] = grid_gdf[corrected_col] - grid_gdf[orig_col]

            # Save to file
            grid_gdf.to_file(self.grid_corr_path.base, driver="GPKG")


if __name__ == "__main__":
    from hhnk_threedi_tools import Folders

    folder_path = r"E:\02.modellen\23_Katvoed"
    folder = Folders(folder_path)

    threedi_result = folder.threedi_results.one_d_two_d["katvoed #1 piek_ghg_T1000"]

    self = ThreediGrid(folder=folder, threedi_result=threedi_result)
    # self = ThreediGrid(threedi_result=threedi_result, waterdeel_path=folder.source_data.damo.path)

    # #Convert netcdf to grid gpkg
    self.netcdf_to_grid_gpkg()

    # #Replace waterlevel of selected cells with avg of neighbours.
    # self.waterlevel_correction(output_col="wlvl_max_replaced")


# %%
