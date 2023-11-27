# %%
from dataclasses import dataclass
from pathlib import Path
from typing import Union

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


@dataclass
class NetcdfToGPKG:
    """Transform netcdf into a gpkg. Can also correct waterlevels
    based on conditions. These are passed when running the function
    .netcdf_to_grid_gpkg.

    Input layers can be passed directly, or when using the htt.Folders
    structure, use the .from_folder classmethod.

    Parameters
    ----------
    threedi_result : hrt.ThreediResult
        path to folder with netcdf and h5 result.
    waterdeel_path : str, optional
        path to waterdeel. If None is passed it wont be used in the selection of cells
    waterdeel_layer : str, optional
        layername if waterdeel is part of a gpkg
    panden_path : str, optional
        path to panden. If None is passed it wont be used in the selection of cells
    panden_layer : str, optional
        layername if panden is part of a gpkg
    """

    threedi_result: hrt.ThreediResult
    waterdeel_path: str = None
    waterdeel_layer: str = None
    panden_path: str = None
    panden_layer: str = None

    @classmethod
    def from_folder(cls, folder: Folders, threedi_result: hrt.ThreediResult, **kwargs):
        """Load from folder structure. Pand and waterdeel is taken from the folder structure."""
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

    @property
    def output_default(self):
        """Default output if no path is specified."""
        return self.threedi_result.full_path("grid_wlvl.gpkg")

    def _calculate_layer_area_per_cell(
        self,
        grid_gdf: gpd.GeoDataFrame,
        layer_path: Union[Path, hrt.File],
        layer_name: str = None,
    ) -> gpd.GeoDataFrame:
        """Calculate for each gridcel the area and percentage of total area of the
        input layer. Returns the area and percentage columns.

        ----------
        grid_gdf : gpd.GeoDataFrame
            gdf with grid cells. Created inside main class
        layer_path : Path or hrt.File
            Path to layer to calculate area and percentage from
        layer_name : str, optional, by default None
            Name of layer if the layer is part of a gpkg
        """

        # Load layer as gdf
        gdf = None
        if (layer_path is not None) and (layer_path.exists()):
            gdf = gpd.read_file(layer_path.path, layer=layer_name)
        else:
            print(f"Couldn't load {layer_path.name}. Ignoring it in correction.")

        if gdf is not None:
            area_col = "area"  # area in m2
            perc_col = "perc"  # percentage of total area

            if area_col in grid_gdf:
                raise ValueError(f"Column {area_col} was already found in grid_gdf.")

            gdf["value"] = 1
            # Overlay grid with input shape.
            overlay_df = gpd.overlay(grid_gdf[["id", "geometry"]], gdf[["value", "geometry"]], how="intersection")

            # Calculate sum of area per cell
            overlay_df[area_col] = overlay_df.area

            # Group by ids so we get the total area per cell
            overlay_df_grouped = overlay_df.groupby("id").agg("sum")

            # Put in area in grid gdf and calculate percentage.
            grid_gdf_merged = grid_gdf.merge(overlay_df_grouped[area_col], left_on="id", right_on="id", how="left")
            grid_gdf_merged[perc_col] = grid_gdf_merged[area_col] / grid_gdf_merged.area * 100
        return grid_gdf_merged[[area_col, perc_col]]

    def netcdf_to_grid_gpkg(
        self,
        output_file=None,
        replace_dem_below_perc: float = 50,
        replace_water_above_perc: float = 95,
        replace_pand_above_perc: float = 99,
        overwrite=False,
    ):
        """Create gpkg of grid with maximum wlvl

        output_file
            When None is passed the output will be placed in the same directory as the netcdf

        replace_dem_below_perc
            if cell area has no dem (isna) above this value waterlevels will be replaced
        replace_water_above_perc
            if cell has water surface area above this value waterlevels will be replaced
        replace_pand_above_perc
            if cell has pand surface area above this value waterlevels will be replaced
        """

        if output_file is None:
            output_file = self.output_default

        create = hrt.check_create_new_file(output_file=output_file, overwrite=overwrite)
        if create:
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

            grid_gdf["water_area", "water_perc"] = self._calculate_layer_area_per_cell(
                grid_gdf=grid_gdf, layer_path=self.waterdeel_path, layer_name=self.waterdeel_layer
            )
            grid_gdf["pand_area", "pand_perc"] = self._calculate_layer_area_per_cell(
                grid_gdf=grid_gdf, layer_path=self.panden_path, layer_name=self.panden_layer
            )

            # Select cells that need replacing of wlvl
            grid_gdf["replace_dem"] = grid_gdf["dem_perc"] < replace_dem_below_perc
            grid_gdf["replace_water"] = grid_gdf["water_perc"] > replace_water_above_perc
            grid_gdf["replace_pand"] = grid_gdf["pand_perc"] > replace_pand_above_perc

            # Write reason of replacing
            grid_gdf["replace_all"] = 0
            grid_gdf.loc[grid_gdf["replace_dem"] == True, "replace_all"] = "dem"
            grid_gdf.loc[grid_gdf["replace_water"] == True, "replace_all"] = "water"
            grid_gdf.loc[grid_gdf["replace_pand"] == True, "replace_all"] = "pand"

            grid_gdf = gpd.GeoDataFrame(grid_gdf, geometry="geometry")

            # Save to file
            grid_gdf.to_file(output_file, driver="GPKG")

    def waterlevel_correction(
        self, orig_col: str, corrected_col: str = None, gpkg_path=None, overwrite: bool = False
    ):
        """Correct the waterlevel for the input columns. Results are only corrected
        for cells where the 'replace_all' value is not False.

        This input needs to exist,
            create it first with self.netcdf_to_grid_gpkg.
            
        ----------
        orig_col : str
            Column with waterlevels that need to be replaced.
        corrected_col : str, optional, by default None
            Output column that will hold the corrected waterlevels. If this value is not passed
            the name will be based on the original col, adding _corr at the end. If the original
            column has _orig at the end it will replace that.
        gpkg_path : path
            if None, use the default value of grid_wlvl.gpkg. This function will add extra columns
            and write to the same file as the input.
        overwrite : bool, optional, by default False
        """
        if gpkg_path is None:
            output_file = self.output_default

            if 

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
