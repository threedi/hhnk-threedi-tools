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
class NetcdfTimeSeries:
    """Timeseries contained in a netcdf"""

    grid: hrt.ThreediResult  # type threedigrid.admin.gridresultadmin.GridH5ResultAdmin

    def __post_init__(self):
        self._wlvl_all = None
        self._vol_all = None

        self.timestamps = self.grid.nodes.timestamps

    @property
    def wlvl_2d_all(self):
        if self._wlvl_all is None:
            self._wlvl_all = self.get_timerseries(param="s1")
        return self._wlvl_all

    @property
    def vol_2d_all(self):
        if self._vol_all is None:
            self._vol_all = self.get_timerseries(param="vol")
        return self._vol_all

    def get_timerseries(self, param):
        """Get all timeseries for all 2d nodes.
        slice(0,-1) doesnt retrieve the last timestep, using timestamp length instead.
        """
        return getattr(
            self.grid.nodes.subset("2D_open_water").timeseries(indexes=slice(0, len(self.timestamps))), param
        )

    def get_waterlevel_and_volume(self, time_seconds: Union[int, str]) -> pd.DataFrame:
        """Retrieve waterlevel and volume from netcdf

        Parameters
        ----------
        time_seconds : Union[int,str]
            time in seconds since start of calculation.
            use "max" to get the max of all timesteps.
        """
        # Waterlevel and volume at all timesteps
        if time_seconds == "max":
            # Retrieve values when wlvl is max
            max_index = self.wlvl_2d_all.argmax(axis=0)
            wlvl = np.round([row[max_index[enum]] for enum, row in enumerate(self.wlvl_2d_all.T)], 5)
            vol = np.round([row[max_index[enum]] for enum, row in enumerate(self.vol_2d_all.T)], 5)
        else:
            abs_diff = np.abs(self.timestamps - time_seconds)
            idx = np.argmin(abs_diff)
            if np.min(abs_diff) > 30:  # seconds diff.
                raise ValueError(
                    f"""Provided time_seconds {time_seconds} not found in netcdf timeseries.
Closest timestep is {self.timestamps[idx]} seconds at index {idx}. \
Debug by checking available timeseries through the (.ts).timeseries attributes"""
                )

            wlvl = np.round([row[idx] for row in self.wlvl_2d_all.T], 5)
            vol = np.round([row[idx] for row in self.vol_2d_all.T], 5)
        return pd.DataFrame([wlvl, vol]).T

    def create_column_base(self, time_seconds):
        """Return a base column name with hours and minutes.

        Same input as self.get_waterlevel_volume.
        """
        # time_seconds = self.timestamps[np.argmin(np.abs(self.timestamps - time_seconds))]
        if time_seconds == "max":
            col_base = time_seconds
        else:
            timestep_h = time_seconds / 3600

            if timestep_h % 1 == 0:  # round hours
                col_base = f"{int(timestep_h)}h"
            else:
                if timestep_h < 1:
                    col_base = f"{int(timestep_h*60)}min"
                else:
                    col_base = f"{int(np.floor(timestep_h))}h{int((timestep_h%1)*60)}min"
        return col_base


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

    def __post_init__(self):
        self._ts = None

    @classmethod
    def from_folder(cls, folder: Folders, threedi_result: hrt.ThreediResult, **kwargs):
        """Initialize from folder structure."""
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

    @property
    def ts(self):
        """Timeseries in the netcdf. Initialize once."""
        if self._ts is None:
            self._ts = NetcdfTimeSeries(grid=self.grid)
        return self._ts

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
        gdf = None
        # Load layer as gdf
        if (layer_path is not None) and (layer_path.exists()):
            gdf = gpd.read_file(str(layer_path), layer=layer_name)
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
        return np.nan

    def create_base_gdf(self):
        """Create  of grid"""
        grid_gdf = gpd.GeoDataFrame()

        # * inputs every element from row as a new function argument, creating a (square) box.
        grid_gdf["geometry"] = [box(*row) for row in self.grid.nodes.subset("2D_ALL").cell_coords.T]
        grid_gdf.crs = "EPSG:28992"

        grid_gdf["id"] = self.grid.cells.subset("2D_open_water").id

        grid_gdf = gpd.GeoDataFrame(grid_gdf, geometry="geometry")

        return grid_gdf

    def add_correction_parameters(
        self,
        grid_gdf: gpd.GeoDataFrame,
        replace_dem_below_perc: float = 50,
        replace_water_above_perc: float = 95,
        replace_pand_above_perc: float = 99,
    ) -> gpd.GeoDataFrame:
        """Determine which cells should have their waterlevel replaced by their neighbours.

        Returns
        -------
        gpd.GeoDataFrame
            extened grid_gdf with correction parameters columns.
        """
        # Percentage of dem in a calculation cell
        # so we can make a selection of cells on model edge that need to be ignored
        grid_gdf["dem_area"] = self.grid.cells.subset("2D_open_water").sumax
        # Percentage dem in calculation cell
        grid_gdf["dem_perc"] = grid_gdf["dem_area"] / grid_gdf.area * 100

        grid_gdf[["water_area", "water_perc"]] = self._calculate_layer_area_per_cell(
            grid_gdf=grid_gdf, layer_path=self.waterdeel_path, layer_name=self.waterdeel_layer
        )
        grid_gdf[["pand_area", "pand_perc"]] = self._calculate_layer_area_per_cell(
            grid_gdf=grid_gdf, layer_path=self.panden_path, layer_name=self.panden_layer
        )

        # Select cells that need replacing of wlvl
        grid_gdf["replace_dem"] = grid_gdf["dem_perc"] < replace_dem_below_perc
        grid_gdf["replace_water"] = grid_gdf["water_perc"] > replace_water_above_perc
        grid_gdf["replace_pand"] = grid_gdf["pand_perc"] > replace_pand_above_perc

        # Write reason of replacing
        grid_gdf["replace_all"] = False
        grid_gdf.loc[grid_gdf["replace_dem"], "replace_all"] = "dem"
        grid_gdf.loc[grid_gdf["replace_water"], "replace_all"] = "water"
        grid_gdf.loc[grid_gdf["replace_pand"], "replace_all"] = "pand"

        # Find neighbour cells and add their id's to a new column
        neighbours = []
        for row in grid_gdf.itertuples():
            # find all indices that touch the cell
            neighbours_ids = grid_gdf[grid_gdf.geometry.touches(row.geometry)].id.tolist()
            # find the id of those indices
            neighbours.append(str(neighbours_ids))
        grid_gdf["neighbour_ids"] = neighbours
        return grid_gdf

    def get_waterlevels(self, grid_gdf, timesteps_seconds: list = None):
        """Retrieve waterlevels at given timesteps"""
        for timestep in timesteps_seconds:
            # Make pretty column names
            col_base = self.ts.create_column_base(time_seconds=timestep)
            # Retrieve timeseries
            grid_gdf[[f"wlvl_{col_base}", f"vol_{col_base}"]] = self.ts.get_waterlevel_and_volume(
                time_seconds=timestep
            )
        return grid_gdf

    def correct_waterlevels(self, grid_gdf, timesteps_seconds):
        """Correct the waterlevel for the given timesteps. Results are only corrected
        for cells where the 'replace_all' value is not False.

        ----------
        """
        # create copy and index the id field so we can use the neighbours_ids column easily
        grid_gdf_local = grid_gdf.copy()
        grid_gdf_local.set_index("id", inplace=True)

        for timestep in timesteps_seconds:
            base_col = self.ts.create_column_base(time_seconds=timestep)
            wlvl_col = f"wlvl_{base_col}"
            wlvl_corr_col = f"{wlvl_col}_corr"
            diff_col = f"diff_{base_col}"
            idx_col = grid_gdf_local.columns.get_loc(wlvl_col) + 1

            # Make copy of original wlvls and set to None when they need to be replaced
            grid_gdf_local.insert(idx_col, wlvl_corr_col, grid_gdf_local[wlvl_col])
            replace_idx = grid_gdf_local["replace_all"] != False  # noqa: E712
            grid_gdf_local.loc[replace_idx, wlvl_corr_col] = None

            # Loop cells that need replacing.
            for row in grid_gdf_local.loc[replace_idx].itertuples():
                # Calculate avg wlvl of neighbours

                neighbour_ids = [int(i) for i in row.neighbour_ids[1:-1].split(",")]  # str list to list
                neighbour_avg_wlvl = np.round(grid_gdf_local.loc[neighbour_ids][wlvl_corr_col].mean(), 5)

                grid_gdf_local.loc[row.Index, wlvl_corr_col] = neighbour_avg_wlvl

            grid_gdf_local.insert(idx_col + 1, diff_col, grid_gdf_local[wlvl_corr_col] - grid_gdf_local[wlvl_col])
        return grid_gdf_local

    def run(
        self,
        output_file=None,
        timesteps_seconds: list[int, str] = ["max"],
        replace_dem_below_perc: float = 50,
        replace_water_above_perc: float = 95,
        replace_pand_above_perc: float = 99,
        wlvl_correction: bool = True,
        overwrite: bool = False,
    ):
        """Transform netcdf into a grid gpkg.

        Parameters
        ----------
        output_file, by default None
            When None is passed the output will be placed in the same directory as the netcdf.
            default name is: grid_wlvl.gpkg
        timesteps_seconds, by default ["max"]
            timesteps to make
            options: int values in seconds
                    "max" - maximum wlvl over calculation
        replace_dem_below_perc : float, optional, by default 50
            if cell area has no dem (isna) above this value waterlevels will be replaced
        replace_water_above_perc : float, optional, by default 95
            if cell has water surface area above this value waterlevels will be replaced
        replace_pand_above_perc : float, optional, by default 99
            if cell has pand surface area above this value waterlevels will be replaced
        wlvl_correction : bool, optional, by default True
            applies waterlevel correction when true.
        overwrite : bool, optional, by default False
            overwrite output if it exists
        """

        if output_file is None:
            output_file = self.output_default

        create = hrt.check_create_new_file(output_file=output_file, overwrite=overwrite)
        if create:
            grid_gdf = self.create_base_gdf()

            if wlvl_correction:
                grid_gdf = self.add_correction_parameters(
                    grid_gdf=grid_gdf,
                    replace_dem_below_perc=replace_dem_below_perc,
                    replace_water_above_perc=replace_water_above_perc,
                    replace_pand_above_perc=replace_pand_above_perc,
                )

            grid_gdf = self.get_waterlevels(grid_gdf=grid_gdf, timesteps_seconds=timesteps_seconds)

            if wlvl_correction:
                grid_gdf = self.correct_waterlevels(grid_gdf=grid_gdf, timesteps_seconds=timesteps_seconds)
            # Save to file
            grid_gdf.to_file(output_file, driver="GPKG")


if __name__ == "__main__":
    from hhnk_threedi_tools import Folders

    folder_path = r"E:\02.modellen\23_Katvoed"
    folder = Folders(folder_path)

    threedi_result = folder.threedi_results.one_d_two_d["katvoed #1 piek_ghg_T1000"]


# %%
