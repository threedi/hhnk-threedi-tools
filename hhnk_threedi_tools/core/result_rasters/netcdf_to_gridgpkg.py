# %%
from dataclasses import dataclass
from pathlib import Path
from typing import Union

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
from matplotlib.widgets import EllipseSelector
from shapely.geometry import box

from hhnk_threedi_tools.core.folders import Folders


class ThreediGrid:
    """TODO Deprecated, remove in later release."""

    def __init__(self, **kwargs):
        raise DeprecationWarning(
            "The ThreediGrid class has been replaced by \
htt.NetcdfToGPKG since v2024.1. Please rewrite your code."
        )


@dataclass
class NetcdfTimeSeries:
    """Timeseries contained in a netcdf"""

    grid: hrt.ThreediResult  # type threedigrid.admin.gridresultadmin.GridH5ResultAdmin

    def __post_init__(self):
        self._wlvl_all = None
        self._vol_all = None
        self._max_index = None

        self.aggregate: bool = self.typecheck_aggregate()

        self.timestamps = self.grid.nodes.timestamps

    @property
    def wlvl_2d_all(self):
        if self._wlvl_all is None:
            if not self.aggregate:
                self._wlvl_all = self.get_timerseries_all(param="s1")
            else:
                self._wlvl_all = self.get_timerseries_all(param="s1_max")

        return self._wlvl_all

    @property
    def vol_2d_all(self):
        if self._vol_all is None:
            self._vol_all = self.get_timerseries_all(param="vol")
        return self._vol_all

    @property
    def max_index(self):
        if self._max_index is None:
            self._max_index = self.wlvl_2d_all.argmax(axis=0)
        return self._max_index

    def typecheck_aggregate(self) -> bool:
        """Check if we have a normal or aggregated netcdf"""
        return str(type(self.grid)) == "<class 'threedigrid.admin.gridresultadmin.GridH5AggregateResultAdmin'>"

    def get_timerseries_all(self, param):
        """Get all timeseries for all 2d nodes.
        slice(0,-1) doesnt retrieve the last timestep, using timestamp length instead.
        """
        if not self.aggregate:
            return getattr(
                self.grid.nodes.subset("2D_open_water").timeseries(indexes=slice(0, len(self.timestamps))), param
            )

        else:
            """Aggregated results return a dict on self.timestamps"""
            return getattr(
                self.grid.nodes.subset("2D_open_water").timeseries(indexes=slice(0, len(self.timestamps[param]))),
                param,
            )

    def get_timeseries_timestamp(self, param: str, time_seconds: Union[int, str]):
        """Retrieve timeseries at given timestamp.

        Parameters
        ----------
        param : str
            options are ['wlvl', 'vol']
        time_seconds : Union[int,str]
            time in seconds since start of calculation.
            use "max" to get the max of all timesteps.
        """

        if time_seconds == "max":
            # Retrieve values when wlvl is max
            ts = np.round(
                [row[self.max_index[enum]] for enum, row in enumerate(getattr(self, f"{param}_2d_all").T)], 5
            )
        else:
            abs_diff = np.abs(self.timestamps - time_seconds)
            idx = np.argmin(abs_diff)
            if np.min(abs_diff) > 30:  # seconds diff.
                raise ValueError(
                    f"""Provided time_seconds {time_seconds} not found in netcdf timeseries.
Closest timestep is {self.timestamps[idx]} seconds at index {idx}. \
Debug by checking available timeseries through the (.ts) timeseries attributes"""
                )
            ts = np.round([row[idx] for row in getattr(self, f"{param}_2d_all").T], 5)

        # Replace -9999 with nan values to prevent -9999 being used in replacing values.
        ts = pd.Series(ts)
        ts.replace(-9999, np.nan, inplace=True)
        return ts

    def create_column_base(self, time_seconds):
        """Return a base column name with hours and minutes."""
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
class ColumnIdx:
    """Find index of columns in dataframe so we can insert new column
    at the correct location. This way we can group column types.
    e.g.
    wlvl_1h wlvl_3h wlvl_1h_corr wlvl_3h_corr diff_1h diff_15h
    """

    gdf: gpd.GeoDataFrame

    def _get_idx(self, search_str) -> int:
        """Get idx based on search pattern, if not found return last index"""
        idxs = self.gdf.columns.get_indexer(
            self.gdf.columns[self.gdf.columns.str.contains(search_str, na=False)]
        ).tolist()
        return (idxs or [len(self.gdf.columns) - 1])[-1] + 1

    @property
    def wlvl(self):
        return self._get_idx(search_str="^wlvl_(?!.*corr).*")

    @property
    def wlvl_corr(self):
        return self._get_idx(search_str="^wlvl_corr_.*")

    @property
    def diff(self):
        return self._get_idx(search_str="^diff_.*")

    @property
    def vol(self):
        return self._get_idx(search_str="^vol_.*")

    @property
    def storage(self):
        return self._get_idx(search_str="^storage_mm_.*")


@dataclass
class NetcdfToGPKG:
    """Transform netcdf into a gpkg. Can also correct waterlevels
    based on conditions. These are passed when running the function
    .run, to turn this behaviour off, use wlvl_correction=False there.

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
    use_aggregate: bool = False

    def __post_init__(self):
        self._ts = None

    @classmethod
    def from_folder(cls, folder: Folders, threedi_result: hrt.ThreediResult, use_aggregate: bool = False, **kwargs):
        """Initialize from folder structure."""
        waterdeel_path = folder.source_data.damo
        waterdeel_layer = "Waterdeel"

        panden_path = folder.source_data.panden
        panden_layer = "panden"

        return cls(
            threedi_result=threedi_result,
            waterdeel_path=waterdeel_path,
            waterdeel_layer=waterdeel_layer,
            panden_path=panden_path,
            panden_layer=panden_layer,
            use_aggregate=use_aggregate,
        )

    @property
    def grid(self):
        """Instance of threedigrid.admin.gridresultadmin.GridH5ResultAdmin or GridH5AggregateResultAdmin"""
        if self.use_aggregate is False:
            return self.threedi_result.grid
        return self.threedi_result.aggregate_grid

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
            overlay_df_grouped = overlay_df.groupby("id")[[area_col]].agg("sum")

            # Put in area in grid gdf and calculate percentage.
            grid_gdf_merged = grid_gdf.merge(overlay_df_grouped[area_col], left_on="id", right_on="id", how="left")
            grid_gdf_merged[perc_col] = grid_gdf_merged[area_col] / grid_gdf_merged.area * 100
            return grid_gdf_merged[[area_col, perc_col]]
        return np.nan

    def create_base_gdf(self):
        """Create base grid from netcdf"""
        grid_gdf = gpd.GeoDataFrame()

        # * inputs every element from row as a new function argument, creating a (square) box.
        grid_gdf.set_geometry(
            [box(*row) for row in self.grid.nodes.subset("2D_ALL").cell_coords.T], crs="EPSG:28992", inplace=True
        )

        grid_gdf["id"] = self.grid.cells.subset("2D_open_water").id
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
        grid_gdf["dem_minimal_m"] = self.grid.cells.subset("2D_open_water").z_coordinate
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

    def get_waterlevels(self, grid_gdf, timesteps_seconds: list):
        """Retrieve waterlevels volume and storage at given timesteps"""

        col_idx = ColumnIdx(gdf=grid_gdf)

        for timestep in timesteps_seconds:
            # Make pretty column names
            col_base = self.ts.create_column_base(time_seconds=timestep)

            # Retrieve timeseries
            try:
                vol_ts = self.ts.get_timeseries_timestamp(param="vol", time_seconds=timestep)
                grid_gdf.insert(
                    col_idx.vol,
                    f"vol_{col_base}",
                    vol_ts,
                )
                grid_gdf.insert(
                    col_idx.storage,
                    f"storage_mm_{col_base}",
                    np.round(grid_gdf[f"vol_{col_base}"] / grid_gdf["dem_area"] * 1000, 2),
                )
            except KeyError:
                print("Volume not found in (aggregated)result")

            grid_gdf.insert(
                col_idx.wlvl,
                f"wlvl_{col_base}",
                self.ts.get_timeseries_timestamp(param="wlvl", time_seconds=timestep),
            )
        return grid_gdf

    def correct_waterlevels(self, grid_gdf, timesteps_seconds: list):
        """Correct the waterlevel for the given timesteps. Results are only corrected
        for cells where the 'replace_all' value is not False.
        """
        # Create copy and set_index the id field so we can use the neighbours_ids column easily
        grid_gdf_local = grid_gdf.copy()
        grid_gdf_local.set_index("id", inplace=True)

        for timestep in timesteps_seconds:
            base_col = self.ts.create_column_base(time_seconds=timestep)
            wlvl_col = f"wlvl_{base_col}"
            wlvl_corr_col = f"wlvl_corr_{base_col}"
            diff_col = f"diff_{base_col}"
            col_idx = ColumnIdx(gdf=grid_gdf_local)

            # Make copy of original wlvls and set to None when they need to be replaced
            grid_gdf_local.insert(col_idx.wlvl_corr, wlvl_corr_col, grid_gdf_local[wlvl_col])
            replace_idx = grid_gdf_local["replace_all"] != False  # noqa: E712
            grid_gdf_local.loc[replace_idx, wlvl_corr_col] = None

            # Loop cells that need replacing.
            for row in grid_gdf_local.loc[replace_idx].itertuples():
                # Dont replace nan values
                if pd.isna(grid_gdf_local.loc[row.Index, wlvl_col]):
                    continue

                # Calculate avg wlvl of neighbours and update in table
                neighbour_ids = [int(i) for i in row.neighbour_ids[1:-1].split(",")]  # str list to list
                neighbour_avg_wlvl = np.round(grid_gdf_local.loc[neighbour_ids][wlvl_corr_col].mean(), 5)
                grid_gdf_local.loc[row.Index, wlvl_corr_col] = neighbour_avg_wlvl

            # Add diff col between corrected and original wlvl
            grid_gdf_local.insert(
                col_idx.diff, diff_col, np.round(grid_gdf_local[wlvl_corr_col] - grid_gdf_local[wlvl_col], 5)
            )
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
            time in seconds since start of calculation. Will create cols for each item in list.
            options:
                int value - seconds since start
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
            grid_gdf.to_file(str(output_file), engine="pyogrio")


# %%
if __name__ == "__main__":
    from hhnk_threedi_tools import Folders

    folder_path = r"E:\02.modellen\HKC23010_Eijerland_WP"
    folder = Folders(folder_path)

    threedi_result = folder.threedi_results.batch["bwn_gxg"].downloads.piek_ghg_T10

    output_file = None
    wlvl_correction = False
    overwrite = True
    self = NetcdfToGPKG(threedi_result=threedi_result.netcdf, use_aggregate=True)
    timesteps_seconds = ["max"]
    self.run(wlvl_correction=wlvl_correction)

# %%
