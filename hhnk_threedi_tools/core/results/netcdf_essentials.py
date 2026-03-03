# %%
"""
Code below is used to transform netcdf files into geopackage files.
It retrieves essential data from the netcdf files and stores it in a geopackage.

The idea is, to extent the functionality to include other derived data, such as
those needed for het Leggertool, the various checks and 'klimaattoets' statistics.
These functionalities can be added to other scripts, but it would be nice to keep all
data in the same geopackage.

The geopackage can then be used for all relevant data annd replace the netCDF, saving considarable
disk space and making it easier and faster to work with the results.

The code below relies on netcdfTimeseries Class for extraction of relevant data that is listed in the
netcdf_essentials.csv (config) file. This file is used to define the relevant data and can be extended
to include other data that is needed for the various checks and statistics.

The code  includes the correction of waterlevels based on the waterdeel and panden layers. These
layers are used to determine which cells need to be corrected. This is done by checking the percentage
of the cell that is covered by water or panden.

@organization: Hoogheemraadschap Hollands Noorderkwartier

"""

import json
import logging
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
from shapely.geometry import LineString, Point, box

from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.core.results import column_index, netcdf_timeseries
from hhnk_threedi_tools.resources.netcdf_essentials_default import DEFAULT_NESS

AVAILABLE_SIMULATION_TYPES = ["0d1d_test", "1d2d_test", "klimaattoets", "breach"]

logger = hrt.logging.get_logger(__name__)


@dataclass
class NetcdfEssentials:
    """
    Parameters
    ----------
    threedi_result : hrt.ThreediResult
        Path or object for the NetCDF and HDF5 result files.
    simulation_type : str, optional
        Calculation type to specify retrieval of which results,
        options are 0d1d_test, 1d2d_test, klimaattoets, breach.
        Pass None to retrieve default set specified in resources/netcdf_essentials
        and user specified time steps.
    user_defined_timesteps : list[int], optional
        List of output time steps (seconds or "max").
    ness : pd.DataFrame, optional
        DataFrame with relevant attributes.
        Should contain columns: 'attribute', 'subset', 'element', 'attribute_name'.
    ness_fp : Path, optional
        Path to a JSON file with relevant attributes.
        If None, the default file will be used.
        default is: hhnk_threedi_tools.resources.netcdf_essentials_default
    use_aggregate : bool, True
        Will process aggregate netcdf if available. All aggregate variables are stored in GeoPackage

    Methods
    -------
    from_folder(folder, threedi_result, use_aggregate, **kwargs)
        Initialize from a Folders structure.
    run(...)
        Main method to process NetCDF and export to GeoPackage.
    create_base_gdf()
    append_data(...)
        Insert timeseries data into GeoDataFrames.
    process_ness(ness)
    load_default_ness(ness_fp)
        Load default ness settings as DataFrame.
    """

    threedi_result: hrt.ThreediResult
    simulation_type: str = None
    user_defined_timesteps: list[int] = None
    ness: pd.DataFrame = None
    ness_fp: Path = None
    use_aggregate: bool = True
    output_fp: Path = None

    def __post_init__(self):
        # Default outputs if no paths are specified.
        if self.output_fp is None:
            self.output_fp = self.threedi_result.full_path("threedi_result.gpkg")  # TODO include via Folders key
        else:
            hrt.SpatialDatabase(self.output_fp)

    @classmethod
    def from_folder(cls, folder: Folders, threedi_result: hrt.ThreediResult, use_aggregate: bool = True):
        """Initialize from folder structure."""

        return cls(
            threedi_result=threedi_result,
            use_aggregate=use_aggregate,
        )

    @property
    def aggregate(self):
        # Check if aggregate netcdf exists
        if self.use_aggregate and self.threedi_result.aggregate_grid_path.exists():
            return True
        elif self.use_aggregate and not self.threedi_result.aggregate_grid_path.exists():
            logger.warning("No aggregate netcdf was found, skipping")
            return False
        else:
            return False

    @property
    def nc_ts(self):
        nc_ts = netcdf_timeseries.NetcdfTimeSeries(threedi_result=self.threedi_result)
        return nc_ts

    @property
    def agg_nc_ts(self):
        nc_agg_ts = netcdf_timeseries.AggregateNetcdfTimeSeries(threedi_result=self.threedi_result)
        return nc_agg_ts

    @property
    def grid(self):
        """Instance of threedigrid.admin.gridresultadmin.GridH5ResultAdmin"""
        return self.threedi_result.grid

    @property
    def grid_agg(self):
        """Instance of threedigrid.admin.gridresultadmin.AggregateGridH5ResultAdmin"""
        return self.threedi_result.aggregate_grid

    @property
    def get_ness(self):
        """Load relevant data for HHNK models"""
        if self.ness is None and self.ness_fp is None:
            ness = pd.DataFrame(DEFAULT_NESS)
            logger.info("Loading default ness from resources.")
        elif self.ness is None and self.ness_fp is not None:
            with open(self.ness_fp, "r", encoding="utf-8") as f:
                data = json.load(f)
            ness = pd.DataFrame(data)
            logger.info(f"Loading ness from {self.ness_fp}")
        else:
            ness = self.ness

        return ness

    def _create_column_base(self, time_seconds: str | int) -> str:
        """Create a base column name with hours and minutes based time_seconds."""
        # Check if time_seconds is a string or int
        if isinstance(time_seconds, str):
            col_base = time_seconds
        elif isinstance(time_seconds, (int, float)):
            timestep_h = time_seconds / 3600
            # Round to nearest hour or minute
            if timestep_h % 1 == 0:  # round hours
                col_base = f"{int(timestep_h)}h"
            else:
                if timestep_h < 1:
                    col_base = f"{int(timestep_h * 60)}min"
                else:
                    col_base = f"{int(np.floor(timestep_h))}h{int((timestep_h % 1) * 60)}min"
        return col_base

    def _set_active_attributes(self, ness: pd.DataFrame) -> pd.DataFrame:
        """
        Take attributes from provided ness settings and check wether
        grid/result has these attributes available and if so for either
        1d or 2d.

        This ensures that only relevant attributes are processed.

        Parameters
        ----------
        ness : pd.DataFrame
            DataFrame with attributes and subsets to check against the grid.
            Should contain columns: 'attribute', 'subset', 'element', 'attribute_name'.
        """
        ness["active"] = True
        if not self.grid.has_1d:
            ness.loc[(ness["subset"] == "1D_All"), "active"] = False
        if not self.grid.has_2d:
            ness.loc[(ness["subset"] == "2D_OPEN_WATER"), "active"] = False
        if not self.grid.has_interception:
            ness.loc[(ness["attribute"] == "intercepted_volume"), "active"] = False
        if not self.grid.has_max_infiltration_capacity:
            ness.loc[(ness["attribute"] == "infiltration_rate_simple"), "active"] = False

        # list active attributes for logging, concatenate attribute_name and subset from dataframe
        active_attributes = ness[ness["active"]]["attribute_name"] + " - " + ness[ness["active"]]["subset"]
        logger.info(f"Active attributes: {active_attributes.tolist()}")
        return ness

    def process_nc_from_ness(
        self,
    ) -> pd.DataFrame:
        """
        Process ness dataframe to set active attributes and retrieve data from result NetCDF.

        Parameters
        ----------
        ness : pd.DataFrame
            DataFrame with attributes and subsets to check against the grid.
            Should contain columns: 'attribute', 'subset', 'element', 'attribute_name'.
        """
        ness = self._set_active_attributes(self.ness)
        # filter ness
        ness = ness[ness["active"]].copy()

        # initialise data columns
        ness["amount"] = None
        ness["data"] = None
        # Retrieve timeseries for each row in ness
        for i, row in ness.iterrows():
            data = self.nc_ts.get_ts(
                attribute=row["attribute"],
                element=row["element"],
                subset=row["subset"],
            )
            getattr(
                getattr(self.grid, row["element"])
                .subset(row["subset"])
                .timeseries(indexes=slice(0, len(self.nc_ts.timestamps))),
                row["attribute"],
            ).T
            # add data as nested array to dataframe
            ness.at[i, "data"] = data  # noqa: PD008 must use .at for nested array
            ness.loc[i, "amount"] = data.shape[0]

        return ness

    def _load_lines(self, subset_str: str) -> gpd.GeoDataFrame:
        line_gdf = gpd.GeoDataFrame()

        # Voeg geometry toe aan line gdf
        xys = self.grid.lines.subset(subset_str).line_geometries  # format [x1, x2, ..., y1, y2, ...]

        # Shapely requires [[x1,y1],[x2,y2],...]
        lines = []
        for xxyy in xys:
            n = int(len(xxyy) / 2)
            xy = []
            for i in range(0, n):
                xy.append([xxyy[i], xxyy[i + n]])
            lines.append(LineString(xy))

        # Add line geometry to gdf
        line_gdf.set_geometry(
            lines,
            crs=f"EPSG:{self.grid.epsg_code}",
            inplace=True,
        )

        # Add relevant metadata
        line_gdf["id"] = self.grid.lines.subset(subset_str).id
        line_gdf["content_pk"] = self.grid.lines.subset(subset_str).content_pk
        line_gdf["exchange_level"] = self.grid.lines.subset(subset_str).dpumax
        line_gdf["line_type_kcu"] = self.grid.lines.subset(subset_str).kcu
        line_gdf["start_node"] = self.grid.lines.subset(subset_str).line[0]
        line_gdf["end_node"] = self.grid.lines.subset(subset_str).line[1]
        line_gdf["zoom_category"] = self.grid.lines.subset(subset_str).zoom_category

        logger.info(f"Created {len(line_gdf)} lines.")

        return line_gdf

    def create_base_gdf(
        self,
    ) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """
        Create empty GeoDataFrames for grid, nodes, and lines.
        Populates them with geometries and relevant metadata from the grid.
        If the grid has 2D cells, it creates polygons for each cell.
        If the grid has 1D nodes, it creates points for each node.
        If the grid has 1D lines, it creates lines for each line segment.

        Returns
        -------
        grid_gdf : gpd.GeoDataFrame
            GeoDataFrame with grid cells.
        node_gdf : gpd.GeoDataFrame
            GeoDataFrame with 1D nodes.
        line_gdf : gpd.GeoDataFrame
            GeoDataFrame with 1D lines.
        metadata : dict
            Dictionary with metadata about the model and grid.
        """
        # Create empty GeoDataFrames for grid, node and line
        grid_gdf = gpd.GeoDataFrame()
        node_gdf = gpd.GeoDataFrame()

        # =========================
        # GRID
        # =========================
        if self.grid.has_2d:
            # * inputs every element from row as a new function argument, creating a (square) box.
            grid_gdf.set_geometry(
                [box(*row) for row in self.grid.nodes.subset("2D_OPEN_WATER").cell_coords.T],
                crs=f"EPSG:{self.grid.epsg_code}",
                inplace=True,
            )

            # Add relevant metadata
            grid_gdf["id"] = self.grid.cells.subset("2D_OPEN_WATER").id
            grid_gdf["calculation_type"] = self.grid.cells.subset("2D_OPEN_WATER").calculation_type
            grid_gdf["dmax_bottom_level"] = self.grid.cells.subset("2D_OPEN_WATER").dmax

            grid_gdf["dem_minimal_m"] = self.grid.cells.subset(
                "2D_OPEN_WATER"
            ).z_coordinate  # FIXME in de testdata is dit alleen nan, misschien dmax? Dit zou bottom level zijn?

            grid_gdf["dem_area"] = self.grid.cells.subset("2D_OPEN_WATER").sumax

            logger.info(f"Created grid with {len(grid_gdf)} cells.")

            # =========================
            # LINES 2D_OPEN_WATER
            # =========================
            line_2d_gdf = self._load_lines(subset_str="2D_OPEN_WATER")

        if self.grid.has_1d:
            # =========================
            # NODES
            # =========================
            # Read 1d node coordinates
            coords_1d = self.grid.nodes.subset("1D_All").coordinates

            # Create a list of Shapely Point objects
            xy = []
            for i in range(0, len(coords_1d[0])):
                xy.append([coords_1d[0, i], coords_1d[1, i]])
            points = [Point(j) for j in xy]

            # Voeg geometry toe aan node gdf
            node_gdf.set_geometry(
                points,
                crs=f"EPSG:{self.grid.epsg_code}",
                inplace=True,
            )

            # Add relevant metadata
            node_gdf["id"] = self.grid.nodes.subset("1D_All").id
            node_gdf["connection_node_id"] = self.grid.nodes.subset("1D_All").content_pk
            node_gdf["initial_waterlevel"] = self.grid.nodes.subset("1D_All").initial_waterlevel
            node_gdf["storage_area"] = self.grid.nodes.subset("1D_All").storage_area
            node_gdf["drain_level"] = self.grid.nodes.subset("1D_All").drain_level
            node_gdf["zoom_category"] = self.grid.nodes.subset("1D_All").zoom_category
            node_gdf["calculation_type"] = self.grid.nodes.subset("1D_All").calculation_type

            logger.info(f"Created {len(node_gdf)} nodes.")

            # =========================
            # LINES 1D_All
            # =========================
            line_1d_gdf = self._load_lines(subset_str="1D_ALL")

        # =========================
        # METADATA
        # =========================
        meta_dict = {
            "model_slug": self.grid.model_slug,
            "model_name": self.grid.model_name,
            "revision_hash": self.grid.revision_hash,
            "revision_nr": self.grid.revision_nr,
            "has_0d": self.grid.has_0d,
            "has_1d": self.grid.has_1d,
            "has_2d": self.grid.has_2d,
            "has_breaches": self.grid.has_interception,
            "has_max_infiltration_capacity": self.grid.has_max_infiltration_capacity,
            "has_simple_infiltration": self.grid.has_simple_infiltration,
            "threedicore_result_version": str(self.grid.threedicore_result_version),
            "epsg_code": self.grid.nodes.epsg_code,
        }
        meta_gdf = gpd.GeoDataFrame(meta_dict, index=[0])  # gdf for writing GoePackage

        logger.info(f"Created metadata for result from model {self.grid.model_name} # {self.grid.revision_nr}")

        # TODO breaches ACTIVE_BREACH add output with only active breaches, if there are active breaches
        # TODO 1d2d 1D2D
        return grid_gdf, node_gdf, line_1d_gdf, line_2d_gdf, meta_gdf

    def get_output_timesteps(
        self,
        simulation_type: str = None,
        user_defined_timesteps: list[int | str] = None,
    ) -> list[int]:
        """
        Set required output for different simulation types depending on the simulation type.

        Parameters
        ----------
        simulation_type: str = None
            Options for simulation types are None, 0d1d_test, 1d2d_test, klimaattoets, breach.
        user_defined_timesteps: list[int | str] = None
            List of user defined time steps in seconds or method e.g. "max" for maximum value over time series.
        """
        timesteps_seconds_output = ["max"]
        if simulation_type is None:
            if user_defined_timesteps is None:
                logger.info("No simulation type or user defined timesteps provided, using only max")
        elif simulation_type in AVAILABLE_SIMULATION_TYPES:
            logger.info(f"Using simulation type {simulation_type} to set output timesteps.")
            if simulation_type == "0d1d_test":
                timesteps_seconds_output = ["max"]  # TODO get from rain times etc, also how to correctly label columns
            elif simulation_type == "1d2d_test":
                timesteps_seconds_output = ["max"]  # TODO find out what is needed for this one
            elif simulation_type == "klimaattoets":
                timesteps_seconds_output = ["max"]  # only maximum is needed for klimaattoets?
            elif simulation_type == "breach":
                timesteps_seconds_output = [3600 * i for i in range(1, 25)] + ["max"]  # TODO where to define?
        else:
            logger.warning(f"Simulation type {simulation_type} not recognized, using only max")

        if user_defined_timesteps is not None:
            # Add user defined time steps to output, if not already included via simulation type
            timesteps_seconds_output = list(set(timesteps_seconds_output + user_defined_timesteps))

        return timesteps_seconds_output

    def append_nc_data(
        self,
        nc_ts: netcdf_timeseries.NetcdfTimeSeries | netcdf_timeseries.AggregateNetcdfTimeSeries,
        ness: pd.DataFrame,
        gdf: gpd.GeoDataFrame,
        subset_str: str,
        timesteps_seconds_output: list = None,
    ) -> gpd.GeoDataFrame:
        """
        Insert data at given timesteps to geodataframe.

        Parameters
        ----------
        ness : pd.DataFrame
            DataFrame with attributes and subsets to check against the grid.
            Should contain columns: 'attribute', 'subset', 'element', 'attribute_name', 'data'.
        gdf : gpd.GeoDataFrame
            GeoDataFrame to append data to.
        timesteps_seconds_output : list[Union[int, str]]
            List of timesteps in seconds for which to append data.
            Can contain int values or "max" for maximum value over the time series.
        """

        col_idx = column_index.ColumnIdx(gdf=gdf)
        for i, row in ness.iterrows():
            data_timestep = np.array([])
            data_method = np.array([])
            if (
                row["active"]
                and row["subset"] == subset_str
                and row["geom_type"] == gdf.geometry.geom_type.unique()[0]
            ):
                if timesteps_seconds_output is not None:
                    # processing user specified timesteps
                    for key in timesteps_seconds_output:
                        if isinstance(key, int):
                            # Check that key is in timestamps, if not log warning and skip
                            ts_idx = nc_ts.get_ts_index(timestamps=nc_ts.timestamps, time_seconds=key)
                            if ts_idx is None:
                                logger.warning(f"Time step {key} not found in netcdf timestamps.")
                                continue

                            # Make pretty column name
                            col_sub = self._create_column_base(time_seconds=key)
                            # Get timeseries data
                            data_timestep = row["data"][:, ts_idx]
                        elif isinstance(key, str):
                            logger.debug(f"Adding user specified method: {key} for {row['attribute_name']}")
                            data_timestep = nc_ts.get_ts_methods(method=key, ts=row["data"])
                            col_sub = key

                        # Insert value in gdf
                        if data_timestep is not None or not (
                            isinstance(data_timestep, np.ndarray) and data_timestep.size == 0
                        ):  # Check that there is data
                            gdf.insert(
                                getattr(col_idx, row["attribute_name"]),
                                f"{row['attribute_name']}_{col_sub}",
                                data_timestep,
                            )
                # processing methods from ness # TODO make separate function?
                if not row["methods"]:
                    for method in row["methods"]:
                        data_method = np.array([])
                        if method in timesteps_seconds_output:
                            continue  # already processed
                        if not method:  # check that method list is not empty
                            logger.debug(f"Adding default method: {method} for {row['attribute_name']}")
                            data_method = nc_ts.get_ts_methods(method=method, ts=row["data"])
                        if data_timestep is not None or not (
                            isinstance(data_timestep, np.ndarray) and data_timestep.size == 0
                        ):  # Check that there is data
                            # Insert method data
                            gdf.insert(
                                getattr(col_idx, row["attribute_name"]),
                                f"{row['attribute_name']}_{method}",
                                data_method,
                            )

        return gdf

    def append_agg_nc_data(
        self,
        agg_nc_ts: netcdf_timeseries.AggregateNetcdfTimeSeries,
        gdf: gpd.GeoDataFrame,
        subset_str: str,
        timesteps_seconds_output: list = None,
    ) -> gpd.GeoDataFrame:
        if self.aggregate:
            # Construct ness from aggregate netcdf variables
            ness_agg = pd.DataFrame()
            for element, attribute in agg_nc_ts.variables.items():
                geom_type = None
                if subset_str == "2D_OPEN_WATER" and element == "nodes":
                    geom_type = "Polygon"
                elif subset_str == "2D_OPEN_WATER" and element == "lines":
                    geom_type = "LineString"
                elif subset_str == "1D_All" and element == "nodes":
                    geom_type = "Point"
                elif subset_str == "1D_All" and element == "lines":
                    geom_type = "LineString"

                # replace s1 with wlvl if in string attribute
                attribute_name = attribute
                if "s1" in attribute:
                    attribute_name = attribute.replace("s1", "wlvl")
                # only keep part before underscore
                attribute_name = attribute_name.split("_")[0]
                # TODO check for other aggregate variables

                ness_agg = pd.concat(
                    [
                        ness_agg,
                        pd.DataFrame(
                            {
                                "name": f"{attribute}_agg",
                                "geom_type": geom_type,
                                "attribute": attribute,
                                "subset": subset_str,
                                "element": element,
                                "attribute_name": f"{attribute_name}",
                                "methods": [["min", "max"]],
                                "active": True,
                                "data": [
                                    agg_nc_ts.get_ts(
                                        attribute=attribute,
                                        element=element,
                                        subset=subset_str,
                                    )
                                ],
                            }
                        ),
                    ],
                    ignore_index=True,
                )
            gdf = self.append_nc_data(
                nc_ts=agg_nc_ts,
                ness=ness_agg,
                gdf=gdf,
                subset_str=subset_str,
                timesteps_seconds_output=timesteps_seconds_output,
            )
        else:
            logger.info("No aggregate netcdf to process.")
        return gdf

    def run(
        self,
        ness: pd.DataFrame = None,
        user_defined_timesteps: list[int] = None,
        simulation_type: str = None,
        overwrite: bool = False,
    ):
        """Transform netcdf into a grid gpkg.

        Parameters
        ----------
        user_defined_timesteps : list[int], optional
            List of output timesteps in seconds or "max" to include maximum water level over calculation.
            If None, only uses max.
        simulation_type: bool = True
            Process aggregate result if available. Creates extra layers with aggregate results.
        simulation_type : str, optional
            Calculation type to specify retrieval of which results,
            options are 0d1d_test, 1d2d_test, klimaattoets, breach.
            Pass None to retrieve default set specified in resources/netcdf_essentials
            and user specified timesteps.
        overwrite : bool, optional, by default False
            overwrite output if it exists
        """
        # initialize logger
        logger = hrt.logging.get_logger(
            __name__, filepath=Path(self.output_fp.path).parent.absolute() / "NetCDFEssentials.log"
        )
        logger.setLevel(logging.INFO)
        logger.info(f"Starting NetcdfEssentials with result: {self.threedi_result}")

        create = hrt.check_create_new_file(output_file=self.output_fp, overwrite=overwrite)
        if create:
            ts_out = self.get_output_timesteps(simulation_type, user_defined_timesteps)
            ness = self.get_ness()
            ness = self.process_nc_from_ness(ness=ness)

            grid_gdf, node_gdf, line_1d_gdf, line_2d_gdf, meta_gdf = self.create_base_gdf()
            grid_nc_gdf = self.append_nc_data(
                nc_ts=self.nc_ts,
                ness=ness,
                gdf=grid_gdf.copy(),
                subset_str="2D_OPEN_WATER",
                timesteps_seconds_output=ts_out,
            )
            node_nc_gdf = self.append_nc_data(
                nc_ts=self.nc_ts, ness=ness, gdf=node_gdf.copy(), subset_str="1D_All", timesteps_seconds_output=ts_out
            )
            line_nc_1d_gdf = self.append_nc_data(
                nc_ts=self.nc_ts,
                ness=ness,
                gdf=line_1d_gdf.copy(),
                subset_str="1D_All",
                timesteps_seconds_output=ts_out,
            )
            line_nc_2d_gdf = self.append_nc_data(
                nc_ts=self.nc_ts,
                ness=ness,
                gdf=line_2d_gdf.copy(),
                subset_str="2D_OPEN_WATER",
                timesteps_seconds_output=ts_out,
            )

            # Save to file
            grid_nc_gdf.to_file(self.output_fp.path, layer="grid_2d", overwrite=overwrite)
            node_nc_gdf.to_file(self.output_fp.path, layer="node_1d", overwrite=overwrite)
            line_nc_1d_gdf.to_file(self.output_fp.path, layer="line_1d", overwrite=overwrite)
            line_nc_2d_gdf.to_file(self.output_fp.path, layer="line_2d", overwrite=overwrite)
            logger.info(f"Saved NetCDF essentials to {self.output_fp.path}")

            # process aggregate NetCDF
            if self.aggregate:
                meta_gdf["agg_variables"] = self.agg_nc_ts.variables
                grid_agg_nc_gdf = self.append_agg_nc_data(
                    agg_nc_ts=self.agg_nc_ts,
                    gdf=grid_gdf.copy(),
                    subset_str="2D_OPEN_WATER",
                    timesteps_seconds_output=ts_out,
                )
                node_agg_nc_gdf = self.append_agg_nc_data(
                    agg_nc_ts=self.agg_nc_ts, gdf=node_gdf.copy(), subset_str="1D_All", timesteps_seconds_output=ts_out
                )
                line_1d_agg_nc_gdf = self.append_agg_nc_data(
                    agg_nc_ts=self.agg_nc_ts,
                    gdf=line_1d_gdf.copy(),
                    subset_str="1D_All",
                    timesteps_seconds_output=ts_out,
                )
                line_2d_agg_nc_gdf = self.append_agg_nc_data(
                    agg_nc_ts=self.agg_nc_ts,
                    gdf=line_2d_gdf.copy(),
                    subset_str="2D_OPEN_WATER",
                    timesteps_seconds_output=ts_out,
                )
                # Save to file
                grid_agg_nc_gdf.to_file(self.output_fp.path, layer="grid_2d_agg", overwrite=overwrite)
                node_agg_nc_gdf.to_file(self.output_fp.path, layer="node_1d_agg", overwrite=overwrite)
                line_1d_agg_nc_gdf.to_file(self.output_fp.path, layer="line_1d_agg", overwrite=overwrite)
                line_2d_agg_nc_gdf.to_file(self.output_fp.path, layer="line_2d_agg", overwrite=overwrite)

                logger.info(f"Saved AggregateNetCDF to {self.output_fp.path}")

            meta_gdf.to_file(
                self.output_fp.path,
                layer="metadata",
                driver="GPKG",
                overwrite=overwrite,
            )

        else:
            logger.warning(f"Output file {self.output_fp.path} already exists. Set overwrite to True to overwrite.")


# TODO remove below
# %% Working code example small model
if __name__ == "__main__":
    from hhnk_threedi_tools import Folders

    folder_path = r"tests\data\model_test"
    folder = Folders(folder_path)

    user_defined_timesteps = ["max", 3600, 5400]
    output_file = None
    ness_fp = None
    overwrite = True
    self = NetcdfEssentials.from_folder(
        folder=folder,
        threedi_result=folder.threedi_results.batch["batch_test"].downloads.piek_glg_T10.netcdf,
        use_aggregate=False,
    )

    self.run(
        user_defined_timesteps=user_defined_timesteps,
        overwrite=overwrite,
    )

# %% Performance test (large model including sewerage)
# if __name__ == "__main__":
#     from hhnk_threedi_tools import Folders

#     folder_path = r"E:\02.modellen\BWN_Castricum_Integraal_10m"
#     folder = Folders(folder_path)
#     threedi_result = folder.threedi_results.batch["rev1"].downloads.piek_ghg_T1000

#     user_defined_timesteps = [3600, 5400]
#     output_file = r"E:\02.modellen\BWN_Castricum_Integraal_10m\test_netcdfessentials_piek_ghg_T1000.gpkg"
#     wlvl_correction = False
#     overwrite = True
#     self = NetcdfEssentials(threedi_result=threedi_result.netcdf, use_aggregate=False)
#     self.run(
#         output_file=output_file,
#         user_defined_timesteps=user_defined_timesteps,
#         overwrite=overwrite,
#     )
# NOTE op volle server geeft dit al memory issues, duurt nu 1m 40 s

# %% Working code example with aggregate result # TODO aggregate
if __name__ == "__main__":
    from hhnk_threedi_tools import Folders

    folder_path = r"Y:\03.resultaten\OverstromingsberekeningenprimairedoorbrakenZeespiegelstijging"
    folder = Folders(folder_path)

    # TODO AttributeError: 'File' object has no attribute 'full_path', graag wilik een losse file kunnen gebruiken zonder de verplichte structuur
    # folder.add_file("result", r"Y:\03.resultaten\OverstromingsberekeningenprimairedoorbrakenZeespiegelstijging\Bres Egmond T10k+3m\results_3di.nc")
    threedi_result = folder.threedi_results.one_d_two_d[2]

    # # get and correct waterlevels
    #  timesteps_seconds = ["max", 3600, 5400]
    # grid_gdf = netcdf_gpkg.get_waterlevels(grid_gdf=grid_gdf, timesteps_seconds=timesteps_seconds)
    # grid_gdf = netcdf_gpkg.correct_waterlevels(grid_gdf=grid_gdf, timesteps_seconds=timesteps_seconds)

    output_file = None
    wlvl_correction = False
    overwrite = True
    use_aggregate = True
    simulation_type = None
    ness_fp = None
    self = NetcdfEssentials(threedi_result=threedi_result, use_aggregate=use_aggregate)
    user_defined_timesteps = [
        3600,
        7200,
        10800,
        21600,
        43200,
        86400,
        172800,
        259200,
        518400,
        1036800,
        1555200,
        1728000,
        "max",
    ]
    # user_defined_timesteps = [
    #     172800,
    #     "max",
    # ]
    self.run(user_defined_timesteps=user_defined_timesteps, overwrite=overwrite)
# %% TODO test model zonder maaiveld

# %% TODO test model met bressen

# TODO ness input run zodat je eigen op kunt geven
