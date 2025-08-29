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

The code  includes the correction of waterlevels based on the waterdeel and panden layers. These l
ayers are used to determine which cells need to be corrected. This is done by checking the percentage
of the cell that is covered by water or panden.


@author: Wietse van Gerwen / Wouter van Esse
@organization: Hoogheemraadschap Hollands Noorderkwartier

"""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Union

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
from shapely.geometry import LineString, Point, box

from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.core.results import netcdf_timeseries

DEFAULT_NESS_FP = Path(__file__).parents[2].absolute() / r"resources\netcdf_essentials.csv"
MINMAX_ELEMENTS = ["u1", "q"]  # TODO use to get both min and max values


@dataclass
class ColumnIdx:
    """
    Utility class to find the index of columns in a GeoDataFrame for inserting new columns at logical positions.
    This helps group related column types together, e.g.:
    wlvl_1h wlvl_3h wlvl_1h_corr wlvl_3h_corr diff_1h diff_15h

    Attributes
    ----------
    gdf : gpd.GeoDataFrame
        The GeoDataFrame whose columns are being indexed.

    Methods
    -------
    _get_idx(search_str)
        Returns the index for inserting a column matching the search pattern.
    Properties for common column types:
        wlvl, wlvl_corr, diff, vol, infilt, incept, rain, q, u1, storage
    TODO uitbreiden met extra elementen
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
    def infilt(self):
        return self._get_idx(search_str="^infilt_.*")

    @property
    def incept(self):
        return self._get_idx(search_str="^incept_.*")

    @property
    def rain(self):
        return self._get_idx(search_str="^rain_.*")

    @property
    def q(self):
        return self._get_idx(search_str="^discharge_.*")

    @property
    def u1(self):
        return self._get_idx(search_str="^vel_.*")

    @property
    def storage(self):
        return self._get_idx(search_str="^storage_mm_.*")


@dataclass
class NetcdfEssentials:
    """
    Parameters
    ----------
    threedi_result : hrt.ThreediResult
        Path or object for the NetCDF and HDF5 result files.
    waterdeel_path : str, optional
        Path to waterdeel layer. If None, not used for cell selection.
    waterdeel_layer : str, optional
        Layer name for waterdeel if part of a GeoPackage.
    panden_path : str, optional
        Path to panden layer. If None, not used for cell selection.
    panden_layer : str, optional
        Layer name for panden if part of a GeoPackage.
    user_defined_timesteps : list[int], optional
        List of output timesteps (seconds or "max").
    ness_fp : str, optional
        Path to netcdf_essentials.csv file with relevant attributes.
    use_aggregate : bool, default False
        If True, use aggregate NetCDF results.

    Methods
    -------
    from_folder(folder, threedi_result, use_aggregate, **kwargs)
        Initialize from a Folders structure.
    run(...)
        Main method to process NetCDF and export to GeoPackage.
    create_base_gdf()
        Create base GeoDataFrames for grid, nodes, lines, and metadata.
    add_correction_parameters(...)
        Add columns to grid_gdf for waterlevel correction logic.
    correct_waterlevels(...)
        Apply waterlevel correction for selected cells.
    append_data(...)
        Insert timeseries data into GeoDataFrames.
    process_ness(ness)
        Process ness DataFrame to retrieve timeseries and indices.
    get_output_timesteps(user_defined_timesteps)
        Set output timesteps for export.
    load_default_ness(ness_fp)
        Load netcdf_essentials.csv as DataFrame.
    _calculate_layer_area_per_cell(...)
        Calculate area and percentage of a layer per grid cell.
    _attronly_schema(df)
        Helper for saving attributes without geometry.
    """

    threedi_result: hrt.ThreediResult
    waterdeel_path: str = None
    waterdeel_layer: str = None
    panden_path: str = None
    panden_layer: str = None
    user_defined_timesteps: list[int] = (
        None  # TODO ik ga max altijd opslaan bij q en u moet nog iets met abs en een richting
    )
    ness_fp: str = None
    use_aggregate: bool = False  # NOTE dus ik moet als gebruiker aangeven of ik aggregate result gebruik

    # def __post_init__(self):
    # # NOTE wat moet hier?

    @classmethod  # # NOTE deels dubbel met timeseries, kan dat slimmer?
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
    def nc_ts(self):
        nc_ts = netcdf_timeseries.NetcdfTimeSeries(
            threedi_result=self.threedi_result, use_aggregate=self.use_aggregate
        )
        return nc_ts

    @property
    def grid(self):  # NOTE dubbel met timeseries, kan dat slimmer?
        """Instance of threedigrid.admin.gridresultadmin.GridH5ResultAdmin or GridH5AggregateResultAdmin"""
        if self.use_aggregate is False:
            return self.threedi_result.grid
        return self.threedi_result.aggregate_grid

    @property
    def ness(self):
        """Relevant data for HHNK models"""
        return self.ness

    @property
    def output_default(self):
        """Default output if no path is specified."""
        return self.threedi_result.full_path("netcdf_essentials.gpkg")

    def load_default_ness(self, ness_fp=None) -> pd.DataFrame:
        """Load relevant data for HHNK models"""
        logger = hrt.logging.get_logger(__name__)
        if ness_fp is None:
            ness = pd.read_csv(DEFAULT_NESS_FP)
            logger.info(f"Loading default ness from {DEFAULT_NESS_FP}")
        else:
            ness = pd.read_csv(ness_fp)
            logger.info(f"Loading ness from {ness_fp}")

        return ness

    def _create_column_base(self, time_seconds) -> str:
        """Create a base column name with hours and minutes based time_seconds."""
        # Check if time_seconds is a string or int
        if isinstance(time_seconds, str):
            col_base = time_seconds
        else:
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

    def _calculate_layer_area_per_cell(  # TODO move to correct waterlevels
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

    def _set_active_attributes(self, ness) -> pd.DataFrame:
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
        logger = hrt.logging.get_logger(__name__)
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

    def _attronly_schema(self, df) -> dict:
        """
        Needed to save attributes without geometry
        see: https://gis.stackexchange.com/questions/396752
        """

        def remap(dtype):
            correction = {
                "int64": "int",
                "int32": "int",
                "float32": "float",
                "float64": "float",
                "object": "str",
            }
            return correction[dtype] if dtype in correction else dtype

        return {
            "geometry": "None",
            "properties": {
                column: remap(str(dtype)) for column, dtype in zip(df.columns, df.dtypes) if column != "geometry"
            },
        }

    def process_ness(self, ness) -> pd.DataFrame:
        """
        Process ness dataframe to set active attributes and retrieve data.

        Parameters
        ----------
        ness : pd.DataFrame
            DataFrame with attributes and subsets to check against the grid.
            Should contain columns: 'attribute', 'subset', 'element', 'attribute_name'.
        """
        logger = hrt.logging.get_logger(__name__)

        ness = self._set_active_attributes(ness)

        # initialise data column
        ness["amount"] = None
        ness["data"] = None
        # Retrieve timeseries for each row in ness
        for i, row in ness.iterrows():
            if row["active"]:
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
                # Replace -9999 with nan values to prevent -9999 being used in replacing values.
                data[data == -9999.0] = np.nan
                # add data as nested array to dataframe
                ness.at[i, "data"] = data  # noqa: PD008 .loc werkt niet met nested array
                ness.loc[i, "amount"] = data.shape[0]
                logger.info(f"Retrieved {row['attribute']} for {data.shape[0]} {row['element']} in {row['subset']}")

        return ness
        return ness

    def create_base_gdf(self) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
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
        logger = hrt.logging.get_logger(__name__)

        # Create empty GeoDataFrames for grid, node and line
        grid_gdf = gpd.GeoDataFrame()
        node_gdf = gpd.GeoDataFrame()
        line_gdf = gpd.GeoDataFrame()

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

            # Voeg geometry toe aan line gdf
            xys = self.grid.lines.subset("1D_All").line_geometries  # format [x1, x2, ..., y1, y2, ...]

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
            line_gdf["id"] = self.grid.lines.subset("1D_All").id
            line_gdf["channel_id"] = self.grid.lines.subset("1D_All").content_pk
            line_gdf["exchange_level"] = self.grid.lines.subset("1D_All").dpumax
            line_gdf["line_type_kcu"] = self.grid.lines.subset("1D_All").kcu
            line_gdf["start_node"] = self.grid.lines.subset("1D_All").line[0]
            line_gdf["end_node"] = self.grid.lines.subset("1D_All").line[1]  # TODO check start and end node
            line_gdf["zoom_category"] = self.grid.lines.subset("1D_All").zoom_category

            logger.info(f"Created {len(node_gdf)} lines.")

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
        meta_gdf = gpd.GeoDataFrame(meta_dict, index=[0])  # zodat ik hem weg kan schrijven naar geopackage
        meta_gdf["geometry"] = None
        meta_gdf.set_geometry("geometry")
        meta_gdf.set_geometry("geometry", inplace=True)

        logger.info(f"Created metadata for result from model {self.grid.model_name} # {self.grid.revision_nr}")

        # TODO breaches
        return grid_gdf, node_gdf, line_gdf, meta_gdf

    def add_correction_parameters(
        self,
        grid_gdf: gpd.GeoDataFrame,
        replace_dem_below_perc: float = 50,
        replace_water_above_perc: float = 95,
        replace_pand_above_perc: float = 99,
    ) -> gpd.GeoDataFrame:  # TODO move to correct waterlevels
        """Determine which cells should have their waterlevel replaced by their neighbours.

        Parameters
        ----------
        grid_gdf : gpd.GeoDataFrame
            GeoDataFrame with grid cells.
        replace_dem_below_perc : float, optional
            Percentage of minimal DEM area in waterlevel, if less replaced, by default 50.
        replace_water_above_perc : float, optional
            Percentage of water area above which the waterlevel should be replaced, by default 95.
        replace_pand_above_perc : float, optional
            Percentage of panden area above which the waterlevel should be replaced, by default 99.

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
        grid_gdf.loc[grid_gdf["replace_dem"], "replace_all"] = "Dem percentage below threshold"
        grid_gdf.loc[grid_gdf["replace_water"], "replace_all"] = "Water percentage above threshold"
        grid_gdf.loc[grid_gdf["replace_pand"], "replace_all"] = "Pand percentage above threshold"

        # Find neighbour cells and add their id's to a new column
        neighbours = []
        for row in grid_gdf.itertuples():
            # find all indices that touch the cell
            neighbours_ids = grid_gdf[grid_gdf.geometry.touches(row.geometry)].id.tolist()
            # find the id of those indices
            neighbours.append(str(neighbours_ids))
        grid_gdf["neighbour_ids"] = neighbours

        return grid_gdf

    def get_output_timesteps(self, user_defined_timesteps: list[int]) -> list[int]:
        """
        Set output timesteps for export.


        In ieder geval:
        * Opgegeven timesteps door gebruiker, incl. max
        * Timesteps nodig voor tests 0d1d en 1d2d
        * Misschien gewoon alle? Maar dan wel iets uniforms?

        TODO WE ik ben nog zoekende of ik dit wil. Omdat ik de hele tijdserie ophaal kan ik ook
        de index van het max bepalen en de data ophalen voor die index.
        NOTE hoe kom ik aan output timestep uit de settings?
        NOTE misschien optie maken voor alle tijstappen?
        NOTE de ness als input?
        """
        logger = hrt.logging.get_logger(__name__)
        if user_defined_timesteps is None:
            # Add only maximum
            timesteps_seconds_output = ["max"]
            logger.info("No output timesteps provided, using only max")
        else:
            timesteps_seconds_output = user_defined_timesteps  # TODO dichtsbijzijnde zoeken ofzo?
        logger.info(f"Using user defined timesteps: {timesteps_seconds_output}")

        return timesteps_seconds_output

    def append_data(self, ness, gdf, timesteps_seconds_output: list) -> gpd.GeoDataFrame:
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

        col_idx = ColumnIdx(gdf=gdf)
        for i, row in ness.iterrows():
            if row["active"] and row["geom_type"] == gdf.geometry.geom_type.unique()[0]:
                for key in timesteps_seconds_output:
                    # TODO min negatieve max etc
                    if key == "max":  # isinstance(time_seconds, str):
                        gdf.insert(
                            getattr(col_idx, row["attribute_name"]),
                            f"{row['attribute_name']}_{key}",
                            np.nanmax(abs(row["data"]), axis=1),
                        )
                    elif isinstance(key, int):
                        # Make pretty column names
                        col_sub = self._create_column_base(time_seconds=key)
                        # Find index of timestep
                        data_timestep = row["data"][:, self.nc_ts.get_ts_index(time_seconds=key)]
                        gdf.insert(
                            getattr(col_idx, row["attribute_name"]),
                            f"{row['attribute_name']}_{col_sub}",
                            data_timestep,
                        )

        return gdf

    def correct_waterlevels(self, grid_gdf, timesteps_seconds_output: list):  # TODO move to correct waterlevels
        """Correct the waterlevel for the given timesteps. Results are only corrected
        for cells where the 'replace_all' value is not False.
        """
        # Create copy and set_index the id field so we can use the neighbours_ids column easily
        grid_gdf_local = grid_gdf.copy()
        grid_gdf_local.set_index("id", inplace=True)

        for timestep in timesteps_seconds_output:
            base_col = self._create_column_base(time_seconds=timestep)
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
        ness_fp=None,
        output_file=None,
        user_defined_timesteps: list[int] = None,
        replace_dem_below_perc: float = 50,
        replace_water_above_perc: float = 95,
        replace_pand_above_perc: float = 99,
        wlvl_correction: bool = True,
        overwrite: bool = False,
    ):
        """Transform netcdf into a grid gpkg.

        Parameters
        ----------
        ness_fp : str, optional
            Path to netcdf_essentials.csv file with relevant attributes.
            If None, the default file will be used.
            default is: hhnk_threedi_tools.data.netcdf_essentials.csv
        output_file : str or hrt.File, optional
            Path to output file where the grid will be saved as a GeoPackage.
            When None is passed the output will be placed in the same directory as the netcdf.
            default name is: grid_wlvl.gpkg
        user_defined_timesteps : list[int], optional
            List of output timesteps in seconds or "max" to include maximum waterlevel over calculation.
            If None, only uses max.
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
        output_file = hrt.SpatialDatabase(output_file)

        # initialize logger
        logger = hrt.logging.get_logger(
            __name__, filepath=Path(output_file.path).parent.absolute() / "NetCDFEssentials.log"
        )
        logger.setLevel(logging.INFO)
        logger.info(f"Starting NetcdfEssentials with result: {self.threedi_result}")

        create = hrt.check_create_new_file(output_file=output_file, overwrite=overwrite)
        if create:
            timesteps_seconds_output = self.get_output_timesteps(user_defined_timesteps)

            ness = self.load_default_ness(ness_fp=ness_fp)
            ness = self.process_ness(ness=ness)

            grid_gdf, node_gdf, line_gdf, meta_gdf = self.create_base_gdf()

            grid_gdf = self.append_data(ness=ness, gdf=grid_gdf, timesteps_seconds_output=timesteps_seconds_output)
            node_gdf = self.append_data(ness=ness, gdf=node_gdf, timesteps_seconds_output=timesteps_seconds_output)
            line_gdf = self.append_data(ness=ness, gdf=line_gdf, timesteps_seconds_output=timesteps_seconds_output)

            if wlvl_correction:
                logger.info("Correcting 2D waterlevels")
                grid_gdf = self.add_correction_parameters(
                    grid_gdf=grid_gdf,
                    replace_dem_below_perc=replace_dem_below_perc,
                    replace_water_above_perc=replace_water_above_perc,
                    replace_pand_above_perc=replace_pand_above_perc,
                )
                grid_gdf = self.correct_waterlevels(
                    grid_gdf=grid_gdf, timesteps_seconds_output=timesteps_seconds_output
                )

            # Save to file
            grid_gdf.to_file(output_file.path, layer="grid_2d", engine="pyogrio", overwrite=overwrite)
            node_gdf.to_file(output_file.path, layer="node_1d", engine="pyogrio", overwrite=overwrite)
            line_gdf.to_file(output_file.path, layer="line_1d", engine="pyogrio", overwrite=overwrite)
            meta_gdf.to_file(
                output_file.path,
                engine="fiona",
                layer="metadata",
                driver="GPKG",
                schema=self._attronly_schema(meta_gdf),  # not supported by pyogrio
                overwrite=overwrite,
                crs=f"EPSG:{self.grid.epsg_code}",
            )
            logger.info(f"Saved NetCDF essentials to {output_file.path}")
        else:
            logger.warning(f"Output file {output_file.path} already exists. Set overwrite to True to overwrite.")


# %% TODO test model zonder maaiveld

# %% TODO test model met bressen
