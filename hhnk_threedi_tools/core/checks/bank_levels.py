# %%
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
from hhnk_research_tools.folder_file_classes.spatial_database_class import SpatialDatabase
from threedigrid_builder import make_gridadmin

from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.core.schematisation.relations import ChannelRelations

# Globals
NEW_STORAGE_AREA_COL = "new_storage_area"

FLOW_1D2D_FLOWLINES_NAME = "stroming_1d2d_flowlines"
FLOW_1D2D_CROSS_SECTIONS_NAME = "stroming_1d2d_cross_sections"
FLOW_1D2D_CHANNELS_NAME = "stroming_1d2d_watergangen"
FLOW_1D2D_MANHOLES_NAME = "stroming_1d2d_putten"


class BankLevelCheck:
    """Performs bank level validation and adjustment checks for 3Di model components.

    This class analyzes and validates bank levels in relation to water levels, obstacles,
    and fixed drainage areas. It identifies potential leakage points and generates
    corrections for:
    - Cross-section bank levels
    - Manhole configurations

    The check includes:
    1. Detection of 1D2D flow line intersections with obstacles or FDLA boundaries
    2. Identification of nodes with divergent water levels
    3. Generation of corrective manholes
    4. Adjustment of cross-section bank levels

    Attributes
    ----------
        folder (Folders): Directory structure containing model and result paths
        database (SpatialDatabase): Spatial database containing the model schematisation
        dem: Digital Elevation Model raster
        lines_1d2d (GeoDataFrame): 1D2D connection lines from the calculation grid
        gridnodes (GeoDataFrame): Grid nodes from the calculation grid
        channel_gdf (GeoDataFrame): Channel data with relations
        cross_section_gdf (GeoDataFrame): Cross-section locations with properties
        connection_node_gdf (GeoDataFrame): Connection nodes with properties
        obstacle_gdf (GeoDataFrame): Obstacles (e.g., levees) in the model
        fixeddrainage_gdf (GeoDataFrame): Fixed drainage level areas
        fixeddrainage_boundary_gdf (GeoDataFrame): Boundaries of fixed drainage areas

    Note:
        The check helps prevent unintended water flows across obstacles and between
        areas with different water level regimes by adjusting bank levels and adding
        control structures where needed.
    """

    def __init__(self, folder: Folders):
        self.folder = folder
        self.database: SpatialDatabase = folder.model.schema_base.database
        self.dem = self.folder.model.schema_base.rasters.dem

    @property
    def results(self) -> dict:
        return {
            "line_intersects": self.available("line_intersects"),
            "diverging_wl_nodes": self.available("diverging_wl_nodes"),
            "manholes_info": self.available("manholes_info"),
            "new_manholes_df": self.available("new_manholes_df"),
            "all_1d2d_flowlines": self.available("lines_1d2d"),
            "cross_loc_new": self.available("cross_loc_new"),
            "new_channels": self.available("new_channels"),
        }

    def available(self, variable: str):
        if hasattr(self, variable):
            return getattr(self, variable)
        else:
            return None

    def prepare_data(self):
        """
        Gathers all information from the model and datachecker that's needed
        to check the manholes and bank levels.
        """
        # Use makegrid to gather lines and models as used in model
        grid = make_gridadmin(self.database.base, self.dem.base)
        gridlines = hrt.df_convert_to_gdf(pd.DataFrame(grid["lines"]), geom_col_type="wkb", src_crs="28992")
        self.lines_1d2d = gridlines[gridlines["kcu"].str.contains("1d2d", case=False, na=False)][
            ["id", "node_1", "node_2", "dpumax", "geometry"]
        ]
        self.gridnodes = hrt.df_convert_to_gdf(pd.DataFrame(grid["nodes"]), geom_col_type="wkb", src_crs="28992")

        # Load relevant layers from model
        channel_rel = ChannelRelations(folder=self.folder)
        self.channel_gdf = channel_rel.gdf
        self.cross_section_gdf = self.database.load(layer="cross_section_location", index_column="id")
        self.cross_section_gdf["cross_loc_id"] = self.cross_section_gdf.index
        self.connection_node_gdf = self.database.load(layer="connection_node", index_column="id")
        self.connection_node_gdf["conn_node_id"] = self.connection_node_gdf.index
        self.obstacle_gdf = self.database.load(layer="obstacle", index_column="id")
        self.obstacle_gdf["obstacle_id"] = self.obstacle_gdf.index

        # Load fixed drainage level areas from datachecker output
        self.fixeddrainage_gdf = self.folder.source_data.datachecker.layers.fixeddrainagelevelarea.load()[
            ["peil_id", "code", "streefpeil_bwn2", "geometry"]
        ]
        self.fixeddrainage_gdf["id"] = self.fixeddrainage_gdf.index

        # Convert drainage areas to boundary geometry
        self.fixeddrainage_boundary_gdf = self.fixeddrainage_gdf.explode(index_parts=True).copy()
        self.fixeddrainage_boundary_gdf["geometry"] = self.fixeddrainage_boundary_gdf.boundary

    def line_intersections(self) -> gpd.GeoDataFrame:
        """
        Create an intersect between 1d2d connections, fixeddrainage borders
        and obstacles. returns a geodataframe self.line_intersects
        """

        # Find intersection of 1d2d calculation grid lines with obstacles.
        obstacle_intersects = gpd.sjoin(self.lines_1d2d, self.obstacle_gdf, lsuffix="1d2d", rsuffix="obstacle")
        obstacle_intersects["intersect_type"] = "1d2d_crosses_obstacle"

        # remove duplicate 1d2d lines, created because multiple obstacles are crossed. Only the highest levee height is taken.
        obstacle_intersects = (
            obstacle_intersects.sort_values("crest_level", ascending=False).drop_duplicates("id_1d2d").sort_index()
        )

        # Do the same for intersections with fixeddrainagelevelarea boundaries
        fixeddrainage_intersects = gpd.sjoin(
            self.lines_1d2d, self.fixeddrainage_boundary_gdf, lsuffix="1d2d", rsuffix="fdlab"
        )
        fixeddrainage_intersects["intersect_type"] = "1d2d_crosses_fixeddrainage"
        fixeddrainage_intersects = fixeddrainage_intersects.drop_duplicates("id_1d2d").sort_index()

        # Combine intersections with obstacles and fixeddrainagelevelarea
        self.line_intersects = pd.concat(
            [obstacle_intersects, fixeddrainage_intersects],
            ignore_index=False,
            sort=False,
        )
        # Drop duplicate node id's, keeping highest levee. Levee intersection takes precedence over fixeddrainage intersection
        self.line_intersects = (
            self.line_intersects.sort_values(["crest_level", "intersect_type", "streefpeil_bwn2"], ascending=False)
            .drop_duplicates("id_1d2d")
            .sort_index()
        )

        # Select intersections that will require a manhole or modified bank level
        self.line_intersects["leak"] = (self.line_intersects["dpumax"] < self.line_intersects["crest_level"]) | (
            self.line_intersects["dpumax"] < self.line_intersects["streefpeil_bwn2"]
        )

        # Add information from nodes
        self.line_intersects = self.line_intersects.merge(
            self.gridnodes.drop(columns="geometry", axis=1), how="left", left_on="node_2", right_on="id"
        )

        return self.line_intersects

    def divergent_waterlevel_nodes(self) -> gpd.GeoDataFrame:
        """
        Create list of connection nodes that do not have the same initial water level as most nodes in that area.
        These connection nodes are made isolated to avoid leaking over boundaries.
        returns a gpd.GeoDataFrame with divergent connection nodes.
        """
        # For all connection nodes, see in which area they are
        # nodes_in_drainage_area
        nodes_with_fixeddrainage_id = gpd.sjoin(
            self.connection_node_gdf, self.fixeddrainage_gdf, lsuffix="conn", rsuffix="fdla"
        )

        # initialize new dataframe
        self.diverging_wl_nodes = gpd.GeoDataFrame()

        # Loop over all nodes, per unique drainage area. Find the mode, and add all connection nodes that do not have an
        # initial waterlevel equal to the mode.
        for p_id in nodes_with_fixeddrainage_id["peil_id"].unique():
            nodes_in_same_area = nodes_with_fixeddrainage_id[nodes_with_fixeddrainage_id["peil_id"] == p_id]

            # Find the most occuring value of initial waterlevel, this is considered the initial
            # waterlevel in the area specified by p_id
            init_waterlevel_mode = nodes_in_same_area["initial_water_level"].mode()[0]

            # Find which nodes have a different waterlevel than the initial waterlevel
            self.diverging_wl_nodes = pd.concat(
                [
                    self.diverging_wl_nodes,
                    nodes_in_same_area[nodes_in_same_area["initial_water_level"] != init_waterlevel_mode],
                ],
                ignore_index=True,
            )

        # Clean up dataframe and add columns so it can be used in the sql creation for manholes on these nodes.
        self.diverging_wl_nodes = self.diverging_wl_nodes[
            ["conn_node_id", "initial_water_level", "storage_area", "geometry"]
        ]
        self.diverging_wl_nodes["drain_level"] = np.nan
        self.diverging_wl_nodes["code"] = self.diverging_wl_nodes["conn_node_id"].apply(
            lambda x: "conn_node_id_" + str(x)
        )
        self.diverging_wl_nodes["type"] = "node_in_wrong_fixeddrainage_area"

        # add gridnode informtation
        self.diverging_wl_nodes = self.diverging_wl_nodes.merge(
            self.gridnodes.drop(columns="geometry", axis=1), how="left", left_on="conn_node_id", right_on="content_pk"
        )

        return self.diverging_wl_nodes

    def get_new_manholes(self) -> gpd.GeoDataFrame:
        """
        Create dataframe of new manholes to be added to model or to update existing to fix problems
        of wrong waterlevel (wrong waterlevel will likely lead to leak) or leak across obstacle
        """
        # List nodes that require manhole (modification) for being in wrong pgb (divergent) and put in new manhole format
        manhole_divergent = self.diverging_wl_nodes
        manhole_divergent["id"] = self.diverging_wl_nodes["conn_node_id"]
        manhole_divergent["manhole_surface_level"] = 10
        manhole_divergent["bottom_level"] = -10
        manhole_divergent["exchange_type"] = 1
        manhole_divergent["exchange_level"] = 99
        manhole_divergent["tags"] = "node wrong initial water level"
        manhole_divergent["storage_area"] = self.diverging_wl_nodes["storage_area_x"]

        self.new_manholes_df = manhole_divergent[
            [
                "id",
                "manhole_surface_level",
                "initial_water_level",
                "bottom_level",
                "exchange_type",
                "exchange_level",
                "storage_area",
                "tags",
            ]
        ]

        # Add nodes from leaking 1d2d obstacle instersections
        manhole_intersects = self.line_intersects[self.line_intersects["content_type"] == "TYPE_V2_CONNECTION_NODES"]
        # Remove nodes already in divergent list
        manhole_intersects = manhole_intersects[
            ~manhole_intersects["content_pk"].isin(self.diverging_wl_nodes["conn_node_id"])
        ]

        manhole_intersects["id"] = manhole_intersects["content_pk"]
        manhole_intersects.rename(columns={"initial_waterlevel": "initial_water_level"}, inplace=True)
        manhole_intersects["manhole_surface_level"] = 10
        manhole_intersects["bottom_level"] = -10
        manhole_intersects["exchange_type"] = 2
        manhole_intersects["exchange_level"] = manhole_intersects["crest_level"]
        manhole_intersects["tags"] = "leak across obstacle from node"
        manhole_intersects["storage_area"] = manhole_intersects[["storage_area", "manhole_surface_level"]].max(axis=1)

        # Combine both lists
        self.new_manholes_df = pd.concat(
            [
                self.new_manholes_df,
                manhole_intersects[
                    [
                        "id",
                        "manhole_surface_level",
                        "initial_water_level",
                        "bottom_level",
                        "exchange_type",
                        "exchange_level",
                        "storage_area",
                        "tags",
                    ]
                ],
            ],
            ignore_index=True,
        )

        return self.new_manholes_df

    def generate_cross_section_locations(self) -> gpd.GeoDataFrame:
        """Generate cross section locations that need new bank levels"""
        # Join initial water levels on cross section locations
        self.cross_loc_new = self.cross_section_gdf.merge(
            self.channel_gdf[["channel_id", "initial_water_level_start", "initial_water_level_end"]],
            how="left",
            on="channel_id",
        )
        # Add maximum initial water level at start and end in new column
        self.cross_loc_new["initial_water_level"] = self.cross_loc_new[
            ["initial_water_level_start", "initial_water_level_end"]
        ].max(axis=1)
        self.cross_loc_new.drop(columns=["initial_water_level_start", "initial_water_level_end"], inplace=True)

        # new bank levels (lowest possible)
        self.cross_loc_new["new_bank_level"] = round(
            self.cross_loc_new[["initial_water_level", "reference_level"]].max(axis=1, skipna=True) + 0.1, 2
        )
        self.cross_loc_new["tags"] = "bank_level reset to lowest possible + 10 cm"

        # Select 1d2d intersections that originate from channel-obstacle intersections
        intersects_on_channel = self.line_intersects[self.line_intersects["content_type"] == "TYPE_V2_CHANNEL"]
        # NOTE banklevels are lowered above, so take all intersections and set the bank level to crest level for these

        # Filter cross sections that need modified bank level to prevent leaks
        crs_raise = self.cross_loc_new.merge(
            intersects_on_channel[["content_pk", "leak", "intersect_type", "crest_level", "streefpeil_bwn2"]],
            how="inner",
            left_on="channel_id",
            right_on="content_pk",
        )

        for index, row in self.cross_loc_new.iterrows():
            # Find matching crs to raise
            crs_intersect = crs_raise[crs_raise["channel_id"] == row["channel_id"]]
            if not crs_intersect.empty:
                # Get maximum of all values available
                new_bank_level = crs_raise[["crest_level", "streefpeil_bwn2"]].max(axis=None)
                self.cross_loc_new.loc[index, "new_bank_level"] = new_bank_level
                self.cross_loc_new.loc[index, "tags"] = (
                    self.cross_loc_new.loc[index, "tags"] + ";" + "bank level raised to obstacle of fdla level"
                )

        # Add column to show difference between old and new bank level
        self.cross_loc_new["bank_level_diff"] = self.cross_loc_new["new_bank_level"] - self.cross_loc_new["bank_level"]

        return self.cross_loc_new

    def generate_channels(self) -> gpd.GeoDataFrame:
        """Create overview of channels, 1d2d lines and cross section location to see differente bank levels"""
        crs_per_channel = (
            self.cross_loc_new.drop_duplicates("channel_id")
            .sort_values("new_bank_level", ascending=True)[["channel_id", "new_bank_level", "bank_level_diff", "tags"]]
            .reset_index(drop=True)
        )
        # join cross locations on channels so we have a bank level per channel
        self.all_channels = self.channel_gdf.reset_index(drop=True).merge(
            crs_per_channel,
            left_on="channel_id",
            right_on="channel_id",
        )
        return self.all_channels

    def run(self):
        # Check grid for leaks
        self.line_intersections()
        self.divergent_waterlevel_nodes()
        # manholes
        self.get_new_manholes()
        # generate locations
        self.generate_cross_section_locations()
        # generate channel overview
        self.generate_channels()

    def write(self, csv_path: Path, gpkg_path: Path):
        """Write selected ouput used in QGIS plugin GUI"""

        def write_csv_gpkg(gdf: gpd.GeoDataFrame, filename: str, csv_path: Path, gpkg_path: Path):
            """Write a csv and geopackage, name is the name without extension"""
            if not gdf.empty:
                hrt.gdf_write_to_csv(gdf, csv_path, filename)
                hrt.gdf_write_to_geopackage(gdf, gpkg_path, filename)

        write_csv_gpkg(self.results["all_1d2d_flowlines"], FLOW_1D2D_FLOWLINES_NAME, csv_path, gpkg_path)
        write_csv_gpkg(self.results["cross_loc_new"], FLOW_1D2D_CROSS_SECTIONS_NAME, csv_path, gpkg_path)
        write_csv_gpkg(self.results["new_channels"], FLOW_1D2D_CHANNELS_NAME, csv_path, gpkg_path)

        hrt.gdf_write_to_csv(gdf=self.results["new_manholes_df"], filename=FLOW_1D2D_MANHOLES_NAME, path=csv_path)


if __name__ == "__main__":
    from tests.config import FOLDER_TEST

    folder = Folders(FOLDER_TEST)
    self = BankLevelCheck(folder)
    self.prepare_data()
    self.line_intersections()
    self.divergent_waterlevel_nodes()
    # manholes
    self.get_new_manholes()
    self.generate_cross_section_locations()
    self.folder = folder
    self.database = folder.model.schema_base.database
    self.dem = self.folder.model.schema_base.rasters.dem
# %%
