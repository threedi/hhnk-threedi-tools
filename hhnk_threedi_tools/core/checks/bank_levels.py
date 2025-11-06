# %%
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 19 09:04:52 2021

@author: chris.kerklaan

Bank level testing made into an object to have more overview

"""

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
from hhnk_research_tools.folder_file_classes.spatial_database_class import SpatialDatabase
from threedigrid_builder import make_gridadmin

# from hhnk_research_tools.threedi.geometry_functions import extract_boundary_from_polygon
# from hhnk_research_tools.threedi.grid import Grid
# Local imports
from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.core.schematisation.relations import ChannelRelations
from hhnk_threedi_tools.variables.bank_levels import (
    bank_level_diff_col,
    new_bank_level_col,
    new_bank_level_source_col,
)
from hhnk_threedi_tools.variables.database_aliases import (
    a_chan_id,
)

# Globals
NEW_STORAGE_AREA_COL = "new_storage_area"

FLOW_1D2D_FLOWLINES_NAME = "stroming_1d2d_flowlines"
FLOW_1D2D_CROSS_SECTIONS_NAME = "stroming_1d2d_cross_sections"
FLOW_1D2D_CHANNELS_NAME = "stroming_1d2d_watergangen"
FLOW_1D2D_MANHOLES_NAME = "stroming_1d2d_putten"

# %%


# %%
class BankLevelCheck:
    """An object that reads and runs bank level checks"""

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
            "all_1d2d_flowlines": self.available("all_1d2d_flowlines"),  # TODO deze heb ik weggehaald, waarvoor nodig?
            "cross_loc_new_filtered": self.available("cross_loc_new_filtered"),
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
        self.cross_section_gdf["crs_id"] = self.cross_section_gdf.index
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
        obstacle_intersects["type"] = "1d2d_crosses_obstacle"

        # remove duplicate 1d2d lines, created because multiple obstacles are crossed. Only the highest levee height is taken.
        obstacle_intersects = (
            obstacle_intersects.sort_values("crest_level", ascending=False).drop_duplicates("id_1d2d").sort_index()
        )

        # Do the same for intersections with fixeddrainagelevelarea boundaries
        fixeddrainage_intersects = gpd.sjoin(
            self.lines_1d2d, self.fixeddrainage_boundary_gdf, lsuffix="1d2d", rsuffix="fdlab"
        )
        fixeddrainage_intersects["type"] = "1d2d_crosses_fixeddrainage"
        fixeddrainage_intersects = fixeddrainage_intersects.drop_duplicates("id_1d2d").sort_index()

        # Combine intersections with obstacles and fixeddrainagelevelarea
        self.line_intersects = pd.concat(
            [obstacle_intersects, fixeddrainage_intersects],
            ignore_index=False,
            sort=False,
        )
        # Drop duplicate node id's, keeping highest levee. Levee intersection takes precedence over fixeddrainage intersection
        self.line_intersects = (
            self.line_intersects.sort_values(["crest_level", "type", "streefpeil_bwn2"], ascending=False)
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

        # TODO WvE de fouten die hier uit komen (leaks bij crest level) moeten naar N&S denk ik.

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

    def generate_cross_section_locations(self, write=False):
        """Generate cross section locations that need new bank levels"""
        # Add nodes from leaking 1d2d obstacle instersections
        intersects_on_channel = self.line_intersects[self.line_intersects["content_type"] == "TYPE_V2_CHANNEL"]

        # Join initial water levels on cross section locations
        crs_data_all = self.cross_section_gdf.merge(
            self.channel_gdf[["channel_id", "initial_water_level_start", "initial_water_level_end"]],
            how="left",
            on="channel_id",
        )
        # Add maximum initial water level at start and end in new column
        crs_data_all["initial_water_level"] = crs_data_all[
            ["initial_water_level_start", "initial_water_level_end"]
        ].max(axis=1)
        # Filter cross sections that need modified bank level to prevent leaks
        crs_leak = crs_data_all.merge(
            intersects_on_channel[["content_pk", "crest_level"]],
            how="inner",
            left_on="channel_id",
            right_on="content_pk",
        )
        crs_data_all[crs_data_all["channel_id"].isin(intersects_on_channel["content_pk"])]

        # update bank levels (lower where possible, raise where needed to prevent leaks)
        crs_data_all["temp_ref_level_added"] = crs_data_all["reference_level"] + 0.1
        crs_data_all["bank_level"] = crs_data_all[["bank_level", "temp_ref_level_added"]].max(axis=1)
        # fill in crest level where higher than current bank level # TODO niet alleen waar hij nu lekt gebruiken als leak, maar alle intersects om opnieuw goed in te stellen
        # crs_data_all["bank_level"] =

        def new_cross_loc_bank_levels(intersect_1d2d_all, channel_line_geo, cross_loc):
            try:
                """2. if the 1d2d line originates from an added calculation node -> find the channel this node is on and keep the
                bank levels for this channel equal to the maximum levee height.

                Function calculates new bank levels. Initially, we set them to either initial water level or
                reference level, depending on which is higher. For cross section locations that cross over obstacles,
                we set the bank level to levee height.

                """

                # filter nodes that need to have channels with bank levels equal to levee height
                nodes_on_channel = intersect_1d2d_all[intersect_1d2d_all["node_type"] == "added_calculation"].copy()
                # nodes_on_channel = nodes_on_channel.rename(columns={'node_geometry': 'geometry'})

                nodes_on_channel.drop(["initial_waterlevel"], axis=1, inplace=True)
                # nodes_on_channel = nodes_on_channel.rename(columns={'node_geometry': 'geometry'})

                # Buffer point to find intersections with the channels (buffering returns point within given distance of geometry)
                nodes_on_channel["geometry"] = nodes_on_channel.buffer(0.1)

                # join channels on these nodes (meaning added calculation nodes) to get the channels that need higher bank levels.
                if not nodes_on_channel.empty:
                    channels_bank_level = gpd.sjoin(nodes_on_channel, channel_line_geo).drop(["index_right"], axis=1)
                else:
                    # Create emtpy df with same columns as the sjoin when there are no nodes_on_channels
                    channels_bank_level = nodes_on_channel.reindex(
                        columns=set(nodes_on_channel.columns.tolist() + channel_line_geo.columns.tolist())
                    )

                # sort so duplicate channel id's are removed, crossings with obstacles take priority over crossings
                # with peilgrenzen (fixeddrainage)
                channels_bank_level.sort_values(by=["channel_id", "type"], ascending=[True, False], inplace=True)
                channels_bank_level.drop_duplicates("channel_id", inplace=True)
                channels_bank_level.set_index(["channel_id"], inplace=True, drop=True)

                # join cross_section_location on these channels
                # get cross section locations where corresponding channel id matches channel id's that need
                # higher bank levels (aka channels that intersect with added calculation nodes)
                cross_loc_levee = cross_loc[cross_loc["channel_id"].isin(channels_bank_level.index.tolist())]

                # Add initial water levels and levee heights to the previously obtained info about channels
                # that need higher bank levels
                cross_loc_levee = cross_loc_levee.join(
                    channels_bank_level[["levee_height", "initial_waterlevel"]], on="channel_id"
                )

                # If a row doesn't have a levee height, the 1d2d line crosses with a fixeddrainagelevelarea (peilgrens).
                cross_loc_fixeddrainage = cross_loc_levee[cross_loc_levee["levee_height"].isna()]

                # If there is a levee height, the 1d2d line crosses with a levee
                cross_loc_levee = cross_loc_levee[cross_loc_levee["levee_height"].notna()]

                # Find initial waterlevels for cross section locations by matching them to corresponding id of channels
                cross_loc_new_all = cross_loc.join(channel_line_geo[["initial_waterlevel"]], on="channel_id")

                # All bank levels are set to initial waterlevel +10cm
                cross_loc_new_all["new_bank_level"] = np.round(
                    cross_loc_new_all["initial_waterlevel"] + 0.1, 3
                ).astype(float)
                cross_loc_new_all["bank_level_source"] = "initial+10cm"

                # We start by setting the bank level of all cross location to either initial waterlevel or reference level
                # If the reference level is higher than the initial waterlevel,
                # use this for the banks. (dry bedding in e.g. wieringermeer)
                ref_higher_than_init = cross_loc_new_all["reference_level"] > cross_loc_new_all["initial_waterlevel"]
                cross_loc_new_all.loc[ref_higher_than_init, "new_bank_level"] = np.round(
                    cross_loc_new_all["reference_level"] + 0.1, 3
                ).astype(float)
                cross_loc_new_all.loc[ref_higher_than_init, "bank_level_source"] = "reference+10cm"

                # The cross locations that need levee height are set here
                cross_loc_new_all.loc[cross_loc_levee.index, "new_bank_level"] = cross_loc_levee[
                    "levee_height"
                ].astype(float)
                cross_loc_new_all.loc[cross_loc_levee.index, "bank_level_source"] = "levee_height"

                # Cross locations that are associated with peilgrenzen get a special label for recognition (values are already set)
                cross_loc_new_all.loc[
                    (cross_loc_new_all.index.isin(cross_loc_fixeddrainage.index)),
                    "bank_level_source",
                ] = cross_loc_new_all["bank_level_source"] + "_fixeddrainage"

                # Calculate difference between new and old bank level
                cross_loc_new_all["bank_level_diff"] = np.round(
                    cross_loc_new_all["new_bank_level"] - cross_loc_new_all["bank_level"], 2
                )

                # reorder columns
                cross_loc_new_all_filtered = cross_loc_new_all[
                    [
                        "cross_loc_id",
                        "channel_id",
                        "reference_level",
                        "initial_waterlevel",
                        "bank_level",
                        "new_bank_level",
                        "bank_level_diff",
                        "bank_level_source",
                        "geometry",
                    ]
                ]

                cross_loc_new_all_filtered.reset_index(drop=True, inplace=True)

                # Filter the results only on cross section locations where a new bank level is proposed.
                # If the new banklevel is a NaN value, remove it from the list as this implicates that the cross section
                # is on a channel with connection nodes that do not have an initial water level
                cross_loc_new = cross_loc_new_all_filtered.loc[
                    (cross_loc_new_all_filtered["bank_level_diff"] != 0)
                    & (cross_loc_new_all_filtered["bank_level"].notna())
                ]
                return cross_loc_new_all_filtered, cross_loc_new
            except Exception as e:
                raise e from None

        self.cross_loc_new_filtered, self.cross_loc_new = new_cross_loc_bank_levels(
            self.line_intersects, self.imports["channels"], self.imports["cross_loc"]
        )
        if write:
            self.cross_loc_new_filtered.to_file("cross_loc_new_filtered.gpkg", driver="GPKG")

    def generate_channels(self, write=False):
        self.new_channels = get_updated_channels(self.imports["channels"], self.cross_loc_new_filtered)

        if write:
            self.new_channels.to_file("new_channels.gpkg", driver="GPKG")

    def run(self):
        # Check grid for leaks
        self.line_intersections()
        self.divergent_waterlevel_nodes()
        # manholes
        self.get_new_manholes()

        # generate locations and update channels
        self.generate_cross_section_locations()
        self.generate_channels()

    def write_csv_gpkg(self, result, filename, csv_path, gpkg_path):
        """writes a csv and geopackage, name is the name wo extension"""
        if not result.empty:
            hrt.gdf_write_to_csv(result, csv_path, filename)
            hrt.gdf_write_to_geopackage(result, gpkg_path, filename)

    def write(self, csv_path, gpkg_path):
        self.write_csv_gpkg(
            self.results["all_1d2d_flowlines"],
            FLOW_1D2D_FLOWLINES_NAME,
            csv_path,
            gpkg_path,
        )

        self.write_csv_gpkg(
            self.results["cross_loc_new"],
            FLOW_1D2D_CROSS_SECTIONS_NAME,
            csv_path,
            gpkg_path,
        )

        self.write_csv_gpkg(self.results["new_channels"], FLOW_1D2D_CHANNELS_NAME, csv_path, gpkg_path)

        hrt.gdf_write_to_csv(gdf=self.results["new_manholes_df"], filename=FLOW_1D2D_MANHOLES_NAME, path=csv_path)

    def write_output(self, name):
        """writes to output folder"""
        new_folder = self.folder.output.bank_levels.full_path(name)
        new_folder.path.mkdir(parents=True, exist_ok=True)
        self.write(str(new_folder), str(new_folder))


def get_updated_channels(channel_line_geo, cross_loc_new_all):
    """With the new (and old) bank levels at cross_section_locations we make a new overview of the channels here.
    In qgis this can be plotted to show how the channels interact with 1d2d (considering bank heights)"""
    cross_locs = cross_loc_new_all.drop_duplicates(a_chan_id)[
        [a_chan_id, new_bank_level_col, bank_level_diff_col, new_bank_level_source_col]
    ].reset_index(drop=True)
    # join cross locations on channels so we have a bank level per channel
    all_channels = pd.merge(
        channel_line_geo.reset_index(drop=True),
        cross_locs,
        left_on=a_chan_id,
        right_on=a_chan_id,
    )
    return all_channels


if __name__ == "__main__":
    from tests.config import FOLDER_TEST

    folder = Folders(FOLDER_TEST)
    self = BankLevelCheck(folder)
    self.prepare_data()
    self.line_intersections()
    self.divergent_waterlevel_nodes()
    # manholes
    self.get_new_manholes()
    self.folder = folder
    self.database = folder.model.schema_base.database
    self.dem = self.folder.model.schema_base.rasters.dem
# %%
