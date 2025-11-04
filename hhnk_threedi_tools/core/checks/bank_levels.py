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
from hhnk_threedi_tools.variables.bank_levels import (
    already_manhole_col,
    bank_level_diff_col,
    connection_val,
    levee_height_col,
    levee_id_col,
    new_bank_level_col,
    new_bank_level_source_col,
    node_geometry_col,
    node_id_col,
    node_type_col,
    one_d_node_id_col,
    one_d_two_d_crosses_fixed,
    storage_area_col,
)
from hhnk_threedi_tools.variables.database_aliases import (
    a_chan_id,
    a_man_conn_id,
    a_man_id,
    df_geo_col,
)
from hhnk_threedi_tools.variables.database_variables import (
    bottom_lvl_col,
    calculation_type_col,
    conn_node_id_col,
    display_name_col,
    manhole_indicator_col,
    shape_col,
    surface_lvl_col,
    width_col,
    zoom_cat_col,
)
from hhnk_threedi_tools.variables.default_variables import DEF_TRGT_CRS

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
            "all_1d2d_flowlines": self.available("all_1d2d_flowlines"),
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
        self.channel_gdf = self.database.load(layer="channel", index_column="id")
        self.channel_gdf["conn_node_id"] = self.channel_gdf.index
        self.cross_section_gdf = self.database.load(layer="cross_section_location", index_column="id")
        self.cross_section_gdf["conn_node_id"] = self.cross_section_gdf.index
        self.connection_node_gdf = self.database.load(layer="connection_node", index_column="id")
        self.connection_node_gdf["conn_node_id"] = self.connection_node_gdf.index
        self.obstacle_gdf = self.database.load(layer="obstacle", index_column="id")

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
        self.intersections = pd.concat(
            [obstacle_intersects, fixeddrainage_intersects],
            ignore_index=False,
            sort=False,
        )
        # Drop duplicate node id's, keeping highest levee. Levee intersection takes precedence over fixeddrainage intersection
        self.intersections = (
            self.intersections.sort_values(["crest_level", "type", "streefpeil_bwn2"], ascending=False)
            .drop_duplicates("id_1d2d")
            .sort_index()
        )

        # Select intersections that will require a manhole or modified bank level
        self.intersections_leak = self.intersections[
            (
                (self.intersections["dpumax"] < self.intersections["crest_level"])
                | (self.intersections["dpumax"] < self.intersections["streefpeil_bwn2"])
            )
        ]

        # TODO WvE nog stap naar gridnodes maken. Verderop lijkt hij dit te verwachten, maar ik heb het niet weggehaald...
        # TODO WvE de fouten die hier uit komen (crest level) moeten naar N&S denk ik.

        return self.intersections

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

        return self.diverging_wl_nodes

    def manhole_information(self, write=False):
        """
        Join the divergent nodes with 1d connection nodes which are not
        manholes.
        returns a dataframe self.manhole_info
        """

        def get_manhole_information(
            intersections: gpd.GeoDataFrame,
            diverging_wl_nodes: gpd.GeoDataFrame,
            manholes: gpd.GeoDataFrame,
        ):
            """
            Use the manhole table from the sqlite and the 1d2d flowlines that originate from a connection node.
            If the connection node is not already a manhole, they are added to the list. This function generates the
            dataframe from which the sql code can be made
            """

            def conn_nodes_without_manholes(intersections: gpd.GeoDataFrame, manholes: gpd.GeoDataFrame):
                try:
                    conn_nodes = intersections[intersections[node_type_col] == connection_val]

                    # Make a list of all connection node id's that are not yet a manhole, then select columns to keep
                    conn_nodes = conn_nodes[~conn_nodes[node_id_col].isin(manholes[a_man_conn_id])][
                        [
                            node_id_col,
                            "initial_water_level",
                            storage_area_col,
                            levee_height_col,
                            "type",
                            node_geometry_col,
                        ]
                    ]
                    conn_nodes.rename(
                        columns={levee_height_col: "drain_level", node_geometry_col: df_geo_col},
                        inplace=True,
                    )
                    conn_nodes[already_manhole_col] = 0
                    # The manholes that have a levee height joined will have to get the levee height as drain level.
                    # If this is not known, the nodes will be made isolated. This is done in another script.
                    conn_nodes["code"] = conn_nodes[node_id_col].apply(lambda x: f"{node_id_col}_" + str(x))
                    return conn_nodes
                except Exception as e:
                    raise e from None

            try:
                node_ids_without_manholes = conn_nodes_without_manholes(intersections, manholes)
                # Combine the three lists: manholes, connection nodes with 1d2d flowline and connection nodes in wrong area
                # Manholes from model
                all_manholes = manholes.copy()
                all_manholes[already_manhole_col] = True
                # default manhole type, if manhole is added through other procedure,
                # this script doesnt know why it was added
                all_manholes["type"] = "unknown"

                # Update current manholes with the type of manhole from intersections.
                all_manholes.set_index(a_man_conn_id, drop=False, inplace=True)

                # FIXME added calc nodes do not have a connection node id. We filter them here to get rid of
                # ValueError: cannot reindex on an axis with duplicate labels. Take this into account on refactor.
                intersections2 = intersections[intersections["node_type"] != "added_calculation"].copy()
                all_manholes.update(intersections2.set_index(node_id_col)["type"])

                # # Add new manholes that are not yet in sqlite (rename is needed because of difference in column names)
                all_manholes = pd.concat(
                    [all_manholes, node_ids_without_manholes.rename(columns={node_id_col: a_man_conn_id})]
                )

                # check if nodes in wrong area (different initial waterlevel than rest in area) don't have manhole yet
                nodes_with_divergent_initial_wtrlvl_no_manhole = diverging_wl_nodes[
                    ~diverging_wl_nodes["conn_node_id"].isin(all_manholes[a_man_conn_id])
                ].copy()
                nodes_with_divergent_initial_wtrlvl_no_manhole[already_manhole_col] = False
                # also add these to the list
                all_manholes = pd.concat(
                    [all_manholes, nodes_with_divergent_initial_wtrlvl_no_manhole], ignore_index=True
                )
                # Drop duplicates that are introduced by nodes_with_divergent_initial_wtrlvl_no_manhole
                all_manholes = (
                    all_manholes.sort_values("drain_level", ascending=False)
                    .drop_duplicates("conn_node_id")
                    .sort_index()
                )
                all_manholes.reset_index(drop=True, inplace=True)
                all_manholes.crs = f"EPSG:{DEF_TRGT_CRS}"
                # all_manholes_gdf = gpd.GeoDataFrame(all_manholes, crs=f"EPSG:{DEF_TRGT_CRS}")
                return all_manholes.copy()
            except Exception as e:
                raise e from None

        self.manholes_info = get_manhole_information(
            intersections=self.line_intersects,
            diverging_wl_nodes=self.diverging_wl_nodes,
            manholes=self.imports["manholes"],
        )
        if write:
            self.manholes_info.to_file("manholes_info.gpkg", driver="GPKG")

    def manholes_to_add_to_model(self, write=False):
        self.new_manholes_df = get_manholes_to_add_to_model(self.manholes_info)

    def flowlines_1d2d(self, write=False):
        self.all_1d2d_flowlines = add_info_intersecting_1d2d_flowlines(
            self.line_intersects, self.imports["lines_1d2d"]
        )
        if write:
            self.all_1d2d_flowlines.to_file("all_1d2d_flowlines.gpkg", driver="GPKG")

    def generate_cross_section_locations(self, write=False):
        """generates cross section locations that need new bank levels"""
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
        self.line_intersections()
        self.flowlines_1d2d()

        # manholes
        self.divergent_waterlevel_nodes()
        self.manhole_information()
        self.manholes_to_add_to_model()

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


def add_info_intersecting_1d2d_flowlines(intersect_1d2d_all, lines_1d2d):
    """Create overview of all 1d2d flowlines and add information if these lines cross a levee"""
    try:
        all_intersecting_1d2d_flowlines = intersect_1d2d_all.drop(
            ["initial_water_level", node_geometry_col, storage_area_col], axis=1
        )
        all_intersecting_1d2d_flowlines = all_intersecting_1d2d_flowlines[
            [
                one_d_node_id_col,
                node_id_col,
                node_type_col,
                levee_id_col,
                levee_height_col,
                "type",
                df_geo_col,
            ]
        ]

        # Start building overview of all 1d2d lines, then add information of the 1d2d lines that cross certain lines
        lines_1d2d_extra = lines_1d2d.copy()[[df_geo_col, one_d_node_id_col, node_id_col, node_type_col]]

        # combine all 1d2d lines and intersecting lines with obstacles
        flowlines_suffix = "_drop"
        all_1d2d_flowlines = pd.merge(
            lines_1d2d_extra.reset_index(drop=True),
            all_intersecting_1d2d_flowlines.reset_index(drop=True),
            on=one_d_node_id_col,
            how="left",
            suffixes=["", flowlines_suffix],
        )
        all_1d2d_flowlines.drop(
            [
                f"{node_id_col}{flowlines_suffix}",
                f"{node_type_col}{flowlines_suffix}",
                f"{df_geo_col}{flowlines_suffix}",
            ],
            axis=1,
            inplace=True,
        )
        return all_1d2d_flowlines
    except Exception as e:
        raise e from None


def get_manholes_to_add_to_model(all_manholes):
    """
    Creates dataframe of new manholes to be added to model, formatted for insertion into the model
    """
    try:
        # Nodes with no manhole id need new manholes. Do not create manhole at fixeddrainage.

        new_manholes_df = all_manholes[
            (all_manholes[a_man_id].isna()) & (all_manholes["type"] != one_d_two_d_crosses_fixed)
        ].drop(df_geo_col, axis=1)

        new_manholes_for_model = dataframe_from_new_manholes(new_manholes_df)
        return new_manholes_for_model
    except Exception as e:
        raise e from None


def dataframe_from_new_manholes(new_manholes):
    try:
        new_manholes_model_df = pd.DataFrame(
            columns=[
                display_name_col,
                "code",
                conn_node_id_col,
                shape_col,
                width_col,
                manhole_indicator_col,
                calculation_type_col,
                "drain_level",
                bottom_lvl_col,
                surface_lvl_col,
                zoom_cat_col,
            ]
        )
        new_manholes_model_df[display_name_col] = new_manholes["code"] + "_" + new_manholes["type"]
        new_manholes_model_df["code"] = new_manholes_model_df[display_name_col]
        new_manholes_model_df[conn_node_id_col] = new_manholes[a_man_conn_id]
        new_manholes_model_df[shape_col] = "00"
        new_manholes_model_df[width_col] = 1
        new_manholes_model_df[manhole_indicator_col] = 0
        new_manholes_model_df[calculation_type_col] = np.where(new_manholes["drain_level"].isna(), 1, 2)
        new_manholes_model_df["drain_level"] = new_manholes["drain_level"]
        new_manholes_model_df.loc[new_manholes_model_df["drain_level"].isna(), "drain_level"] = "null"

        new_manholes_model_df[bottom_lvl_col] = -10
        new_manholes_model_df[surface_lvl_col] = new_manholes["initial_water_level"]
        new_manholes_model_df[zoom_cat_col] = 0
        new_manholes_model_df.set_index(conn_node_id_col)
        new_manholes_model_df[storage_area_col] = new_manholes[storage_area_col]
        # Add new storage area column where appropriate
        new_manholes_model_df.insert(
            new_manholes_model_df.columns.get_loc(storage_area_col) + 1,
            NEW_STORAGE_AREA_COL,
            np.where(np.isnan(new_manholes_model_df[storage_area_col]), 2.0, "-"),
        )
        return new_manholes_model_df
    except Exception as e:
        raise e from None


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
        cross_loc_new_all["new_bank_level"] = np.round(cross_loc_new_all["initial_waterlevel"] + 0.1, 3).astype(float)
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
        cross_loc_new_all.loc[cross_loc_levee.index, "new_bank_level"] = cross_loc_levee["levee_height"].astype(float)
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
            (cross_loc_new_all_filtered["bank_level_diff"] != 0) & (cross_loc_new_all_filtered["bank_level"].notna())
        ]
        return cross_loc_new_all_filtered, cross_loc_new
    except Exception as e:
        raise e from None


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
    self.folder = folder
    self.database = folder.model.schema_base.database
    self.dem = self.folder.model.schema_base.rasters.dem
# %%
