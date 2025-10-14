# %%
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 24 14:11:45 2021

@author: chris.kerklaan

@revised oktober 2025 by Wouter van Esse
"""

import os

import fiona
import geopandas as gpd
import hhnk_research_tools as hrt
import pandas as pd
import xarray as xr
from shapely import get_point, wkt
from shapely.geometry import Point
from threedigrid_builder import make_gridadmin

from hhnk_threedi_tools.core.checks.sqlite.structure_control import StructureControl
from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.core.schematisation.relations import ChannelRelations, StructureRelations
from hhnk_threedi_tools.utils.queries import (
    geometry_check_query,
)
from hhnk_threedi_tools.utils.queries_general_checks import ModelCheck
from hhnk_threedi_tools.variables.database_aliases import (
    a_chan_bed_struct_code,
    a_geo_end_coord,
    a_geo_end_node,
    a_geo_start_coord,
    a_geo_start_node,
    a_watersurf_conn_id,
)

DEM_MAX_VALUE = 400

logger = hrt.logging.get_logger(name=__name__)


# %%
class HhnkSchematisationChecks:
    def __init__(self, folder: Folders):
        self.folder = folder

        self.output_fd = self.folder.output.hhnk_schematisation_checks

        self.database = self.folder.model.schema_base.database
        self.dem = self.folder.model.schema_base.rasters.dem
        self.datachecker = self.folder.source_data.datachecker
        self.damo = self.folder.source_data.damo
        self.channels_from_profiles = self.folder.source_data.modelbuilder.channel_from_profiles

        self.layer_fixeddrainage = self.folder.source_data.datachecker.layers.fixeddrainagelevelarea

        # this dict we can populate with files and layers we can check using verify_inputs
        self.inputs = {
            "run_imp_surface_area": [{"file": self.folder.source_data.polder_polygon.path, "layer": None}],
            "run_struct_channel_bed_level": [{"file": self.folder.source_data.damo.path}],
        }  # TODO doen we hier iets mee?

        self.results = {}

    def verify_inputs(self, function) -> bool:
        """Check if the input of a function (if defined in self.inputs) exists."""
        exist = True
        # if function does not exist in self.inputs we can totally ignore this function
        if function in self.inputs.keys():
            for layer_input in self.inputs[function]:
                # check if file doesn't exist
                if not layer_input["file"].exists():
                    exist = False

                # optionally check if layer exists in file
                elif "layer" in layer_input.keys():
                    if layer_input["layer"] is not None:
                        if layer_input["layer"] not in fiona.listlayers(layer_input["file"]):
                            exist = False
        return exist

    def run_controlled_structures(self, overwrite=False):  # TODO
        """Create leayer with structure control in schematisation"""
        self.structure_control = StructureControl(
            model=self.database,
            hdb_control_layer=self.folder.source_data.hdb.layers.sturing_kunstwerken,
            output_file=self.output_fd.gestuurde_kunstwerken.base,
        )
        self.structure_control.run(overwrite=overwrite)

    def run_dem_max_value(self) -> str:
        stats = self.dem.statistics()
        if stats["max"] > DEM_MAX_VALUE:
            result = f"Maximale waarde DEM: {stats['max']} is te hoog"
        else:
            result = f"Maximale waarde DEM: {stats['max']} voldoet aan de norm"
        self.results["dem_max_value"] = result
        return result

    def run_dewatering_depth(self, overwrite=False):
        """
        Compare initial water level from fixed drainage level areas with
        surface level in DEM of model. Initial water level should mostly
        be below surface level.
        """
        drooglegging_raster = self.output_fd.drooglegging
        create = hrt.check_create_new_file(output_file=drooglegging_raster.path, overwrite=overwrite)

        if create:
            # Rasterize fixeddrainage
            wlvl_raster = self.output_fd.streefpeil
            dtype = "float32"
            nodata = hrt.variables.DEFAULT_NODATA_VALUES[dtype]

            fixeddrainage_gdf = self.layer_fixeddrainage.load()
            hrt.gdf_to_raster(
                gdf=fixeddrainage_gdf,
                value_field="streefpeil_bwn2",
                raster_out=wlvl_raster,
                nodata=nodata,
                metadata=self.dem.metadata,
                read_array=False,
                overwrite=overwrite,
            )

            # Load data arrays
            da_dem = self.dem.open_rxr()
            da_wlvl = wlvl_raster.open_rxr()
            crs = da_dem.rio.crs  # CRS is lost during xr.where; preserve it.

            da_out = da_dem - da_wlvl

            da_out = xr.where(da_dem == 10, nodata, da_out)

            # Write to file
            da_out.rio.write_crs(crs)  # Reapply crs
            drooglegging_raster = hrt.Raster.write(
                drooglegging_raster,
                result=da_out,
                nodata=nodata,
                dtype=dtype,
                chunksize=None,
            )

    def run_model_checks(self):  # TODO Wat doet dit, en waar wordt het gebruikt?
        """Collect all queries that are part of general model checks (see general_checks_queries file)
        and executes them
        """

        df = self.database.execute_sql_selection(query=ModelCheck.get_query())

        self.results["model_checks"] = df
        return df

    def run_geometry_checks(self) -> gpd.GeoDataFrame:
        """
        Deze test checkt of de geometrie van een channel en culvert in het model correspondeert met de start- of end node in de
        v2_connection_nodes tabel. Als de verkeerde ids worden gebruikt geeft dit fouten in het model.
        """
        struct_rel = StructureRelations(folder=self.folder, structure_table="culvert")
        culvert_gdf = struct_rel.gdf

        channel_rel = ChannelRelations(folder=self.folder)
        channel_gdf = channel_rel.gdf

        columns = ["geometry", "code", "connection_node_id_start", "connection_node_id_end"]
        gdf = pd.concat([culvert_gdf[columns], channel_gdf[columns]])
        gdf["start_coord"] = gpd.GeoSeries(get_point(gdf.geometry, 0), crs="EPSG:28992")
        gdf["end_coord"] = gpd.GeoSeries(get_point(gdf.geometry, -1), crs="EPSG:28992")

        connection_node_gdf = self.database.load(layer="connection_node", index_column="id")["geometry"]

        gdf = gdf.merge(
            connection_node_gdf.rename("node_geom_start"),
            how="left",
            left_on="connection_node_id_start",
            right_index=True,
        ).merge(
            connection_node_gdf.rename("node_geom_end"),
            how="left",
            left_on="connection_node_id_end",
            right_index=True,
        )

        gdf["start_dist_ok"] = gdf.start_coord.intersects(gdf.node_geom_start)
        gdf["end_dist_ok"] = gdf.end_coord.intersects(gdf.node_geom_end)

        # Only rows where at least one of start_dist_ok and end_dist_ok is false
        result_db = gdf[~gdf[["start_dist_ok", "end_dist_ok"]].all(axis=1)].copy()
        if not result_db.empty:
            result_db["error"] = "Error: Start or endnode from channel or culvert is not on referenced connection node"
        self.results["geometry_checks"] = result_db
        return result_db

    def run_imp_surface_area(self) -> str:
        """Calculate
        1. the impervious surface area in the model
        2. the area of the polder (based on the polder shapefile)
        3. the difference between the two.
        """
        imp_surface_db = self.database.load(layer="surface", index_column="id")

        polygon_imp_surface = self.folder.source_data.polder_polygon.load()

        db_surface = int(imp_surface_db.area.sum() / 10000)
        polygon_surface = int(polygon_imp_surface.area.to_numpy()[0] / 10000)
        area_diff = db_surface - polygon_surface

        result_txt = (
            f"Totaal ondoorlatend oppervlak: {db_surface} ha\n"
            f"Gebied polder: {polygon_surface} ha\n"
            f"Verschil: {area_diff} ha\n"
        )
        self.results["imp_surface_area"] = result_txt
        return result_txt

    def run_isolated_channels(self) -> tuple[gpd.GeoDataFrame, str]:
        """
        Test bepaalt welke watergangen isolated zijn, dus geen verbinding maken met het maaiveld of 2D rekenrooster.
        De test berekent tevens de totale lengte van watergangen en welk deel daarvan geïsoleerd is.
        """
        channel_gdf = self.database.load(layer="channel", index_column="id")
        channel_gdf["length_in_meters"] = round(channel_gdf["geometry"].length, 2)
        (
            isolated_channels_gdf,
            isolated_length,
            total_length,
            percentage,
        ) = _calc_len_percentage(channel_gdf)
        result = (
            f"Totale lengte watergangen {total_length} km\n"
            f"Totale lengte geïsoleerde watergangen {isolated_length} km\n"
            f"Percentage geïsoleerde watergangen {percentage}%\n"
        )
        self.results["isolated_channels"] = {
            "gdf": isolated_channels_gdf,
            "result": result,
        }
        return isolated_channels_gdf, result

    def run_used_profiles(self) -> gpd.GeoDataFrame:
        """
        Koppelt de v2_cross_section_definition laag van het model (discrete weergave van de natuurlijke geometrie van de
        watergangen) aan de v2_channel laag (informatie over watergangen in het model). Het resultaat van deze toets is een
        weergave van de breedtes en dieptes van watergangen in het model ter controle.
        """
        # Load channels and cross sections from model
        channel_rel = ChannelRelations(folder=self.folder)
        channel_gdf = channel_rel.gdf

        self.results["used_profiles"] = channel_gdf
        # TODO confige plugin so than minimum depth is somehow shown, this is what is actually interesting
        return channel_gdf

    def run_struct_channel_bed_level(self) -> gpd.GeoDataFrame:
        """
        Check whether the reference level of the cross section location reference level is below the orifice crest
        level or the culvert inlet level of the adjacent channels.

        # FIXME this is not what the code below does. It only loaded culverts and looked at damo if it had some
        assumption. It did not look at channels or bed levels at all. It only returns a list of culverts with an
        indication of an assumption. But it was also the wrong source for that, since this is in the datachecker

        StructureRelation gives the lowest of the reference levels of the connected channels, but it takes into account
        only to use the closest cross section location per channel.

        Connection nodes with manholes are not considered.

        If the structure levels are below the lowest reference level, 3Di model may crash (when water level drops
        below reference level).
        """
        logger.warning("Check 'run_struct_channel_bed_level' does not do what it is supposed to do")

        # Get profiles that cause structures to be sunk
        wrong_profile_dict: dict[str, gpd.GeoDataFrame] = {}
        for structure_table in ["culvert", "orifice"]:
            struct_rel = StructureRelations(folder=self.folder, structure_table=structure_table)
            for side in ["start", "end"]:
                # Get wrong profiles on both sides of structure
                wrong_profiles_side = struct_rel.get_wrong_profile(side=side)
                wrong_profile_dict[f"{structure_table}_{side}"] = wrong_profiles_side
        # Combine wrong profiles
        wrong_profiles_gdf = pd.concat(wrong_profile_dict.values(), ignore_index=True)

        # Sort on lowest minimal_crest_level and remove duplicates (using only lowest reference level per location)
        wrong_profiles_gdf = wrong_profiles_gdf.sort_values(by=["proposed_reference_level"]).drop_duplicates(
            subset=["cross_section_location_id"], keep="first"
        )

        # Load datacheck to determine whether structure reference lever was based on assumption
        dc_culvert_gdf = self.folder.source_data.datachecker.layers.culvert.load()
        dc_bridge_gdf = self.folder.source_data.datachecker.layers.bridge.load()

        # Filter when these have an assumption
        culvert_assump_down_gdf = dc_culvert_gdf[
            (dc_culvert_gdf["aanname"].str.contains("bed_level_down")) & ~(dc_culvert_gdf["aanname"].isna())
        ]
        culvert_assump_up_gdf = dc_culvert_gdf[
            (dc_culvert_gdf["aanname"].str.contains("bed_level_up")) & ~(dc_culvert_gdf["aanname"].isna())
        ]
        bridge_assump_gdf = dc_bridge_gdf[
            (dc_bridge_gdf["aanname"].str.contains("bottom_level")) & ~(dc_bridge_gdf["aanname"].isna())
        ]

        # List of structures where the bed/bottem level is sunk below reference level, but the bed/bottom level is based on assumption
        assump_start = pd.concat([culvert_assump_up_gdf["code"], bridge_assump_gdf["code"]])
        assump_end = pd.concat([culvert_assump_down_gdf["code"], bridge_assump_gdf["code"]])

        # Filter and split wrong profiles
        wrong_profiles_no_assumption_gdf = pd.concat(
            [
                wrong_profiles_gdf[
                    (~wrong_profiles_gdf["structure_code"].isin(assump_start))
                    & (wrong_profiles_gdf["structure_side"] == "start")
                ],
                wrong_profiles_gdf[
                    (~wrong_profiles_gdf["structure_code"].isin(assump_end))
                    & (wrong_profiles_gdf["structure_side"] == "end")
                ],
            ]
        )
        # Flag wrong profiles
        wrong_profiles_gdf["ref_level_based_on_assumption"] = False
        wrong_profiles_gdf["ref_level_based_on_assumption"] = ~wrong_profiles_gdf["cross_section_location_id"].isin(
            wrong_profiles_no_assumption_gdf["cross_section_location_id"]
        )

        # TODO This should be loaded into the plugin
        # return wrong_profiles_gdf, culvert_assump_down_gdf, culvert_assump_up_gdf, bridge_assump_gdf

        ############################
        # Original code modified, working towards original result #FIXME use above in plugin, remove below

        # Load culvert and bridges including relations
        culvert_rel = StructureRelations(folder=self.folder, structure_table="culvert")
        culvert_gdf = culvert_rel.gdf.rename(columns={"code": "struct_code"})
        # Load source data to determine whether structure reference lever was based on assumption
        datachecker_culvert_layer = self.folder.source_data.datachecker.layers.culvert
        damo_duiker_sifon_layer = self.folder.source_data.damo.layers.DuikerSifonHevel

        # See git issue about below statements
        gdf_with_damo = _add_damo_info(layer=damo_duiker_sifon_layer, gdf=culvert_gdf)
        gdf_with_datacheck = _add_datacheck_info(datachecker_culvert_layer, gdf_with_damo)
        gdf_with_datacheck.loc[:, "beneden_has_assumption"] = gdf_with_datacheck[
            "hoogte_binnen_onderkant_beneden"
        ].isna()
        gdf_with_datacheck.loc[:, "boven_has_assumption"] = gdf_with_datacheck["hoogte_binnen_onderkant_boven"].isna()
        self.results["struct_channel_bed_level"] = gdf_with_datacheck
        return gdf_with_datacheck  # culverts met beneden beneden_has_assumption en boven_has assumption, maar hoe gebruikt

    def run_watersurface_area(self) -> tuple[gpd.GeoDataFrame, str]:
        """
        Deze test controleert per peilgebied in het model hoe groot het gebied
        is dat het oppervlaktewater beslaat in het model. Dit totaal is opgebouwd
        uit de kolom `storage_area` uit de `connection_node` in de sqlite opgeteld
        bij het oppervlak van de watergangen (uitgelezen uit `channel_surface_from_profiles`)
        shapefile. Vervolgens worden de totalen per peilgebied vergeleken met diezelfde
        totalen uit de waterdelen in DAMO.

        De kolom namen in het resultaat zijn als volgt:
        From connection_nodes -> area_nodes_m2
        From channel_surface_from_profiles -> area_channels_m2
        From DAMO -> area_waterdeel_m2
        """

        # read inputs
        fixeddrainage_gdf = self.folder.source_data.datachecker.layers.fixeddrainagelevelarea.load()[
            ["peil_id", "code", "streefpeil_bwn2", "geometry"]
        ]
        modelbuilder_waterdeel_gdf = self.channels_from_profiles.load(layer="channel_surface_from_profiles")
        damo_waterdeel_gdf = self.folder.source_data.damo.layers.waterdeel.load()
        conn_nodes_gdf = self.database.load(layer="connection_node", index_column="id").rename(
            columns={"id": a_watersurf_conn_id}
        )
        # Explode fixed drainage level area polygons to ensure no multipolygons are present
        fixeddrainage_gdf = _expand_multipolygon(fixeddrainage_gdf)

        # Add area from connection nodes to each fixed drainage level area
        fixeddrainage_gdf = _add_nodes_area(fixeddrainage_gdf, conn_nodes_gdf)
        # Add area from BGT waterdelen to each fixed drainage level area
        fixeddrainage_gdf = _add_waterdeel(fixeddrainage_gdf, damo_waterdeel_gdf)
        fixeddrainage_gdf.rename(columns={"area": "area_waterdeel_m2"}, inplace=True)
        # Add area from model channels to each fixed drainage level area
        fixeddrainage_gdf = _add_waterdeel(fixeddrainage_gdf, modelbuilder_waterdeel_gdf)
        fixeddrainage_gdf.rename(columns={"area": "area_channels_m2"}, inplace=True)
        # Total water area in model
        fixeddrainage_gdf["area_model_m2"] = fixeddrainage_gdf["area_channels_m2"] + fixeddrainage_gdf["area_nodes_m2"]
        # Difference in water area model and waterdeel
        fixeddrainage_gdf["area_diff"] = fixeddrainage_gdf["area_model_m2"] - fixeddrainage_gdf["area_waterdeel_m2"]
        fixeddrainage_gdf["area_diff_perc"] = fixeddrainage_gdf.apply(
            lambda row: _calc_perc(row["area_diff"], row["area_waterdeel_m2"]),
            axis=1,
        )

        result_txt = """Gebied open water BGT: {} ha\nGebied open water model: {} ha""".format(
            round(fixeddrainage_gdf["area_waterdeel_m2"].sum() / 10000, 2),
            round(fixeddrainage_gdf["area_model_m2"].sum() / 10000, 2),
        )
        self.results["watersurface_area"] = {
            "fixeddrainage": fixeddrainage_gdf,
            "result_txt": result_txt,
        }
        return fixeddrainage_gdf, result_txt

    def run_weir_floor_level(self) -> tuple[gpd.GeoDataFrame, str]:
        """
        Check whether minimum crest height of weir is under reference level found in the v2_cross_section_location layer.
        This is not allowed, so if this is the case, we have to update the reference level.
        """

        # Use SchructureRelations to get relations between weirs, channels and cross section locations
        struct_rel = StructureRelations(folder=self.folder, structure_table="weir")

        # Get wrong profiles on both sides of structure
        wrong_profiles_start = struct_rel.get_wrong_profile(side="start")
        wrong_profiles_end = struct_rel.get_wrong_profile(side="end")
        wrong_profiles_gdf = pd.concat([wrong_profiles_start, wrong_profiles_end])

        # Sort on lowest minimal_crest_level and remove duplicates (using only lowest reference level per location)
        wrong_profiles_gdf = wrong_profiles_gdf.sort_values(by=["proposed_reference_level"]).drop_duplicates(
            subset=["cross_section_location_id"], keep="first"
        )

        update_query = hrt.sql_create_update_case_statement(
            df=wrong_profiles_gdf,
            layer="cross_section_location",
            df_id_col="cross_section_location_id",
            db_id_col="id",
            new_val_col="proposed_reference_level",
            old_val_col="reference_level",
        )
        self.results["weir_floor_level"] = {
            "wrong_profiles_gdf": wrong_profiles_gdf,
            "update_query": update_query,
        }

        return wrong_profiles_gdf, update_query  # TODO check of sql werkt in de plugin

    def create_grid_from_schematisation(self, output_folder):
        """
        Create grid from schematisation (gpkg), this includes cells, lines and nodes.
        Returns Geopackage named grid.gpkg in output folder.
        """

        grid = make_gridadmin(self.database.base, self.dem.base)

        output_fp = os.path.join(output_folder, "grid.gpkg")
        # using output here results in error, so we use the returned dict
        # TODO plugin fix loading from 1 gropackage instead of three in
        for grid_type in ["cells", "lines", "nodes"]:
            df = pd.DataFrame(grid[grid_type])
            gdf = hrt.df_convert_to_gdf(df, geom_col_type="wkb", src_crs="28992")
            gdf.to_file(driver="GPKG", filename=output_fp, index=False, layer=grid_type)

    def run_cross_section_duplicates(self, database: hrt.SpatialDatabase) -> gpd.GeoDataFrame:
        """Check for duplicate geometries in cross_section_locations.

        Duplicates are defined as cross sections that are within 0.5 meter of each other.

        Returns a GeoDataFrame with the duplicate cross section locations.

        Parameters
        ----------
        database : hrt.SpatialDatabase
            The database to load the cross_section_location layer from.

        Returns
        -------
        gpd.GeoDataFrame
            A GeoDataFrame with the intersected points.

        """
        cross_section_point = database.load(layer="cross_section_location", index_column="id")

        # Make buffer of the points to identify if we have cross setion overlapping each other.
        cross_section_buffer_gdf = cross_section_point.copy()
        cross_section_buffer_gdf["geometry"] = cross_section_buffer_gdf.buffer(0.5)

        # Make spatial join between the buffer and the cross section point
        cross_section_join = gpd.sjoin(
            cross_section_buffer_gdf, cross_section_point, how="inner", predicate="intersects"
        )

        # Duplicates in cross_loc in this join are duplicated cross_section_locations
        index_duplicates = cross_section_join[cross_section_join.index.duplicated()].index
        intersected_points = cross_section_point.loc[index_duplicates]

        return intersected_points

    def run_cross_section_no_vertex(self, database: hrt.SpatialDatabase) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """
        Check for cross_sections that are not located on the  vertex of a channel. Includes check for cross sections
        that are located on the wrong channel (i.e. not the channel they are linked to).
        """
        # load cross locs and channels from sqlite
        cross_section_point = database.load(layer="cross_section_location", index_column="id")

        channels_gdf = database.load(layer="channel", index_column="id")
        # add column channel id to come to same result as before migration from sqlite to gpkg
        channels_gdf["channel_id"] = channels_gdf.index

        # Create gdf of all channel vertices and buffer them
        channels_gdf["points"] = channels_gdf["geometry"].apply(lambda x: [Point(coord) for coord in x.coords])
        vertices_gdf = gpd.GeoDataFrame(
            [
                (idx, item)
                for idx, sublist in zip(channels_gdf["channel_id"], channels_gdf["points"])
                for item in sublist
            ],
            columns=["channel_id", "geometry"],
            geometry="geometry",
            crs="EPSG:28992",
        )
        vertices_gdf["geometry"] = vertices_gdf.buffer(0.001)

        # Join cross section points and buffered vertices.
        v_cs = gpd.sjoin(cross_section_point, vertices_gdf, how="inner", predicate="intersects")

        # Error cross loc op verkeerde channel
        v_cs_channel_mismatch = v_cs[v_cs["channel_id_left"] != v_cs["channel_id_right"]]
        if len(v_cs_channel_mismatch) > 0:
            logger.error(
                f"cross loc ids {v_cs_channel_mismatch.index.to_numpy()} are located on a vertex of a different channel"
            )

        # cross sections that didnt get joined are missing a vertex.
        cross_no_vertex = cross_section_point[~cross_section_point.index.isin(v_cs.index.values)].copy()

        # Find distance to nearest vertex
        nearest_point = cross_no_vertex.sjoin_nearest(vertices_gdf)

        def get_distance(row):
            dist = row.geometry.distance(vertices_gdf.loc[row.index_right, "geometry"])
            return round(dist, 2)

        cross_no_vertex.loc[:, ["distance_to_vertex"]] = nearest_point.apply(get_distance, axis=1)

        return cross_no_vertex, v_cs_channel_mismatch


# TODO add monhole bottom level is lower than structure level check

## Helper functions
# TODO zet in functies als het maar paar regels is


def _add_distance_checks(gdf):
    # Load as valid geometry type
    gdf["start_coord"] = gdf["start_coord"].apply(wkt.loads)
    gdf["start_node"] = gdf["start_node"].apply(wkt.loads)
    gdf["end_coord"] = gdf["end_coord"].apply(wkt.loads)
    gdf["end_node"] = gdf["end_node"].apply(wkt.loads)
    # Set as geometry column (geopandas doesn't support having more than one)
    gdf_start_coor = gdf.set_geometry(col="start_coord")
    gdf_start_node = gdf.set_geometry(col="start_node")
    gdf["start_dist_ok"] = round(gdf_start_node.distance(gdf_start_coor), 5) < 0.1
    gdf_end_coor = gdf.set_geometry(col="end_coord")
    gdf_end_node = gdf.set_geometry(col="end_node")
    gdf["end_dist_ok"] = round(gdf_end_node.distance(gdf_end_coor), 5) < 0.1


def _calc_len_percentage(channels_gdf):
    total_length = round(channels_gdf.geometry.length.sum() / 1000, 2)
    isolated_channels_gdf = channels_gdf[channels_gdf["exchange_type"] == 101]
    if not isolated_channels_gdf.empty:
        isolated_length = round(isolated_channels_gdf.geometry.length.sum() / 1000, 2)
    else:
        isolated_length = 0
    percentage = round((isolated_length / total_length) * 100, 0)
    return isolated_channels_gdf, isolated_length, total_length, percentage


def _add_damo_info(layer, gdf):
    try:
        damo_gdb = layer.load()
        new_gdf = gdf.merge(
            damo_gdb[["CODE", "HOOGTEBINNENONDERKANTBENE", "HOOGTEBINNENONDERKANTBOV"]],
            how="left",
            left_on="struct_code",
            right_on="CODE",
        )
        new_gdf.rename(
            columns={
                "HOOGTEBINNENONDERKANTBENE": "hoogte_binnen_onderkant_beneden",
                "HOOGTEBINNENONDERKANTBOV": "hoogte_binnen_onderkant_boven",
                "CODE": "damo_code",
            },
            inplace=True,
        )
    except Exception as e:
        raise e from None
    else:
        return new_gdf


def _add_datacheck_info(layer, gdf):
    try:
        datachecker_gdb = layer.load()
        new_gdf = gdf.merge(
            datachecker_gdb[["code", "aanname"]],
            how="left",
            left_on=a_chan_bed_struct_code,
            right_on="code",
        )
        new_gdf.rename(
            columns={"aanname": "assumptions"},
            inplace=True,
        )
    except Exception as e:
        raise e from None
    else:
        return new_gdf


def _expand_multipolygon(df):
    """New version using explode, old version returned pandas dataframe not geopandas
    geodataframe (missing last line), I think it works now?
    """
    try:
        exploded = df.set_index(["peil_id"])["geometry"]
        exploded = exploded.explode(index_parts=True)
        exploded = exploded.reset_index()
        exploded = exploded.rename(columns={0: "geometry", "level_1": "multipolygon_level"})
        merged = exploded.merge(df.drop("geometry", axis=1), left_on="peil_id", right_on="peil_id")
        merged = merged.set_geometry("geometry", crs=df.crs)
        return merged
    except Exception as e:
        raise e from None


def _add_nodes_area(fixeddrainage, conn_nodes_geo):
    try:
        # join on intersection of geometries
        joined = gpd.sjoin(
            fixeddrainage,
            conn_nodes_geo,
            how="left",
            predicate="intersects",
            lsuffix="fd",
            rsuffix="conn",
        )
        # Combine all rows with same peil_id and multipolygon level and sum their area
        group = joined.groupby(["peil_id", "multipolygon_level"])["storage_area"].sum()
        # Add the aggregated area column to the original dataframe
        fixeddrainage = fixeddrainage.merge(group, how="left", on=["peil_id", "multipolygon_level"])
        fixeddrainage.rename(columns={"storage_area": "area_nodes_m2"}, inplace=True)
        return fixeddrainage
    except Exception as e:
        raise e from None


def _add_waterdeel(fixeddrainage, to_add):
    try:
        # create dataframe containing overlaying geometry
        overl = gpd.overlay(fixeddrainage, to_add, how="intersection")
        # add column containing size of overlaying areas
        overl["area"] = overl["geometry"].area
        # group overlaying area gdf by id's
        overl = overl.groupby(["peil_id", "multipolygon_level"])["area"].sum()
        # merge overlapping area size into fixeddrainage
        merged = fixeddrainage.merge(overl, how="left", on=["peil_id", "multipolygon_level"])
        merged["area"] = round(merged["area"], 0)
        merged["area"] = merged["area"].fillna(0)
    except Exception as e:
        raise e from None
    return merged


def _calc_perc(diff, waterdeel):
    try:
        return round((diff / waterdeel) * 100, 1)
    except:
        if diff == waterdeel:
            return 0.0
        else:
            return 100.0


# %%

if __name__ == "__main__":
    from tests.config import FOLDER_TEST

    folder = Folders(FOLDER_TEST)
    # results = {}
    self = HhnkSchematisationChecks(folder=folder)  # , results=results)
    database = folder.model.schema_base.database

    # self.verify_inputs("run_imp_surface_area")
