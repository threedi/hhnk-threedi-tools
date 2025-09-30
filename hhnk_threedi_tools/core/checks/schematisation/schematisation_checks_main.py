# %%
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 24 14:11:45 2021

@author: chris.kerklaan
"""

# Third-party imports
from pathlib import Path

import fiona
import geopandas as gpd

# research-tools
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
import xarray as xr
from shapely import wkt
from shapely.geometry import Point
from threedigrid_builder import make_gridadmin

from hhnk_threedi_tools.core.checks.sqlite.structure_control import StructureControl

# Local imports
from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.core.schematisation.relations import StructureRelations

logger = hrt.logging.get_logger(name=__name__)

# queries
from hhnk_threedi_tools.utils.queries import (
    channels_query,
    cross_section_location_query,
    geometry_check_query,
    impervious_surface_query,
    isolated_channels_query,
    profiles_used_query,
    struct_channel_bed_query,
    watersurface_conn_node_query,
    weir_height_query,
)
from hhnk_threedi_tools.utils.queries_general_checks import ModelCheck
from hhnk_threedi_tools.variables.database_aliases import (
    a_chan_bed_struct_code,
    a_chan_bed_struct_id,
    a_chan_id,
    a_geo_end_coord,
    a_geo_end_node,
    a_geo_start_coord,
    a_geo_start_node,
    a_watersurf_conn_id,
    a_weir_code,
    a_weir_conn_node_end_id,
    a_weir_conn_node_start_id,
    a_weir_cross_loc_id,
    a_zoom_cat,
    df_geo_col,
)
from hhnk_threedi_tools.variables.database_variables import (
    action_col,
    calculation_type_col,
    code_col,
    cross_sec_loc_layer,
    height_col,
    id_col,
    initial_waterlevel_col,
    reference_level_col,
    storage_area_col,
    width_col,
)
from hhnk_threedi_tools.variables.datachecker_variables import (
    COL_STREEFPEIL_BWN,
    geometry_col,
    peil_id_col,
)
from hhnk_threedi_tools.variables.definitions import (
    DEM_MAX_VALUE,
    channels_isolated_calc_type,
)

# variables
from hhnk_threedi_tools.variables.sqlite import (
    area_diff_col,
    area_diff_perc,
    datachecker_assumption_alias,
    down_has_assumption,
    height_inner_lower_down,
    height_inner_lower_up,
    length_in_meters_col,
    max_depth_col,
    primary_col,
    up_has_assumption,
    water_level_width_col,
    watersurface_channels_area,
    watersurface_model_area,
    watersurface_nodes_area,
    watersurface_waterdeel_area,
)
from hhnk_threedi_tools.variables.weirs import (
    diff_crest_ref,
    min_crest_height,
    new_ref_lvl,
    wrong_profile,
)

# Globals
# controlled
START_ACTION = "start_action_value"
MIN_ACTION = "min_action_value"
MAX_ACTION = "max_action_value"
HDB_KRUIN_MIN = "hdb_kruin_min"
HDB_KRUIN_MAX = "hdb_kruin_max"
HDB_STREEFPEIL = "hdb_streefpeil"

# structures on channels
DAMO_FIELDS = ["CODE", "HOOGTEBINNENONDERKANTBENE", "HOOGTEBINNENONDERKANTBOV"]
DAMO_LINK_ON = "CODE"
DATACHECKER_FIELDS = ["code", "aanname"]
DATACHECKER_LINK_ON = "code"
DATACHECKER_ASSUMPTION_FIELD = "aanname"


# weir heights
OUTPUT_COLS = [
    a_weir_code,
    a_weir_conn_node_start_id,
    a_weir_conn_node_end_id,
    a_weir_cross_loc_id,
    a_chan_id,
    min_crest_height,
    reference_level_col,
    new_ref_lvl,
    df_geo_col,
]


# %%
class HhnkSchematisationChecks:
    def __init__(
        self,
        folder: Folders,
    ):
        self.folder = folder

        self.output_fd = self.folder.output.hhnk_schematisation_checks

        self.model = self.folder.model.schema_base.database
        self.dem = self.folder.model.schema_base.rasters.dem
        self.datachecker = self.folder.source_data.datachecker
        self.damo = self.folder.source_data.damo
        self.channels_from_profiles = self.folder.source_data.modelbuilder.channel_from_profiles

        self.layer_fixeddrainage = self.folder.source_data.datachecker.layers.fixeddrainagelevelarea

        # this dict we can populate with files and layers we can check using verify_inputs
        self.inputs = {
            "run_imp_surface_area": [{"file": self.folder.source_data.polder_polygon.path, "layer": None}],
            "run_struct_channel_bed_level": [{"file": self.folder.source_data.damo.path}],
        }

        self.results = {}

    def verify_inputs(self, function):
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

    def run_controlled_structures(self, overwrite=False):
        """Create leayer with structure control in schematisation"""
        self.structure_control = StructureControl(
            model=self.model,
            hdb_control_layer=self.folder.source_data.hdb.layers.sturing_kunstwerken,
            output_file=self.output_fd.gestuurde_kunstwerken.base,
        )
        self.structure_control.run(overwrite=overwrite)

    def run_dem_max_value(self):
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
                value_field=COL_STREEFPEIL_BWN,
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
            da_out.rio.set_crs(crs)  # Reapply crs
            drooglegging_raster = hrt.Raster.write(
                drooglegging_raster,
                result=da_out,
                nodata=nodata,
                dtype=dtype,
                chunksize=None,
            )

    def run_model_checks(self):
        """Collect all queries that are part of general model checks (see general_checks_queries file)
        and executes them
        """

        df = self.model.execute_sql_selection(query=ModelCheck.get_query())

        self.results["model_checks"] = df
        return df

    def run_geometry_checks(self):
        """
        Deze test checkt of de geometrie van een object in het model correspondeert met de start- of end node in de
        v2_connection_nodes tafel. Als de verkeerde ids worden gebruikt geeft dit fouten in het model.
        """
        gdf = self.model.execute_sql_selection(
            query=geometry_check_query,
        )

        gdf["start_check"] = gdf[a_geo_start_node] == gdf[a_geo_start_coord]
        gdf["end_check"] = gdf[a_geo_end_node] == gdf[a_geo_end_coord]
        _add_distance_checks(gdf)
        # Only rows where at least one of start_dist_ok and end_dist_ok is false
        result_db = gdf[~gdf[["start_dist_ok", "end_dist_ok"]].all(axis=1)]
        if not result_db.empty:
            result_db["error"] = "Error: mismatched geometry"
        self.results["geometry_checks"] = result_db
        return result_db

    def run_imp_surface_area(self):
        """Calculate
        1. the impervious surface area in the model
        2. the area of the polder (based on the polder shapefile)
        3. the difference between the two.
        """
        imp_surface_db = self.model.execute_sql_selection(impervious_surface_query)
        imp_surface_db.set_index("id", inplace=True)

        polygon_imp_surface = self.folder.source_data.polder_polygon.load()

        db_surface, polygon_surface, area_diff = _calc_surfaces_diff(imp_surface_db, polygon_imp_surface)
        result_txt = (
            f"Totaal ondoorlatend oppervlak: {db_surface} ha\n"
            f"Gebied polder: {polygon_surface} ha\n"
            f"Verschil: {area_diff} ha\n"
        )
        self.results["imp_surface_area"] = result_txt
        return result_txt

    def run_isolated_channels(self):
        """
        Test bepaalt welke watergangen niet zijn aangesloten op de rest van de watergangen. Deze watergangen worden niet
        meegenomen in de uitwisseling in het watersysteem. De test berekent tevens de totale lengte van watergangen en welk
        deel daarvan geïsoleerd is.
        """
        channels_gdf = self.model.execute_sql_selection(query=isolated_channels_query)
        channels_gdf[length_in_meters_col] = round(channels_gdf[df_geo_col].length, 2)
        (
            isolated_channels_gdf,
            isolated_length,
            total_length,
            percentage,
        ) = _calc_len_percentage(channels_gdf)
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

    def run_used_profiles(self):
        """
        Koppelt de v2_cross_section_definition laag van het model (discrete weergave van de natuurlijke geometrie van de
        watergangen) aan de v2_channel laag (informatie over watergangen in het model). Het resultaat van deze toets is een
        weergave van de breedtes en dieptes van watergangen in het model ter controle.
        """
        # TODO use hrt.sqlite_table_to_gdf instead?
        channels_gdf = self.model.execute_sql_selection(query=profiles_used_query)
        # If zoom category is 4, channel is considered primary
        channels_gdf[primary_col] = channels_gdf[a_zoom_cat].apply(lambda zoom_cat: zoom_cat == 4)
        channels_gdf[width_col] = channels_gdf[width_col].apply(_split_round)
        channels_gdf[height_col] = channels_gdf[height_col].apply(_split_round)
        channels_gdf[water_level_width_col] = channels_gdf.apply(func=_calc_width_at_waterlevel, axis=1)
        channels_gdf[max_depth_col] = channels_gdf.apply(func=_get_max_depth, axis=1)
        # Conversion to string because lists are not valid for storing in gpkg
        channels_gdf[width_col] = channels_gdf[width_col].astype(str)
        channels_gdf[height_col] = channels_gdf[height_col].astype(str)
        self.results["used_profiles"] = channels_gdf
        return channels_gdf

    def run_struct_channel_bed_level(self):
        """Check whether the reference level of any of the adjacent cross section locations
        (channels) to a structure is lower than the reference level for that structure
        (3di crashes if it is)
        """
        datachecker_culvert_layer = self.folder.source_data.datachecker.layers.culvert
        damo_duiker_sifon_layer = self.folder.source_data.damo.layers.DuikerSifonHevel

        below_ref_query = struct_channel_bed_query
        gdf_below_ref = self.model.execute_sql_selection(query=below_ref_query)
        gdf_below_ref.rename(columns={"id": a_chan_bed_struct_id}, inplace=True)

        # See git issue about below statements
        gdf_with_damo = _add_damo_info(layer=damo_duiker_sifon_layer, gdf=gdf_below_ref)
        gdf_with_datacheck = _add_datacheck_info(datachecker_culvert_layer, gdf_with_damo)
        gdf_with_datacheck.loc[:, down_has_assumption] = gdf_with_datacheck[height_inner_lower_down].isna()
        gdf_with_datacheck.loc[:, up_has_assumption] = gdf_with_datacheck[height_inner_lower_up].isna()
        self.results["struct_channel_bed_level"] = gdf_with_datacheck
        return gdf_with_datacheck

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
        conn_nodes_gdf = self.model.load(layer="connection_node", index_column="id").rename(
            columns={"id": a_watersurf_conn_id}
        )
        # Explode fixed drainage level area polygons to ensure no multipolygons are present
        fixeddrainage_gdf = _expand_multipolygon(fixeddrainage_gdf)  # TODO deal with helper functions

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

    def run_weir_floor_level(self, database: hrt.SpatialDatabase) -> tuple[gpd.GeoDataFrame, str]:
        """
        Check whether minimum crest height of weir is under reference level found in the v2_cross_section_location layer.
        This is not allowed, so if this is the case, we have to update the reference level.
        """

        # Use SchructureRelations to get relations between weirs, channels and cross section locations
        struct_rel = StructureRelations(folder=self.folder, structure_table="weir")
        weir_gdf = struct_rel.relations()

        # Get lowest value from min_crest_level_control and crest_level if not nan
        weir_gdf["minimal_crest_level"] = np.nanmin(
            [weir_gdf["min_crest_level_control"], weir_gdf["crest_level"]], axis=0
        )
        weir_gdf = weir_gdf[
            [
                "weir_id",
                "cs_id_min_ref_level_start",
                "min_ref_level_start",
                "cs_id_min_ref_level_end",
                "min_ref_level_end",
                "crest_level",
                "min_crest_level_control",
                "minimal_crest_level",
                "geometry",
            ]
        ]
        # Get sunk weir crest levels at start and end node
        weir_sunk_start_gdf = weir_gdf[weir_gdf["minimal_crest_level"] < weir_gdf["min_ref_level_start"]].copy()
        weir_sunk_start_gdf["cs_id_min_ref_level"] = weir_sunk_start_gdf["cs_id_min_ref_level_start"]
        weir_sunk_end_gdf = weir_gdf[weir_gdf["minimal_crest_level"] < weir_gdf["min_ref_level_end"]].copy()
        weir_sunk_end_gdf["cs_id_min_ref_level"] = weir_sunk_end_gdf["cs_id_min_ref_level_end"]
        # Combine into one list
        wrong_profiles_gdf = pd.concat([weir_sunk_start_gdf, weir_sunk_end_gdf])
        # Sort on lowest minimal_crest_level and remove duplicates (using only lowest reference level per location)
        wrong_profiles_gdf = wrong_profiles_gdf.sort_values(by=["minimal_crest_level"]).drop_duplicates(
            subset=["cs_id_min_ref_level"], keep="first"
        )

        update_query = hrt.sql_create_update_case_statement(
            df=wrong_profiles_gdf,
            layer="cross_section_location",
            df_id_col="cs_id_min_ref_level",
            db_id_col="cs_id_min_ref_level",
            new_val_col="minimal_crest_level",
            old_val_col="reference_level",
        )
        self.results["weir_floor_level"] = {
            "wrong_profiles_gdf": wrong_profiles_gdf,
            "update_query": update_query,
        }
        return wrong_profiles_gdf, update_query  # TODO check of sql werkt in de plugin

    def create_grid_from_schematisation(
        self, output_folder
    ):  # FIXME #27143 makegrid werkt niet in nieuwe python versie
        """Create grid from schematisation (gpkg), this includes cells, lines and nodes."""
        # grid = make_gridadmin(self.model.base, self.dem.base)

        # # using output here results in error, so we use the returned dict
        # for grid_type in ["cells", "lines", "nodes"]:
        #     df = pd.DataFrame(grid[grid_type])
        #     gdf = hrt.df_convert_to_gdf(df, geom_col_type="wkb", src_crs="28992")
        #     gdf.to_file(driver="GPKG", filename=Path(output_folder) / f"{grid_type}.gpkg", index=False)

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


def _calc_surfaces_diff(db_imp_surface, polygon_imp_surface):
    db_surface = int(db_imp_surface.sum() / 10000)
    polygon_surface = int(polygon_imp_surface.area.values[0] / 10000)
    area_diff = db_surface - polygon_surface
    return db_surface, polygon_surface, area_diff


def _calc_len_percentage(channels_gdf):
    total_length = round(channels_gdf.geometry.length.sum() / 1000, 2)
    isolated_channels_gdf = channels_gdf[channels_gdf[calculation_type_col] == channels_isolated_calc_type]
    if not isolated_channels_gdf.empty:
        isolated_length = round(isolated_channels_gdf.geometry.length.sum() / 1000, 2)
    else:
        isolated_length = 0
    percentage = round((isolated_length / total_length) * 100, 0)
    return isolated_channels_gdf, isolated_length, total_length, percentage


def _calc_width_at_waterlevel(row):
    """Bereken de breedte van de watergang op het streefpeil"""
    x_pos = [b / 2 for b in row[width_col]]
    y = [row.reference_level + b for b in row[height_col]]
    ini = row[initial_waterlevel_col]

    # Interpoleer tussen de x en y waarden (let op: de x en y zijn hier verwisseld)
    width_wl = round(np.interp(ini, xp=y, fp=x_pos), 2) * 2
    return width_wl


def _split_round(item):
    """Split items in width and height columns by space,
    round all items in resulting list and converts to floats
    """
    return [round(float(n), 2) for n in str(item).split(" ")]


def _get_max_depth(row):
    """Calculate difference between initial waterlevel and reference level"""
    return round(float(row[initial_waterlevel_col]) - float(row[reference_level_col]), 2)


def _add_damo_info(layer, gdf):
    try:
        damo_gdb = layer.load()
        new_gdf = gdf.merge(
            damo_gdb[DAMO_FIELDS],
            how="left",
            left_on=a_chan_bed_struct_code,
            right_on=DAMO_LINK_ON,
        )
        new_gdf.rename(
            columns={
                "HOOGTEBINNENONDERKANTBENE": height_inner_lower_down,
                "HOOGTEBINNENONDERKANTBOV": height_inner_lower_up,
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
            datachecker_gdb[DATACHECKER_FIELDS],
            how="left",
            left_on=a_chan_bed_struct_code,
            right_on=DATACHECKER_LINK_ON,
        )
        new_gdf.rename(
            columns={DATACHECKER_ASSUMPTION_FIELD: datachecker_assumption_alias},
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
        exploded = df.set_index([peil_id_col])[geometry_col]
        exploded = exploded.explode(index_parts=True)
        exploded = exploded.reset_index()
        exploded = exploded.rename(columns={0: geometry_col, "level_1": "multipolygon_level"})
        merged = exploded.merge(df.drop(geometry_col, axis=1), left_on=peil_id_col, right_on=peil_id_col)
        merged = merged.set_geometry(geometry_col, crs=df.crs)
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
        group = joined.groupby([peil_id_col, "multipolygon_level"])[storage_area_col].sum()
        # Add the aggregated area column to the original dataframe
        fixeddrainage = fixeddrainage.merge(group, how="left", on=[peil_id_col, "multipolygon_level"])
        fixeddrainage.rename(columns={storage_area_col: watersurface_nodes_area}, inplace=True)
        return fixeddrainage
    except Exception as e:
        raise e from None


def _add_waterdeel(fixeddrainage, to_add):
    try:
        # create dataframe containing overlaying geometry
        overl = gpd.overlay(fixeddrainage, to_add, how="intersection")
        # add column containing size of overlaying areas
        overl["area"] = overl[geometry_col].area
        # group overlaying area gdf by id's
        overl = overl.groupby([peil_id_col, "multipolygon_level"])["area"].sum()
        # merge overlapping area size into fixeddrainage
        merged = fixeddrainage.merge(overl, how="left", on=[peil_id_col, "multipolygon_level"])
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
    self = HhnkSchematisationChecks(folder=folder)
    database = folder.model.schema_base.database
    # a, b = self.run_weir_floor_level(database=database)
    a, b = self.run_watersurface_area()

    # TODO self.create_grid_from_schematisation(output_folder=folder.output.base)
    # self.verify_inputs("run_imp_surface_area")


# %%


# %%
if __name__ == "__main__":
    from hhnk_threedi_tools.core.folders import Folders
    from hhnk_threedi_tools.core.schematisation.relations import StructureRelations
    from tests.config import TEST_DIRECTORY

    folder = Folders(TEST_DIRECTORY / "model_test")
    # database = folder.model.schema_base.database
    self = StructureRelations(folder=folder, structure_table="weir")
    self.structure_table = "weir"
    structure_table = "weir"
    side = "end"
    self.relations()


# %%
