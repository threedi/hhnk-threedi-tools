# -*- coding: utf-8 -*-
"""
Created on Tue Aug 24 14:11:45 2021

@author: chris.kerklaan
"""
# First-party imports
import os

# Third-party imports
import numpy as np
import geopandas as gpd
import pandas as pd
from shapely import wkt

# research-tools
import hhnk_research_tools as hrt
from hhnk_research_tools.variables import OPEN_FILE_GDB_DRIVER, ESRI_DRIVER, GPKG

# Local imports
from hhnk_threedi_tools.core.folders import Folders, create_tif_path
from threedigrid_builder import make_gridadmin


# %%
# queries
from hhnk_threedi_tools.utils.queries import (
    controlled_structures_query,
    geometry_check_query,
    impervious_surface_query,
    isolated_channels_query,
    profiles_used_query,
    struct_channel_bed_query,
    watersurface_conn_node_query,
    weir_height_query,
    cross_section_location_query,
    channels_query,
)
from hhnk_threedi_tools.utils.queries_general_checks import ModelCheck

# variables
from hhnk_threedi_tools.variables.sqlite import (
    watersurface_nodes_area,
    watersurface_waterdeel_area,
    watersurface_channels_area,
    watersurface_model_area,
    area_diff_col,
    area_diff_perc,
    down_has_assumption,
    up_has_assumption,
    height_inner_lower_down,
    height_inner_lower_up,
    datachecker_assumption_alias,
    primary_col,
    water_level_width_col,
    max_depth_col,
    length_in_meters_col,
)
from hhnk_threedi_tools.variables.weirs import (
    min_crest_height,
    diff_crest_ref,
    wrong_profile,
    new_ref_lvl,
)

from hhnk_threedi_tools.variables.definitions import (
    DEM_MAX_VALUE,
    channels_isolated_calc_type,
)

from hhnk_threedi_tools.variables.database_variables import (
    id_col,
    calculation_type_col,
    width_col,
    height_col,
    initial_waterlevel_col,
    storage_area_col,
    target_type_col,
    reference_level_col,
    cross_sec_loc_layer,
    action_col,
    weir_layer,
    code_col,
)


from hhnk_threedi_tools.variables.database_aliases import (
    a_geo_end_coord,
    a_geo_end_node,
    a_geo_start_coord,
    a_geo_start_node,
    a_zoom_cat,
    a_chan_bed_struct_id,
    a_chan_bed_struct_code,
    a_watersurf_conn_id,
    a_weir_code,
    a_weir_conn_node_start_id,
    a_weir_conn_node_end_id,
    a_weir_cross_loc_id,
    a_chan_id,
    df_geo_col,
)

from hhnk_threedi_tools.variables.datachecker_variables import (
    peil_id_col,
    streefpeil_bwn_col,
    geometry_col,
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


class SqliteTest:
    def __init__(
        self,
        folder: Folders,
        # model_path=None,
        # dem_path=None,
        # datachecker_path=None,
        # damo_path=None,
        # hdb_path=None,
        # polder_polygon_path=None,
        # channels_from_profiles_path=None,
    ):
        self.fenv = folder

        self.model = self.fenv.model.schema_base.database.path
        self.dem = self.fenv.model.schema_base.rasters.dem.path
        self.datachecker = self.fenv.source_data.datachecker.path
        self.damo = self.fenv.source_data.damo.path
        self.hdb_path = self.fenv.source_data.hdb.path
        self.polder_polygon = self.fenv.source_data.polder_polygon.path
        self.channels_from_profiles = (
            self.fenv.source_data.modelbuilder.channel_from_profiles.path
        )
        self.datachecker_fixeddrainage = (
            self.fenv.source_data.datachecker_fixed_drainage
        )

        self.results = {}

    # @classmethod
    # def from_path(cls, path_to_polder, **kwargs):
    #     return cls(Folders(path_to_polder), **kwargs)

    def run_controlled_structures(self):
        """
        Deze test selecteert alle gestuurde kunstwerken (uit de v2_culvert, v2_orifice en v2_weir tafels van het model) op
        basis van de v2_control_table tafel. Per kunstwerk worden actiewaarden opgevraagd. Per gevonden gestuurd kunstwerk
        wordt ook relevante informatie uit de HDB database toegevoegd, zoals het streefpeil en minimale en maximale kruin
        hoogtes.
        """
        hdb_layer = str(self.fenv.source_data.hdb_sturing_3di_layer)

        try:
            # TODO sqlite_table_to_gdf better application?
            model_control_db = hrt.execute_sql_selection(
                query=controlled_structures_query, database_path=self.model
            )
            model_control_gdf = hrt.df_convert_to_gdf(df=model_control_db)
            model_control_gdf[
                [START_ACTION, MIN_ACTION, MAX_ACTION]
            ] = model_control_gdf.apply(get_action_values, axis=1, result_type="expand")
            hdb_stuw_gdf = gpd.read_file(
                self.hdb_path, driver=OPEN_FILE_GDB_DRIVER, layer=hdb_layer
            )[["CODE", "STREEFPEIL", "MIN_KRUINHOOGTE", "MAX_KRUINHOOGTE"]]
            hdb_stuw_gdf.rename(
                columns={
                    "CODE": code_col,
                    "STREEFPEIL": HDB_STREEFPEIL,
                    "MIN_KRUINHOOGTE": HDB_KRUIN_MIN,
                    "MAX_KRUINHOOGTE": HDB_KRUIN_MAX,
                },
                inplace=True,
            )
            control_final = model_control_gdf.merge(
                hdb_stuw_gdf, on=code_col, how="left"
            )
            self.results["controlled_strucures_test"] = control_final
            return control_final
        except Exception as e:
            raise e from None

    def run_dem_max_value(self):
        try:
            dem_array, dem_nodata, dem_metadata = hrt.load_gdal_raster(self.dem)
            if np.max(dem_array) > DEM_MAX_VALUE:
                result = f"Maximale waarde DEM: {np.max(dem_array)} is te hoog"
            else:
                result = f"Maximale waarde DEM: {np.max(dem_array)} voldoet aan de norm"
            self.results["dem_max_value"] = result
            return result
        except Exception as e:
            raise e from None

    def run_dewatering_depth(self, output_file):
        """
        Compares initial water level from fixed drainage level areas with
        surface level in DEM of model. Initial water level should be below
        surface level.
        """
        # This add .tif extension to output file name, is needed for save_raster_array_to_tif function

        init_waterlevel_value_field = self.fenv.source_data.init_waterlevel_val_field

        try:
            # Load layers
            fixeddrainage = gpd.read_file(
                self.datachecker,
                driver=OPEN_FILE_GDB_DRIVER,
                layer=self.datachecker_fixeddrainage,
            )
            dem_array, dem_nodata, dem_metadata = hrt.load_gdal_raster(self.dem)
            # Rasterize fixeddrainage
            initial_water_level_arr = hrt.gdf_to_raster(
                gdf=fixeddrainage,
                value_field=init_waterlevel_value_field,
                raster_out="",
                nodata=dem_nodata,
                metadata=dem_metadata,
                driver="MEM",
            )
            dewatering_array = np.subtract(dem_array, initial_water_level_arr)
            # restore nodata pixels using mask
            nodata_mask = dem_array == dem_nodata

            dewatering_array[nodata_mask] = dem_nodata
            # Save array to raster
            hrt.save_raster_array_to_tiff(
                output_file=output_file,
                raster_array=dewatering_array,
                nodata=dem_nodata,
                metadata=dem_metadata,
            )
            self.results["dewatering_depth"] = output_file
            return output_file
        except Exception as e:
            raise e from None

    def run_model_checks(self):
        """
        Collects all queries that are part of general model checks (see general_checks_queries file)
        and executes them
        """
        try:
            queries_lst = [item for item in vars(ModelCheck()).values()]
            query = "UNION ALL\n".join(queries_lst)
            db = hrt.execute_sql_selection(
                query=query, database_path=self.model
            )  # , index_col=id_col #door index_col mee te geven verwdijnt deze.

            self.results["model_checks"] = db
            return db
        except Exception as e:
            raise e from None

    def run_geometry_checks(self):
        """
        Deze test checkt of de geometrie van een object in het model correspondeert met de start- of end node in de
        v2_connection_nodes tafel. Als de verkeerde ids worden gebruikt geeft dit fouten in het model.
        """
        try:
            query = geometry_check_query
            gdf = hrt.sqlite_table_to_gdf(
                query=query, database_path=self.model, id_col=id_col
            )
            gdf["start_check"] = gdf[a_geo_start_node] == gdf[a_geo_start_coord]
            gdf["end_check"] = gdf[a_geo_end_node] == gdf[a_geo_end_coord]
            add_distance_checks(gdf)
            # Only rows where at least one of start_dist_ok and end_dist_ok is false
            result_db = gdf[~gdf[["start_dist_ok", "end_dist_ok"]].all(axis=1)]
            if not result_db.empty:
                result_db["error"] = "Error: mismatched geometry"
            self.results["geometry_checks"] = result_db
            return result_db
        except Exception as e:
            raise e from None

    def run_imp_surface_area(self):
        """
        Calculates the impervious surface area (in the model), the area of the polder (based on the polder shapefile) and
        the difference between the two.
        """

        try:
            imp_surface_db = hrt.execute_sql_selection(
                query=impervious_surface_query,
                database_path=self.model,
                index_col=id_col,
            )  # conn=test_params.conn, index_col=DEFAULT_INDEX_COLUMN
            polygon_imp_surface = gpd.read_file(
                self.polder_polygon, driver=ESRI_DRIVER
            )  # read_file(polder_file, driver=ESRI_DRIVER)
            db_surface, polygon_surface, area_diff = calc_surfaces_diff(
                imp_surface_db, polygon_imp_surface
            )
            result_txt = (
                f"Totaal ondoorlatend oppervlak: {db_surface} ha\n"
                f"Gebied polder: {polygon_surface} ha\n"
                f"Verschil: {area_diff} ha\n"
            )
            self.results["imp_surface_area"] = result_txt
            return result_txt
        except Exception as e:
            raise e from None

    def run_isolated_channels(self):
        """
        Test bepaalt welke watergangen niet zijn aangesloten op de rest van de watergangen. Deze watergangen worden niet
        meegenomen in de uitwisseling in het watersysteem. De test berekent tevens de totale lengte van watergangen en welk
        deel daarvan geïsoleerd is.
        """
        try:
            channels_df = hrt.execute_sql_selection(
                query=isolated_channels_query, database_path=self.model
            )
            channels_gdf = hrt.df_convert_to_gdf(df=channels_df)
            channels_gdf[length_in_meters_col] = round(
                channels_gdf[df_geo_col].length, 2
            )
            (
                isolated_channels_gdf,
                isolated_length,
                total_length,
                percentage,
            ) = calc_len_percentage(channels_gdf)
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
        except Exception as e:
            raise e from None

    def run_used_profiles(self):
        """
        Koppelt de v2_cross_section_definition laag van het model (discrete weergave van de natuurlijke geometrie van de
        watergangen) aan de v2_channel laag (informatie over watergangen in het model). Het resultaat van deze toets is een
        weergave van de breedtes en dieptes van watergangen in het model ter controle.
        """
        try:

            # TODO use hrt.sqlite_table_to_gdf instead?
            channels_df = hrt.execute_sql_selection(
                query=profiles_used_query, database_path=self.model
            )
            channels_gdf = hrt.df_convert_to_gdf(df=channels_df)
            # If zoom category is 4, channel is considered primary
            channels_gdf[primary_col] = channels_gdf[a_zoom_cat].apply(
                lambda zoom_cat: zoom_cat == 4
            )
            channels_gdf[width_col] = channels_gdf[width_col].apply(split_round)
            channels_gdf[height_col] = channels_gdf[height_col].apply(split_round)
            channels_gdf[water_level_width_col] = channels_gdf.apply(
                func=calc_width_at_waterlevel, axis=1
            )
            channels_gdf[max_depth_col] = channels_gdf.apply(func=get_max_depth, axis=1)
            # Conversion to string because lists are not valid for storing in gpkg
            channels_gdf[width_col] = channels_gdf[width_col].astype(str)
            channels_gdf[height_col] = channels_gdf[height_col].astype(str)
            self.results["used_profiles"] = channels_gdf
            return channels_gdf
        except Exception as e:
            raise e from None

    def run_struct_channel_bed_level(self):
        """
        Checks whether the reference level of any of the adjacent cross section locations (channels) to a structure
        is lower than the reference level for that structure (3di crashes if it is)
        """
        datachecker_culvert_layer = str(self.fenv.source_data.datachecker_culvert)
        damo_duiker_sifon_layer = str(self.fenv.source_data.damo_duiker_sifon_layer)

        try:
            below_ref_query = struct_channel_bed_query
            gdf_below_ref = hrt.sqlite_table_to_gdf(
                query=below_ref_query,
                id_col=a_chan_bed_struct_id,
                database_path=self.model,
            )
            # See git issue about below statements
            gdf_with_damo = add_damo_info(
                damo_path=self.damo, layer=damo_duiker_sifon_layer, gdf=gdf_below_ref
            )
            gdf_with_datacheck = add_datacheck_info(
                self.datachecker, datachecker_culvert_layer, gdf_with_damo
            )
            gdf_with_datacheck.loc[:, down_has_assumption] = gdf_with_datacheck[
                height_inner_lower_down
            ].isna()
            gdf_with_datacheck.loc[:, up_has_assumption] = gdf_with_datacheck[
                height_inner_lower_up
            ].isna()
            self.results["struct_channel_bed_level"] = gdf_with_datacheck
            return gdf_with_datacheck
        except Exception as e:
            raise e from None

    def run_watersurface_area(self):
        """
        Deze test controleert per peilgebied in het model hoe groot het gebied is dat het oppervlaktewater beslaat in het
        model. Dit totaal is opgebouwd uit de ```storage_area``` uit de ```v2_connection_nodes``` tafel opgeteld bij het
        oppervlak van de watergangen (uitgelezen uit de ```channel_surface_from_profiles```) shapefile. Vervolgens worden de
        totalen per peilgebied vergeleken met diezelfde totalen uit de DAMO database.

        De kolom namen in het resultaat zijn als volgt:
        From v2_connection_nodes -> area_nodes_m2
        From channel_surface_from_profiles -> area_channels_m2
        From DAMO -> area_waterdeel_m2
        """

        try:
            (
                fixeddrainage,
                modelbuilder_waterdeel,
                damo_waterdeel,
                conn_nodes_geo,
            ) = read_input(
                database_path=self.model,
                datachecker_path=self.datachecker,
                channel_profile_path=self.channels_from_profiles,
                damo_path=self.damo,
                datachecker_layer=self.datachecker_fixeddrainage,
                damo_layer=self.fenv.source_data.damo_waterdeel_layer,
            )
            fixeddrainage = calc_area(
                fixeddrainage, modelbuilder_waterdeel, damo_waterdeel, conn_nodes_geo
            )
            result_txt = """Gebied open water BGT: {} ha\nGebied open water model: {} ha""".format(
                round(fixeddrainage.sum()[watersurface_waterdeel_area] / 10000, 2),
                round(fixeddrainage.sum()[watersurface_model_area] / 10000, 2),
            )
            self.results["watersurface_area"] = {
                "fixeddrainage": fixeddrainage,
                "result_txt": result_txt,
            }
            return fixeddrainage, result_txt
        except Exception as e:
            raise e from None

    def run_weir_floor_level(self):
        """
        Check whether minimum crest height of weir is under reference level found in the v2_cross_section_location layer.
        This is not allowed, so if this is the case, we have to update the reference level.
        """
        try:
            # TODO use hrt.sqlite_table_to_gdf instead?
            weirs_df = hrt.execute_sql_selection(
                query=weir_height_query, database_path=self.model
            )
            weirs_gdf = hrt.df_convert_to_gdf(df=weirs_df)
            # Bepaal de minimale kruinhoogte uit de action table
            weirs_gdf[min_crest_height] = [
                min([float(b.split(";")[1]) for b in a.split("#")])
                for a in weirs_gdf[action_col]
            ]
            # Bepaal het verschil tussen de minimale kruinhoogte en reference level.
            weirs_gdf[diff_crest_ref] = (
                weirs_gdf[min_crest_height] - weirs_gdf[reference_level_col]
            )
            # Als dit verschil negatief is, betekent dit dat de bodem hoger ligt dan de minimale hoogte van de stuw.
            # Dit mag niet, en daarom moet er iets aan het bodemprofiel gebeuren.
            weirs_gdf[wrong_profile] = weirs_gdf[diff_crest_ref] < 0
            # Add proposed new reference levels
            weirs_gdf.loc[weirs_gdf[wrong_profile] == 1, new_ref_lvl] = round(
                weirs_gdf.loc[weirs_gdf[wrong_profile] == 1, min_crest_height] - 0.01, 2
            )
            wrong_profiles_gdf = weirs_gdf[weirs_gdf[wrong_profile]][OUTPUT_COLS]
            update_query = hrt.sql_create_update_case_statement(
                df=wrong_profiles_gdf,
                layer=cross_sec_loc_layer,
                df_id_col=a_weir_cross_loc_id,
                db_id_col=id_col,
                new_val_col=new_ref_lvl,
                old_val_col=reference_level_col,
            )
            self.results["weir_floor_level"] = {
                "wrong_profiles_gdf": wrong_profiles_gdf,
                "update_query": update_query,
            }
            return wrong_profiles_gdf, update_query
        except Exception as e:
            raise e from None

    def create_grid_from_sqlite(self, sqlite_path, dem_path, output_folder):
        """Create grid from sqlite, this includes cells, lines and nodes."""
        grid = make_gridadmin(
            sqlite_path, dem_path
        )  # using output here results in error, so we use the returned dict

        for i in ["cells", "lines", "nodes"]:
            _write_grid_to_file(
                grid=grid,
                grid_type=i,
                output_path=os.path.join(output_folder, f"{i}.gpkg"),
            )

    def run_cross_secction (self):

        try:
            cross_section_df = hrt.execute_sql_selection(
                query =cross_section_location_query , database_path=self.model
            )

            cross_section_point = hrt.df_convert_to_gdf(df=cross_section_df)
            cross_section_buffer_gdf = cross_section_point
            cross_section_buffer_gdf["geometry"] = cross_section_buffer_gdf.buffer(0.5) 
            
            cross_section_join =  gpd.sjoin(cross_section_buffer_gdf, cross_section_point,
                              how="inner", op="intersects")
            cross_section_join['new'] = np.where((cross_section_join["channel_id_right"]==cross_section_join["channel_id_left"]), cross_section_join['cross_loc_id_left'], np.nan)
            cross_section_loc_id = cross_section_join.set_index('new')
            cross_sec_id = [] 
            counter = []
            cross_location_id = []
            
            for id in cross_section_loc_id.index:
                print(id)
                cross_sec_id.append(id)
            for _ in cross_sec_id:    
                cross_location_id.append(_)
                count = cross_sec_id.count(_)
                counter.append(count)

            gdf_data = gpd.GeoDataFrame({"cross_loc_id_left":cross_location_id, 'count': counter})
            gdf_data =  gdf_data.drop_duplicates(['cross_loc_id_left','count'],keep = 'last')
            gdf_data = gdf_data[gdf_data['count']> 1 ]
            list_id = gdf_data.cross_loc_id_left.values.tolist()
            cross_section_warning = cross_section_point[cross_section_point['cross_loc_id'].isin(list_id)]
            return cross_section_warning 
        except Exception as e:
            raise e from None


    def run_cross_secction_vertex (self):

        try:
            cross_section_point = hrt.execute_sql_selection(
                query =cross_section_location_query , database_path=self.model
            )
            channels = hrt.execute_sql_selection(
                query=channels_query, database_path=self.model)
            cross_section_point = hrt.df_convert_to_gdf(df=cross_section_point)
            channels_gdf = hrt.df_convert_to_gdf(df=channels)
            coord_x = []
            coord_y = []
            idxs=[]
            for idx, row in channels_gdf.iterrows():
                # ignore start and end vertex of channel lines
                points_string_xy= row.geometry.xy
                cnt = len(points_string_xy[0])
                for i, idx1 in enumerate(points_string_xy[0]):
                    if i == 0 or i==cnt-1:
                        continue
                    coord_x.append(idx1)
                for i, idx1 in enumerate(points_string_xy[1]):
                    if i == 0 or i==cnt-1:
                        continue
                    coord_y.append(idx1)
                    idxs.append(row["channel_id"])
            coordinates_dataframe = pd.DataFrame(data = {'x':coord_x, 'y':coord_y, "channel_id":idxs})
            #All vertices of all channels to dataframe, buffer by x meters.
            vertices_buffer = gpd.GeoDataFrame(coordinates_dataframe, geometry =gpd.points_from_xy(coordinates_dataframe.x, coordinates_dataframe.y, crs="EPSG:28992"))
            vertices_buffer["geometry"] = vertices_buffer.buffer(0.05)
            vertices_buffer.rename({"geometry": "geometry_line"}, axis=1, inplace=True)
            cross_section_point.rename({"geometry": "geometry_point"}, axis=1, inplace=True)
            # merge cross section and channel vertices on channels.
            vertices_cross_merge = pd.merge(vertices_buffer, cross_section_point, left_on="channel_id", right_on="channel_id", how="left")
            #Check if the cross section is within the buffered distance of a vertex
            vertices_cross_intersect=vertices_cross_merge[gpd.GeoSeries.intersects(gpd.GeoSeries(vertices_cross_merge["geometry_line"]), gpd.GeoSeries(vertices_cross_merge["geometry_point"]))]
            #Select cross sections that do not have a vertex within buffered distance
            cross_no_vertex = cross_section_point[~cross_section_point["cross_loc_id"].isin(vertices_cross_intersect["cross_loc_id"].values)]
            cross_no_vertex = gpd.GeoDataFrame(cross_no_vertex, geometry="geometry_point")
            return cross_no_vertex
        except Exception as e:
            raise e from None

## helper functions
def get_action_values(row):
    if row[target_type_col] is weir_layer:
        action_values = [float(b.split(";")[1]) for b in row[action_col].split("#")]
    else:
        action_values = [
            float(b.split(";")[1].split(" ")[0]) for b in row[action_col].split("#")
        ]
    return action_values[0], min(action_values), max(action_values)


def add_distance_checks(gdf):
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


def calc_surfaces_diff(db_imp_surface, polygon_imp_surface):
    db_surface = int(db_imp_surface.sum() / 10000)
    polygon_surface = int(polygon_imp_surface.area.values[0] / 10000)
    area_diff = db_surface - polygon_surface
    return db_surface, polygon_surface, area_diff


def calc_len_percentage(channels_gdf):
    total_length = round(channels_gdf.geometry.length.sum() / 1000, 2)
    isolated_channels_gdf = channels_gdf[
        channels_gdf[calculation_type_col] == channels_isolated_calc_type
    ]
    if not isolated_channels_gdf.empty:
        isolated_length = round(isolated_channels_gdf.geometry.length.sum() / 1000, 2)
    else:
        isolated_length = 0
    percentage = round((isolated_length / total_length) * 100, 0)
    return isolated_channels_gdf, isolated_length, total_length, percentage


def calc_width_at_waterlevel(row):
    """Bereken de breedte van de watergang op het streefpeil"""
    x_pos = [b / 2 for b in row[width_col]]
    y = [row.reference_level + b for b in row[height_col]]
    ini = row[initial_waterlevel_col]

    # Interpoleer tussen de x en y waarden (let op: de x en y zijn hier verwisseld)
    width_wl = round(np.interp(ini, xp=y, fp=x_pos), 2) * 2
    return width_wl


def split_round(item):
    """
    Split items in width and height columns by space, round all items in resulting list and converts to floats
    """
    return [round(float(n), 2) for n in str(item).split(" ")]


def get_max_depth(row):
    """
    calculates difference between initial waterlevel and reference level
    """
    return round(
        float(row[initial_waterlevel_col]) - float(row[reference_level_col]), 2
    )


def add_damo_info(damo_path, layer, gdf):
    try:
        damo_gdb = gpd.read_file(damo_path, driver=OPEN_FILE_GDB_DRIVER, layer=layer)
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


def add_datacheck_info(datachecker_path, layer, gdf):
    try:
        datachecker_gdb = gpd.read_file(
            datachecker_path, driver=OPEN_FILE_GDB_DRIVER, layer=layer
        )
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


def expand_multipolygon(df):
    """
    New version using explode, old version returned pandas dataframe not geopandas
    geodataframe (missing last line), I think it works now?
    """
    try:
        exploded = df.set_index([peil_id_col])[geometry_col]
        exploded = exploded.explode()
        exploded = exploded.reset_index()
        exploded = exploded.rename(
            columns={0: geometry_col, "level_1": "multipolygon_level"}
        )
        merged = exploded.merge(
            df.drop(geometry_col, axis=1), left_on=peil_id_col, right_on=peil_id_col
        )
        merged = merged.set_geometry(geometry_col, crs=df.crs)
        return merged
    except Exception as e:
        raise e from None


def read_input(
    database_path,
    datachecker_path,
    channel_profile_path,
    damo_path,
    datachecker_layer,
    damo_layer,
):
    try:
        fixeddrainage = gpd.read_file(
            datachecker_path, layer=datachecker_layer, reader=GPKG
        )[[peil_id_col, code_col, streefpeil_bwn_col, geometry_col]]
        fixeddrainage = expand_multipolygon(fixeddrainage)
        modelbuilder_waterdeel = gpd.read_file(channel_profile_path, driver=ESRI_DRIVER)
        damo_waterdeel = gpd.read_file(damo_path, layer=damo_layer, reader=GPKG)
        conn_nodes_geo = hrt.sqlite_table_to_gdf(
            query=watersurface_conn_node_query,
            id_col=a_watersurf_conn_id,
            database_path=database_path,
        )
        return fixeddrainage, modelbuilder_waterdeel, damo_waterdeel, conn_nodes_geo
    except Exception as e:
        raise e from None


def add_nodes_area(fixeddrainage, conn_nodes_geo):
    try:
        # join on intersection of geometries
        joined = gpd.sjoin(
            fixeddrainage,
            conn_nodes_geo,
            how="left",
            op="intersects",  # FIXME in future pythion version this will be 'predicate'
            lsuffix="fd",
            rsuffix="conn",
        )
        # Combine all rows with same peil_id and multipolygon level and sum their area
        group = joined.groupby([peil_id_col, "multipolygon_level"])[
            storage_area_col
        ].sum()
        # Add the aggregated area column to the original dataframe
        fixeddrainage = fixeddrainage.merge(
            group, how="left", on=[peil_id_col, "multipolygon_level"]
        )
        fixeddrainage.rename(
            columns={storage_area_col: watersurface_nodes_area}, inplace=True
        )
        return fixeddrainage
    except Exception as e:
        raise e from None


def add_waterdeel(fixeddrainage, to_add):
    try:
        # create dataframe containing overlaying geometry
        overl = gpd.overlay(fixeddrainage, to_add, how="intersection")
        # add column containing size of overlaying areas
        overl["area"] = overl[geometry_col].area
        # group overlaying area gdf by id's
        overl = overl.groupby([peil_id_col, "multipolygon_level"])["area"].sum()
        # merge overlapping area size into fixeddrainage
        merged = fixeddrainage.merge(
            overl, how="left", on=[peil_id_col, "multipolygon_level"]
        )
        merged["area"] = round(merged["area"], 0)
        merged["area"] = merged["area"].fillna(0)
    except Exception as e:
        raise e from None
    return merged


def calc_perc(diff, waterdeel):
    try:
        return round((diff / waterdeel) * 100, 1)
    except:
        if diff == waterdeel:
            return 0.0
        else:
            return 100.0


def calc_area(fixeddrainage, modelbuilder_waterdeel, damo_waterdeel, conn_nodes_geo):
    try:
        fixeddrainage = add_nodes_area(fixeddrainage, conn_nodes_geo)
        fixeddrainage = add_waterdeel(fixeddrainage, damo_waterdeel)
        fixeddrainage.rename(
            columns={"area": watersurface_waterdeel_area}, inplace=True
        )
        fixeddrainage = add_waterdeel(fixeddrainage, modelbuilder_waterdeel)
        fixeddrainage.rename(columns={"area": watersurface_channels_area}, inplace=True)
        fixeddrainage[watersurface_model_area] = (
            fixeddrainage[watersurface_channels_area]
            + fixeddrainage[watersurface_nodes_area]
        )
        fixeddrainage[area_diff_col] = (
            fixeddrainage[watersurface_model_area]
            - fixeddrainage[watersurface_waterdeel_area]
        )
        fixeddrainage[area_diff_perc] = fixeddrainage.apply(
            lambda row: calc_perc(row[area_diff_col], row[watersurface_waterdeel_area]),
            axis=1,
        )
        return fixeddrainage
    except Exception as e:
        raise e from None


def _write_grid_to_file(grid, grid_type, output_path):
    df = pd.DataFrame(grid[grid_type])
    gdf = hrt.df_convert_to_gdf(df, geom_col_type="wkb", src_crs="28992")
    hrt.gdf_write_to_geopackage(gdf, filepath=output_path)
