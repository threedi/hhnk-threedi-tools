# -*- coding: utf-8 -*-
"""
Created on Mon Aug 16 12:04:33 2021

@author: chris.kerklaan

All sqlite queries are stored within this script. Queries are listed per table.


"""
# Third-part imports
import numpy as np
import hhnk_research_tools as hrt

# Local imports - starts
# Note that this is not preffered due to ambiguity, however due to the large amount of queries it is practical
from hhnk_threedi_tools.variables.database_variables import *
from hhnk_threedi_tools.variables.database_aliases import *

from hhnk_threedi_tools.variables.bank_levels import (
    new_bank_level_col,
)
from hhnk_threedi_tools.variables.weirs import new_ref_lvl

from hhnk_threedi_tools.variables.default_variables import DEF_TRGT_CRS

from hhnk_threedi_tools.variables.model_state import (
    hydraulic_test_state,
    one_d_two_d_state,
    manholes_new_calc_type,
    weirs_new_width_col,
    channels_new_calc_type,
    global_settings_new_col_name,
)

from hhnk_threedi_tools.variables.backups_table_names import (
    BANK_LVLS_LAST_CALC,
    GLOBAL_SETTINGS_TABLE,
    CONTR_WEIR_WIDTH_BACKUP,
)

# Strings
# Global settings
all_global_settings = f"""
    SELECT * FROM {GLOBAL_SETTINGS_TABLE}
    WHERE {{}}
    """

# Connection nodes
conn_nodes_query = f"""
    SELECT {id_col} as {a_conn_node_id}, \
    {initial_waterlevel_col}, \
    {storage_area_col}, \
    {f_aswkt}({geo_col}) as {df_geo_col} \
    FROM {connection_nodes_layer}
    """

watersurface_conn_node_query = f"""
    SELECT {id_col} as {a_watersurf_conn_id},
    {initial_waterlevel_col},
    {storage_area_col},
    {f_aswkt}({geo_col}) as {df_geo_col}
    FROM {connection_nodes_layer}
    """

# Manhole
manholes_query = f"""
    SELECT
    {manhole_layer}.{id_col} as {a_man_id},
    {manhole_layer}.{conn_node_id_col} as {a_man_conn_id},
    {manhole_layer}.{code_col},
    {manhole_layer}.{drain_level_col},
    {connection_nodes_layer}.{initial_waterlevel_col},
    {connection_nodes_layer}.{storage_area_col},
    {f_aswkt}({connection_nodes_layer}.{geo_col}) as {df_geo_col}
    FROM {manhole_layer}
    LEFT JOIN {connection_nodes_layer}
    ON {manhole_layer}.{conn_node_id_col} == {connection_nodes_layer}.{id_col}
    """

# channels
channels_query = f"""
    SELECT \
    {channels_layer}.{id_col} as {a_chan_id},
    {a_conn_nodes}.{initial_waterlevel_col}, {f_aswkt}({channels_layer}.{geo_col}) as {df_geo_col}
    FROM {channels_layer}
    LEFT JOIN {connection_nodes_layer} as {a_conn_nodes}
    ON {a_conn_nodes}.{id_col} = {channels_layer}.{conn_node_start_id_col}
    """

cross_section_location_query = f"""
    SELECT
    {id_col} as {a_cross_loc_id},
    {channel_id_col},
    {reference_level_col},
    {bank_level_col}, {f_aswkt}({geo_col}) as {df_geo_col}
    FROM {cross_sec_loc_layer}
    """
profiles_used_query = f"""
            SELECT 
            {channels_layer}.{id_col} AS {a_chan_id},
            {channels_layer}.{code_col} AS {a_chan_code},
            {channels_layer}.{conn_node_end_id_col} AS {a_chan_node_id},
            {channels_layer}.{zoom_cat_col} AS {a_zoom_cat},
            {f_aswkt}({channels_layer}.{geo_col}) as {df_geo_col},
            {cross_sec_loc_layer}.{id_col} as {a_cross_sec_loc_id},
            {cross_sec_loc_layer}.{code_col} as {a_cross_sec_loc_code},
            {cross_sec_loc_layer}.{def_id_col},
            {cross_sec_loc_layer}.{bank_level_col},
            {cross_sec_loc_layer}.{reference_level_col},
            {cross_sec_def_layer}.{id_col} AS {a_cross_sec_def_id},
            {cross_sec_def_layer}.{width_col},
            {cross_sec_def_layer}.{height_col},
            {connection_nodes_layer}.{initial_waterlevel_col}
            FROM {channels_layer}
            LEFT JOIN {cross_sec_loc_layer} ON {cross_sec_loc_layer}.{channel_id_col} = {channels_layer}.{id_col}
            LEFT JOIN {cross_sec_def_layer} ON {cross_sec_loc_layer}.{def_id_col} = {cross_sec_def_layer}.{id_col}
            LEFT JOIN {connection_nodes_layer} ON {connection_nodes_layer}.{id_col} = {channels_layer}.{conn_node_end_id_col}
            GROUP BY {channels_layer}.{id_col}
            """
bank_lvls_source_creation_query = (
    f"create table {BANK_LVLS_LAST_CALC}(id int PRIMARY KEY, dt datetime)"
)
bank_lvls_source_update_query = (
    f"INSERT OR REPLACE INTO {BANK_LVLS_LAST_CALC}\nVALUES(1, current_timestamp)"
)
bank_lvls_last_changed = (
    f"SELECT datetime(dt, 'localtime') AS dt FROM {BANK_LVLS_LAST_CALC} limit 1"
)

isolated_channels_query = f"""
SELECT {id_col},
{calculation_type_col},
{f_aswkt}({geo_col}) as {df_geo_col}
FROM {channels_layer}
"""

## weirs
weir_width_backup_query = f"""
    SELECT * from {cross_sec_def_layer} WHERE {id_col} in (
    SELECT
    {weir_layer}.{cross_def_id_col}
    FROM {weir_layer}
    INNER JOIN {cross_sec_def_layer} ON {weir_layer}.{cross_def_id_col} = {cross_sec_def_layer}.{id_col}
    INNER JOIN {control_table_layer} ON {weir_layer}.{id_col} = {control_table_layer}.{target_id_col}
    )
    """
weir_widths_from_backup_query = f"""
    SELECT
    {weir_layer}.{cross_def_id_col} as {a_weir_cross_def_id},
    {weir_layer}.{code_col} as {a_weir_code},
    {weir_layer}.{id_col} as {a_weir_id},
    {CONTR_WEIR_WIDTH_BACKUP}.{width_col}
    FROM {weir_layer}
    INNER JOIN {CONTR_WEIR_WIDTH_BACKUP} ON {weir_layer}.{cross_def_id_col} = {CONTR_WEIR_WIDTH_BACKUP}.{id_col}
    INNER JOIN {control_table_layer} ON {weir_layer}.{id_col} = {control_table_layer}.{target_id_col}
    """
controlled_weirs_selection_query = f"""
    SELECT
    {weir_layer}.{cross_def_id_col} as {a_weir_cross_def_id},
    {weir_layer}.{code_col} as {a_weir_code},
    {weir_layer}.{id_col} as {a_weir_id},
    {cross_sec_def_layer}.{width_col}
    FROM {weir_layer}
    INNER JOIN {cross_sec_def_layer} ON {weir_layer}.{cross_def_id_col} = {cross_sec_def_layer}.{id_col}
    INNER JOIN {control_table_layer} ON {weir_layer}.{id_col} = {control_table_layer}.{target_id_col}
    """

weir_height_query = f"""
    SELECT w.{code_col} as {a_weir_code},
    w.{conn_node_start_id_col} as {a_weir_conn_node_start_id},
    w.{conn_node_end_id_col} as {a_weir_conn_node_end_id},
    {control_table_layer}.{action_col},
    {channels_layer}.{conn_node_start_id_col} as {a_weir_chan_conn_start_id},
    {channels_layer}.{conn_node_end_id_col} as {a_weir_chan_conn_end_id},
    {channels_layer}.{id_col} as {a_chan_id},
    {cross_sec_loc_layer}.{reference_level_col},
    {cross_sec_loc_layer}.{id_col} as {a_weir_cross_loc_id},
    {f_aswkt}({cross_sec_loc_layer}.{geo_col}) as {df_geo_col}
    FROM {weir_layer} as w
        INNER JOIN {control_table_layer}
        ON w.{id_col} = {control_table_layer}.{target_id_col}
        LEFT JOIN {channels_layer}
        ON (w.{conn_node_end_id_col} in ({channels_layer}.{conn_node_start_id_col}, {channels_layer}.{conn_node_end_id_col})
        OR w.{conn_node_start_id_col} in ({channels_layer}.{conn_node_start_id_col}, {channels_layer}.{conn_node_end_id_col}))
        AND {channels_layer}.{id_col}
        LEFT JOIN {cross_sec_loc_layer}
        ON {channels_layer}.{id_col} = {cross_sec_loc_layer}.{channel_id_col}
    WHERE {control_table_layer}.{target_type_col} = '{weir_layer}'
    """

# Water surface

# Impervious surface
impervious_surface_query = f"""
SELECT {area_col},
{id_col}
FROM {impervious_surface_layer};
"""
# Geometry
geometry_check_query_base = f"""\
    SELECT *
    FROM (
        SELECT {{table}}.{id_col},
        '{{table}}' as table_name,
        {{table}}.{conn_node_start_id_col},
        {{table}}.{conn_node_end_id_col},
        {f_aswkt}({f_transform}({{table}}.{geo_col}, {{projection}})) as {df_geo_col},
        {f_aswkt}({f_transform}({f_pointn}({{table}}.{geo_col}, 1), {{projection}})) as {a_geo_start_coord},
        {f_aswkt}({f_transform}({f_pointn}({{table}}.{geo_col}, {f_numpoints}({{table}}.{geo_col})), {{projection}})) as {a_geo_end_coord},
        {f_aswkt}({f_transform}(connection_nodes_start.{geo_col}, {{projection}})) as {a_geo_start_node},
        {f_aswkt}({f_transform}(connection_nodes_end.{geo_col}, {{projection}})) as {a_geo_end_node}
        FROM
        {{table}}
        LEFT JOIN
        {connection_nodes_layer} as {a_geo_conn_nodes_end}
        ON
        connection_nodes_end.{id_col} IS {{table}}.{conn_node_end_id_col}
        LEFT JOIN
        {connection_nodes_layer} as {a_geo_conn_nodes_start}
        ON
        connection_nodes_start.{id_col} IS {{table}}.{conn_node_start_id_col}
    )
    WHERE {a_geo_start_node} IS NOT {a_geo_start_coord} OR {a_geo_end_node} IS NOT {a_geo_end_coord}
    """


# Functions
# Global settings
def create_global_settings_from_backup_query(to_state):
    where_clause = f"{name_col} {{}} '{zero_d_one_d_val}'"
    if to_state == hydraulic_test_state:
        where_clause = where_clause.format("==")
    elif to_state == one_d_two_d_state:
        where_clause = where_clause.format("!=")
    query = all_global_settings.format(where_clause)
    return query


def create_global_settings_rows_update_query(
    excluded_ids=[], ids_to_add=[], ids_to_delete=[]
):
    """
    Add rows from backup, delete rows from model
    Once the correct rows are in the model, set model control
    """
    add_rows_query = f"""
    INSERT INTO {global_settings_layer}
    SELECT *
    FROM {GLOBAL_SETTINGS_TABLE}
    WHERE {id_col} in ({{}})
    AND {id_col} NOT IN ({', '.join(map(str, excluded_ids))})
    """

    delete_rows_query = f"""
    DELETE FROM {global_settings_layer}
    WHERE {id_col} IN ({{}})
    AND {id_col} NOT IN ({', '.join(map(str, excluded_ids))})
    """
    query_list = []
    if ids_to_add:
        query_list.append(add_rows_query.format(", ".join(map(str, ids_to_add))))
    if ids_to_delete:
        query_list.append(delete_rows_query.format(", ".join(map(str, ids_to_delete))))
    query = ";\n".join(query_list)
    return query


def construct_global_settings_control_group_query(
    global_settings_to_update_df, excluded_ids=[]
):
    query = hrt.sql_create_update_case_statement(
        df=global_settings_to_update_df,
        layer=global_settings_layer,
        df_id_col=id_col,
        db_id_col=id_col,
        old_val_col=control_group_col,
        new_val_col=global_settings_new_col_name,
        excluded_ids=excluded_ids,
    )
    return query


# Manholes
def create_new_manholes_query(new_manholes_df, excluded_ids):
    """Maak sql statement dat gebruikt wordt om manholes te maken op de boven en benendenstroomse connection nodes
    van kunstwerken op peilgrens. Omdat deze manholes isolated zijn is er geen stroming meer over de peilgrens nodig.
    Ook wordt een storage area toegevoegd aan de connection nodes waar manholes aan worden toegevoegd als die nog
    niet gespecificeerd is
    """
    query = ""
    query += (
        f"INSERT INTO {manhole_layer} "
        f"({display_name_col}, {code_col}, {conn_node_id_col}, {shape_col},"
        f"{width_col}, {manhole_indicator_col}, {calculation_type_col}, {drain_level_col}, "
        f"{bottom_lvl_col}, {surface_lvl_col}, {zoom_cat_col})\n"
    )
    query += "VALUES "
    sql_body = []
    for index, row in new_manholes_df.iterrows():
        if row[conn_node_id_col] not in excluded_ids:
            sql_body.append(
                f"('{row[display_name_col]}', '{row[code_col]}', {row[conn_node_id_col]}, '{row[shape_col]}', "
                f"{row[width_col]}, {row[manhole_indicator_col]}, {row[calculation_type_col]}, {row[drain_level_col]}, "
                f"{row[bottom_lvl_col]}, {row[surface_lvl_col]}, {row[zoom_cat_col]})"
            )
    if not sql_body:
        return None
    else:
        query += ",\n".join(sql_body) + ";"
        query += create_update_storage_area_sql(new_manholes_df, excluded_ids)
        return query


def construct_manholes_update_query(manholes_to_update_df, excluded_ids=[]):
    try:
        query = hrt.sql_create_update_case_statement(
            df=manholes_to_update_df,
            layer=manhole_layer,
            df_id_col=id_col,
            db_id_col=id_col,
            old_val_col=calculation_type_col,
            new_val_col=manholes_new_calc_type,
            excluded_ids=excluded_ids,
        )
        return query
    except Exception as e:
        raise e from None


def create_update_storage_area_sql(new_manholes_df, excluded_ids):
    update_storage_ids = []
    update_storage_area_rows = new_manholes_df[
        np.isnan(new_manholes_df[storage_area_col])
    ]
    update_storage_area_rows = update_storage_area_rows[
        ~update_storage_area_rows[conn_node_id_col].isin(excluded_ids)
    ]
    if not update_storage_area_rows.empty:
        update_storage_ids = [
            item for item in update_storage_area_rows[conn_node_id_col].tolist()
        ]
    update_storage_area_ids_string = ",".join(map(str, update_storage_ids))
    return f"""
    UPDATE {connection_nodes_layer}
    SET {storage_area_col} = 2
    WHERE {id_col} IN ({update_storage_area_ids_string})
    """


# Channels
def create_bank_levels_update_query(new_bank_levels_df, excluded_ids):
    """Door een analyse op de resultaten weten we welke watergangen een 1d2d verbinding hebben over levees heen. Alleen voor deze
    watergangen worden de bank levels gelijk gezet aan de levee hoogte om vroegtijdige uitwisseling te voorkomen. De rest van de
    bank levels komt op streefpeil+10cm te staan.
    Bovenstaande geldt als we de bank levels opnieuw berekenen. In alle andere gevallen
    worden waarden gebruikt uit een backup of die handmatig zijn aangepast door de gebruiker
    """
    try:
        query = hrt.sql_create_update_case_statement(
            df=new_bank_levels_df,
            layer=cross_sec_loc_layer,
            df_id_col=a_cross_loc_id,
            db_id_col=id_col,
            new_val_col=new_bank_level_col,
            old_val_col=bank_level_col,
            excluded_ids=excluded_ids,
        )
        return query
    except Exception as e:
        raise e from None


def construct_channels_update_statement(channels_to_update_df, excluded_ids=[]):
    try:
        query = hrt.sql_create_update_case_statement(
            df=channels_to_update_df,
            layer=channels_layer,
            df_id_col=id_col,
            db_id_col=id_col,
            old_val_col=calculation_type_col,
            new_val_col=channels_new_calc_type,
            excluded_ids=excluded_ids,
        )
        return query
    except Exception as e:
        raise e from None


def create_update_reference_level_query(wrong_profiles_gdf, excluded_ids=[]):
    """
    Creates qsl query to update reference level where minimum weir height is below
    ground level (any deselected id's are skipped)
    """
    query = hrt.sql_create_update_case_statement(
        df=wrong_profiles_gdf,
        layer=cross_sec_loc_layer,
        df_id_col=a_weir_cross_loc_id,
        db_id_col=id_col,
        old_val_col=reference_level_col,
        new_val_col=new_ref_lvl,
        excluded_ids=excluded_ids,
    )
    return query


def __construct_channel_bed_query_inner(struct, startend):
    # removes v2_ from type string
    type = struct[3:]
    if struct == culvert_layer:
        if startend == "start":
            reference_parameter = invert_lvl_start_col
        else:
            reference_parameter = invert_lvl_end_col
    else:
        reference_parameter = crest_level_col
    if startend == "start":
        conn_node_id = conn_node_start_id_col
    else:
        conn_node_id = conn_node_end_id_col
    query = f"""SELECT *
    FROM (
        SELECT 
        '{type}' as {a_chan_bed_struct_type}, 
        {struct}.{id_col} as {a_chan_bed_struct_id},
        {channels_layer}.{id_col} as {a_chan_bed_channel_id},
        {struct}.{code_col} as {a_chan_bed_struct_code},
        {struct}.{conn_node_id} as {a_chan_bed_conn_id}, 
        {struct}.{reference_parameter} as {a_chan_bed_struct_ref_lvl},
        {cross_sec_loc_layer}.{reference_level_col} as {a_chan_bed_cross_ref_lvl},
        {cross_sec_loc_layer}.{id_col} as {a_chan_bed_cross_id},
        {f_distance}({cross_sec_loc_layer}.{geo_col}, {connection_nodes_layer}.{geo_col}) as {a_chan_bed_dist_cross_struct}, 
        {f_aswkt}({f_makeline}({cross_sec_loc_layer}.{geo_col},{connection_nodes_layer}.{geo_col})) as {df_geo_col}
        FROM {struct}
        INNER JOIN {channels_layer}
        ON {struct}.{conn_node_id} IN ({channels_layer}.{conn_node_start_id_col}, {channels_layer}.{conn_node_end_id_col})
        INNER JOIN {cross_sec_loc_layer}
        ON {channels_layer}.{id_col} = {cross_sec_loc_layer}.{channel_id_col}
        LEFT JOIN {connection_nodes_layer}
        ON {struct}.{conn_node_id} = {connection_nodes_layer}.{id_col}
        GROUP BY {a_chan_bed_conn_id}, {a_chan_bed_dist_cross_struct}
        )
    GROUP BY {a_chan_bed_conn_id}"""
    return query


def construct_struct_channel_bed_query():
    """
    Add cross section and channel information to controlled structures
    """
    query = f"SELECT * FROM (\
    {__construct_channel_bed_query_inner(culvert_layer, 'start')} \
    UNION {__construct_channel_bed_query_inner(culvert_layer, 'end')}\
    UNION {__construct_channel_bed_query_inner(orifice_layer, 'start')} \
    UNION {__construct_channel_bed_query_inner(orifice_layer, 'end')}) \
    WHERE {a_chan_bed_struct_ref_lvl} < {a_chan_bed_cross_ref_lvl};"
    return query


struct_channel_bed_query = construct_struct_channel_bed_query()


# Weirs
def construct_weir_height_update_statement(weir_widths_to_update_df, excluded_ids=[]):
    try:
        query = hrt.sql_create_update_case_statement(
            df=weir_widths_to_update_df,
            layer=cross_sec_def_layer,
            df_id_col=a_weir_cross_def_id,
            db_id_col=id_col,
            old_val_col=width_col,
            new_val_col=weirs_new_width_col,
            excluded_ids=excluded_ids,
        )
        return query
    except Exception as e:
        raise e from None


# Control
def construct_controlled_structures_query_inner(structure):
    query = f"""
    SELECT {control_table_layer}.{id_col} as {a_contr_struct_contr_id}, 
    {control_table_layer}.{action_col}, 
    {control_table_layer}.{target_type_col},
    {control_table_layer}.{target_id_col}, 
    {structure}.{code_col}, 
    {f_aswkt}({f_makeline}(v2_connection_nodes2.{geo_col},{connection_nodes_layer}.{geo_col})) as {df_geo_col}
    FROM {structure}
    INNER JOIN {control_table_layer}
    ON {structure}.{id_col} = {control_table_layer}.{target_id_col}
    LEFT JOIN {connection_nodes_layer}
    ON {structure}.{conn_node_start_id_col} = {connection_nodes_layer}.{id_col}
    LEFT JOIN {control_layer}
    ON {control_layer}.{control_id_col} = {control_table_layer}.{id_col}
    LEFT JOIN {control_measure_map}
    ON {control_measure_map}.{measure_group_id_col} = {control_layer}.{measure_group_id_col}
    LEFT JOIN {connection_nodes_layer} as v2_connection_nodes2
    ON {control_measure_map}.{object_id_col} = v2_connection_nodes2.id
    WHERE {control_table_layer}.{target_type_col}='{structure}'
    """
    return query


def construct_controlled_structures_query():
    query = f"""SELECT * FROM (
    {construct_controlled_structures_query_inner(weir_layer)}
    UNION {construct_controlled_structures_query_inner(culvert_layer)}
    UNION {construct_controlled_structures_query_inner(orifice_layer)})"""
    return query


controlled_structures_query = construct_controlled_structures_query()


def construct_geometry_query(table_names, dst_crs=DEF_TRGT_CRS):
    queries_lst = []
    for table in table_names:
        queries_lst.append(
            geometry_check_query_base.format(table=table, projection=dst_crs)
        )
    query = "\nUNION ALL\n".join(queries_lst)
    return query


geometry_check_query = construct_geometry_query(
    table_names=[channels_layer, culvert_layer]
)
