from ....variables.database_variables import id_col, code_col, conn_node_end_id_col, zoom_cat_col, \
    geo_col, channels_layer, f_aswkt, channel_id_col, def_id_col, bank_level_col, reference_level_col, \
    cross_sec_loc_layer, cross_sec_def_layer, width_col, height_col, initial_waterlevel_col, \
    connection_nodes_layer, calculation_type_col, area_col, impervious_surface_layer, \
    control_table_layer, action_col, target_type_col, target_id_col, f_makeline, conn_node_start_id_col, \
    control_layer, control_id_col, control_measure_map, measure_group_id_col, object_id_col, weir_layer, \
    culvert_layer, orifice_layer, f_numpoints, f_transform, f_pointn, crest_level_col, invert_lvl_end_col, \
    invert_lvl_start_col, f_distance, storage_area_col
from ....variables.database_aliases import df_geo_col, a_chan_id, a_chan_code, a_chan_node_id, \
    a_zoom_cat, a_cross_sec_loc_code, a_cross_sec_loc_id, a_cross_sec_def_id, a_contr_struct_contr_id, \
    a_weir_code, a_weir_conn_node_end_id, a_weir_conn_node_start_id, a_weir_chan_conn_start_id, \
    a_weir_chan_conn_end_id, a_weir_cross_loc_id, a_geo_start_coord, a_geo_end_coord, a_geo_start_node, \
    a_geo_end_node, a_geo_conn_nodes_start, a_geo_conn_nodes_end, a_chan_bed_struct_type, a_chan_bed_struct_id, \
    a_chan_bed_channel_id, a_chan_bed_struct_code, a_chan_bed_conn_id, a_chan_bed_struct_ref_lvl, \
    a_chan_bed_cross_ref_lvl, a_chan_bed_cross_id, a_chan_bed_dist_cross_struct, a_watersurf_conn_id
from ....variables.default_variables import DEF_TRGT_CRS

#--------------------------------------------------------------------------------
# Profiles used queries
#--------------------------------------------------------------------------------
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
#--------------------------------------------------------------------------------
# Isolated channels query
#--------------------------------------------------------------------------------

isolated_channels_query = f"""
    SELECT {id_col},
    {calculation_type_col},
    {f_aswkt}({geo_col}) as {df_geo_col}
    FROM {channels_layer}
    """
#--------------------------------------------------------------------------------
# Impervious surface query
#--------------------------------------------------------------------------------

impervious_surface_query = f"""
    SELECT {area_col},
    {id_col}
    FROM {impervious_surface_layer};
    """
#--------------------------------------------------------------------------------
# Controlled structs query
#--------------------------------------------------------------------------------

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

#--------------------------------------------------------------------------------
# Weir height query
#--------------------------------------------------------------------------------

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

#--------------------------------------------------------------------------------
# Geometry checks query
#--------------------------------------------------------------------------------

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

def construct_geometry_query(table_names, dst_crs=DEF_TRGT_CRS):
    queries_lst = []
    for table in table_names:
        queries_lst.append(geometry_check_query_base.format(table=table, projection=dst_crs))
    query = "\nUNION ALL\n".join(queries_lst)
    return query

geometry_check_query = construct_geometry_query(table_names=[channels_layer, culvert_layer])

#--------------------------------------------------------------------------------
# Geometry checks query
#--------------------------------------------------------------------------------

def __construct_channel_bed_query_inner(struct, startend):
    # removes v2_ from type string
    type = struct[3:]
    if struct == culvert_layer:
        if startend == 'start':
            reference_parameter = invert_lvl_start_col
        else:
            reference_parameter = invert_lvl_end_col
    else:
        reference_parameter = crest_level_col
    if startend == 'start':
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

#--------------------------------------------------------------------------------
# Watersurface area query
#--------------------------------------------------------------------------------

watersurface_conn_node_query = f"""
    SELECT {id_col} as {a_watersurf_conn_id},
    {initial_waterlevel_col},
    {storage_area_col},
    {f_aswkt}({geo_col}) as {df_geo_col}
    FROM {connection_nodes_layer}
    """
