from ....variables.database_variables import id_col, code_col, conn_node_end_id_col, zoom_cat_col, \
    geo_col, channels_layer, f_aswkt, channel_id_col, def_id_col, bank_level_col, reference_level_col, \
    cross_sec_loc_layer, cross_sec_def_layer, width_col, height_col, initial_waterlevel_col, \
    connection_nodes_layer, calculation_type_col, area_col, impervious_surface_layer, \
    control_table_layer, action_col, target_type_col, target_id_col, f_makeline, conn_node_start_id_col, \
    control_layer, control_id_col, control_measure_map, measure_group_id_col, object_id_col, weir_layer, \
    culvert_layer, orifice_layer
from ....variables.database_aliases import df_geo_col, a_chan_id, a_chan_code, a_chan_node_id, \
    a_zoom_cat, a_cross_sec_loc_code, a_cross_sec_loc_id, a_cross_sec_def_id, a_contr_struct_contr_id, \
    a_weir_code, a_weir_conn_node_end_id, a_weir_conn_node_start_id, a_weir_chan_conn_start_id, \
    a_weir_chan_conn_end_id, a_weir_cross_loc_id

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
