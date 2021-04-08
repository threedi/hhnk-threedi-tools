from ....variables.database_variables import id_col, code_col, conn_node_end_id_col, zoom_cat_col, \
    geo_col, channels_layer, f_aswkt, channel_id_col, def_id_col, bank_level_col, reference_level_col, \
    cross_sec_loc_layer, cross_sec_def_layer, width_col, height_col, initial_waterlevel_col, \
    connection_nodes_layer
from ....variables.database_aliases import df_geo_col, a_chan_id, a_chan_code, a_chan_node_id, \
    a_zoom_cat, a_cross_sec_loc_code, a_cross_sec_loc_id, a_cross_sec_def_id
from ....queries.query_functions import construct_select_query

#--------------------------------------------------------------------------------
# Profiles used queries
#--------------------------------------------------------------------------------
profiles_used_query = \
    f"""
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
