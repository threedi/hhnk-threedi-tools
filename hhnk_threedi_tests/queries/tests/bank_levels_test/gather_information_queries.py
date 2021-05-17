from ....variables.database_variables import id_col, conn_node_id_col, drain_level_col, code_col, \
    manhole_layer, channels_layer, initial_waterlevel_col, f_aswkt, \
    connection_nodes_layer, conn_node_start_id_col, channel_id_col, \
    reference_level_col, geo_col, bank_level_col, cross_sec_loc_layer, \
    storage_area_col
from ....variables.database_aliases import a_man_id, a_man_conn_id, df_geo_col, \
    a_chan_id, a_conn_nodes, a_cross_loc_id, a_conn_node_id

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

conn_nodes_query = f"""
    SELECT {id_col} as {a_conn_node_id}, \
    {initial_waterlevel_col}, \
    {storage_area_col}, \
    {f_aswkt}({geo_col}) as {df_geo_col} \
    FROM {connection_nodes_layer}
    """
