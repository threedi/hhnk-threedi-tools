from hhnk_threedi_tools.variables.database_variables import (
    action_col,
    channel_id_col,
    channels_layer,
    conn_node_end_id_col,
    conn_node_id_col,
    conn_node_start_id_col,
    connection_nodes_layer,
    control_table_layer,
    crest_level_col,
    cross_def_id_col,
    cross_sec_def_layer,
    cross_sec_loc_layer,
    culvert_layer,
    def_id_col,
    discharge_coefficient_neg_col,
    discharge_coefficient_pos_col,
    height_col,
    id_col,
    impervious_surface_id_col,
    impervious_surface_layer,
    impervious_surface_map_layer,
    initial_waterlevel_col,
    invert_lvl_end_col,
    invert_lvl_start_col,
    manhole_layer,
    one_d_boundary_cond_layer,
    orifice_layer,
    percentage_col,
    pipe_layer,
    pump_station_layer,
    reference_level_col,
    shape_col,
    start_level_col,
    surface_id_col,
    surface_layer,
    surface_map_layer,
    weir_layer,
    width_col,
)

# Error/warning/info messages
msg_height_not_used_for_shape = "WARNING: cross section definition height not used for shape type 1"
msg_uneven_width_height = "ERROR: multiple height and width entries must have the same count"
msg_impervious_not_in_map = "WARNING: impervious surface is not in mapping table"
msg_impervious_map_refers_to_none = "ERROR: impervious surface map refers to non-existent node"
msg_surface_not_in_mapping = "WARNING: surface is not in mapping table"
msg_impervious_surface_map_not_in_impervious = "WARNING: impervious surface map is not in impervious surface layer"
msg_conn_node_without_imp_surface = "WARNING: connection node without impervious surface"
msg_channel_without_cross_loc = "ERROR: channel without cross section location"
msg_cross_sec_loc_without_def = "ERROR: cross section location without definition"
msg_culvert_without_def = "ERROR: culvert without cross section definition"
msg_weir_without_def = "ERROR: weir without cross section definition"
msg_orifice_without_def = "ERROR: orifice without cross section definition"
msg_manhole_without_conn_node = "ERROR: manhole without connection node"
msg_one_d_boundary_cond_without_conn = "WARNING: 1d boundary without connection node"
msg_cross_sec_loc_without_ref_lvl = "ERROR: cross section location without reference level"
msg_conn_node_without_init_water_lvl = "WARNING: connection node without initial waterlevel"
msg_conn_node_without_conn = "ERROR: node without connection"
msg_strt_lvl_not_close_init_waterlvl = "WARNING: start level for pump station not close to initial waterlevel"
msg_undefined_shape_or_width = "ERROR: shape or width is not defined"
msg_pump_station_same_node = "ERROR: pumpstation from and to the same connection node"
msg_weir_same_node = "ERROR: weir from and to the same connection node"
msg_orifice_same_node = "ERROR: orifice from and to the same connection node"
msg_culvert_same_node = "ERROR: culvert from and to the same connection node"
msg_weir_start_lvl_not_close_to_init_waterlvl = (
    "'WARNING: start level ' || {} || ' not close to initial waterlevel '" " || {} || ', ' || {}"
)
msg_channel_strt_end_not_same_init_waterlvl = (
    "'WARNING: initial water level at start and end node are not equal ' " "|| {} || ', ' || {}"
)
msg_orifice_strt_end_not_same_init_waterlvl = (
    "'WARNING: initial water level at start and end node are not equal ' " "|| {} || ', ' || {}"
)
msg_culvert_strt_end_not_same_init_waterlvl = (
    "'WARNING: Initial waterlevel at start en end node not equal' || {} || " "', ' || {}"
)
msg_imp_surface_perc = "WARNING: percentage = 100 and should be 14.4 or 11.5"
msg_control_table_too_many_chars = "ERROR: action_table has more than 1000 characters (model will crash)"


def constr_in_clause(innotin, sel=False, frm=False, where=None):
    """
        constr_in_clause(
            innotin (string: either 'IN' or 'NOT IN')
            sel -> False (bool: add 'SELECT {}' line)
            frm -> False (bool: add 'FROM {}' line)
            where -> False (bool: add 'WHERE {}' line)
            )

    Constructs a IN or NOT IN selection clause.
    Ex:
        IN
        (SELECT {}
        FROM {}
        WHERE {})

    Which can then be formatted to insert table names and condition etc
    """
    res = " " + innotin + "(\n"
    if sel:
        res += "SELECT {}\n"
    if frm:
        res += "FROM {}\n"
    if where:
        res += "WHERE {}" + where
    res += ")"
    return res


def construct_sel_from_where_query(sel="{}", frm="{}", where="{}", left_join={}, inner_join={}):
    """
    Constructs select / from / where query with possible join clauses (inner or left join)

        construct_sel_from_where_query(
                sel -> '{}' (string, whatever comes after 'SELECT ' keyword)
                from -> '{}' (string, whatever comes after 'FROM ' keyword)
                where -> '{}' (string, whatever comes after 'WHERE ' keyword)
                left_join -> {} (dictionary, returned as 'LEFT JOIN {key} ON {value} for every entry in dict)
                inner_join -> {} (dictionary, returned as 'INNER JOIN {key} ON {value} for every entry in dict)
            )

    Returns either full query or template to format later depending on input
    """
    res = ""
    if sel:
        res += "SELECT " + sel + "\n"
    if frm:
        res += "FROM " + frm + "\n"
    for key in left_join:
        res += "LEFT JOIN " + key + "\nON " + left_join[key] + "\n"
    for key in inner_join:
        res += "INNER JOIN " + key + "\nON " + inner_join[key] + "\n"
    if where:
        res += "WHERE " + where + "\n"
    return res


def construct_query_head(table, msg):
    res = f"'{table}' as table_name,\n" f"{table}.{id_col} as id,\n" f"'{msg}' as error"
    return res


def construct_query_head_no_quotes(table, msg):
    res = f"'{table}' as table_name,\n" f"{table}.{id_col} as id,\n" f"{msg} as error"
    return res


def constr_conn_nodes_without_conn_query():
    query = ""
    layers = [
        pump_station_layer,
        channels_layer,
        culvert_layer,
        pipe_layer,
        weir_layer,
        orifice_layer,
    ]
    length = len(layers)
    for i, layer in enumerate(layers):
        for node in conn_node_start_id_col, conn_node_end_id_col:
            query += construct_sel_from_where_query(where="{} IS NOT NULL").format(node, layer, node)
            if i is not length - 1 or (i is length - 1 and node is conn_node_start_id_col):
                query += "UNION ALL\n"
    return query


def constr_from_to_same_node_query():
    query = ""
    layers = [pump_station_layer, weir_layer, orifice_layer, culvert_layer]
    length = len(layers)
    for i, layer in enumerate(layers):
        if layer == pump_station_layer:
            msg = msg_pump_station_same_node
        elif layer == weir_layer:
            msg = msg_weir_same_node
        elif layer == orifice_layer:
            msg = msg_orifice_same_node
        else:
            msg = msg_culvert_same_node
        query += construct_sel_from_where_query(where="{} = {}").format(
            construct_query_head(layer, msg),
            layer,
            f"{layer}.{conn_node_start_id_col}",
            f"{layer}.{conn_node_end_id_col}",
        )
        if i is not length - 1:
            query += "UNION ALL\n"
    return query


model_checks = {
    ######################################################################################
    "height_not_used_for_shape": construct_sel_from_where_query(where="{} IS NOT NULL AND {} = 1").format(
        construct_query_head(cross_sec_def_layer, msg_height_not_used_for_shape),
        cross_sec_def_layer,
        f"'{height_col}'",
        f"'{shape_col}'",
    ),
    ######################################################################################
    "multiple_heights_widths_same_count": construct_sel_from_where_query(where="{} - {} <> {} - {}").format(
        construct_query_head(cross_sec_def_layer, msg_uneven_width_height),
        cross_sec_def_layer,
        f"length('{height_col}')",
        f"length(replace('{height_col}', ' ', ''))",
        f"length('{width_col}')",
        f"length(replace('{width_col}', ' ', ''))",
    ),
    ######################################################################################
    "impervious_surface_not_in_mapping": construct_sel_from_where_query(
        where="{} " + constr_in_clause(innotin="NOT IN", sel=True, frm=True)
    ).format(
        construct_query_head(impervious_surface_layer, msg_impervious_not_in_map),
        impervious_surface_layer,
        f"{impervious_surface_layer}.{id_col}",
        impervious_surface_id_col,
        impervious_surface_map_layer,
    ),
    ######################################################################################
    "impervious_surface_map_refers_to_inv_conn_node": construct_sel_from_where_query(
        where="{} " + constr_in_clause(innotin="NOT IN", sel=True, frm=True)
    ).format(
        construct_query_head(impervious_surface_map_layer, msg_impervious_map_refers_to_none),
        impervious_surface_map_layer,
        f"{impervious_surface_map_layer}.{conn_node_id_col}",
        id_col,
        connection_nodes_layer,
    ),
    ######################################################################################
    "surface_not_in_mapping_table": construct_sel_from_where_query(
        where="{} " + constr_in_clause(innotin="NOT IN", sel=True, frm=True)
    ).format(
        construct_query_head(surface_layer, msg_surface_not_in_mapping),
        surface_layer,
        f"{surface_layer}.{id_col}",
        surface_id_col,
        surface_map_layer,
    ),
    ######################################################################################
    "impervious_surface_map_refers_to_inv_imp_surf": construct_sel_from_where_query(
        where="{} " + constr_in_clause(innotin="NOT IN", sel=True, frm=True)
    ).format(
        construct_query_head(impervious_surface_map_layer, msg_impervious_surface_map_not_in_impervious),
        impervious_surface_map_layer,
        f"{impervious_surface_map_layer}.{id_col}",
        impervious_surface_id_col,
        impervious_surface_layer,
    ),
    ######################################################################################
    "conn_node_without_imp_surface": construct_sel_from_where_query(
        where="{}"
        + constr_in_clause(
            "IN",
            sel=True,
            frm=True,
            where=constr_in_clause("NOT IN", sel=True, frm=True),
        )
    ).format(
        construct_query_head(connection_nodes_layer, msg_conn_node_without_imp_surface),
        connection_nodes_layer,
        f"{connection_nodes_layer}.{id_col}",
        f"{impervious_surface_map_layer}.{conn_node_id_col}",
        impervious_surface_map_layer,
        f"{impervious_surface_map_layer}.{impervious_surface_id_col}",
        f"{impervious_surface_layer}.{id_col}",
        impervious_surface_layer,
    ),
    ######################################################################################
    "channel_without_cross_loc": construct_sel_from_where_query(
        where="{} IS NULL", left_join={"{}": "{} = {}"}
    ).format(
        construct_query_head(channels_layer, msg_channel_without_cross_loc),
        channels_layer,
        cross_sec_loc_layer,
        f"{channels_layer}.{id_col}",
        f"{cross_sec_loc_layer}.{channel_id_col}",
        f"{cross_sec_loc_layer}.{id_col}",
    ),
    ######################################################################################
    "cross_section_location_without_definition": construct_sel_from_where_query(
        where="{} IS NULL", left_join={"{}": "{} = {}"}
    ).format(
        construct_query_head(cross_sec_loc_layer, msg_cross_sec_loc_without_def),
        cross_sec_loc_layer,
        cross_sec_def_layer,
        f"{cross_sec_loc_layer}.{def_id_col}",
        f"{cross_sec_def_layer}.{id_col}",
        f"{cross_sec_def_layer}.{id_col}",
    ),
    ######################################################################################
    "culvert_without_cross_definition": construct_sel_from_where_query(
        where="{} IS NULL", left_join={"{}": "{} = {}"}
    ).format(
        construct_query_head(culvert_layer, msg_culvert_without_def),
        culvert_layer,
        cross_sec_def_layer,
        f"{culvert_layer}.{cross_def_id_col}",
        f"{cross_sec_def_layer}.{id_col}",
        f"{cross_sec_def_layer}.{id_col}",
    ),
    ######################################################################################
    "weir_without_cross_definition": construct_sel_from_where_query(
        where="{} IS NULL", left_join={"{}": "{} = {}"}
    ).format(
        construct_query_head(weir_layer, msg_weir_without_def),
        weir_layer,
        cross_sec_def_layer,
        f"{weir_layer}.{cross_def_id_col}",
        f"{cross_sec_def_layer}.{id_col}",
        f"{cross_sec_def_layer}.{id_col}",
    ),
    ######################################################################################
    "orifice_without_cross_definition": construct_sel_from_where_query(
        where="{} IS NULL", left_join={"{}": "{} = {}"}
    ).format(
        construct_query_head(orifice_layer, msg_orifice_without_def),
        orifice_layer,
        cross_sec_def_layer,
        f"{orifice_layer}.{cross_def_id_col}",
        f"{cross_sec_def_layer}.{id_col}",
        f"{cross_sec_def_layer}.{id_col}",
    ),
    ######################################################################################
    "manhole_without_conn_node": construct_sel_from_where_query(
        where="{} IS NULL", left_join={"{}": "{} = {}"}
    ).format(
        construct_query_head(manhole_layer, msg_manhole_without_conn_node),
        manhole_layer,
        connection_nodes_layer,
        f"{manhole_layer}.{conn_node_id_col}",
        f"{connection_nodes_layer}.{id_col}",
        f"{connection_nodes_layer}.{id_col}",
    ),
    ######################################################################################
    "one_d_boundary_without_conn_node": construct_sel_from_where_query(
        where="{} IS NULL", left_join={"{}": "{} = {}"}
    ).format(
        construct_query_head(one_d_boundary_cond_layer, msg_one_d_boundary_cond_without_conn),
        one_d_boundary_cond_layer,
        connection_nodes_layer,
        f"{one_d_boundary_cond_layer}.{conn_node_id_col}",
        f"{connection_nodes_layer}.{id_col}",
        f"{connection_nodes_layer}.{id_col}",
    ),
    ######################################################################################
    "cross_sec_loc_without_ref_lvl": construct_sel_from_where_query(where="{} IS NULL").format(
        construct_query_head(cross_sec_loc_layer, msg_cross_sec_loc_without_ref_lvl),
        cross_sec_loc_layer,
        reference_level_col,
    ),
    ######################################################################################
    "conn_node_without_init_water_lvl": construct_sel_from_where_query(where="{} IS NULL").format(
        construct_query_head(connection_nodes_layer, msg_conn_node_without_init_water_lvl),
        connection_nodes_layer,
        initial_waterlevel_col,
    ),
    ######################################################################################
    "conn_node_without_conn": construct_sel_from_where_query(
        where=f"""{{}} NOT IN ({constr_conn_nodes_without_conn_query()})"""
    ).format(
        construct_query_head(connection_nodes_layer, msg_conn_node_without_conn),
        connection_nodes_layer,
        f"{connection_nodes_layer}.{id_col}",
    ),
    ######################################################################################
    "start_lvl_not_close_to_init_waterlvl": construct_sel_from_where_query(
        where="abs({} - {}) > 0.05", left_join={"{}": "{} = {}"}
    ).format(
        construct_query_head(pump_station_layer, msg_strt_lvl_not_close_init_waterlvl),
        pump_station_layer,
        connection_nodes_layer,
        f"{pump_station_layer}.{conn_node_start_id_col}",
        f"{connection_nodes_layer}.{id_col}",
        f"{connection_nodes_layer}.{initial_waterlevel_col}",
        f"{pump_station_layer}.{start_level_col}",
    ),
    ######################################################################################
    "undefined_shape_or_width": construct_sel_from_where_query(where="""{} IS NULL OR {} IS NULL OR {} = ''""").format(
        construct_query_head(cross_sec_def_layer, msg_undefined_shape_or_width),
        cross_sec_def_layer,
        shape_col,
        width_col,
        width_col,
    ),
    ######################################################################################
    "from_to_same_node": constr_from_to_same_node_query(),
    ######################################################################################
    "weir_start_lvl_not_close_to_init_waterlvl": construct_sel_from_where_query(
        left_join={"{} as conn1": "{} = {}", "{} as conn2": "{} = {}"},
        where="abs({} - {}) > 0.05 AND abs({} - {}) > 0.05 AND {} != 15",
    ).format(
        construct_query_head_no_quotes(
            weir_layer,
            msg_weir_start_lvl_not_close_to_init_waterlvl.format(
                f"{weir_layer}.{crest_level_col}",
                f"conn1.{initial_waterlevel_col}",
                f"conn2.{initial_waterlevel_col}",
            ),
        ),
        weir_layer,
        connection_nodes_layer,
        f"{weir_layer}.{conn_node_start_id_col}",
        f"conn1.{id_col}",
        connection_nodes_layer,
        f"{weir_layer}.{conn_node_end_id_col}",
        f"conn2.{id_col}",
        f"conn1.{initial_waterlevel_col}",
        f"{weir_layer}.{crest_level_col}",
        f"conn2.{initial_waterlevel_col}",
        f"{weir_layer}.{crest_level_col}",
        f"{weir_layer}.{crest_level_col}",
    ),
    ######################################################################################
    "channel_strt_end_not_same_init_waterlvl": construct_sel_from_where_query(
        left_join={"{} as conn1": "{} = {}", "{} as conn2": "{} = {}"}, where="{} != {}"
    ).format(
        construct_query_head_no_quotes(
            channels_layer,
            msg_channel_strt_end_not_same_init_waterlvl.format(
                f"conn1.{initial_waterlevel_col}", f"conn2.{initial_waterlevel_col}"
            ),
        ),
        channels_layer,
        connection_nodes_layer,
        f"{channels_layer}.{conn_node_start_id_col}",
        f"conn1.{id_col}",
        connection_nodes_layer,
        f"{channels_layer}.{conn_node_end_id_col}",
        f"conn2.{id_col}",
        f"conn1.{initial_waterlevel_col}",
        f"conn2.{initial_waterlevel_col}",
    ),
    ######################################################################################
    "orifice_strt_end_not_same_init_waterlvl": construct_sel_from_where_query(
        left_join={"{} as conn1": "{} = {}", "{} as conn2": "{} = {}"},
        where="{} != {} AND {} != 0 AND {} != 0 AND {} < max({}, {})",
    ).format(
        construct_query_head_no_quotes(
            orifice_layer,
            msg_orifice_strt_end_not_same_init_waterlvl.format(
                f"conn1.{initial_waterlevel_col}", f"conn2.{initial_waterlevel_col}"
            ),
        ),
        orifice_layer,
        connection_nodes_layer,
        f"{orifice_layer}.{conn_node_start_id_col}",
        f"conn1.{id_col}",
        connection_nodes_layer,
        f"{orifice_layer}.{conn_node_end_id_col}",
        f"conn2.{id_col}",
        f"conn1.{initial_waterlevel_col}",
        f"conn2.{initial_waterlevel_col}",
        f"{orifice_layer}.{discharge_coefficient_pos_col}",
        f"{orifice_layer}.{discharge_coefficient_neg_col}",
        f"{orifice_layer}.{crest_level_col}",
        f"conn1.{initial_waterlevel_col}",
        f"conn2.{initial_waterlevel_col}",
    ),
    ######################################################################################
    "culvert_strt_end_not_same_init_waterlvl": construct_sel_from_where_query(
        left_join={"{} as conn1": "{} = {}", "{} as conn2": "{} = {}"},
        where="{} != {} AND {} != 0 AND {} != 0 AND max({}, {}) < max({}, {})",
    ).format(
        construct_query_head_no_quotes(
            culvert_layer,
            msg_orifice_strt_end_not_same_init_waterlvl.format(
                f"conn1.{initial_waterlevel_col}", f"conn2.{initial_waterlevel_col}"
            ),
        ),
        culvert_layer,
        connection_nodes_layer,
        f"{culvert_layer}.{conn_node_start_id_col}",
        f"conn1.{id_col}",
        connection_nodes_layer,
        f"{culvert_layer}.{conn_node_end_id_col}",
        f"conn2.{id_col}",
        f"conn1.{initial_waterlevel_col}",
        f"conn2.{initial_waterlevel_col}",
        f"{culvert_layer}.{discharge_coefficient_pos_col}",
        f"{culvert_layer}.{discharge_coefficient_neg_col}",
        f"{culvert_layer}.{invert_lvl_start_col}",
        f"{culvert_layer}.{invert_lvl_end_col}",
        f"conn1.{initial_waterlevel_col}",
        f"conn2.{initial_waterlevel_col}",
    ),
    ######################################################################################
    "imp_surface_perc": construct_sel_from_where_query(where="{} = 100").format(
        construct_query_head(impervious_surface_map_layer, msg_imp_surface_perc),
        impervious_surface_map_layer,
        f"{impervious_surface_map_layer}.{percentage_col}",
    ),
    ######################################################################################
    "action_table_char_count": construct_sel_from_where_query(where="length({}) > 1000").format(
        construct_query_head(control_table_layer, msg_control_table_too_many_chars),
        control_table_layer,
        f"{control_table_layer}.{action_col}",
    ),
}


class ModelCheck:
    """
    Holds all tests that are part of the general model checks for easier access
    """

    def __init__(self):
        self.height_not_used_for_shape = model_checks["height_not_used_for_shape"]
        self.multiple_heights_widths_count = model_checks["multiple_heights_widths_same_count"]
        self.impervious_surface_not_in_mapping = model_checks["impervious_surface_not_in_mapping"]
        self.impervious_surface_map_refers_to_inv_conn_node = model_checks[
            "impervious_surface_map_refers_to_inv_conn_node"
        ]
        self.surface_not_in_mapping = model_checks["surface_not_in_mapping_table"]
        self.impervious_map_not_in_imp_surface = model_checks["impervious_surface_map_refers_to_inv_imp_surf"]
        self.conn_node_without_imp_surface = model_checks["conn_node_without_imp_surface"]
        self.channel_without_cross_section_location = model_checks["channel_without_cross_loc"]
        self.cross_section_loc_without_definition = model_checks["cross_section_location_without_definition"]
        self.culvert_without_cross_section_definition = model_checks["culvert_without_cross_definition"]
        self.weir_without_cross_section_definition = model_checks["weir_without_cross_definition"]
        self.orifice_without_cross_section_definition = model_checks["orifice_without_cross_definition"]
        self.manhole_without_conn_node = model_checks["manhole_without_conn_node"]
        self.one_d_boundary_without_conn_node = model_checks["one_d_boundary_without_conn_node"]
        self.cross_sec_loc_without_ref_lvl = model_checks["cross_sec_loc_without_ref_lvl"]
        self.conn_node_without_init_water_lvl = model_checks["conn_node_without_init_water_lvl"]
        self.conn_node_without_conn = model_checks["conn_node_without_conn"]
        self.start_lvl_not_close_init_waterlvl = model_checks["start_lvl_not_close_to_init_waterlvl"]
        self.shape_or_width_undefined = model_checks["undefined_shape_or_width"]
        self.from_to_same_node = model_checks["from_to_same_node"]
        self.weir_start_lvl_not_close_to_init_waterlvl = model_checks["weir_start_lvl_not_close_to_init_waterlvl"]
        self.channel_strt_end_not_same_init_waterlvl = model_checks["channel_strt_end_not_same_init_waterlvl"]
        self.orifice_strt_end_not_same_init_waterlvl = model_checks["orifice_strt_end_not_same_init_waterlvl"]
        self.culvert_strt_end_not_same_init_waterlvl = model_checks["culvert_strt_end_not_same_init_waterlvl"]
        self.impervious_surface_perc = model_checks["imp_surface_perc"]
        self.action_table_too_many_chars = model_checks["action_table_char_count"]

    @classmethod
    def get_query(cls):
        """Get a all variables as query to execute on model"""
        queries_lst = [item for item in vars(cls()).values()]
        return "UNION ALL\n".join(queries_lst)
