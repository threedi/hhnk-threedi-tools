from ...variables.database_variables import weir_layer, cross_def_id_col, code_col, id_col, \
    cross_sec_def_layer, width_col, control_table_layer, target_id_col
from ...variables.database_aliases import a_weir_cross_def_id, a_weir_code, a_weir_id

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
