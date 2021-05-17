from ..variables.database_variables import cross_sec_def_layer, id_col, weir_layer, cross_def_id_col, \
    control_table_layer, target_id_col

weir_width_backup_query = f"""
    SELECT * from {cross_sec_def_layer} WHERE {id_col} in (
    SELECT
    {weir_layer}.{cross_def_id_col}
    FROM {weir_layer}
    INNER JOIN {cross_sec_def_layer} ON {weir_layer}.{cross_def_id_col} = {cross_sec_def_layer}.{id_col}
    INNER JOIN {control_table_layer} ON {weir_layer}.{id_col} = {control_table_layer}.{target_id_col}
    )
    """
