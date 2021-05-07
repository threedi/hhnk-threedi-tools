import numpy as np
from hhnk_threedi_tools.query_functions import create_update_case_statement
from ..variables.database_variables import cross_sec_loc_layer, id_col, bank_level_col, \
    connection_nodes_layer, storage_area_col, manhole_layer, display_name_col, code_col, \
    shape_col, width_col, manhole_indicator_col, calculation_type_col, drain_level_col, \
    bottom_lvl_col, surface_lvl_col, zoom_cat_col, conn_node_id_col
from ..variables.database_aliases import a_cross_loc_id, a_man_conn_id
from ..tests.bank_levels.variables.dataframe_variables import new_bank_level_col

def create_bank_levels_update_query(new_bank_levels_df, excluded_ids):
    """Door een analyse op de resultaten weten we welke watergangen een 1d2d verbinding hebben over levees heen. Alleen voor deze
    watergangen worden de bank levels gelijk gezet aan de levee hoogte om vroegtijdige uitwisseling te voorkomen. De rest van de
    bank levels komt op streefpeil+10cm te staan.
    Bovenstaande geldt als we de bank levels opnieuw berekenen. In alle andere gevallen
    worden waarden gebruikt uit een backup of die handmatig zijn aangepast door de gebruiker
    """
    try:
        query = create_update_case_statement(df=new_bank_levels_df,
                                             layer=cross_sec_loc_layer,
                                             df_id_col=a_cross_loc_id,
                                             db_id_col=id_col,
                                             new_val_col=new_bank_level_col,
                                             old_val_col=bank_level_col,
                                             excluded_ids=excluded_ids)
        return query
    except Exception as e:
        raise e from None

def create_update_storage_area_sql(new_manholes_df, excluded_ids):
    update_storage_ids = []
    update_storage_area_rows = new_manholes_df[np.isnan(new_manholes_df[storage_area_col])]
    update_storage_area_rows = update_storage_area_rows[~update_storage_area_rows[conn_node_id_col].isin(excluded_ids)]
    if not update_storage_area_rows.empty:
        update_storage_ids = [item for item in update_storage_area_rows[conn_node_id_col].tolist()]
    update_storage_area_ids_string = ','.join(map(str, update_storage_ids))
    return f"""
    UPDATE {connection_nodes_layer}
    SET {storage_area_col} = 2
    WHERE {id_col} IN ({update_storage_area_ids_string})
    """

def create_new_manholes_query(new_manholes_df, excluded_ids):
    """Maak sql statement dat gebruikt wordt om manholes te maken op de boven en benendenstroomse connection nodes
    van kunstwerken op peilgrens. Omdat deze manholes isolated zijn is er geen stroming meer over de peilgrens nodig.
    Ook wordt een storage area toegevoegd aan de connection nodes waar manholes aan worden toegevoegd als die nog
    niet gespecificeerd is
    """
    query = ""
    query += f"INSERT INTO {manhole_layer} "\
             f"({display_name_col}, {code_col}, {conn_node_id_col}, {shape_col}," \
             f"{width_col}, {manhole_indicator_col}, {calculation_type_col}, {drain_level_col}, " \
             f"{bottom_lvl_col}, {surface_lvl_col}, {zoom_cat_col})\n"
    query += "VALUES "
    sql_body = []
    for index, row in new_manholes_df.iterrows():
        if row[conn_node_id_col] not in excluded_ids:
            sql_body.append(f"('{row[display_name_col]}', '{row[code_col]}', {row[conn_node_id_col]}, '{row[shape_col]}', "
                            f"{row[width_col]}, {row[manhole_indicator_col]}, {row[calculation_type_col]}, {row[drain_level_col]}, "
                            f"{row[bottom_lvl_col]}, {row[surface_lvl_col]}, {row[zoom_cat_col]})")
    if not sql_body:
        return None
    else:
        query += ',\n'.join(sql_body) + ';'
        query += create_update_storage_area_sql(new_manholes_df, excluded_ids)
        return query
