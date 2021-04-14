import pandas as pd
import numpy as np
from ...variables.database_variables import display_name_col, code_col, conn_node_id_col, shape_col, \
    width_col, manhole_indicator_col, calculation_type_col, drain_level_col, bottom_lvl_col, surface_lvl_col, \
    zoom_cat_col, type_col, initial_waterlevel_col, storage_area_col
from ...variables.database_aliases import a_man_conn_id

new_storage_area_col = 'new_storage_area'

def dataframe_from_new_manholes(new_manholes):
    try:
        new_manholes_model_df = pd.DataFrame(columns=[display_name_col, code_col, conn_node_id_col,
                                                      shape_col, width_col, manhole_indicator_col,
                                                      calculation_type_col, drain_level_col,
                                                      bottom_lvl_col, surface_lvl_col, zoom_cat_col])
        new_manholes_model_df[display_name_col] = new_manholes[code_col] + '_' + new_manholes[type_col]
        new_manholes_model_df[code_col] = new_manholes_model_df[display_name_col]
        new_manholes_model_df[conn_node_id_col] = new_manholes[a_man_conn_id]
        new_manholes_model_df[shape_col] = '00'
        new_manholes_model_df[width_col] = 1
        new_manholes_model_df[manhole_indicator_col] = 0
        new_manholes_model_df[calculation_type_col] = np.where(np.isnan(new_manholes[drain_level_col]), 1, 2)
        new_manholes_model_df[drain_level_col] = np.where(np.isnan(new_manholes[drain_level_col]),
                                                          'null', new_manholes[drain_level_col])
        new_manholes_model_df[bottom_lvl_col] = -10
        new_manholes_model_df[surface_lvl_col] = new_manholes[initial_waterlevel_col]
        new_manholes_model_df[zoom_cat_col] = 0
        new_manholes_model_df.set_index(conn_node_id_col)
        new_manholes_model_df[storage_area_col] = new_manholes[storage_area_col]
        # Add new storage area column where appropriate
        new_manholes_model_df.insert(new_manholes_model_df.columns.get_loc(storage_area_col) + 1,
                                     new_storage_area_col,
                                     np.where(np.isnan(new_manholes_model_df[storage_area_col]), 2.0, '-'))
        return new_manholes_model_df
    except Exception as e:
        raise e from None
