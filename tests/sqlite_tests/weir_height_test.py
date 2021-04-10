from ...sql_interaction.sql_functions import execute_sql_selection
from ...dataframe_functions.conversion import convert_df_to_gdf
from ...variables.database_aliases import a_weir_code, a_weir_conn_node_start_id, \
    a_weir_conn_node_end_id, a_weir_cross_loc_id, a_chan_id, df_geo_col
from ...variables.database_variables import reference_level_col, action_col, id_col, cross_sec_loc_layer
from ...queries.tests.sqlite_tests.quick_tests_selection_queries import weir_height_query

min_crest_height = 'min_crest_height'
diff_crest_ref = 'diff_crest_reference'
wrong_profile = 'wrong_profile'
new_ref_lvl = 'proposed_reference_level'

output_cols = [a_weir_code, a_weir_conn_node_start_id, a_weir_conn_node_end_id, a_weir_cross_loc_id, a_chan_id,
               min_crest_height, reference_level_col, new_ref_lvl, df_geo_col]

def create_update_reference_level_query(wrong_profiles_gdf):
    """
    Creates qsl query to update reference level where minimum weir height is below
    ground level
    """
    vals_list = [(ref_lvl, nw_ref_lvl, wr_id) for ref_lvl, nw_ref_lvl, wr_id in
                 zip(wrong_profiles_gdf[reference_level_col], wrong_profiles_gdf[new_ref_lvl],
                     wrong_profiles_gdf[a_weir_cross_loc_id])]
    statement_list = [f"WHEN {wr_id} THEN {round(nw_ref_lvl, 2)} -- Previous {ref_lvl}"
                      for ref_lvl, nw_ref_lvl, wr_id in vals_list]
    statement_string = ',\n'.join(statement_list)
    query = f"""
    UPDATE {cross_sec_loc_layer}
    SET {reference_level_col} = CASE {id_col}
    {statement_string}
    ELSE {reference_level_col}
    """
    return query

def check_weir_floor_level(test_env):
    """
    Check whether minimum height of weir is under reference level. This is not allowed, so if this is
    the case, we have to update the reference level.
    """
    try:
        model_path = test_env.src_paths['model']
        weirs_df = execute_sql_selection(query=weir_height_query, database_path=model_path)
        weirs_gdf = convert_df_to_gdf(df=weirs_df)
        # Bepaal de minimale kruinhoogte uit de action table
        weirs_gdf[min_crest_height] = [min([float(b.split(';')[1]) for b in a.split('#')])
                                       for a in weirs_gdf[action_col]]
        # Bepaal het verschil tussen de minimale kruinhoogte en reference level.
        weirs_gdf[diff_crest_ref] = weirs_gdf[min_crest_height] - weirs_gdf[reference_level_col]
        # Als dit verschil negatief is, betekent dit dat de bodem hoger ligt dan de minimale hoogte van de stuw.
        # Dit mag niet, en daarom moet er iets aan het bodemprofiel gebeuren.
        weirs_gdf[wrong_profile] = weirs_gdf[diff_crest_ref] < 0
        # Add proposed new reference levels
        weirs_gdf.loc[weirs_gdf[wrong_profile] == 1, new_ref_lvl] = \
            round(weirs_gdf.loc[weirs_gdf[wrong_profile] == 1, min_crest_height] - 0.01, 2)
        wrong_profiles_gdf = weirs_gdf[weirs_gdf[wrong_profile]][output_cols]
        update_query = create_update_reference_level_query(wrong_profiles_gdf)
        return wrong_profiles_gdf, update_query
    except Exception as e:
        raise e from None
