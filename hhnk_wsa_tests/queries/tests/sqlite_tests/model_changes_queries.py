from ....variables.database_aliases import a_weir_cross_loc_id
from ....variables.database_variables import reference_level_col, cross_sec_loc_layer, id_col
from ....tests.sqlite_tests.weir_height_test import new_ref_lvl

def create_update_reference_level_query(wrong_profiles_gdf):
    """
    Creates qsl query to update reference level where minimum weir height is below
    ground level
    """
    vals_list = [(ref_lvl, nw_ref_lvl, wr_id) for ref_lvl, nw_ref_lvl, wr_id in
                 zip(wrong_profiles_gdf[reference_level_col], wrong_profiles_gdf[new_ref_lvl],
                     wrong_profiles_gdf[a_weir_cross_loc_id])]
    statement_list = [f"WHEN {wr_id} THEN {nw_ref_lvl} -- Previous {ref_lvl}"
                      for ref_lvl, nw_ref_lvl, wr_id in vals_list]
    statement_string = ',\n'.join(statement_list)
    query = f"""
    UPDATE {cross_sec_loc_layer}
    SET {reference_level_col} = CASE {id_col}
    {statement_string}
    ELSE {reference_level_col}
    END
    """
    return query