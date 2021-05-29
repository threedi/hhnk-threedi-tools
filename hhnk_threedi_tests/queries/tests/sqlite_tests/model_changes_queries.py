from ....variables.database_aliases import a_weir_cross_loc_id
from ....variables.database_variables import reference_level_col, cross_sec_loc_layer, id_col
from ....tests.sqlite_tests.weir_height_test import new_ref_lvl
import hhnk_research_tools as hrt


def create_update_reference_level_query(wrong_profiles_gdf, excluded_ids=[]):
    """
    Creates qsl query to update reference level where minimum weir height is below
    ground level (any deselected id's are skipped)
    """
    query = hrt.sql_create_update_case_statement(df=wrong_profiles_gdf,
                                         layer=cross_sec_loc_layer,
                                         df_id_col=a_weir_cross_loc_id,
                                         db_id_col=id_col,
                                         old_val_col=reference_level_col,
                                         new_val_col=new_ref_lvl,
                                         excluded_ids=excluded_ids)
    return query
