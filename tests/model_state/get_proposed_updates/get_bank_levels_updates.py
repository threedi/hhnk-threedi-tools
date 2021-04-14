# from ..variables.definitions import one_d_two_d_state, one_d_two_d_from_backup
# from ....variables.database_variables import cross_sec_loc_layer, id_col, bank_level_col
# from ....variables.database_aliases import a_cross_loc_id
# from ....dataframe_functions.conversion import gdf_from_sql
# from ....tests.bank_levels.variables.dataframe_variables import new_bank_level_col
#
# def get_bank_levels_to_update_from_backup(test_env, backup_table):
#     try:
#         model_path = test_env.paths['model']
#         model_bank_levels_df = gdf_from_sql(database_path=model_path,
#                                                table_name=cross_sec_loc_layer)
#         backup_bank_levels_df = gdf_from_sql(database_path=model_path,
#                                                 table_name=backup_table)
#         bank_levels_to_update = select_values_to_update_from_backup(model_df=model_bank_levels_df,
#                                                                     backup_df=backup_bank_levels_df,
#                                                                     left_id_col=id_col,
#                                                                     right_id_col=id_col,
#                                                                     old_val_col=bank_level_col,
#                                                                     new_val_col=new_bank_level_col)
#         if bank_levels_to_update is not None and not bank_levels_to_update.empty:
#             bank_levels_to_update.rename(columns={id_col: a_cross_loc_id},
#                                          inplace=True)
#             return bank_levels_to_update
#         else:
#             return None
#     except Exception as e:
#         raise e from None
#
# def get_proposed_adjustments_bank_levels(test_env):
#     try:
#         to_state = test_env.conversion_vars.to_state
#         from_state = test_env.conversion_vars.from_state
#         if to_state == one_d_two_d_state:
#             if from_state == one_d_two_d_from_backup:
#                 bank_levels_to_update = get_bank_levels_to_update_from_backup()
#     except Exception as e:
#         raise e from None
