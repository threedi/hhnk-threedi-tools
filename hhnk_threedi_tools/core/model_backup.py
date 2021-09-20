# -*- coding: utf-8 -*-
"""
Created on Fri Aug 27 14:42:27 2021

@author: chris.kerklaan
"""
# Third-party imports
import pandas as pd
import hhnk_research_tools as hrt

# Local imports
from hhnk_threedi_tools.variables.model_state import undefined_state, one_d_two_d_state
from hhnk_threedi_tools.variables.backups_table_names import (
    GLOBAL_SETTINGS_TABLE,
    MANHOLES_TABLE,
    CONTR_WEIR_WIDTH_BACKUP,
    CHANNELS_TABLE,
    BANK_LVLS_LAST_CALC,
)
from hhnk_threedi_tools.variables.database_variables import (
    global_settings_layer,
    channels_layer,
    cross_sec_def_layer,
    manhole_layer,
)
from hhnk_threedi_tools.utils.queries import (
    weir_width_backup_query,
    bank_lvls_source_update_query,
    bank_lvls_source_creation_query,
)


def create_backups(model_path, state=None, manholes_bank_levels_only=False):
    """
    Creates backups based on current model state. If manholes_bank_levels_only is true,
    the function is being called after successful changes by bank_levels function.

    This way, if the bank_levels test was run already, we can use those manholes and bank_levels
    instead of recalculating.
    """
    try:
        if manholes_bank_levels_only == False:
            if state == undefined_state:
                hrt.sqlite_replace_or_add_table(
                    db=model_path,
                    dst_table_name=GLOBAL_SETTINGS_TABLE,
                    src_table_name=global_settings_layer,
                )
                # hrt.sqlite_replace_or_add_table(db=model_path,
                #                      dst_table_name=BANK_LVLS_TABLE,
                #                      src_table_name=cross_sec_loc_layer)
                hrt.sqlite_replace_or_add_table(
                    db=model_path,
                    dst_table_name=CHANNELS_TABLE,
                    src_table_name=channels_layer,
                )
                hrt.sqlite_replace_or_add_table(
                    db=model_path,
                    dst_table_name=CONTR_WEIR_WIDTH_BACKUP,
                    src_table_name=cross_sec_def_layer,
                    select_statement=weir_width_backup_query,
                )
                hrt.sqlite_replace_or_add_table(
                    db=model_path,
                    dst_table_name=MANHOLES_TABLE,
                    src_table_name=manhole_layer,
                )
            elif state == one_d_two_d_state:
                # hrt.sqlite_replace_or_add_table(db=model_path,
                #                      dst_table_name=BANK_LVLS_TABLE,
                #                      src_table_name=cross_sec_loc_layer)
                hrt.sqlite_replace_or_add_table(
                    db=model_path,
                    dst_table_name=MANHOLES_TABLE,
                    src_table_name=manhole_layer,
                )
                hrt.sqlite_replace_or_add_table(
                    db=model_path,
                    dst_table_name=CHANNELS_TABLE,
                    src_table_name=channels_layer,
                )
                hrt.sqlite_replace_or_add_table(
                    db=model_path,
                    dst_table_name=CONTR_WEIR_WIDTH_BACKUP,
                    src_table_name=cross_sec_def_layer,
                    select_statement=weir_width_backup_query,
                )
        else:
            hrt.sqlite_replace_or_add_table(
                db=model_path,
                dst_table_name=MANHOLES_TABLE,
                src_table_name=manhole_layer,
            )
    except Exception as e:
        raise e from None


def select_values_to_update_from_backup(
    model_df, backup_df, left_id_col, right_id_col, old_val_col, new_val_col
):
    """
    Merges the backup into the representation of the model, then checks
    whether the relevant value has changed. If so, we keep this row,
    otherwise we drop it
    """
    try:
        to_update = pd.DataFrame()
        in_common_df = model_df.merge(
            right=backup_df[[right_id_col, old_val_col]],
            how="inner",
            left_on=left_id_col,
            right_on=right_id_col,
            suffixes=("_model", "_backup"),
        )
        if not in_common_df.empty:
            in_common_df.rename(
                columns={
                    f"{old_val_col}_model": old_val_col,
                    f"{old_val_col}_backup": new_val_col,
                },
                inplace=True,
            )
            new_val_col_vals = in_common_df[new_val_col]
            columns_list = in_common_df.columns.tolist()
            columns_list.remove(new_val_col)
            to_update = in_common_df[columns_list]
            to_update.insert(
                to_update.columns.get_loc(old_val_col) + 1,
                new_val_col,
                new_val_col_vals,
            )
            to_update = to_update.query(f"{old_val_col} != {new_val_col}")
        return to_update
    except Exception as e:
        raise e from None


def update_bank_levels_last_calc(db):
    """
    Everytime we calculate the bank levels again, we update the timestamp when they were last calculated
    """
    try:
        if not hrt.sql_table_exists(database_path=db, table_name=BANK_LVLS_LAST_CALC):
            hrt.execute_sql_changes(query=bank_lvls_source_creation_query, database=db)
        hrt.execute_sql_changes(query=bank_lvls_source_update_query, database=db)
    except Exception as e:
        raise e from None
