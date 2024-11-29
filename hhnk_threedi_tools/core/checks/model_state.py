# -*- coding: utf-8 -*-
"""
Created on Fri Aug 27 14:31:40 2021

@author: chris.kerklaan

This is for once not a object, but these are helper functions which are
used in folder structures

"""
# system imports
import os

import hhnk_research_tools as hrt

# Third-party imports
import pandas as pd

from hhnk_threedi_tools.core.schematisation.model_backup import (
    select_values_to_update_from_backup,
)
from hhnk_threedi_tools.utils.queries import (
    construct_channels_update_statement,
    construct_global_settings_control_group_query,
    construct_manholes_update_query,
    construct_weir_height_update_statement,
    create_bank_levels_update_query,
    create_global_settings_from_backup_query,
    create_global_settings_rows_update_query,
    create_new_manholes_query,
)
from hhnk_threedi_tools.utils.queries import (
    controlled_weirs_selection_query as from_model_query,
)
from hhnk_threedi_tools.utils.queries import (
    weir_widths_from_backup_query as from_backup_query,
)
from hhnk_threedi_tools.variables.backups_table_names import (
    CHANNELS_TABLE,
    GLOBAL_SETTINGS_TABLE,
    MANHOLES_TABLE,
)
from hhnk_threedi_tools.variables.bank_levels import new_bank_level_col
from hhnk_threedi_tools.variables.database_aliases import (
    a_cross_loc_id,
    a_weir_cross_def_id,
    a_weir_id,
)
from hhnk_threedi_tools.variables.database_variables import (
    bank_level_col,
    calculation_type_col,
    channels_layer,
    conn_node_id_col,
    control_group_col,
    cross_sec_def_layer,
    cross_sec_loc_layer,
    global_settings_layer,
    id_col,
    manhole_layer,
    name_col,
    width_col,
    zero_d_one_d_val,
)
from hhnk_threedi_tools.variables.definitions import proposed_value_col

# local imports
from hhnk_threedi_tools.variables.model_state import (
    channels_new_calc_type,
    global_settings_new_col_name,
    hydraulic_test_state,
    invalid_path,
    manholes_new_calc_type,
    one_d_two_d_state,
    undefined_state,
    weirs_new_width_col,
    zero_d_one_d_name,
)


def detect_model_states(model_path):
    """
    Detects the current state of the model by checking:
    It a backup of v2_global settings exists (if not, we assume an undefined state (possibly
    state right after modelbuilder)

    If so we check:
    1. How many entries global_settings has
        If 4: if none of the control_group_id column is NULL and none of name column is '0d1d_test'
        --> 1d2d state
        If 1: if control_group_id column is NULL and name column is '0d1d_test'
        --> Hydraulic test state
    In all other cases, we return undefined
    """

    def is_unique(column):
        a = column.to_numpy()
        return (a[0] == a).all()

    if model_path is None or not os.path.exists(model_path):
        return invalid_path
    try:
        global_settings_df = hrt.execute_sql_selection(
            query=hrt.sql_construct_select_query(table=global_settings_layer),
            database_path=model_path,
        )
        global_settings_backup_exists = hrt.sql_table_exists(
            database_path=model_path, table_name=GLOBAL_SETTINGS_TABLE
        )
        if not global_settings_df.empty and global_settings_backup_exists:
            number_of_rows = global_settings_df.shape[0]
            control_group_value_unique = is_unique(global_settings_df[control_group_col])
            if (
                control_group_value_unique
                and not global_settings_df[control_group_col].iloc[0]
                and number_of_rows == 1
                and global_settings_df[name_col].iloc[0] == zero_d_one_d_name
            ):
                return hydraulic_test_state
            elif (
                global_settings_df[control_group_col].notnull().all()
                and number_of_rows == 4
                and global_settings_df[global_settings_df[name_col] == zero_d_one_d_name].empty
            ):
                return one_d_two_d_state
        return undefined_state
    except Exception as e:
        raise e from None


def get_all_update_queries(
    global_settings_df=None,
    global_settings_to_add=[],
    global_settings_to_delete=[],
    global_settings_excluded=[],
    bank_levels_df=None,
    bank_levels_excluded=[],
    new_manholes_df=None,
    new_manholes_excluded=[],
    update_manholes_df=None,
    update_manholes_excluded=[],
    weir_width_df=None,
    weir_width_excluded=[],
    channels_df=None,
    channels_excluded=[],
):
    """
    Function collects all queries that together make up the model state conversion and combines them. We have to
    execute them all at once because of an issue with sqlite3: its 'execute_script' function issues a commit before
    execution. Therefore, if the third script fails, we can't roll back the changes made by the first two executions.
    """
    try:
        query_list = []
        if global_settings_df is not None and not global_settings_df.empty:
            query = create_global_settings_rows_update_query(
                excluded_ids=global_settings_excluded,
                ids_to_add=global_settings_to_add,
                ids_to_delete=global_settings_to_delete,
            )
            if query is not None:
                query_list.append(query)
            rows_not_to_delete = [item for item in global_settings_to_delete if item in global_settings_excluded]
            # We have to filter the excluded ids and take out the ones that are excluded from being
            # removed (as they will be in the model). List now contains all id's that are not being added
            # and not the ones not being deleted
            update_skip_ids = [item for item in global_settings_excluded if item not in rows_not_to_delete]
            query = construct_global_settings_control_group_query(
                global_settings_to_update_df=global_settings_df,
                excluded_ids=update_skip_ids,
            )
            if query is not None:
                query_list.append(query)
        if bank_levels_df is not None and not bank_levels_df.empty:
            query = create_bank_levels_update_query(
                new_bank_levels_df=bank_levels_df, excluded_ids=bank_levels_excluded
            )
            if query is not None:
                query_list.append(query)
        if new_manholes_df is not None and not new_manholes_df.empty:
            query = create_new_manholes_query(new_manholes_df=new_manholes_df, excluded_ids=new_manholes_excluded)
            if query is not None:
                query_list.append(query)
        if update_manholes_df is not None and not update_manholes_df.empty:
            query = construct_manholes_update_query(
                manholes_to_update_df=update_manholes_df,
                excluded_ids=update_manholes_excluded,
            )
            if query is not None:
                query_list.append(query)
        if weir_width_df is not None and not weir_width_df.empty:
            query = construct_weir_height_update_statement(
                weir_widths_to_update_df=weir_width_df, excluded_ids=weir_width_excluded
            )
            if query is not None:
                query_list.append(query)
        if channels_df is not None and not channels_df.empty:
            query = construct_channels_update_statement(
                channels_to_update_df=channels_df, excluded_ids=channels_excluded
            )
            if query is not None:
                query_list.append(query)
        if query_list:
            full_query = ";\n".join(query_list)
        else:
            full_query = ""
        return full_query
    except Exception as e:
        raise e from None


def collect_excluded(
    global_settings_excluded=None,
    bank_levels_excluded=None,
    new_manholes_excluded=None,
    manhole_updates_excluded=None,
    weirs_heights_excluded=None,
    channels_excluded=None,
):
    exception_format = "Id's overgeslagen:\nTabel: {}\nKolom: {}\nIds: {}"
    exceptions_list = []
    if global_settings_excluded:
        global_settings_body = exception_format.format(
            global_settings_layer, id_col, ", ".join(map(str, global_settings_excluded))
        )
        exceptions_list.append("Global settings overgeslagen ids\n" + global_settings_body)
    if bank_levels_excluded:
        bank_levels_body = exception_format.format(
            cross_sec_loc_layer, id_col, ", ".join(map(str, bank_levels_excluded))
        )
        exceptions_list.append("Bank levels die niet zijn aangepast\n" + bank_levels_body)
    if new_manholes_excluded:
        new_manholes_body = exception_format.format(
            manhole_layer, conn_node_id_col, ", ".join(map(str, new_manholes_excluded))
        )
        exceptions_list.append("Connection nodes waar geen putten aan zijn toegevoegd\n" + new_manholes_body)
    if manhole_updates_excluded:
        update_manholes_body = exception_format.format(
            manhole_layer,
            conn_node_id_col,
            ", ".join(map(str, manhole_updates_excluded)),
        )
        exceptions_list.append("Manholes aanpassingen overgeslagen ids\n" + update_manholes_body)
    if weirs_heights_excluded:
        update_weirs_body = exception_format.format(
            cross_sec_def_layer, id_col, ", ".join(map(str, weirs_heights_excluded))
        )
        exceptions_list.append("Gestuurde stuwen waar breedte niet van is aangepast\n" + update_weirs_body)
    if channels_excluded:
        update_channels_body = exception_format.format(channels_layer, id_col, ", ".join(map(str, channels_excluded)))
        exceptions_list.append("Calculation type watergangen niet aangepast\n" + update_channels_body)
    exceptions_string = "\n\n".join(exceptions_list)
    return exceptions_string


def collect_manual_adjustments(
    global_settings_manual_df=None,
    bank_levels_manual_df=None,
    manhole_update_manual_df=None,
    weir_widths_manual_df=None,
    channels_manual_df=None,
):
    """
    Add the possibility to make columns editable for changes that affect columns (so not the ones
    deleting or adding rows currently). We collect these manual changes and can return them to the user
    for logging purposes (or to make further adjustments).
    """
    try:
        queries_list = []
        if global_settings_manual_df is not None and not global_settings_manual_df.empty:
            queries_list.append(
                hrt.sql_create_update_case_statement(
                    df=global_settings_manual_df,
                    layer=global_settings_layer,
                    df_id_col=id_col,
                    db_id_col=id_col,
                    old_col_name=control_group_col,
                    old_val_col=proposed_value_col,
                    new_val_col=global_settings_new_col_name,
                    show_proposed=True,
                )
            )
        if bank_levels_manual_df is not None and not bank_levels_manual_df.empty:
            queries_list.append(
                hrt.sql_create_update_case_statement(
                    df=bank_levels_manual_df,
                    layer=cross_sec_loc_layer,
                    df_id_col=a_cross_loc_id,
                    db_id_col=id_col,
                    old_col_name=bank_level_col,
                    old_val_col=proposed_value_col,
                    new_val_col=new_bank_level_col,
                    show_proposed=True,
                )
            )
        if manhole_update_manual_df is not None and not manhole_update_manual_df.empty:
            queries_list.append(
                hrt.sql_create_update_case_statement(
                    df=manhole_update_manual_df,
                    layer=manhole_layer,
                    df_id_col=id_col,
                    db_id_col=id_col,
                    old_col_name=calculation_type_col,
                    old_val_col=proposed_value_col,
                    new_val_col=manholes_new_calc_type,
                    show_proposed=True,
                )
            )
        if weir_widths_manual_df is not None and not weir_widths_manual_df.empty:
            queries_list.append(
                hrt.sql_create_update_case_statement(
                    df=weir_widths_manual_df,
                    layer=cross_sec_def_layer,
                    df_id_col=a_weir_cross_def_id,
                    db_id_col=id_col,
                    old_col_name=width_col,
                    old_val_col=proposed_value_col,
                    new_val_col=weirs_new_width_col,
                    show_proposed=True,
                )
            )
        if channels_manual_df is not None and not channels_manual_df.empty:
            queries_list.append(
                hrt.sql_create_update_case_statement(
                    df=channels_manual_df,
                    layer=channels_layer,
                    df_id_col=id_col,
                    db_id_col=id_col,
                    old_col_name=calculation_type_col,
                    old_val_col=proposed_value_col,
                    new_val_col=channels_new_calc_type,
                    show_proposed=True,
                )
            )
        return "\n".join(queries_list)
    except Exception as e:
        raise e from None


def get_proposed_adjustments_channels(model_path, from_state, to_state):
    """
    If the model is currently in hydraulic test state, we need to reset the width
    of controlled weirs to what they were before
    If we are converting to hydraulic test state, we need to multiply them by 10
    """
    try:
        channels_in_model_df = hrt.sqlite_table_to_df(database_path=model_path, table_name=channels_layer)
        if from_state == hydraulic_test_state:
            # reset backup
            channels_backup_df = hrt.sqlite_table_to_df(database_path=model_path, table_name=CHANNELS_TABLE)
            channels_to_update_df = select_values_to_update_from_backup(
                model_df=channels_in_model_df,
                backup_df=channels_backup_df,
                left_id_col=id_col,
                right_id_col=id_col,
                old_val_col=calculation_type_col,
                new_val_col=channels_new_calc_type,
            )
        elif to_state == hydraulic_test_state:
            # set to isolated
            channels_in_model_df.insert(
                channels_in_model_df.columns.get_loc(calculation_type_col) + 1,
                channels_new_calc_type,
                101,
            )
            channels_to_update_df = channels_in_model_df[
                channels_in_model_df[calculation_type_col] != channels_new_calc_type
            ]
        return channels_to_update_df
    except Exception as e:
        raise e from None


def get_rows_to_add(model_path, to_state, rows_in_model_df, id_column):
    """
    Gets all rows that should be in the new model state from the backup, checks
    which ones are already in the model, returns list of the ones that are not
    """
    try:
        query = create_global_settings_from_backup_query(to_state=to_state)
        rows_should_be_in_model = hrt.execute_sql_selection(query=query, database_path=model_path)
        return rows_should_be_in_model.loc[
            ~rows_should_be_in_model[id_column].isin(rows_in_model_df[id_column].tolist())
        ]
    except Exception as e:
        raise e from None


def get_rows_to_delete(rows_in_model_df, to_state, selection_col, id_column):
    """
    Selects ids that are currently in the model and selects which ones
    should be deleted based on the new model state
    """
    delete_ids = []
    if to_state == hydraulic_test_state:
        delete_ids = rows_in_model_df[rows_in_model_df[selection_col] != zero_d_one_d_val][id_column].tolist()
    elif to_state == one_d_two_d_state:
        delete_ids = rows_in_model_df[rows_in_model_df[selection_col] == zero_d_one_d_val][id_column].tolist()
    return delete_ids


def get_global_settings_model(model_path):
    try:
        query = hrt.sql_construct_select_query(table=global_settings_layer)
        global_settings_model = hrt.execute_sql_selection(query=query, database_path=model_path)
        return global_settings_model
    except Exception as e:
        raise e from None


def get_proposed_adjustments_global_settings(model_path, to_state):
    """
    Creates widget to display proposed changes to global settings
    If current state is modelbuilder -> delete rows and turn model control on or off based on state to convert to
    If current state is hydraulic test, delete all rows, add rows from backup, make sure model control is on
    If current state is 1d2d test, delete all rows, add rows from backup, make sure model control is off
    """
    try:
        rows_in_model = get_global_settings_model(model_path=model_path)
        rows_to_add = get_rows_to_add(
            model_path=model_path,
            to_state=to_state,
            rows_in_model_df=rows_in_model,
            id_column=id_col,
        )
        rows_to_delete = get_rows_to_delete(
            rows_in_model_df=rows_in_model,
            to_state=to_state,
            selection_col=name_col,
            id_column=id_col,
        )
        preview_df = pd.concat([rows_in_model, rows_to_add])[[id_col, name_col, control_group_col]]
        if to_state == hydraulic_test_state:
            new_value = None
        elif to_state == one_d_two_d_state:
            new_value = 1
        else:
            raise Exception("No new state defined (or unknown)")
        preview_df.insert(
            preview_df.columns.get_loc(control_group_col) + 1,
            global_settings_new_col_name,
            new_value,
        )
        return preview_df, rows_to_delete, rows_to_add[id_col].tolist()
    except Exception as e:
        raise e from None


def get_proposed_updates_manholes(model_path, to_state, from_state):
    """
    If the model is currently in hydraulic test state, we need to reset the original values for the calculation_type
    field from the backup.
    If we are converting to hydraulic test state, we need to set all calculation_type values to isolated
    (in this case: 1)
    """
    try:
        manholes_in_model_df = hrt.sqlite_table_to_df(database_path=model_path, table_name=manhole_layer)
        if to_state == hydraulic_test_state:
            # We have to set all calculation types to isolated
            manholes_in_model_df.insert(
                manholes_in_model_df.columns.get_loc(calculation_type_col) + 1,
                manholes_new_calc_type,
                1,
            )
            manholes_to_update = manholes_in_model_df[
                manholes_in_model_df[calculation_type_col] != manholes_in_model_df[manholes_new_calc_type]
            ]
        elif from_state == hydraulic_test_state:
            # we have to restore original calculation types from backup
            manholes_backup_df = hrt.sqlite_table_to_df(database_path=model_path, table_name=MANHOLES_TABLE)
            manholes_to_update = select_values_to_update_from_backup(
                model_df=manholes_in_model_df,
                backup_df=manholes_backup_df,
                left_id_col=id_col,
                right_id_col=id_col,
                old_val_col=calculation_type_col,
                new_val_col=manholes_new_calc_type,
            )
        return manholes_to_update
    except Exception as e:
        raise e from None


def get_proposed_adjustments_weir_width(model_path, from_state, to_state):
    """
    If the model is currently in hydraulic test state, we need to reset the width
    of controlled weirs to what they were before
    If not, we need to multiply them by 10
    """
    try:
        weir_widths_in_model_df = hrt.execute_sql_selection(query=from_model_query, database_path=model_path)
        if from_state == hydraulic_test_state:
            # reset backup
            weir_widths_backup_df = hrt.execute_sql_selection(query=from_backup_query, database_path=model_path)
            weir_widths_to_update = select_values_to_update_from_backup(
                model_df=weir_widths_in_model_df,
                backup_df=weir_widths_backup_df,
                left_id_col=a_weir_id,
                right_id_col=a_weir_id,
                old_val_col=width_col,
                new_val_col=weirs_new_width_col,
            )
        elif to_state == hydraulic_test_state:
            # multiply by 10
            weir_widths_in_model_df[width_col] = pd.to_numeric(weir_widths_in_model_df[width_col])
            weir_widths_in_model_df.insert(
                weir_widths_in_model_df.columns.get_loc(width_col) + 1,
                weirs_new_width_col,
                weir_widths_in_model_df[width_col].apply(lambda x: round((x * 10), 3)),
            )
            weir_widths_to_update = weir_widths_in_model_df
        return weir_widths_to_update
    except Exception as e:
        raise e from None
