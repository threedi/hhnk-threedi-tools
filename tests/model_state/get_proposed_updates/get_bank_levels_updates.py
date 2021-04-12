from ..variables.definitions import one_d_two_d_state, one_d_two_d_from_calc, one_d_two_d_from_backup

show_columns = [a_cross_id, a_chan_id, ref_lvl_col, bank_level, new_banklevel_col]

def get_bank_levels_widget(bank_levels_to_update, col_editable=True, rows_selectable=True):
    try:
        bank_levels_preview = bank_levels_to_update[show_columns]
        old_idx = bank_levels_preview.columns.get_loc(bank_level)
        new_idx = bank_levels_preview.columns.get_loc(new_banklevel_col)
        print(f'old index {old_idx} new index {new_idx} col_editable {col_editable}')
        widget = ModelStateChangesPreview(window_title="Voorgestelde aanpassingen bank levels",
                                          description="Waarden in rood worden vervangen voor waarden in het groen."
                                                       "Deselecteer een rij om de wijziging over te slaan",
                                          df=bank_levels_preview,
                                          id_col=a_cross_id,
                                          old_col_idx=old_idx,
                                          new_col_idx=new_idx,
                                          new_col_editable=col_editable,
                                          rows_selectable=rows_selectable)
        return widget
    except Exception as e:
        raise e from None

def get_bank_levels_to_update_from_backup(test_env, backup_table):
    try:
        model_path = test_env.paths['model']
        model_bank_levels_df = get_table_as_df(database_path=model_path,
                                               table_name=cross_sec_loc_layer)
        backup_bank_levels_df = get_table_as_df(database_path=model_path,
                                                table_name=backup_table)
        bank_levels_to_update = select_values_to_update_from_backup(model_df=model_bank_levels_df,
                                                                    backup_df=backup_bank_levels_df,
                                                                    left_id_col=id,
                                                                    right_id_col=id,
                                                                    old_val_col=bank_level,
                                                                    new_val_col=new_banklevel_col)
        if bank_levels_to_update is not None and not bank_levels_to_update.empty:
            bank_levels_to_update.rename(columns={id: a_cross_id},
                                         inplace=True)
            return bank_levels_to_update
        else:
            return None
    except Exception as e:
        raise e from None

def get_proposed_adjustments_to_1d2d(test_env, states):
    try:
        update_bank_levels_widget = None
        if states.one_d_two_d_from == one_d_two_d_from_calc:
            if states.bank_levels_from_calc is not None and not states.bank_levels_from_calc.empty:
                update_bank_levels_widget = get_bank_levels_widget(states.bank_levels_from_calc)
        elif states.one_d_two_d_from == one_d_two_d_from_1d2d_backup:
            bank_levels_to_update = get_bank_levels_to_update_from_backup(
                test_env=test_env,
                backup_table=BANK_LVLS_1D2D_TABLE)
            if bank_levels_to_update is not None and not bank_levels_to_update.empty:
                update_bank_levels_widget = get_bank_levels_widget(bank_levels_to_update)
        return update_bank_levels_widget
    except Exception as e:
        raise e from None

def get_proposed_adjustments_bank_levels(test_env):
    try:
        to_state = test_env.conversion_vars.to_state
        from_state = test_env.conversion_vars.from_state
        if to_state == one_d_two_d_state:
            if from_state == one_d_two_d_from_calc:
                pass
            elif from_state == one_d_two_d_from_backup:
                get_bank_levels_to_update_from_backup()
    except Exception as e:
        raise e from None
