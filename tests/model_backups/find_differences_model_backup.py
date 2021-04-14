def select_values_to_update_from_backup(model_df,
                                        backup_df,
                                        left_id_col,
                                        right_id_col,
                                        old_val_col,
                                        new_val_col):

    try:
        to_update = None
        in_common_df = model_df.merge(right=backup_df[[right_id_col, old_val_col]],
                                      how='inner',
                                      left_on=left_id_col,
                                      right_on=right_id_col,
                                      suffixes=('_model', '_backup'))
        if not in_common_df.empty:
            in_common_df.rename(columns={f'{old_val_col}_model': old_val_col,
                                         f'{old_val_col}_backup': new_val_col},
                                inplace=True)
            to_update = in_common_df.query(f'{old_val_col} != {new_val_col}')
        return to_update
    except Exception as e:
        raise e from None
