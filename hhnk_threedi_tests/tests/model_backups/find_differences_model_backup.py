import pandas as pd

def select_values_to_update_from_backup(model_df,
                                        backup_df,
                                        left_id_col,
                                        right_id_col,
                                        old_val_col,
                                        new_val_col):
    """
    Merges the backup into the representation of the model, then checks
    whether the relevant value has changed. If so, we keep this row,
    otherwise we drop it
    """
    try:
        to_update = pd.DataFrame()
        in_common_df = model_df.merge(right=backup_df[[right_id_col, old_val_col]],
                                      how='inner',
                                      left_on=left_id_col,
                                      right_on=right_id_col,
                                      suffixes=('_model', '_backup'))
        if not in_common_df.empty:
            in_common_df.rename(columns={f'{old_val_col}_model': old_val_col,
                                         f'{old_val_col}_backup': new_val_col},
                                inplace=True)
            new_val_col_vals = in_common_df[new_val_col]
            columns_list = in_common_df.columns.tolist()
            columns_list.remove(new_val_col)
            to_update = in_common_df[columns_list]
            to_update.insert(to_update.columns.get_loc(old_val_col) + 1, new_val_col, new_val_col_vals)
            to_update = to_update.query(f'{old_val_col} != {new_val_col}')
        return to_update
    except Exception as e:
        raise e from None
