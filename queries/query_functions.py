def create_update_case_statement(df, layer, df_id_col, db_id_col, new_val_col, excluded_ids=[],
                                 old_val_col=None, old_col_name=None, show_prev=False, show_proposed=False):
    """
    Creates an sql statement with the following structure:
    UPDATE (table_name)
    SET (database_column_to_change) = CASE (database_id_col)
    WHEN (id) THEN (new value associated with id) OPTIONAL -- ['Previous' or 'Proposed'] previous or proposed value

    Initialization:

    """
    if show_proposed and show_prev:
        raise Exception("create_update_case_statement: "
                        "Only one of show_prev and show_proposed can be True")
    try:
        query = None
        if not show_prev and not show_proposed:
            vals_list = [(idx, val) for idx, val in zip(df[df_id_col], df[new_val_col])
                         if idx not in excluded_ids]
            statement_list = [f"WHEN {idx} THEN {val if not val is None else 'null'}"
                              for idx, val in vals_list]
        else:
            comment = 'Previous:' if show_prev else 'Proposed'
            vals_list = [(old_val, new_val, cur_id) for old_val, new_val, cur_id in
                         zip(df[old_val_col], df[new_val_col], df[df_id_col])
                         if cur_id not in excluded_ids]
            statement_list = [f"WHEN {cur_id} THEN {new_val if not new_val is None else 'null'} -- {comment} {old_val}"
                              for old_val, new_val, cur_id in vals_list]
        if statement_list:
            statement_string = '\n'.join(statement_list)
            query = f"""
            UPDATE {layer}
            SET {old_col_name if old_col_name is not None else old_val_col} = CASE {db_id_col}
            {statement_string}
            ELSE {old_val_col}
            END
            """
        return query
    except Exception as e:
        raise e from None

def construct_select_query(table, columns=None):
    """
    This functions constructs sql queries that select either all
    or specified columns from a table.

    Columns has to be a list. If a list item is a tuple, it will be interpreted as:
    (column, alias). In other cases, we assume the item to be a valid column name.
    """
    base_query = "SELECT {columns} \nFROM {table}"
    try:
        if columns is not None:
            selection_lst = []
            if type(columns) == dict:
                for key, value in columns.items():
                    if value is not None:
                        selection_lst.append(f'{key} AS {value}')
                    else:
                        selection_lst.append(key)
            elif type(columns) == list:
                selection_lst = columns
            query = base_query.format(columns=',\n'.join(selection_lst), table=table)
        else:
            query = base_query.format(columns="*", table=table)
        return query
    except Exception as e:
        raise e from None
