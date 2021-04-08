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
