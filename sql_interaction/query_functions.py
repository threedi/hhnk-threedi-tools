def construct_select_query(table, columns=None):
    """
    This functions constructs sql queries that select either all
    or specified columns from a table.

    Columns has to be a list. If a list item is a tuple, it will be interpreted as:
    (column, alias). In other cases, we assume the item to be a valid column name.
    """
    base_query = "SELECT {columns} FROM {table}"
    try:
        if columns is not None:
            selection_lst = []
            for item in columns:
                if type(item) == tuple:
                    selection_lst.append(f"{item[0]} AS {item[1]}")
                else:
                    selection_lst.append(item)
        else:
            query = base_query.format(columns="*", table=table)
        return query
    except Exception as e:
        raise e from None
