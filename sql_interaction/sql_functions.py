import sqlite3
import pandas as pd
from ..queries.query_functions import construct_select_query

def create_sqlite_connection(database_path):
    try:
        conn = sqlite3.connect(database_path)
        conn.enable_load_extension(True)
        conn.execute("SELECT load_extension('mod_spatialite')")
        return conn
    except Exception as e:
        raise e from None

def table_exists(database_path, table_name):
    """
    Checks if a table name exists in the specified database
    """
    try:
        query = f"""PRAGMA table_info({table_name})"""
        df = execute_sql_selection(query=query, database_path=database_path)
        return not df.empty
    except Exception as e:
        raise e from None

def execute_sql_selection(query, conn=None, database_path=None, **kwargs):
    """
    Execute sql query. Creates own connection if database path is given.
    Returns pandas dataframe
    """
    kill_connection = conn is None
    try:
        if database_path is None and conn is None:
            raise Exception("No connection or database path provided")
        if database_path is not None:
            conn = create_sqlite_connection(database_path=database_path)
        db = pd.read_sql(query, conn, **kwargs)
        return db
    except Exception as e:
        raise e from None
    finally:
        if kill_connection and conn is not None:
            conn.close()

def execute_sql_changes(query, database=None, conn=None):
    """
    Takes a query that changes the database and tries
    to execute it. On success, changes are committed.
    On a failure, rolls back to the state before
    the query was executed that caused the error

    The explicit begin and commit statements are necessary
    to make sure we can roll back the transaction
    """
    conn_given = True
    try:
        if not conn:
            conn_given = False
            conn = create_sqlite_connection(database)
        try:
            conn.executescript(f'BEGIN; {query}; COMMIT')
        except Exception as e:
            conn.rollback()
            raise e from None
    except Exception as e:
        raise e from None
    finally:
        if not conn_given and conn:
            conn.close()

def get_creation_statement_from_table(src_table_name, dst_table_name, cursor):
    try:
        creation_sql = f"""
                    SELECT sql
                    FROM sqlite_master
                    WHERE type = 'table'
                    AND name = '{src_table_name}'
                    """
        # Replace the original table name with the new name to make the creation statement
        create_statement = cursor.execute(creation_sql).fetchone()[0]
        to_list = create_statement.split()
        all_but_name = [item if index != 2 else f'"{dst_table_name}"' for index, item in enumerate(to_list)]
        creation_statement = " ".join(all_but_name)
        return creation_statement
    except Exception as e:
        raise e from None

def replace_or_add_table(db,
                         dst_table_name,
                         src_table_name,
                         select_statement=None):
    """
    This functions maintains the backup tables.
    Tables are created if they do not exist yet.
    After that, rows are replaced if their id is already
    in the backup, otherwise they are just inserted.

    columns HAS to be a list of tuples containing the name
    of the column and it's type
    """
    try:
        print(f"Replace or add table {src_table_name} {dst_table_name}")
        print(select_statement)
        query_list = []
        conn = create_sqlite_connection(database_path=db)
        curr = conn.cursor()
        # Check if table exists
        exists = curr.execute(f"SELECT count() from sqlite_master "
                              f"WHERE type='table' and name='{dst_table_name}'").fetchone()[0]
        if exists == 0:
            print("Doesn't exist yet")
            # Get the original creation statement from the table we are backing up if the new table doesn't exist
            if select_statement is None:
                select_statement = f"SELECT * from {src_table_name}"
            creation_statement = get_creation_statement_from_table(src_table_name=src_table_name,
                                                                   dst_table_name=dst_table_name,
                                                                   cursor=curr)
            print(f"creation statement {creation_statement}")
            query_list.append(creation_statement)
            # Create the statement that copies the columns specified in select statement or copies the entire table
            copy_statement = f"INSERT INTO {dst_table_name} {select_statement}"
            query_list.append(copy_statement)
            query = ';\n'.join(query_list)
        else:
            # If the backup table exists, we replace any rows that are changed since last backup
            query = f"REPLACE INTO {dst_table_name} " \
                    f"SELECT * from {src_table_name}"
        execute_sql_changes(query=query, conn=conn)
    except Exception as e:
        raise e from None
    finally:
        if conn:
            conn.close()

def get_table_as_df(database_path, table_name, columns=None):
    conn = None
    try:
        conn = create_sqlite_connection(database_path=database_path)
        query = construct_select_query(table_name, columns)
        df = execute_sql_selection(query=query, conn=conn)
        return df
    except Exception as e:
        raise e from None
    finally:
        if conn:
            conn.close()
