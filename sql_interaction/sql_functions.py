import sqlite3
import pandas as pd

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
        if conn is None and database_path is not None:
            conn = create_sqlite_connection(database_path=database_path)
        else:
            raise Exception("No connection or database path provided")
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
