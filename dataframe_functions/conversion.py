import geopandas as gpd
from shapely import wkt
from ..variables.definitions import WKT
from ..variables.default_variables import DEF_GEOMETRY_COL, DEF_SRC_CRS, DEF_TRGT_CRS
from ..sql_interaction.sql_functions import execute_sql_selection, create_sqlite_connection

def set_geometry_by_type(df, geom_col_type, col=DEF_GEOMETRY_COL):
    """
    Converts geometry if necessary, depending on geometry column type

        set_geometry_by_type(
            df (pandas DataFrame),
            geom_col_type (string: type of geometry)
            col -> 'geometry' (string: name of column containing geometry in df)

    replaces geometry column with converted values
    """
    if geom_col_type == WKT:
        try:
            df[col] = df[col].apply(wkt.loads)
        except Exception as e:
            raise e from None

def convert_df_to_gdf(df, geom_col_type=WKT, geometry_col=DEF_GEOMETRY_COL,
                      src_crs=DEF_SRC_CRS, trgt_crs=DEF_TRGT_CRS):
    """
    Convert a pandas DataFrame to a geopandas GeoDataFrame

        convert_df_to_gdf(
            df (original pandas dataframe)
            geom_col_type -> WKT (type of geometry column to make sure geometry is interpreted correctly)
            geometry_col -> 'geometry' (string: name of column in df to be used as geometry)
            src_crs -> 4326 (original projection geometry)
            trgt_crs -> 28992 (crs to convert geometry to)
            )
    """
    src_epsg = f'EPSG:{src_crs}'
    try:
        set_geometry_by_type(df, geom_col_type, geometry_col)
        gdf = gpd.GeoDataFrame(df, geometry=geometry_col, crs=src_epsg)
        gdf.to_crs(epsg=trgt_crs, inplace=True)
        return gdf
    except Exception as e:
        raise e from None

def gdf_from_sql(query, id_col, to_gdf=True, conn=None, database_path=None):
    """
    Returns DataFrame or GeoDataFrame from database query.

        gdf_from_sql(
                query (string)
                id_col (identifying column)
                to_gdf -> True (if False, DataFrame is returned)
                conn -> None (sqlite3 connection object)
                database_path -> None (path to database)

                Supply either conn or database path.
                )
    """
    if (conn is None and database_path is None) or (conn is not None and database_path is not None):
        raise Exception("Provide exactly one of conn or database_path")
    try:
        kill_conn = conn is None
        if conn is None:
            conn = create_sqlite_connection(database_path=database_path)
        df = execute_sql_selection(query=query, conn=conn)
        if to_gdf:
            df = convert_df_to_gdf(df=df)
        df.set_index(id_col, drop=False, inplace=True)
        return df
    except Exception as e:
        raise e from None
    finally:
        if kill_conn and conn is not None:
            conn.close()

def create_gdf_from_df(df, geometry_col, crs=DEF_TRGT_CRS):
    """
    Creates geopandas GeoDataFrame from pandas DataFrame

        create_gdf_from_df(
                df (pandas DataFrame)
                geometry_col (Geometry column for GeoDataFrame)
                crs (projection) -> 28992
            )

    return value: GeoDataFrame with df as data, geometry_col as geometry and crs as crs
    """
    trgt_crs = f'EPSG:{crs}'
    try:
        gdf = gpd.GeoDataFrame(df, geometry=geometry_col, crs=trgt_crs)
        return gdf
    except Exception as e:
        raise e from None
