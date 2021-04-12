import geopandas as gpd
from shapely import wkt
from ..variables.definitions import WKT
from ..variables.default_variables import DEF_GEOMETRY_COL, DEF_SRC_CRS, DEF_TRGT_CRS
from ..sql_interaction.sql_functions import execute_sql_selection

def set_geometry_by_type(db, geom_col_type, col=DEF_GEOMETRY_COL):
    if geom_col_type == WKT:
        try:
            db[col] = db[col].apply(wkt.loads)
        except Exception as e:
            raise e from None

def convert_df_to_gdf(df, geom_col_type=WKT, geometry_col=DEF_GEOMETRY_COL,
                      src_crs=DEF_SRC_CRS, trgt_crs=DEF_TRGT_CRS):
    src_epsg = f'EPSG:{src_crs}'
    try:
        set_geometry_by_type(df, geom_col_type, geometry_col)
        gdf = gpd.GeoDataFrame(df, geometry=geometry_col, crs=src_epsg)
        gdf.to_crs(epsg=trgt_crs, inplace=True)
        return gdf
    except Exception as e:
        raise e from None

def gdf_from_sql(conn, query, id_col, to_gdf=True):
    try:
        df = execute_sql_selection(query=query, conn=conn)
        if to_gdf:
            df = convert_df_to_gdf(df=df)
        df.set_index(id_col, drop=False, inplace=True)
        return df
    except Exception as e:
        raise e from None
