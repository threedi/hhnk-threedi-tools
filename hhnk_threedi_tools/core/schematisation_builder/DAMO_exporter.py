# %%
"""DAMO exporter based on model extent"""

import geopandas as gpd
from hhnk_research_tools.sql_functions import (
    database_to_gdf,
    sql_builder_select_by_location,
)
from hhnk_threedi_plugin.local_settings import DATABASES
from shapely.geometry import box


def DAMO_exporter(model_extent, table_names, EPSG_CODE="28992"):
    """Exports data from DAMO for polder of interest.
    
    Parameters
    ----------
    model_extent : (MULTI)POLYGON
        Select geometry in .gpkg file of polder
    table_names : list
        f"landuse_{landuse_name}.tif" -> name to use in the output. 'landuse_' will be prepended.
    ESPG_CODE : str
        Default is "28992"

    Returns
    -------
    gpkg -> output from DAMO for each table.
    """

    db_dicts = {
        "aquaprd": DATABASES.get("aquaprd_lezen", None),
        "bgt": DATABASES.get("bgt_lezen", None),
    }

    schema = "DAMO_W"

    # make bbox --> to simply string request to DAMO db
    bbox_model = box(*model_extent.total_bounds)

    dict_gdfs_damo = {}
    for table in table_names:
        # Build sql to select by input polygon
        sql = sql_builder_select_by_location(
            schema=schema, table_name=table, epsg_code=EPSG_CODE, polygon_wkt=bbox_model, simplify=True
        )
        db_dict = db_dicts["aquaprd"]
        columns = None

        bbox_gdf, sql2 = database_to_gdf(db_dict=db_dict, sql=sql, columns=columns)

        # clip gdf to model_extent again
        gdf_model = gpd.clip(bbox_gdf, model_extent)

        # add gdf of table export from DAMO to dictonary
        dict_gdfs_damo[table] = gdf_model

    return dict_gdfs_damo


# %%
# Test
if __name__ == "__main__":
    POLDERS_PATH = r"\\corp.hhnk.nl\data\Hydrologen_data\Data\09.modellen_speeltuin\egmondermeer_leggertool\01_source_data\polder_polygon.shp"
    POLDERS = gpd.read_file(POLDERS_PATH, engine="pyogrio")
    model_extent = POLDERS
    output_DAMO = DAMO_exporter(model_extent, ["HYDROOBJECT"])
