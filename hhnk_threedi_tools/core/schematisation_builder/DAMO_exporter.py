# -*- coding: utf-8 -*-
"""
Created on Fri Nov 29 09:17:01 2024

@author: esther.vanderlaan
"""

from hhnk_research_tools import sql_functions
from local_settings import DATABASES
 

INPUT_DIRECTORY = "area_test_sql_helsdeur.gpkg" # verwijzing naar project directory - model extent
EPSG_CODE = "28992"


 """test_database_to_gdf"""
# %%

def DAMO_exporter(model_extent):

    db_dicts = {
        "aquaprd": DATABASES.get("aquaprd_lezen", None),
        "bgt": DATABASES.get("bgt_lezen", None),
    }
    
    db_dict = db_dicts["bgt"]
    columns = None
    
    #TODO: hoe is DAMO database opgebouw? Welke table_name moeten we hebben? Of meerdere?
    schema = "DAMO_W"
    table_name = "GEMAAL"
    
    # Load area for selection
    model_extent_gdf = gpd.read_file(model_extent, engine="pyogrio")
    polygon_wkt = model_extent_gdf.iloc[0]["geometry"]
    
    # Build sql to select by input polygon
    sql = sql_builder_select_by_location(
        schema=schema, table_name=table_name, epsg_code=EPSG_CODE, polygon_wkt=polygon_wkt, simplify=True
    )
    db_dict = db_dicts["aquaprd"]
    columns = None
    
    
    gdf, sql2 = database_to_gdf(db_dict=db_dict, sql=sql, columns=columns)
    assert gdf.loc[0, "code"] == "KGM-Q-29234"