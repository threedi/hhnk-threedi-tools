# %%
"""DAMO exporter based on model extent"""

from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import pandas as pd
from hhnk_research_tools.sql_functions import (
    database_to_gdf,
    sql_builder_select_by_location,
)
from shapely.geometry import box

try:
    from local_settings_htt import DATABASES, DB_LAYERS_MAPPING
except ImportError as e:
    raise ImportError(
        "The 'local_settings_htt' module is missing. Get it from D:\github\evanderlaan\local_settings_htt.py and place it in \hhnk_threedi_tools\core\schematisation_builder"
    ) from e


def DAMO_exporter(
    model_extent_gdf: gpd.GeoDataFrame,
    table_names: list[str],
    output_file: Path,
    EPSG_CODE: str = "28992",
    logger=None,
) -> list:
    """
    Export data from DAMO for polder of interest.

    Parameters
    ----------
    model_extent_gdf : GeoDataFrame
        GeoDataFrame of the selected polder
    table_names : list[str]
        f"landuse_{landuse_name}.tif" -> name to use in the output. 'landuse_' will be prepended.
    output_file: Path
        path to output gpkg file in project directory.
    ESPG_CODE : str, default is "28992"
        Projection string epsg code

    Writes
    ------
    output_file : Path
        GPKG file containing the exported tables from DAMO.

    Returns
    -------
    logging_DAMO : list
        Error logs when a table export fails
    """
    if not logger:
        logger = hrt.logging.get_logger(__name__)
    logger.info("Start export from DAMO database")

    db_dicts = {
        "aquaprd": DATABASES.get("aquaprd_lezen", None),
        "bgt": DATABASES.get("bgt_lezen", None),
    }

    schema = "DAMO_W"

    # make bbox --> to simplify string request to DAMO db
    bbox_model = box(*model_extent_gdf.total_bounds)

    logging_DAMO = []
    for table in table_names:
        try:
            logger.info(f"Start export of table {table} from DAMO database")
            # Build sql string for request to DAMO db for given input polygon
            sql = sql_builder_select_by_location(
                schema=schema, table_name=table, epsg_code=EPSG_CODE, polygon_wkt=bbox_model, simplify=True
            )
            layer_db = DB_LAYERS_MAPPING.get(table, None)
            db_dict = db_dicts[layer_db]
            logger.info(f"Get out of database {layer_db} for table {table}")
            columns = None
            # exports data from DAMO database
            bbox_gdf, sql2 = database_to_gdf(db_dict=db_dict, sql=sql, columns=columns)

            # select all objects which (partly) lay within the model extent
            gdf_model = bbox_gdf[bbox_gdf["geometry"].intersects(model_extent_gdf["geometry"][0])]

            # make sure that all colums which can contain dates has the type datetime
            for col in gdf_model.columns:
                if "date" in col or "datum" in col:
                    gdf_model[col] = pd.to_datetime(gdf_model[col], errors="coerce")

            # adds table to geopackage file as a layer
            gdf_model.to_file(output_file, layer=table, driver="GPKG")

            logger.info(f"Finished export of table {table} from DAMO database")
        except Exception as e:
            error = f"An error occured while exporting data of table {table} from DAMO: {e}"
            logger.error(error)
            logging_DAMO.append(error)

    return logging_DAMO
