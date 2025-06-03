# %%
"""DAMO exporter based on model extent"""

from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import pandas as pd
from hhnk_research_tools.sql_functions import (
    database_to_gdf,
    sql_builder_select_by_id_list_statement,
    sql_builder_select_by_location,
)
from shapely.geometry import box

from hhnk_threedi_tools.resources.schematisation_builder.db_layer_mapping import DB_LAYER_MAPPING

try:
    from hhnk_threedi_tools.resources.schematisation_builder.local_settings_htt import DATABASES
except ImportError as e:
    raise ImportError(
        r"The 'local_settings_htt' module is missing. Get it from \\corp.hhnk.nl\data\Hydrologen_data\Data\01.basisgegevens\00.HDB\SettingsAndRecourses\local_settings_htt.py and place it in \hhnk_threedi_tools\core\resources\schematisation_builder"
    ) from e

# From mapping json list top level keys
tables_default = list(DB_LAYER_MAPPING.keys())


# %%
def DAMO_exporter(  # TODO rename to DB_exporter
    model_extent_gdf: gpd.GeoDataFrame,
    output_file: Path,
    table_names: list[str] = tables_default,
    buffer_distance: float = 0.5,
    EPSG_CODE: str = "28992",
    logger=None,
) -> list:
    """
    Export data from DAMO for polder of interest.

    A simplified SQL request based on the bounding box is used since otherwise the SQL statement
    would become too long. For sub tables a sub-SQl list is created in the database since passing
    an actual list may also cause the SQl to become too long. Works to up to 2 sub tables.

    Defauls table list form 3Di model generation can be found under recourses/schemitasaion_builder.

    Parameters
    ----------
    model_extent_gdf : GeoDataFrame
        GeoDataFrame of the selected polder
    output_file: Path
        path to output gpkg file in project directory.
    table_names : list[str]
        List of table names to be included in export, deafaults to all needed for model generation
    buffer_distance: float
        Distance to buffer the polder polygon before selection
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
    if not logger:  # moet dit if logger is None zijn?
        logger = hrt.logging.get_logger(__name__)
    logger.info("Start export")

    # Apply buffer distance
    model_extent_gdf["geometry"] = model_extent_gdf.buffer(buffer_distance)

    # make bbox --> to simplify string request to DAMO db
    bbox_model = box(*model_extent_gdf.total_bounds)

    logging = []
    for table in table_names:
        layer_db = DB_LAYER_MAPPING.get(table, None).get("source", None)
        geomcolumn = DB_LAYER_MAPPING.get(table, None).get("geomcolumn", None)
        columns = DB_LAYER_MAPPING.get(table, None).get("columns", None)
        if DB_LAYER_MAPPING.get(table, None).get("table_name", None) is not None:
            table_name = DB_LAYER_MAPPING.get(table, None).get("table_name", None)
        else:
            table_name = table

        if geomcolumn is not None:
            columns = [geomcolumn] + columns

        db_dict = DATABASES[layer_db]
        service_name = db_dict.get("service_name", None)
        try:
            logger.info(f"Start export of table {table_name} from {service_name}")

            # Build sql string for request to db for given input polygon
            sql = sql_builder_select_by_location(
                schema=DB_LAYER_MAPPING.get(table, None).get("schema", None),
                table_name=table_name,
                geomcolumn=geomcolumn,
                epsg_code=EPSG_CODE,
                polygon_wkt=bbox_model,
                simplify=True,
            )

            logger.info(f"Created sql statement {table} from from {service_name}")

            # exports data from DAMO database
            bbox_gdf, sql2 = database_to_gdf(db_dict=db_dict, sql=sql, columns=columns)

            # select all objects which (partly) lay within the model extent
            model_gdf = bbox_gdf[bbox_gdf["geometry"].intersects(model_extent_gdf["geometry"][0])].copy()

            # make sure that all colums which can contain dates has the type datetime
            for col in model_gdf.columns:
                if "date" in col or "datum" in col:
                    model_gdf[col] = pd.to_datetime(model_gdf[col], errors="coerce")

            # update layername if provided
            if DB_LAYER_MAPPING.get(table, None).get("layername", None) is not None:
                layername = DB_LAYER_MAPPING.get(table, None).get("layername", None)
            else:
                layername = table

            # adds table to geopackage file as a layer
            model_gdf.to_file(output_file, layer=layername, driver="GPKG", engine="pyogrio")

            logger.info(f"Finished export of {len(model_gdf)} elements from table {table} from {service_name}")

            # Include table that depend on this table
            if DB_LAYER_MAPPING.get(table, None).get("required_sub_table", None) is not None:
                sub_table = DB_LAYER_MAPPING.get(table, None).get("required_sub_table", None)
                id_link_column = DB_LAYER_MAPPING.get(table, None).get("id_link_column", None)
                sub_id_column = DB_LAYER_MAPPING.get(table, None).get("sub_id_column", None)
                sub_columns = DB_LAYER_MAPPING.get(table, None).get("sub_columns", None)

                logger.info(f"Start export of table non-geometric table {sub_table} from {service_name}")

                # Modify sql to contain only id link column
                sql_from = sql[sql.index("FROM") :]
                sub_id_list_sql = f"SELECT {id_link_column} {sql_from}"

                # Create new SQL statement
                sub_sql = sql_builder_select_by_id_list_statement(
                    sub_id_list_sql=sub_id_list_sql,
                    schema=DB_LAYER_MAPPING.get(table, None).get("schema", None),
                    sub_table=sub_table,
                    sub_id_column=sub_id_column,
                )

                # exports data from DAMO database
                sub_bbox_df, sub_sql2 = database_to_gdf(db_dict=db_dict, sql=sub_sql, columns=sub_columns)

                # filter items outside shape (that where in boundingbox)
                idlist = model_gdf[id_link_column.lower()].unique().tolist()
                sub_model_df = sub_bbox_df[sub_bbox_df[sub_id_column.lower()].isin(idlist)]

                # Write to geopackage
                sub_model_gdf = gpd.GeoDataFrame(sub_model_df)
                sub_model_gdf.to_file(output_file, layer=sub_table, driver="GPKG", engine="pyogrio")

                logger.info(
                    f"Finished export of {len(sub_model_gdf)} elements from table {sub_table} from {service_name}"
                )

                # Include table that depend on the sub-table (voor profielen, zucht)
                if DB_LAYER_MAPPING.get(table, None).get("required_sub2_table", None) is not None:
                    sub2_table = DB_LAYER_MAPPING.get(table, None).get("required_sub2_table", None)
                    sub_id_link_column = DB_LAYER_MAPPING.get(table, None).get("sub_id_link_column", None)
                    sub2_id_column = DB_LAYER_MAPPING.get(table, None).get("sub2_id_column", None)
                    sub2_columns = DB_LAYER_MAPPING.get(table, None).get("sub2_columns", None)

                    logger.info(f"Start export of table non-geometric table {sub2_table} from {service_name}")

                    # Modify sql to contain only id link column
                    sub_sql_from = sub_sql[sub_sql.index("FROM") :]
                    sub2_id_list_sql = f"SELECT {sub_id_link_column} {sub_sql_from}"

                    # Create new SQL statement
                    sub2_sql = sql_builder_select_by_id_list_statement(
                        sub_id_list_sql=sub2_id_list_sql,
                        schema=DB_LAYER_MAPPING.get(table, None).get("schema", None),
                        sub_table=sub2_table,
                        sub_id_column=sub2_id_column,
                    )

                    # exports data from DAMO database
                    sub2_bbox_df, sub2_sql2 = database_to_gdf(db_dict=db_dict, sql=sub2_sql, columns=sub2_columns)

                    # filter items outside shape (that where in boundingbox)
                    idlist2 = sub_model_gdf[sub_id_link_column.lower()].unique().tolist()
                    sub2_model_df = sub2_bbox_df[sub2_bbox_df[sub2_id_column.lower()].isin(idlist2)]

                    # Write to geopackage
                    sub2_model_df = gpd.GeoDataFrame(sub_model_df)
                    sub2_model_df.to_file(output_file, layer=sub2_table, driver="GPKG", engine="pyogrio")

                    logger.info(
                        f"Finished export of {len(sub2_model_df)} elements from table {sub2_table} from {service_name}"
                    )

        except Exception as e:
            if DB_LAYER_MAPPING.get(table, None) is None:
                error = f"{table} not found in database mapping {e}"
            else:
                error = f"An error occured while exporting data of table {table} from {service_name} {e}"

            logger.error(error)
            logging.append(error)

    return logging


# %%
