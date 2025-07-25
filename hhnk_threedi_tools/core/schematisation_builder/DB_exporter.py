# %%
"""DAMO exporter based on model extent"""

import os
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import pandas as pd
from dotenv import load_dotenv
from hhnk_research_tools.sql_functions import (
    database_to_gdf,
    get_table_domains_from_oracle,
    sql_builder_select_by_id_list_statement,
    sql_builder_select_by_location,
)
from shapely.geometry import box

from hhnk_threedi_tools.resources.schematisation_builder.db_layer_mapping import DB_LAYER_MAPPING

try:
    from hhnk_threedi_tools.local_settings_htt import DATABASES
except ImportError as e:
    load_dotenv()
    if os.getenv("SKIP_DATABASE") == "1":
        DATABASES = {}  # No DB in CI environment
    else:
        raise ImportError(
            r"The 'local_settings_htt' module is missing. Get it from \\corp.hhnk.nl\data\Hydrologen_data\Data\01.basisgegevens\00.HDB\SettingsAndRecourses\local_settings_htt.py and place it in \hhnk_threedi_tools\ "
        )

# From mapping json list top level keys
tables_default = list(DB_LAYER_MAPPING.keys())
EPSG_CODE = "28992"


# %%
def update_model_extent_from_combinatiepeilgebieden(
    model_extent_gdf: gpd.GeoDataFrame,
    db_dict: dict = DATABASES.get("csoprd_lezen"),
    logger: hrt.logging = None,
) -> gpd.GeoDataFrame:
    """
    Update the model extent GeoDataFrame with the geometry from the Combinatiepeilgebieden table.

    Parameters
    ----------
    model_extent_gdf : GeoDataFrame
        GeoDataFrame of the selected polder
    db_dict : dict, optional
        Dictionary containing database connection details, by default DATABASES["csoprd_lezen"]

    Returns
    -------
    model_extent_gdf : GeoDataFrame
        Updated GeoDataFrame with the geometry from Combinatiepeilgebieden.
    """
    if not logger:  # moet dit if logger is None zijn?
        logger = hrt.logging.get_logger(__name__)

    # Get the geometry from the Combinatiepeilgebieden table
    if db_dict is None:
        db_dict = DATABASES["csoprd_lezen"]

    bbox_model = box(*model_extent_gdf.buffer(10).total_bounds)

    sql = sql_builder_select_by_location(
        schema="CS_OBJECTEN",
        table_name="COMBINATIEPEILGEBIED",
        epsg_code="28992",
        polygon_wkt=bbox_model,
    )

    cpgb_gdf, _ = database_to_gdf(db_dict=db_dict, sql=sql, columns=["shape", "code"])

    # Create most representatieve point per peilgebied
    cpgb_rp_gdf = cpgb_gdf.copy()
    cpgb_rp_gdf["geometry"] = cpgb_rp_gdf["geometry"].representative_point()

    # Select points that intersect with original model extent
    cpgb_rp_gdf = cpgb_rp_gdf[cpgb_rp_gdf["geometry"].intersects(model_extent_gdf["geometry"].iloc[0])]

    # Check if this results in any peilgebieden
    if cpgb_rp_gdf.empty:
        logger.warning(
            "No Combinatiepeilgebieden found in the database for the given model extent. Returning original model extent."
        )
    else:
        # Filter combinatiepeilgebieden en merge
        model_extent_gdf["geometry"] = cpgb_gdf[cpgb_gdf["code"].isin(cpgb_rp_gdf["code"])].geometry.unary_union

    return model_extent_gdf


def export_sub_layer(
    sub_table: str,
    id_link_column: str,
    sub_id_column: str,
    sub_columns: list[str],
    model_gdf: gpd.GeoDataFrame,
    sql: str,
    db_dict: dict,
    schema: str,
) -> tuple[str, gpd.GeoDataFrame]:
    """
    Export sub layer from DAMO/CSO database based on the main table and sub table mapping.
    This function is used to export non-geometric tables that are linked to a main table.

    It collects the information for a table without geometry that depends on another table with
    geometry.     The function is very similar to the next one, and even uses the select SQL of the
    parent table to make the correct selection of elements. It is not robust to put a list of IDs
    in the SQL, because there is a chance that the string becomes too long.For profiles, there is
    even a double dependency, so this is called twice in the db_exporter. It saves a few lines of
    code this way.

    In resources/db_layer_mapping.py, it must be specified in JSON format that there is a sub layer
    and via which ID it can be linked.


    Parameters
    ----------
    sub_table : str
        Name of the sub table to be exported, e.g. "POMP", etc.
    id_link_column : str
        Name of the column in the parent table that links to the sub id column.
    sub_id_column : str
        Name of the column in the sub table that links to the parent table.
    sub_columns : str
        List of columns to be exported from the sub table.
        If None, alls columns are exported.
    model_gdf : GeoDataFrame
        GeoDataFrame of the selected polder, used to filter the sub table data.
    sql : str
        SQL query string to select data from the main table.
    db_dict : dict
        Dictionary containing database connection details.
    schema : str
        Schema name in the database where the sub table is located.
    sub_id_list_sql : str

    Returns
    -------
    sub_sql : str
        Modified SQL string containing the select query used te retrieve data from the database.
    sub_model_gdf : gpd.GeoDataFrame
        GeoDataFrame containing the exported data from the sub table, filtered by the model extent.
    """

    # Modify sql to contain only id link column
    sql_from = sql[sql.index("FROM") :]
    sub_id_list_sql = f"SELECT {id_link_column} {sql_from}"

    # Create new SQL statement
    sub_sql = sql_builder_select_by_id_list_statement(
        sub_id_list_sql=sub_id_list_sql,
        schema=schema,
        sub_table=sub_table,
        sub_id_column=sub_id_column,
    )

    # exports data from DAMO database
    sub_bbox_df, sub_sql2 = database_to_gdf(db_dict=db_dict, sql=sub_sql, columns=sub_columns)

    # filter items outside shape (that where in boundingbox)
    idlist = model_gdf[id_link_column.lower()].unique().tolist()
    sub_model_df = sub_bbox_df[sub_bbox_df[sub_id_column.lower()].isin(idlist)]

    sub_model_gdf = gpd.GeoDataFrame(sub_model_df)

    return sub_sql, sub_model_gdf


def update_table_domains(
    model_gdf: gpd.GeoDataFrame,
    db_dict: dict,
    schema: str,
    table_name: str,
) -> gpd.GeoDataFrame:
    """
    Convert domain codes for tables from schema DAMO W to their descriptions.
    Original codes are preserved in a new column right after the original column.

    Parameters
    ----------
    model_gdf : gpd.GeoDataFrame
        geodataframe of table from DAMO W schema, e.g. HYDROOBJECT
    db_dict : dict
        Dictionary containing database connection details.
    schema : str
        Schema name in the database where the sub table is located.
    table : str
        Name of the table to update domains

    Returns
    -------
    gpd.GeoDataFrame
        geodataframe of table from DAMO W schema with updated domains
    """

    logger = hrt.logging.get_logger(__name__)

    # Retrieve available domains
    domains = get_table_domains_from_oracle(
        db_dict=db_dict,
        schema=schema,
        table_name=table_name,
    )

    # Check if empmty
    if domains.empty:
        logger.warning(f"No domains found for table {table_name} in schema {schema}. Skipping domain update.")
        return model_gdf

    # Ensure domain codes are strings
    domains["codedomeinwaarde"] = domains["codedomeinwaarde"].astype(str)

    # Identify columns to remap
    domain_columns = [col for col in model_gdf.columns if col in domains["damokolomnaam"].unique()]

    # Loop over each domain column
    for col in domain_columns:
        # if col == "categorieoppwaterlichaam":
        #     break
        new_code_col = f"{col}code"

        # Preserve code column right after the original column
        model_gdf.insert(
            loc=model_gdf.columns.get_loc(col) + 1,
            column=new_code_col,
            value=model_gdf[col].astype(str),
        )

        # Build lookup: index=codedomeinwaarde → naamdomeinwaarde
        lookup = domains.loc[domains["damokolomnaam"] == col, ["codedomeinwaarde", "naamdomeinwaarde"]].set_index(
            "codedomeinwaarde"
        )["naamdomeinwaarde"]

        # Vectorized replacement (NaN if no match)
        model_gdf[col] = model_gdf[new_code_col].map(lookup)

    return model_gdf


def db_exporter(
    model_extent_gdf: gpd.GeoDataFrame,
    output_file: Path,
    table_names: list[str] = tables_default,
    buffer_distance: float = 0.5,
    epsg_code: str = EPSG_CODE,
    logger: hrt.logging = None,
    update_extent: bool = True,
) -> list[str]:
    """
    Export data from DAMO for polder of interest.

    A simplified SQL request based on the bounding box is used since otherwise the SQL statement
    would become too long. For sub tables a sub-SQl list is created in the database since passing
    an actual list may also cause the SQl to become too long. Works to up to 2 sub tables.

    Defauls table list from 3Di model generation can be found under recourses/schemitasaion_builder.

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
    logging_db_exporter : list[str]
        Error logs when a table export fails
    """
    if logger is None:
        logger = hrt.logging.get_logger(__name__)
    logger.info("Start export")

    # Update model extent with geometry from Combinatiepeilgebieden
    if update_extent:
        logger.info("Updating model extent with geometry from Combinatiepeilgebieden")
        # Update model extent with geometry from Combinatiepeilgebieden
        model_extent_gdf = update_model_extent_from_combinatiepeilgebieden(model_extent_gdf)

    # Apply buffer distance
    model_extent_gdf["geometry"] = model_extent_gdf.buffer(buffer_distance)

    # make bbox --> to simplify string request to DAMO db
    bbox_model = box(*model_extent_gdf.total_bounds)

    logging_bd_exporter = []
    for table in table_names:
        table_config = DB_LAYER_MAPPING.get(table, None)
        layer_db = table_config.get("source", None)
        schema = table_config.get("schema", None)
        geomcolumn = table_config.get("geomcolumn", None)
        columns = table_config.get("columns", None)
        table_name = table_config.get("table_name", table)

        if geomcolumn is not None:
            columns = [geomcolumn] + columns

        db_dict = DATABASES[layer_db]
        service_name = db_dict.get("service_name", None)
        try:
            logger.info(f"Start export of table {table_name} from {service_name}")

            # Build sql string for request to db for given input polygon
            sql = sql_builder_select_by_location(
                schema=schema,
                table_name=table_name,
                geomcolumn=geomcolumn,
                epsg_code=epsg_code,
                polygon_wkt=bbox_model,
                simplify=True,
            )

            logger.info(f"Created sql statement {table} from {service_name}")

            # exports data from DAMO database
            bbox_gdf, sql2 = database_to_gdf(db_dict=db_dict, sql=sql, columns=columns)

            # select all objects which (partly) lay within the model extent
            model_gdf = bbox_gdf[bbox_gdf["geometry"].intersects(model_extent_gdf["geometry"][0])].copy()

            # make sure that all colums which can contain dates has the type datetime
            for col in model_gdf.columns:
                if "date" in col or "datum" in col:
                    model_gdf[col] = pd.to_datetime(model_gdf[col], errors="coerce")

            # update layername if provided
            layername = table_config.get("layername", table)

            # Update table domain code to descriptions
            if schema == "DAMO_W":
                logger.info(f"Updating domains for {table} from {service_name}")
                model_gdf = update_table_domains(
                    model_gdf=model_gdf,
                    db_dict=db_dict,
                    schema=schema,
                    table_name=table_name,
                )

            # adds table to geopackage file as a layer
            model_gdf.to_file(output_file, layer=layername, driver="GPKG", engine="pyogrio")

            logger.info(f"Finished export of {len(model_gdf)} elements from table {table} from {service_name}")

            # Include table that depend on this table
            if table_config.get("required_sub_table", None) is not None:
                # Get sub table information from mapping
                sub_table = table_config.get("required_sub_table", None)
                id_link_column = table_config.get("id_link_column", None)
                sub_id_column = table_config.get("sub_id_column", None)
                sub_columns = table_config.get("sub_columns", None)
                logger.info(f"Start export of table non-geometric sub table {sub_table} from {service_name}")

                # run sub export
                sub_sql, sub_model_gdf = export_sub_layer(
                    sub_table=sub_table,
                    id_link_column=id_link_column,
                    sub_id_column=sub_id_column,
                    sub_columns=sub_columns,
                    model_gdf=model_gdf,
                    sql=sql,
                    db_dict=db_dict,
                    schema=schema,
                )

                # Write to geopackage
                sub_model_gdf.to_file(output_file, layer=sub_table, driver="GPKG", engine="pyogrio")

                logger.info(
                    f"Finished export of {len(sub_model_gdf)} elements from table {sub_table} from {service_name}"
                )

                if table_config.get("required_sub2_table", None) is not None:
                    # If there is a second sub table, export it as well
                    sub2_table = table_config.get("required_sub2_table", None)
                    sub_id_link_column = table_config.get("sub_id_link_column", None)
                    sub2_id_column = table_config.get("sub2_id_column", None)
                    sub2_columns = table_config.get("sub2_columns", None)
                    logger.info(f"Start export of table non-geometric sub table {sub2_table} from {service_name}")

                    # run sub export
                    sub2_sql, sub2_model_gdf = export_sub_layer(
                        sub_table=sub2_table,
                        id_link_column=sub_id_link_column,
                        sub_id_column=sub2_id_column,
                        sub_columns=sub2_columns,
                        model_gdf=sub_model_gdf,
                        sql=sub_sql,
                        db_dict=db_dict,
                        schema=schema,
                    )

                    logger.info(
                        f"Finished export of {len(sub2_model_gdf)} elements from table {sub_table} from {service_name}"
                    )

        except Exception as e:
            if table_config is None:
                error = f"{table} not found in database mapping {e}"
            else:
                error = f"An error occured while exporting data of table {table} from {service_name} {e}"

            logger.error(error)
            logging_bd_exporter.append(error)

    return logging_bd_exporter
