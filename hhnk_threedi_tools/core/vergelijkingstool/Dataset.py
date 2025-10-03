# %%

import json
import math
import sqlite3

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Polygon

from hhnk_threedi_tools.core.vergelijkingstool.config import *
from hhnk_threedi_tools.core.vergelijkingstool.utils import ModelInfo

# %%


class DataSet:
    """
    Parent class of DAMO and Threedimodel
    """

    def __init__(self, model_info: ModelInfo):
        """
        Initialisation of a dataset.
        """

        self.model_name = model_info.model_name
        self.data = None
        # Set priority columns
        self.priority_columns = {}

    def add_layer_styling(self, fn_export_gpkg, layer_styles):
        """
        Adds and fills a helper table 'layer_styles' to the GeoPackage containing the style per layer.
        When the GeoPackage is loaded into QGIS, the layers are auto-loaded

        :param fn_export_gpkg: Filename of the GeoPackage to write to
        :param layer_styles: Dataframe containing the QML styles for the different layers
        :return: None
        """
        con = sqlite3.connect(fn_export_gpkg)
        cur = con.cursor()

        # drop layer_styles table if it already exists and create a new one
        SQL_DROP_LAYER_STYLES = """DROP TABLE IF EXISTS layer_styles"""
        SQL_CREATE_LAYER_STYLES = """CREATE TABLE "layer_styles" ("id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, "f_table_catalog" TEXT(256), "f_table_schema" TEXT(256), "f_table_name" TEXT(256), "f_geometry_column" TEXT(256), "styleName" TEXT(30), "styleQML" TEXT, "styleSLD" TEXT, "useAsDefault" BOOLEAN, "description" TEXT, "owner" TEXT(30), "ui" TEXT(30), "update_time" DATETIME DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));"""
        con.execute(SQL_DROP_LAYER_STYLES)
        con.execute(SQL_CREATE_LAYER_STYLES)

        # add layer_styles to gpkg
        SQL_INSERT_GPKG_CONTENT = (
            "INSERT INTO gpkg_contents (table_name, data_type) values ('layer_styles','attributes')"
        )
        con.execute(SQL_INSERT_GPKG_CONTENT)

        # fill layer_styles
        for row in layer_styles.itertuples():
            data = {
                "id": row.id,
                "f_table_catalog": row.f_table_catalog,
                "f_table_schema": row.f_table_schema,
                "f_table_name": row.f_table_name,
                "f_geometry_column": "geom",
                "styleName": row.styleName,
                "styleQML": row.styleQML,
                "useAsDefault": row.useAsDefault,
            }
            con.execute(
                "INSERT INTO layer_styles (id, f_table_catalog, f_table_schema, f_table_name, f_geometry_column, styleName, styleQML, useAsDefault) VALUES (:id, :f_table_catalog, :f_table_schema, :f_table_name, :f_geometry_column, :styleName, :styleQML, :useAsDefault)",
                data,
            )
        con.commit()
        cur.close()
        con.close()

    def get_significant_geometry(self, dataset, geometry_A, geometry_B):
        """
        Returns the significant geometry based on the origin of the geometry in the datasets.
        If an asset occurs in dataset A (self) or in A AND B, geometry A is returned.
        If an asset occurs ONLY in dataset B, geometry B is returned.

        :param dataset: String indicating in which dataset the asset is found
        :param geometry_A: Shapely geometry
        :param geometry_B: Shapely geometry
        :return: Shapely geometry
        """
        if dataset == f"{model_name} new" or dataset == f"{model_name} both":
            return geometry_A
        else:
            return geometry_B

    def create_structure_geometry(self, point_start, point_end):
        """
        Creates a LineString from structures that are only defined by a start and end point.
        If only a start point is supplied, the start point is returned

        :param point_start: Shapely geometry of the start point
        :param point_end: Shapely geometry of the end point
        :return: Shapely Point or Linestring
        """
        if point_end is None:
            output_geometry = point_start
        else:
            output_geometry = LineString([point_start, point_end])

        return output_geometry

    def add_geometry_info(self, gdf):
        """
        Adds columns regarding the geometry type, length and area as attributes to a GeoDatarame

        :param gdf: GeoDataframe to be analyzed
        :return: GeoDataframe with added columns
        """
        if "geometry" in gdf.columns:
            gdf["geom_type"] = gdf["geometry"].apply(lambda geom: geom.geom_type if geom is not None else None)
            gdf["geom_length"] = gdf.geometry.length
            gdf["geom_area"] = gdf.geometry.area
        return gdf

    def drop_unused_geoseries(self, gdf, keep="geometry"):
        """
        Removes all GeoSeries from a GeoDataframe except the column indicated with the 'keep' parameter.
        Used because for the export to GeoPackage, a GeoDataframe is only allowed to have 1 geometry column

        :param gdf: GeoDataframe to be stripped of unused GeoSeries
        :param keep: Name of the geometry column to keep
        :return: GeoDataframe stripped of unused GeoSeries
        """
        drop_columns = []
        for column in gdf.columns:
            if isinstance(gdf[column], gpd.geoseries.GeoSeries) and column != keep:
                drop_columns.append(column)
        gdf = gdf.drop(columns=drop_columns)
        return gdf

    def compare_function(self, table, function):
        """
        Apply a function with arguments on a table.
        Function contains

        :param table: GeoDataframe on which the function is applied
        :param function: Dictionary containing the function and arguments
        :return:
        """
        function_name = list(function.keys())[0]

        def resolve_parameter(table, parameter):
            """
            Resolves the parameters of a function. If the parameter is a string, the column in the table with that header is returned.
            If the parameter is a dict, a recursive function is applied.
            If the parameter is a number, the value is used

            :param table: GeoDataframe on which the function is applied
            :param parameter: String, dict or number
            :return:
            """
            if isinstance(parameter, str):
                try:
                    return table[parameter]
                except KeyError:
                    self.logger.error(f"Could not resolve comperation parameter {parameter}")
                    return None
            elif isinstance(parameter, dict):
                compare, *_ = self.compare_function(table, parameter)
                return compare
            elif isinstance(parameter, int) or isinstance(parameter, float):
                return parameter
            else:
                self.logger.error(f"Could not resolve comperation parameter {parameter}")
                return None

        try:
            param_left = function[function_name]["left"]
            param_right = function[function_name]["right"]
            left = resolve_parameter(table, param_left)
            right = resolve_parameter(table, param_right)

            try:
                skipna = function[function_name]["skipna"]
                self.logger.debug(f"Adding skipna to function within function {function_name}")
            except:
                skipna = False

            # check if there was a change from NaN to a number, or from a number to NaN
            if isinstance(left, pd.Series) and isinstance(right, pd.Series):
                change_na = (left.isna() & right.notna()) | (left.notna() & right.isna())
            else:
                change_na = None

            try:
                change_na = (left.isna() & right.notna()) | (left.notna() & right.isna())
            except:
                change_na = None

            ### DIFFERENCE ###
            if function_name == "difference":
                result = left - right
            ### ADD ###
            elif function_name == "add":
                result = left + right
            ### MINIMUM
            elif function_name == "minimum":
                result = pd.concat([left, right], axis=1).min(axis=1, skipna=skipna)
            ### MAXIMUM
            elif function_name == "maximum":
                result = pd.concat([left, right], axis=1).max(axis=1, skipna=skipna)
            elif function_name == "multiply":
                result = left * right

            else:
                self.logger.error(f"Comparison function {function_name} not recognized")
                result = None

            return result, change_na

        except TypeError:
            self.logger.error(f"TypeError: Unable to calculate {function_name} on {param_left} and {param_right}")
            return None, None

        except AttributeError as err:
            self.logger.error(
                f"AttributeError: Unable to calculate {function_name} on {param_left} and {param_right}. One of the parameters might not be available in the data. Skipping comparison"
            )
            return None, None

    def compare_category(self, row, priority):
        """
        Compares two values in a row on category. Returns a value only if the asset occurs in A and B (or DAMO and Model)
        Returns not changed if there is no change in the category

        :param row: row containing the left and right column to be compared and the 'in_both' column.
        :return: String containing the change in category, if any.
        """

        if row.in_both == "AB" or row.in_both == "both":
            left = row[0]
            right = row[1]

            if isinstance(left, float):
                if left.is_integer():
                    left = str(int(left))
            if isinstance(right, float):
                if right.is_integer():
                    right = str(int(right))

            if left != right:
                # if left and right are nan, don't fill anything, else fill "left -> right"
                if str(left) == "nan" and str(right) == "nan":
                    return (None, None)
                else:
                    return ((str(left) + " -> " + str(right)), priority)
            else:
                # if left and right the same and not nan
                if isinstance(left, float):
                    if ~math.isnan(left):
                        return ("not changed", None)
                else:
                    return ("not changed", None)
        return (None, None)

    def compare_attribute(self, df, comparison):
        """
        For a comparison, determine the type and resolve the function. Creates a new column with the result of the comparison

        :param df: (Geo)Dataframe on which a comparison is applied
        :param comparison: Dictionary with the comparison rules
        :return:
        """
        self.logger.debug(f"Applying comparison with the following data: {comparison}")

        name = comparison["name"]
        self.logger.debug(f"Start comparison: {name}")
        comparison_type = comparison["type"]
        table_name = comparison["table"]
        table = df[table_name]

        # Compare only with the selection that has been made, table_names comes from names of the of the table dictionary
        # used in the function apply_attribute_comparison
        print(f"the name to compare is {table_name}")

        # Try reading the priority, if not set, use "critical"
        try:
            priority = comparison["priority"]
        except KeyError:
            self.logger.info(f'No priority set for comparison {name}, setting it to "critical"')
            priority = "critical"

        if comparison_type == "numeric":
            function = comparison["function"]
            self.logger.debug(f"Comparison type: numeric")

            # Try to read the numerical threshold, if not set, use default value
            try:
                threshold = comparison["threshold"]
            except KeyError:
                self.logger.info(
                    f"No numeric threshold set for comparison {name}, using default value of {COMPARISON_GENERAL_THRESHOLD}"
                )
                threshold = COMPARISON_GENERAL_THRESHOLD

            # add new column with the comparison name, and result from the compare function
            try:
                column_name_nan_change = name + "_change_NaN"
                column_name_priority = name + "_priority"
                compare_result, compare_nan_change = self.compare_function(table, function)

                # apply threshold
                try:
                    compare_result[abs(compare_result) < threshold] = 0
                except TypeError:
                    self.logger.debug(f"Unable to apply threshold on comparison {name}")

                # store in dataframe
                if isinstance(compare_result, pd.Series):
                    df[table_name][name] = compare_result
                    df[table_name][column_name_nan_change] = compare_nan_change
                    df[table_name][column_name_priority] = pd.Series(
                        [priority if x else "" for x in abs(compare_result) > 0]
                    )

                    # Add column_name_priority to the priority_columns dict
                    if table_name not in self.priority_columns.keys():
                        self.priority_columns[table_name] = []

                    self.priority_columns[table_name].append(column_name_priority)
                else:
                    self.logger.warning(f"Comparison {name} had None as result")

            except TypeError:
                self.logger.error(f"Unable to apply comparation {name}, skipping")
                # raise

        elif comparison_type == "category":
            self.logger.debug(f"Comparison type: category")
            left = comparison["left"]
            right = comparison["right"]

            column_name_priority = name + "_priority"
            if table_name not in self.priority_columns.keys():
                self.priority_columns[table_name] = []

            self.priority_columns[table_name].append(column_name_priority)

            df[table_name][[name, column_name_priority]] = df[table_name][[left, right, "in_both"]].apply(
                lambda row: self.compare_category(row, priority), axis=1, result_type="expand"
            )
        else:
            self.logger.debug(f"Comparison type {comparison_type} not implemented")
        return df

    def summarize_attribute_comparison(self, table):
        """
        Analyzes the priority columns in the compared table and summarizes per row how many critical, warning, and
        info's ar counted
        :param table: Dictionary containing all the comparison tables
        :return: Dictionary containing all the comparison tables including the summary columns
        """

        # create lambda function
        count_occurrences = lambda row, string: row.tolist().count(string)
        for layer in self.priority_columns.keys():
            # If there is a priority_column
            if layer:
                columns = self.priority_columns[layer]
                table[layer]["number_of_info"] = table[layer][columns].apply(
                    lambda row: count_occurrences(row, "info"), axis=1
                )
                table[layer]["number_of_warning"] = table[layer][columns].apply(
                    lambda row: count_occurrences(row, "warning"), axis=1
                )
                table[layer]["number_of_critical"] = table[layer][columns].apply(
                    lambda row: count_occurrences(row, "critical"), axis=1
                )
        return table

    def apply_attribute_comparison(self, attribute_comparison, table):
        """
        Loads a attribute comparison json file and applies the comparison rules in the json on the supplied table

        :param attribute_comparison: Path to file containing comparison rules
        :param table: Table to apply the comparison rules on
        :return:
        """

        self.logger.debug(f"Start applying attribute comparison")

        try:
            with open(attribute_comparison) as file:
                att_comp = json.load(file)
                for comparison in att_comp["comparisons"]:
                    description = comparison["description"]
                    table_name = comparison["table"]

                    if table_name in table.keys():
                        print(f"comparing {description}")
                        table = self.compare_attribute(table, comparison)
                    else:
                        # table = self.compare_attribute(table, comparison)
                        print(f"The table {table_name} is not included in the comparision process")

        except json.decoder.JSONDecodeError as err:
            self.logger.error(
                f"Unable to load attribute comparison file. JSON structure incorrect. {err.args[0]}. Skipping attribute comparison"
            )
        except FileNotFoundError as err:
            self.logger.error(
                f"Unable to load attribute comparison file. File {err.filename} does not exist. Skipping attribute comparison"
            )
        return table

    def get_structure_by_code(self, code_start: str, layers):
        """
        Searches in all layers for entries based on code.
        If a rows 'code' column starts with code_start, it is appended to the return table,
        keeping all columns from different sources.
        Example, when searching for 'KDU' it returns all assets (in this case 'Duikers'),
        both from the v2_orifice as the v2_culvert layer

        :param code_start: Start of code to search for
        :return: DataFrame containing all found entries
        """
        tabel = pd.DataFrame()
        for layer in layers:
            codes = []
            # (print("Capas disponibles:", list(self.data.keys())))
            for i in self.data[layer].code.values:
                if i is not None:
                    i = str(i)
                    if i.startswith(code_start):
                        codes.append(i)
            if codes:
                layer_data = self.data[layer][self.data[layer]["code"].isin(codes)].copy()
                layer_data["origin"] = layer
                tabel = pd.concat((tabel, layer_data))

        return tabel

    def export_statistics(self, statistics, filename):
        """
        Export an attribute-only table to the GeoPackage

        :param statistics: Dataframe containing statistics
        :param filename: GeoPackage filename to export to
        :return: None
        """

        self.logger.info(f"Exporting statistics to {filename}")
        # For export to GeoPackage a geometry column is needed, fill it with None to create attribute only layer
        statistics["geometry"] = None
        gpd.GeoDataFrame(statistics, geometry="geometry").to_file(filename, layer="statistics", driver="GPKG")
