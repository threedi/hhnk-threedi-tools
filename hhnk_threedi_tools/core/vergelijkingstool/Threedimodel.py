# importing external dependencies
import json
import logging
import os
import re
import sqlite3
from pathlib import Path

# from pd.errors import DatabaseError #FIXME vanaf pd 1.5.3 beschikbaar. Als qgis zover is overzetten.
from sqlite3 import DatabaseError

import geopandas as gpd
import numpy as np
import pandas as pd

import hhnk_threedi_tools.core.vergelijkingstool.config as config
from hhnk_threedi_tools.core.vergelijkingstool import name_date
from hhnk_threedi_tools.core.vergelijkingstool.config import *
from hhnk_threedi_tools.core.vergelijkingstool.Dataset import DataSet
from hhnk_threedi_tools.core.vergelijkingstool.styling import *


class Threedimodel(DataSet):
    def __init__(self, filename, translation=None):
        """
        Creates a Threedimodel object and reads the data from the 3Di schematisation sqlite.
        If translation dictionaries are supplied, layer and column names are mapped.

        :param filename: Path of the .sqlite file to be loaded
        :param translation: Path of the translation dictionary to be used
        """

        self.logger = logging.getLogger("Threedimodel")
        self.logger.debug("Created Threedimodel object")
        self.data = self.from_sqlite(filename, translation)
        self.join_cross_section_definition()

        # Set priority columns
        self.priority_columns = {}

        for key in self.data.keys():
            self.__setattr__(key, self.data[key])

    def join_cross_section_definition(self):
        """
        Method to join the v2_cross_section_definition with the layers in self.data.
        From the tabulated height and width, the maximum value is determined and added to the self.data layer.

        :return: None
        """

        def max_value_from_tabulated(tabulated_string):
            """
            Determines the maximum value in a tabultated string
            :param tabulated_string:
            :return: Maximum value in a tabulated string
            """
            if tabulated_string is not None:
                tabulated_array = np.fromstring(tabulated_string, dtype=float, sep=" ")
                return np.max(tabulated_array)
            else:
                return None

        cross_section_definition = self.data["v2_cross_section_definition"].set_index("id")

        cross_section_definition["cross_section_max_width"] = cross_section_definition["width"].apply(
            max_value_from_tabulated
        )

        cross_section_definition["cross_section_max_height"] = cross_section_definition["height"].apply(
            max_value_from_tabulated
        )

        for layer in self.data:
            if "cross_section_definition_id" in list(self.data[layer].keys()):
                self.data[layer] = self.data[layer].merge(
                    cross_section_definition[["cross_section_max_width", "cross_section_max_height"]],
                    how="left",
                    left_on="cross_section_definition_id",
                    right_on="id",
                )

    def add_geometry_from_connection_nodes(self, layer_data, data_connection_nodes):
        """
        Add a geometry column to a layer based on the start/end connection nodes. Used for example for v2_orifice,
        as orifices are defined by the start/end connection nodes instead of by a geometry.

        :param layer_data: GeoDataframe of layer to have a geometry added
        :param data_connection_nodes: GeoDataframe containing the connection nodes
        :return: GeoDataframe with appended geometry
        """

        layer_data["geometry"] = None
        subset_data_connection_nodes = data_connection_nodes[["id", "geometry"]]
        layer_data = layer_data.merge(
            subset_data_connection_nodes.fillna(-9999),
            how="left",
            left_on="connection_node_start_id",
            right_on="id",
            suffixes=(None, "_start"),
        )
        layer_data = layer_data.merge(
            subset_data_connection_nodes.fillna(-9999),
            how="left",
            left_on="connection_node_end_id",
            right_on="id",
            suffixes=(None, "_end"),
        )
        layer_data["geometry"] = layer_data.apply(
            lambda x: self.create_structure_geometry(x["geometry_start"], x["geometry_end"]), axis=1
        )
        layer_data.drop(["geometry_start", "geometry_end"], axis=1, inplace=True)
        # result = gpd.GeoDataFrame(layer_data, geometry='geom')

        return layer_data

    def from_sqlite(self, filename, translation=None):
        """
        Load data from 3Di .sqlite file

        :param filename: Path of the .sqlite file
        :param translation: Path of the translation file
        :return: Dictionary containing layer names (keys) and GeoDataFrames (values)
        """

        # Define empty data dictionary, to be filled with layer data from .sqlite
        data = {}

        self.logger.debug("called from_sqlite")

        # Create connection with database
        con = sqlite3.connect(filename)
        cur = con.cursor()

        # Load spatiallite extension. Make sure mod_spatialite.dll is in the System32 folder, see module description
        # for installation instructions
        self.logger.debug("Try loading mod_spatialite")
        con.enable_load_extension(True)
        cur.execute('SELECT load_extension("mod_spatialite")')

        # Get overview of layers
        res = con.execute(f"SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%';")
        db_layers = res.fetchall()
        if db_layers:
            self.logger.debug(f"Layers results: {db_layers}")
        else:
            self.logger.error("No layers found in .sqlite, or file does not exist")
            raise Exception("Error reading 3Di .sqlite")

        # Loop over all layers starting with v2_ and put them in the data object
        for layer in config.SQLITE_LAYERS:
            self.logger.debug(f"Loading layer {layer}")

            # Catch database error in case the table does not contain a geometry
            try:
                layer_data = pd.read_sql(f"SELECT *, ST_AsText(the_geom) as wkt_geom FROM {layer};", con)
                gdf_layer_data = gpd.GeoDataFrame(
                    layer_data, geometry=gpd.geoseries.from_wkt(layer_data["wkt_geom"].to_list())
                )
                # layer_data['geom'] = gpd.geoseries.from_wkt(layer_data["wkt_geom"])
            except:
                self.logger.debug(f"Layer {layer} does not contain a geometry layer, loading as a table")
                layer_data = pd.read_sql(f"SELECT * FROM {layer};", con)
                try:
                    # Try to create a geometry using the connection nodes
                    layer_data = self.add_geometry_from_connection_nodes(layer_data, data["v2_connection_nodes"])
                except KeyError:
                    # Add without a geometry
                    self.logger.debug(
                        f"Layer {layer} does not have a geometry or reference to connection nodes. Import as normal "
                        f"table"
                    )
                gdf_layer_data = gpd.GeoDataFrame(layer_data)

            # Add layer data to data object
            data[layer] = gdf_layer_data
        self.logger.debug("Done loading layers")

        # Start translation
        if translation is not None:
            self.logger.debug("Start mapping layer and column names")
            # load file
            f = open(translation)

            try:
                mapping = json.loads(json.dumps(json.load(f)).lower())
            except json.decoder.JSONDecodeError:
                self.logger.error("Structure of translation file is incorrect")
                raise

            translate_layers = {}
            for layer in data.keys():
                # Check if the layer name is mapped in the translation file
                for layer_name in mapping.keys():
                    if layer == layer_name:
                        # Map column names
                        self.logger.debug(f"Mapping column names of layer {layer}")
                        data[layer].rename(columns=mapping[layer]["columns"], inplace=True)

                        # Store layer mapping in dict to be mapped later
                        translate_layers[layer_name] = mapping[layer]["name"]

            # Map layer names
            for old, new in translate_layers.items():
                data[new] = data.pop(old)

        self.logger.debug("Done loading .sqlite")
        return data

    def determine_statistics(self, table_C):
        """
        Per layer aggregate statistics like amount of entries, total length or total area for a layer,
        and compare these model statistics with DAMO statistics.

        :param table_C: Dict with GeoDataFrames containing the compared data
        :return: DataFrame containing statistics
        """

        statistics = pd.DataFrame(
            columns=[
                "Count DAMO",
                "Count 3Di",
                "Count difference",
                "Length DAMO",
                "Length 3Di",
                "Length difference",
                "Area DAMO",
                "Area 3Di",
                "Area difference",
            ]
        )

        model_name = name_date.model_name
        for i, layer_name in enumerate(table_C):
            count_model = len(
                table_C[layer_name].loc[
                    (table_C[layer_name]["in_both"] == f"{model_name} sqlite")
                    | (table_C[layer_name]["in_both"] == f"{model_name} both")
                ]
            )
            count_DAMO = len(
                table_C[layer_name].loc[
                    (table_C[layer_name]["in_both"] == f"{model_name} damo")
                    | (table_C[layer_name]["in_both"] == f"{model_name} both")
                ]
            )
            count_diff = count_model - count_DAMO

            length_model = sum(
                table_C[layer_name]
                .loc[
                    (table_C[layer_name]["in_both"] == f"{model_name} sqlite")
                    | (table_C[layer_name]["in_both"] == f"{model_name} both")
                ]
                .geom_length_model
            )
            length_DAMO = sum(
                table_C[layer_name]
                .loc[
                    (table_C[layer_name]["in_both"] == f"{model_name} damo")
                    | (table_C[layer_name]["in_both"] == f"{model_name} both")
                ]
                .geom_length_damo
            )
            length_diff = length_model - length_DAMO

            area_model = sum(
                table_C[layer_name]
                .loc[
                    (table_C[layer_name]["in_both"] == f"{model_name} sqlite")
                    | (table_C[layer_name]["in_both"] == f"{model_name} both")
                ]
                .geom_area_model
            )
            area_DAMO = sum(
                table_C[layer_name]
                .loc[
                    (table_C[layer_name]["in_both"] == f"{model_name} damo")
                    | (table_C[layer_name]["in_both"] == f"{model_name} both")
                ]
                .geom_area_damo
            )
            area_diff = area_model - area_DAMO
            statistics.loc[layer_name, :] = [
                count_DAMO,
                count_model,
                count_diff,
                length_DAMO,
                length_model,
                length_diff,
                area_DAMO,
                area_model,
                area_diff,
            ]
        statistics = statistics.fillna(0).astype(int)
        return statistics

    def export_comparison(self, table_C, statistics, filename, overwrite=False, styling_path=None):
        """
        Exports all compared layers and statistics to a GeoPackage.

        :param table_C: Dictionary containing a GeoDataframe per layer
        :param statistics: Dataframe containing the statistics
        :param filename: Filename of the GeoPackage to export to
        :param overwrite: If true it will delete the old GeoPackage
        :param styling_path: Path to folder containing .qml files. For each layer in table_C it will lookup a .qml file
        with the exact same name as the layer
        :return:
        """
        model_name = name_date.model_name
        table = []
        if Path(filename).exists():
            if overwrite:
                Path.unlink(filename)
            else:
                raise FileExistsError(
                    f'The file "{filename}" already exists. If you want to overwrite the existing file, add overwrite=True to the function.'
                )

        # Explode multipart geometries to singleparts
        for layer_name in list(table_C):
            table_C[layer_name] = table_C[layer_name].explode(index_parts=True)

        # export result to gpkg
        for layer_name in list(table_C):
            if len(table_C[layer_name].geometry.type.unique()) > 1:
                self.logger.debug(
                    f"Layer {layer_name} exists of the following geometry types: {table_C[layer_name].geometry.type.unique()}. Using representative point."
                )
                table_C[layer_name].geometry = table_C[layer_name].geometry.representative_point()

        for i, layer_name in enumerate(table_C):
            # Check if the layer name has a style in the styling folder
            if styling_path is not None:
                qml_name = layer_name + ".qml"
                qml_file = (styling_path) / qml_name

                # if file exists, use that for the styling
                if qml_file.exists():
                    self.logger.debug(f"Style layer for layer {layer_name} found, adding it to the GeoPackage")
                    with open(qml_file, "r") as file:
                        style = file.read()

                    # Function to replace labels based on their current value
                    def replace_label(match):
                        label_value = match.group(3)
                        value_value = match.group(1)
                        symbol_value = match.group(2)
                        if value_value.__contains__("both"):
                            return f'value="{model_name} both" symbol="2" label="{model_name} both"'

                        elif value_value.__contains__("damo"):
                            return f'value="{model_name} damo" symbol="0" label="Damo {model_name} {name_date.date_new_damo}"'

                        elif value_value.__contains__("sqlite"):
                            return f'value="{model_name} sqlite" symbol="1" label="Model {model_name} {name_date.date_sqlite}"'

                        # else:
                        #     if value_value.startswith(model_name) and label_value.startswith(model_name):
                        #         return match.group(0)

                    # Use a regular expression to find and replace label values
                    style = re.sub(r'value="([^"]*)" symbol="([^"]*)" label="([^"]*)"', replace_label, style)

                    # Write the modified content back to the QML file
                    with open(qml_file, "w") as file:
                        file.write(style)

                    with open(qml_file, "r") as file:
                        style = file.read()
                        style_name = layer_name + "_style"
                        table.append(
                            [
                                i,
                                None,
                                None,
                                layer_name,
                                table_C[layer_name]._geometry_column_name,
                                style_name,
                                style,
                                None,
                                "false",
                                None,
                                None,
                                None,
                                None,
                            ]
                        )

                else:
                    if table_C[layer_name].geometry.type.unique() is None:
                        pass
                    if table_C[layer_name].geometry.type.unique() == "Point":
                        table.append(
                            [
                                i,
                                None,
                                None,
                                layer_name,
                                table_C[layer_name]._geometry_column_name,
                                "point_style",
                                STYLING_POINTS_THREEDI,
                                None,
                                "false",
                                None,
                                None,
                                None,
                                None,
                            ]
                        )
                    if (
                        table_C[layer_name].geometry.type.unique() == "LineString"
                        or table_C[layer_name].geometry.type.unique() == "MultiLineString"
                    ):
                        table.append(
                            [
                                i,
                                None,
                                None,
                                layer_name,
                                table_C[layer_name]._geometry_column_name,
                                "line_style",
                                STYLING_LINES_THREEDI,
                                None,
                                "false",
                                None,
                                None,
                                None,
                                None,
                            ]
                        )
                    if table_C[layer_name].geometry.type.unique() == "MultiPolygon":
                        table.append(
                            [
                                i,
                                None,
                                None,
                                layer_name,
                                table_C[layer_name]._geometry_column_name,
                                "polygon_style",
                                STYLING_POLYGONS_THREEDI,
                                None,
                                "false",
                                None,
                                None,
                                None,
                                None,
                            ]
                        )

            self.logger.info(f"Export results of comparing DAMO/3Di layer {layer_name} to {filename}")
            table_C[layer_name].to_file(filename, layer=layer_name, driver="GPKG", crs=self.crs)

        # add styling to layers
        layer_styles = gpd.GeoDataFrame(columns=STYLING_BASIC_TABLE_COLUMNS, data=table)
        layer_styles.fillna("", inplace=True)
        self.add_layer_styling(fn_export_gpkg=filename, layer_styles=layer_styles)

        # Export statistics
        self.export_statistics(statistics, filename)
        self.logger.info(f"Finished exporting to {filename}")

    def compare_with_DAMO(
        self,
        DAMO,
        attribute_comparison=None,
        filename=None,
        overwrite=False,
        styling_path=None,
        threedi_layer_selector=False,
        threedi_structure_selection=None,
        damo_structure_selection=None,
        structure_codes=None,
    ):
        """
        Compares the Threedi object with a DAMO object.
        Looks in all layers containing structures (defined in the config.py) for the 'code' column. Creates a joined
        GeoDataFrame for each structure code ('KDU','KBR', etc., also defined in config.py)

        :param DAMO: DAMO object
        :param attribute_comparison: Path to JSON file containing attribute comparison
        :param filename: Path to GeoPackage file to export the comparison to
        :param overwrite: Boolean indicating if the export file can be overwritten if it already exists
        :param styling_path: Path to folder containing .qml files for styling the layers
        :return: Dictionary containing GeoDataframes with compared data and a Dataframe with statistics
        """

        # sort model and damo data by structure code instead of model and damo layers
        model_name = name_date.model_name
        table_struc_model = {}
        table_struc_DAMO = {}
        base_output = r"E:\02.modellen\castricum\01_source_data\vergelijkingsTool\output"
        if threedi_layer_selector is True:
            THREEDI_STRUCTURE_LAYERS = threedi_structure_selection
            DAMO_HDB_STRUCTURE_LAYERS = damo_structure_selection
            STRUCTURE_CODES = structure_codes
        else:
            STRUCTURE_CODES = config.STRUCTURE_CODES
            print("threedi layer selection is OFF")
            THREEDI_STRUCTURE_LAYERS = config.THREEDI_STRUCTURE_LAYERS
            DAMO_HDB_STRUCTURE_LAYERS = config.DAMO_HDB_STRUCTURE_LAYERS

        for struc in STRUCTURE_CODES:
            table_struc_model[struc] = self.get_structure_by_code(struc, THREEDI_STRUCTURE_LAYERS)

            name = "check_table_model_" + struc + ".csv"
            path_table = os.path.join(base_output, name)
            table_struc_model[struc].to_csv(path_table, sep=";", index=False)

            # print(f'3di_type {(table_struc_model[struc])}')
            table_struc_DAMO[struc] = DAMO.get_structure_by_code(struc, DAMO_HDB_STRUCTURE_LAYERS)

            name = "check_table_DAMO_" + struc + ".csv"
            path_table = os.path.join(base_output, name)
            table_struc_DAMO[struc].to_csv(path_table, sep=";", index=False)

            # print(struc)
            # print(f'the crs is {table_struc_DAMO[struc].crs} for the strcuture {struc}')
            # print(f'damo_type {type(table_struc_DAMO[struc])}')
        table_C = {}
        for layer in table_struc_model.keys():
            print(f"the crs for {layer} in DAMO is {table_struc_DAMO[layer].crs}")
            if table_struc_DAMO[layer].crs == table_struc_model[layer].crs:
                self.logger.debug(f"CRS of DAMO and model data {layer} is equal")
            else:
                self.logger.debug(f"CRS of DAMO and model data {layer} is not equal")

                table_struc_model[layer] = (
                    table_struc_model[layer].set_crs(epsg=4326).to_crs(crs=table_struc_DAMO[layer].crs)
                )
            self.crs = table_struc_DAMO[layer].crs

            # Add geometry information (length/area) to dataframe
            table_struc_model[layer] = self.add_geometry_info(table_struc_model[layer])
            table_struc_DAMO[layer] = self.add_geometry_info(table_struc_DAMO[layer])

            # Add 'dataset' column, after merging this will become 'dataset_model' and 'dataset_damo'
            table_struc_model[layer]["dataset"] = True
            table_struc_DAMO[layer]["dataset"] = True

            # outer merge on the two tables with suffixes
            table_struc_model[layer] = (
                table_struc_model[layer].add_suffix("_model").rename(columns={"code_model": "code"})
            )
            table_struc_DAMO[layer] = table_struc_DAMO[layer].add_suffix("_damo").rename(columns={"code_damo": "code"})

            table_merged = table_struc_model[layer].merge(table_struc_DAMO[layer], how="outer", on="code")
            table_merged["geometry"] = None
            table_merged = gpd.GeoDataFrame(table_merged, geometry="geometry")
            # fillna values of the two columns by False
            table_merged[["dataset_model", "dataset_damo"]] = table_merged[["dataset_model", "dataset_damo"]].fillna(
                value=False
            )

            # add column with values model, damo or both, depending on code
            inboth = []

            for i in range(len(table_merged)):
                if table_merged["dataset_model"][i] & table_merged["dataset_damo"][i]:
                    inboth.append(f"{model_name} both")
                elif table_merged["dataset_model"][i] and not table_merged["dataset_damo"][i]:
                    inboth.append(f"{model_name} sqlite")
                else:
                    inboth.append(f"{model_name} damo")
            table_merged["in_both"] = inboth

            # use geometry of model when feature exists in model or in both model and damo. Use geometry of damo when feature only exists in damo
            mask_geom_model = table_merged.loc[
                (table_merged["in_both"] == f"{model_name} sqlite") | (table_merged["in_both"] == f"{model_name} both")
            ]
            mask_geom_DAMO = table_merged.loc[table_merged["in_both"] == (f"{model_name} damo")]
            geom = pd.concat([mask_geom_model["geometry_model"], mask_geom_DAMO["geometry_damo"]])
            table_merged["geometry"] = geom
            table_merged = self.drop_unused_geoseries(table_merged, keep="geometry")
            if table_merged.columns.__contains__("the_geom_model"):
                table_merged.drop(columns=["the_geom_model"], inplace=True)
            table_C[layer] = gpd.GeoDataFrame(
                self.drop_unused_geoseries(table_merged, keep="geometry"), geometry="geometry"
            )

        # Apply attribute comparison
        if attribute_comparison is not None:
            table_C = self.apply_attribute_comparison(attribute_comparison, table_C)
            table_C = self.summarize_attribute_comparison(table_C)

        # determine statistics: count amount of shapes per layer for model and damo
        statistics = self.determine_statistics(table_C)

        # export to filename
        if filename is not None:
            self.export_comparison(table_C, statistics, filename, overwrite=overwrite, styling_path=styling_path)

        # statistics.to_csv(r"E:\02.modellen\castricum\01_source_data\vergelijkingsTool\output\statistics_threedi.csv", sep = ';')
        # table_C[layer].to_csv(r"E:\02.modellen\castricum\01_source_data\vergelijkingsTool\output\TableC_threedi.csv", sep = ';')

        return table_C, statistics
