# %%
# importing external dependencies
import json
import logging
import re
import sqlite3
from pathlib import Path

# from pd.errors import DatabaseError #FIXME vanaf pd 1.5.3 beschikbaar. Als qgis zover is overzetten.
# from sqlite3 import DatabaseError
import fiona
import geopandas as gpd
import numpy as np
import pandas as pd

import hhnk_threedi_tools.core.vergelijkingstool.config as config
from hhnk_threedi_tools.core.vergelijkingstool import styling
from hhnk_threedi_tools.core.vergelijkingstool.config import *
from hhnk_threedi_tools.core.vergelijkingstool.Dataset import DataSet
from hhnk_threedi_tools.core.vergelijkingstool.styling import *
from hhnk_threedi_tools.core.vergelijkingstool.utils import ModelInfo


class Threedimodel(DataSet):
    def __init__(self, filename, model_info: ModelInfo, translation=None):
        """
        Creates a Threedimodel object and reads the data from the 3Di schematisation sqlite.
        If translation dictionaries are supplied, layer and column names are mapped.

        :param filename: Path of the .sqlite file to be loaded
        :param translation: Path of the translation dictionary to be used
        """
        # super().__init__(model_info)
        self.model_info = model_info
        self.model_name = model_info.model_name
        self.sourcedata = model_info.source_data
        self.logger = logging.getLogger("Threedimodel")
        self.logger.debug("Created Threedimodel object")
        # self.data = self.from_sqlite(filename, translation)
        self.data = self.from_geopackage(filename, translation)
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
                data = [list(map(float, line.split(","))) for line in tabulated_string.strip().split("\n")]

                # Transform the data into array
                array = np.array(data)
                # split the array in x (width) and y (height)
                x = array[:, 0]
                y = array[:, 1]

                # find max value in x (width)
                max_x = np.max(x)

                # find max value in y (height)
                max_y = np.max(y)
                return max_x, max_y
            else:
                return None

        cross_section_definition = self.data["cross_section_location"]

        cross_section_definition[["cross_section_max_width", "cross_section_max_height"]] = (
            cross_section_definition["cross_section_table"].apply(max_value_from_tabulated).to_list()
        )

        for layer in self.data:
            if "cross_section_location" in list(self.data[layer].keys()):
                self.data[layer] = self.data[layer].merge(
                    cross_section_definition[["cross_section_max_width", "cross_section_max_height"]],
                    how="left",
                    left_on="cross_section_definition_id",
                    right_on="id",
                )

    def from_geopackage(self, filename, translation=None):
        """
        Load data from GeoPackage (.gpkg) file

        :param filename: Path of the .gpkg file
        :param translation: Path of the translation file (optional)
        :return: Dictionary containing layer names (keys) and GeoDataFrames (values)
        """

        data = {}
        self.logger.debug("called from_geopackage")

        # Get layer in the geopackge
        layers = fiona.listlayers(filename)
        if not layers:
            self.logger.error("No layers found in .gpkg, or file does not exist")
            raise Exception("Error reading GeoPackage")

        self.logger.debug(f"Layers results: {layers}")

        # loop over all layers and save them in a dictionary
        for layer in layers:
            self.logger.debug(f"Loading layer {layer}")
            try:
                gdf_layer_data = gpd.read_file(filename, layer=layer)
            except Exception as e:
                self.logger.error(f"Error loading layer {layer}: {e}")
                continue

            # Save to dictionary
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
        self.logger.debug("Done loading .gpkg")
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

        model_name = self.model_name
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

    def export_comparison_3di(self, table_C, statistics, filename, overwrite=False, styling_path=None, crs=None):
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

        # add styling to layers
        layer_styles = styling.export_comparison_3di(
            table_C,
            statistics,
            filename,
            model_info=self.model_info,
            overwrite=overwrite,
            styling_path=styling_path,
            crs=self.crs,
        )
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
        model_name = self.model_name
        table_struc_model = {}
        table_struc_DAMO = {}

        # base_     output = r"E:\02.modellen\castricum\01_source_data\vergelijkingsTool\output"
        base_output = self.sourcedata / "vergelijkingsTool\output"  # TODO THIS NEED TO BE FIX
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
            table_struc_DAMO[struc] = DAMO.get_structure_by_code(struc, DAMO_HDB_STRUCTURE_LAYERS)

        table_C = {}
        for layer in table_struc_model.keys():
            # print(f"the crs for {layer} in DAMO is {table_struc_DAMO[layer].crs}")
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
            self.export_comparison_3di(
                table_C, statistics, filename, overwrite=overwrite, styling_path=styling_path, crs=self.crs
            )

        # statistics.to_csv(r"E:\02.modellen\castricum\01_source_data\vergelijkingsTool\output\statistics_threedi.csv", sep = ';')
        # table_C[layer].to_csv(r"E:\02.modellen\castricum\01_source_data\vergelijkingsTool\output\TableC_threedi.csv", sep = ';')

        return table_C, statistics


# %%
