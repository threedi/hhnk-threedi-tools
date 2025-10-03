# %%
import json
import logging
import re
from pathlib import Path

import fiona
import geopandas as gpd
import pandas as pd

from hhnk_threedi_tools.core.vergelijkingstool import styling, utils
from hhnk_threedi_tools.core.vergelijkingstool.config import *
from hhnk_threedi_tools.core.vergelijkingstool.Dataset import DataSet
from hhnk_threedi_tools.core.vergelijkingstool.styling import *


class DAMO(DataSet):
    def __init__(
        self,
        damo_filename,
        hdb_filename,
        translation_DAMO=None,
        translation_HDB=None,
        clip_shape=None,
        layer_selection=None,
        layers_input_hdb_selection=[],
        layers_input_damo_selection=[],
    ):
        """
        Creates a DAMO object and reads the data from the DAMO and HDB FileGeoDatabase.
        If translation dictionaries are supplied, layer and column names are mapped.

        :param damo_filename: Path to the GDB folder with DAMO data.
        :param hdb_filename: Path to the GDB folder with HDB data.
        :param translation_DAMO: Path to the DAMO translation dictionary.
        :param translation_HDB: Path to the HDB translation dictionary.
        :param clip_shape: Shapely shape to subset data on
        """

        # Set up logger
        self.logger = logging.getLogger("DAMO")
        self.logger.debug("Created DAMO object")

        # Load data
        self.data = self.from_file(
            damo_filename,
            hdb_filename,
            translation_DAMO,
            translation_HDB,
            layer_selection,
            layers_input_hdb_selection,
            layers_input_damo_selection,
        )

        # Set priority columns
        self.priority_columns = {}

        # Clip data
        if clip_shape is not None:
            if isinstance(clip_shape, (str, Path)):
                clip_shape = gpd.read_file(clip_shape).geometry.union_all()
        self.data = self.clip_data(self.data, clip_shape)

        # Add mapped layer names as DAMO attributes
        for key in self.data.keys():
            self.__setattr__(key, self.data[key])

    def clip_data(self, data, shape):
        """
        Interates over all layers within the DAMO/HDB data, and only keeps entries that have an intersection
        (or no geometry) with the supplied shape. Original geometries are preserved, so the data is clipped, not the
        geometries

        :param data: Data to be clipped
        :param shape: (Multi)Polygon used for the clipping of the data
        :return: Clipped data
        """
        for layer in data.keys():
            self.logger.debug(f"Check if layer {layer} has geometry")
            gdf = data[layer]
            intersections = gdf[gdf.geometry.intersects(shape) | gdf.geometry.isnull()]
            data[layer] = intersections
        return data

    def translate(self, data, translation_file):
        """
        Loads a translation file and translates the data datastructure.
        Renames tables and columns as indicated in the translation_file

        :param data: Data to be translated
        :param translation_file: Path to translation file
        :return: Translated data
        """

        # load file
        f = open(translation_file)

        try:
            mapping = json.loads(json.dumps(json.load(f)).lower())
        except json.decoder.JSONDecodeError:
            self.logger.error("Structure of DAMO-translation file is incorrect, check brackets and commas")
            raise

        translate_layers = {}
        # Iterate over the gdf within the data that has inside of it the names of the table from the sqlite or gpkg
        for layer in data.keys():
            # Check if the layer name is mapped in the translation file
            for layer_name in mapping.keys():
                if layer == layer_name:
                    # Map column names
                    self.logger.debug(f"Mapping column names of layer {layer}")

                    # Rename de columns within the data dictionary following the damo_translation.json file
                    data[layer].rename(columns=mapping[layer]["columns"], inplace=True)

                    # Store layer mapping in dict to be mapped later
                    translate_layers[layer_name] = mapping[layer]["name"]

        # Map layer names
        for old, new in translate_layers.items():
            data[new] = data.pop(old)

        return data

    def from_file(
        self,
        damo_filename,
        hdb_filename,
        translation_DAMO=None,
        translation_HDB=None,
        layer_selection=None,
        layers_input_hdb_selection=None,
        layers_input_damo_selection=None,
    ):
        """
        Function that loads the data in GeoDataFrames and applies layer and column mapping

        :param damo_filename: Path to the .gbd folder with DAMO data
        :param hdb_filename: Path to the .gbd folder with HDB data
        :param translation_DAMO: Path to the DAMO translation dictionary
        :param translation_HDB: Path to the HDB translation dictionary
        :return: Dictionary containing layer names (keys) and GeoDataFrames (values)
        """

        # Define empty data dictionary, to be filled with layer data from file
        data = {}

        # Load layers within .gdb datasets
        self.logger.debug("Find layer names within geopackages")

        if layer_selection == True:
            # layers_input_damo = str(input('Type the layer from DAMO you want to compare'))
            layers_gdb_damo = layers_input_damo_selection
            # layers_gdb_damo = ['AfvoergebiedAanvoergebied', 'Bergingsgebied', 'DuikerSifonHevel']
            # layers_input_hdb = str(input('Type the layer from HDB you want to compare'))
            layers_gdb_hdb = layers_input_hdb_selection

        else:
            layers_gdb_damo = fiona.listlayers(damo_filename)
            layers_gdb_hdb = fiona.listlayers(hdb_filename)
            # layers_gdb_damo = layers_gdb_damo
            # layers_gdb_hdb = layers_gdb_hdb

        # Start reading DAMO
        for layer in layers_gdb_damo:
            # Check if the layer name is mapped in the translation file
            if layer in DAMO_LAYERS:
                # Map column names
                self.logger.debug(f"Reading DAMO layer {layer}")
                gdf_damo = gpd.read_file(damo_filename, layer=layer)
                gdf_damo.columns = gdf_damo.columns.str.lower()

                # Make the name of the DAMO layer lowecase and the save it in the dictionary
                data[layer.lower()] = gdf_damo

        # Start reading HDB
        for layer in layers_gdb_hdb:
            # Check if the layer name is mapped in the translation file
            if layer in HDB_LAYERS:
                # Map column names
                self.logger.debug(f"Reading HDB layer {layer}")
                gdf_hdb = gpd.read_file(hdb_filename, layer=layer)
                gdf_hdb.columns = gdf_hdb.columns.str.lower()

                # Make the name of the HDB layer lowecase and the save it in the dictionary
                data[layer.lower()] = gdf_hdb

        # Start translation DAMO
        if translation_DAMO is not None:
            self.logger.debug("Start mapping layer and column names of DAMO layers")
            data = self.translate(data, translation_DAMO)

        # Start translation DAMO
        if translation_HDB is not None:
            self.logger.debug("Start mapping layer and column names of DAMO layers")
            data = self.translate(data, translation_HDB)

        return data

    def export_comparison_new(self, table_C, statistics, filename, overwrite=False, styling_path=None):
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

        layer_styles = styling.export_comparison_DAMO(
            table_C, statistics, filename, overwrite=overwrite, styling_path=styling_path
        )
        self.add_layer_styling(fn_export_gpkg=filename, layer_styles=layer_styles)

        # Export statistics
        self.export_statistics(statistics, filename)
        self.logger.info(f"Finished exporting to {filename}")

    def compare_with_damo(self, damo_b, attribute_comparison=None, filename=None, overwrite=False, styling_path=None):
        """
        Compare two DAMO objects with eachother, self (damo_a) and other (damo_b).
        For every layer in the datasets (brug, duikersyfonhevel, etc.) an outer join based on the code will be made.
        For every asset the presence in each dataset will be available

        :param damo_b: DAMO object to compare with
        :param filename: Path to export the results as a Geopackage
        :param overwrite: Overwrite results
        :return: Tables with all data assets and in which dataset it is present (A = only present in this object,
        B = only present in the other object, AB = present in both objects). If asset is available in AB, but the
        geometries differ, the geometry of A is exported.
        Additionally, a table with statistics is returned
        """

        # copy damo data and add column with True values
        self.logger.debug("create copies of tables")
        table_A = self.data.copy()
        table_B = damo_b.data.copy()
        table_C = {}

        for layer in table_A.keys():
            if not table_B.keys().__contains__(layer):
                self.logger.warning(f"Layer {layer} does not exist in the other dataset, skipping layer")
                continue

            self.logger.debug(f"Merging {layer} of A and B datasets")

            # Reproject crs B to A if crs are different
            if table_A[layer].crs == table_B[layer].crs:
                self.logger.debug(f"CRS of layer {layer} is equal")
            else:
                table_B[layer].to_crs(crs=table_A[layer].crs)
            crs = table_A[layer].crs

            try:
                # Add geometry information (length/area) to dataframe
                self.logger.debug(f"Adding geometry to table_A[{layer}]")
                table_A[layer] = self.add_geometry_info(table_A[layer])
                self.logger.debug(f"Adding geometry to table_B[{layer}]")
                table_B[layer] = self.add_geometry_info(table_B[layer])

                # Add 'dataset' column, after merging this will become 'dataset_A' and 'dataset_B'
                table_A[layer]["dataset"] = True
                table_B[layer]["dataset"] = True

                # Add layer of origin to dataset
                table_A[layer]["origin"] = layer
                table_B[layer]["origin"] = layer

                # outer merge on the two tables with suffixes
                table_A[layer] = table_A[layer].add_suffix("_A").rename(columns={"code_A": "code"})
                table_B[layer] = table_B[layer].add_suffix("_B").rename(columns={"code_B": "code"})

                # make sure the code column is of type string for proper merging
                table_A[layer]["code"] = table_A[layer]["code"].astype("string")
                table_B[layer]["code"] = table_B[layer]["code"].astype("string")

                table_merged = table_A[layer].merge(table_B[layer], how="outer", on="code")  # Futurewarning
                table_merged["geometry"] = None
                table_merged = gpd.GeoDataFrame(table_merged, geometry="geometry")

                # fillna values of the two columns by False
                table_merged[["dataset_A", "dataset_B"]] = table_merged[["dataset_A", "dataset_B"]].fillna(value=False)

                # add column with values A, B or AB, depending on code
                inboth = []
                geometry_adjusted = []
                for i in range(len(table_merged)):
                    if table_merged["dataset_A"][i] & table_merged["dataset_B"][i]:
                        inboth.append(f"{model_name} both")
                        # geometry_adjusted.append(table_merged['geometry_A'][i] != table_merged['geometry_B'][i])
                    elif table_merged["dataset_A"][i] and not table_merged["dataset_B"][i]:
                        inboth.append(f"{model_name} new")
                        # geometry_adjusted.append(None)
                    else:
                        inboth.append(f"{model_name} old")
                        # geometry_adjusted.append(None)
                table_merged["in_both"] = inboth
                table_merged["geometry_adjusted"] = table_merged["geometry_A"] != table_merged["geometry_B"]

                # use geometry of A (new) when feature exists in A or in A and B, use geometry of B (old) when feature
                # only exists in B
                if layer in [x.lower() for x in GEOMETRICAL_COMPARISON_LAYERS]:
                    if layer == "waterdeel":
                        # For waterdeel we cannot compare based on code. So we treat A and B as one big polygon and
                        # determine the intersection and the differences
                        union_A = table_merged.geometry_A.unary_union
                        union_B = table_merged.geometry_B.unary_union
                        intersection = union_A.intersection(union_B)
                        diff_A = union_A.difference(union_B)
                        diff_B = union_B.difference(union_A)

                        df_intersections = (
                            gpd.GeoDataFrame(gpd.GeoSeries([intersection, diff_A, diff_B]))
                            .rename(columns={0: "geometry"})
                            .set_geometry("geometry")
                        )
                        df_intersections["in_both"] = pd.Series(
                            [f"{model_name} both", f"{model_name} new", f"{model_name} old"]
                        )
                        df_intersections = df_intersections.explode(column="geometry", index_parts=True)
                        table_merged = df_intersections[df_intersections.geometry.geom_type == "Polygon"].copy()

                        table_merged.loc[
                            table_merged["in_both"].isin([f"{model_name} new", f"{model_name} both"]), "geom_area_A"
                        ] = table_merged["geometry"].area
                        table_merged.loc[
                            ~table_merged["in_both"].isin([f"{model_name} new", f"{model_name} both"]), "geom_area_A"
                        ] = None

                        table_merged.loc[
                            table_merged["in_both"].isin([f"{model_name} old", f"{model_name} both"]), "geom_area_B"
                        ] = table_merged["geometry"].area
                        table_merged.loc[
                            ~table_merged["in_both"].isin([f"{model_name} old", f"{model_name} both"]), "geom_area_B"
                        ] = None

                        table_merged["geom_length_A"] = 0
                        table_merged["geom_length_B"] = 0
                    else:
                        # hier iets om geometrieen te bepalen
                        intersection = gpd.GeoDataFrame(
                            pd.concat(
                                [
                                    table_merged.code,
                                    table_merged["geometry_A"].intersection(table_merged["geometry_B"]),
                                ],
                                axis=1,
                            )
                        ).rename(columns={0: "geometry_diff"})
                        intersection["origin"] = "intersection"
                        diff_A = gpd.GeoDataFrame(
                            pd.concat(
                                [table_merged.code, table_merged["geometry_A"].difference(table_merged["geometry_B"])],
                                axis=1,
                            )
                        ).rename(columns={0: "geometry_diff"})
                        diff_A["origin"] = "diff_A"
                        diff_B = gpd.GeoDataFrame(
                            pd.concat(
                                [table_merged.code, table_merged["geometry_B"].difference(table_merged["geometry_A"])],
                                axis=1,
                            )
                        ).rename(columns={0: "geometry_diff"})
                        diff_B["origin"] = "diff_B"

                        df_intersections = pd.concat([intersection, diff_A, diff_B])
                        df_intersections = df_intersections[df_intersections["geometry_diff"].notna()]
                        table_merged = table_merged.merge(df_intersections, how="outer", on="code")

                        # table_merged = table_merged.explode(column='geometry_diff', index_parts=True)
                        # table_merged = table_merged[table_merged.geometry_diff.geom_type == 'Polygon']

                        table_merged["geometry_diff"] = table_merged.apply(
                            lambda x: self.get_significant_geometry(x["in_both"], x["geometry_A"], x["geometry_B"])
                            if (x["geometry_diff"] is None)
                            else x["geometry_diff"],
                            axis=1,
                        )
                        table_merged["origin"].fillna(table_merged["in_both"], inplace=True)
                        table_merged["geometry"] = table_merged["geometry_diff"]

                        table_merged = table_merged.explode(column="geometry", index_parts=True)
                        table_merged = table_merged[table_merged.geometry.geom_type == "Polygon"]

                else:
                    table_merged["geometry"] = table_merged.apply(
                        lambda x: self.get_significant_geometry(x["in_both"], x["geometry_A"], x["geometry_B"]), axis=1
                    )
                # remove all geometry columns except 'geometry'
                table_merged = self.drop_unused_geoseries(table_merged, keep="geometry")
                table_C[layer] = gpd.GeoDataFrame(table_merged, geometry="geometry", crs=crs)

            except KeyError as err:
                self.logger.warning(f"Column {err.args[0]} not found in layer {layer}, skipping layer")
                continue

        # Apply attribute comparison
        if attribute_comparison is not None:
            table_C = self.apply_attribute_comparison(attribute_comparison, table_C)
            table_C = self.summarize_attribute_comparison(table_C)

        # determine statistics: count amount of shapes per layer for dataset A and B
        self.logger.debug("create statistics")
        statistics = pd.DataFrame(
            columns=[
                "Count A",
                "Count B",
                "Count difference",
                "Length A",
                "Length B",
                "Length difference",
                "Area A",
                "Area B",
                "Area difference",
            ]
        )
        for i, layer_name in enumerate(table_C):
            self.logger.debug(f"Layer name: {layer_name}")
            count_A = len(
                table_C[layer_name].loc[
                    (table_C[layer_name]["in_both"] == f"{model_name} new")
                    | (table_C[layer_name]["in_both"] == f"{model_name} both")
                ]
            )
            count_B = len(
                table_C[layer_name].loc[
                    (table_C[layer_name]["in_both"] == f"{model_name} old")
                    | (table_C[layer_name]["in_both"] == f"{model_name} both")
                ]
            )
            count_diff = count_B - count_A
            length_A = sum(
                table_C[layer_name]
                .loc[
                    (table_C[layer_name]["in_both"] == f"{model_name} new")
                    | (table_C[layer_name]["in_both"] == f"{model_name} both")
                ]
                .geom_length_A
            )
            length_B = sum(
                table_C[layer_name]
                .loc[
                    (table_C[layer_name]["in_both"] == f"{model_name} old")
                    | (table_C[layer_name]["in_both"] == f"{model_name} both")
                ]
                .geom_length_B
            )
            length_diff = length_B - length_A
            area_A = sum(
                table_C[layer_name]
                .loc[
                    (table_C[layer_name]["in_both"] == f"{model_name} new")
                    | (table_C[layer_name]["in_both"] == f"{model_name} both")
                ]
                .geom_area_A
            )
            area_B = sum(
                table_C[layer_name]
                .loc[
                    (table_C[layer_name]["in_both"] == f"{model_name} old")
                    | (table_C[layer_name]["in_both"] == f"{model_name} both")
                ]
                .geom_area_B
            )
            area_diff = area_B - area_A
            statistics.loc[layer_name, :] = [
                count_A,
                count_B,
                count_diff,
                length_A,
                length_B,
                length_diff,
                area_A,
                area_B,
                area_diff,
            ]
        statistics = statistics.fillna(0).astype("int64")

        # check if filename already exists. Cr
        if filename is not None:
            print(filename)
            self.export_comparison_new(table_C, statistics, filename, overwrite=overwrite, styling_path=styling_path)

        statistics.to_csv(r"E:\02.modellen\castricum\01_source_data\vergelijkingsTool\output\statistics.csv", sep=";")
        # table_C.to_csv(r"E:\02.modellen\castricum\01_source_data\vergelijkingsTool\output\TableC.csv", sep = ';')

        return table_C, statistics


# %%
# import hhnk_research_tools as hrt
# gpkg = r"E:\02.modellen\RegionalFloodModel\work in progress\schematisation\RegionalFloodModel.gpkg"
# fgdb = hrt.FileGDB(gpkg)
# fgdb.add_layer("linear_obstacle")
# gdf = fgdb.layers.linear_obstacle.load()

# # %%
# gdf[gdf['id'].duplicated()]
