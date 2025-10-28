# %%
import logging
from pathlib import Path

import geopandas as gpd
import pandas as pd

from hhnk_threedi_tools.core.vergelijkingstool import json_files as json_files_path
from hhnk_threedi_tools.core.vergelijkingstool import styling, utils
from hhnk_threedi_tools.core.vergelijkingstool.config import *
from hhnk_threedi_tools.core.vergelijkingstool.Dataset import DataSet
from hhnk_threedi_tools.core.vergelijkingstool.qml_styling_files import DAMO as DAMO_styling_path
from hhnk_threedi_tools.core.vergelijkingstool.styling import *
from hhnk_threedi_tools.core.vergelijkingstool.utils import ModelInfo


class DAMO(DataSet):
    def __init__(
        self,
        model_info: ModelInfo,
        damo_filename,
        hdb_filename,
        clip_shape=None,
        layer_selection=None,
        layers_input_hdb_selection=None,
        layers_input_damo_selection=None,
    ):
        """
        Create a DAMO object and reads the data from the DAMO and HDB FileGeoDatabase.
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

        self.styling_path = Path(DAMO_styling_path.__file__).resolve().parent
        self.json_files_path = Path(json_files_path.__file__).resolve().parent

        self.model_info = model_info
        self.model_name = model_info.model_name
        # Load data
        layers_input_hdb_selection = layers_input_hdb_selection or []
        layers_input_damo_selection = layers_input_damo_selection or []

        self.data = utils.load_file_and_translate(
            damo_filename=damo_filename,
            hdb_filename=hdb_filename,
            translation_DAMO=self.json_files_path / "damo_translation.json",
            translation_HDB=self.json_files_path / "hdb_translation.json",
            layer_selection=layer_selection,
            layers_input_damo_selection=layers_input_damo_selection,
            layers_input_hdb_selection=layers_input_hdb_selection,
            mode="damo",
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
        layers_to_remove = []
        for layer in data.keys():
            # layers_to_remove = []
            gdf = data[layer]
            if isinstance(gdf, gpd.GeoDataFrame):
                self.logger.debug(f"Check if layer {layer} has geometry")
                gdf = data[layer]
                intersections = gdf[gdf.geometry.intersects(shape) | gdf.geometry.isnull()]
                data[layer] = intersections
            else:
                self.logger.debug(f"Layer '{layer}' is not a GeoDataFrame, skipping geometry clipping")
                layers_to_remove.append(layer)

        if len(layers_to_remove) != 0:
            for layer in layers_to_remove:
                del data[layer]

        return data

    def export_comparison_new(self, table_C, statistics, filename, overwrite=False):
        """
        Export all compared layers and statistics to a GeoPackage.

        :param table_C: Dictionary containing a GeoDataframe per layer
        :param statistics: Dataframe containing the statistics
        :param filename: Filename of the GeoPackage to export to
        :param overwrite: If true it will delete the old GeoPackage
        :param styling_path: Path to folder containing .qml files. For each layer in table_C it will lookup a .qml file
        with the exact same name as the layer
        :return:
        """

        layer_styles = styling.export_comparison_DAMO(
            table_C,
            statistics,
            filename,
            model_info=self.model_info,
            overwrite=overwrite,
            styling_path=self.styling_path,
        )
        self.add_layer_styling(fn_export_gpkg=filename, layer_styles=layer_styles)

        # Export statistics
        self.export_statistics(statistics, filename)
        self.logger.info(f"Finished exporting to {filename}")

    def compare_with_damo(
        self,
        damo_b,
        filename=None,
        overwrite=False,
    ):
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
        attribute_comparison = self.json_files_path / "damo_attribute_comparison.json"
        # copy damo data and add column with True values
        self.logger.debug("create copies of tables")
        table_A = self.data.copy()
        table_B = damo_b.data.copy()
        table_C = {}

        for layer in table_A.keys():
            print(layer)
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
                table_A[layer] = table_A[layer].add_suffix("_New").rename(columns={"code_New": "code"})
                table_B[layer] = table_B[layer].add_suffix("_Old").rename(columns={"code_Old": "code"})

                # make sure the code column is of type string for proper merging
                table_A[layer]["code"] = table_A[layer]["code"].astype("string")
                table_B[layer]["code"] = table_B[layer]["code"].astype("string")

                table_merged = table_A[layer].merge(table_B[layer], how="outer", on="code")  # Futurewarning
                table_merged["geometry"] = None
                table_merged = gpd.GeoDataFrame(table_merged, geometry="geometry")

                # fillna values of the two columns by False
                table_merged[["dataset_New", "dataset_Old"]] = table_merged[["dataset_New", "dataset_Old"]].fillna(value=False)

                # add column with values A, B or AB, depending on code
                inboth = []
                geometry_adjusted = []
                for i in range(len(table_merged)):
                    if table_merged["dataset_New"][i] & table_merged["dataset_Old"][i]:
                        inboth.append(f"{self.model_name} both")
                        # geometry_adjusted.append(table_merged['geometry_A'][i] != table_merged['geometry_B'][i])
                    elif table_merged["dataset_New"][i] and not table_merged["dataset_Old"][i]:
                        inboth.append(f"{self.model_name} new")
                        # geometry_adjusted.append(None)
                    else:
                        inboth.append(f"{self.model_name} old")
                        # geometry_adjusted.append(None)
                table_merged["in_both"] = inboth
                table_merged["geometry_adjusted"] = table_merged["geometry_New"] != table_merged["geometry_Old"]

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
                            [f"{self.model_name} both", f"{self.model_name} new", f"{self.model_name} old"]
                        )
                        df_intersections = df_intersections.explode(column="geometry", index_parts=True)
                        table_merged = df_intersections[df_intersections.geometry.geom_type == "Polygon"].copy()

                        table_merged.loc[
                            table_merged["in_both"].isin([f"{self.model_name} new", f"{self.model_name} both"]),
                            "geom_area_New",
                        ] = table_merged["geometry"].area
                        table_merged.loc[
                            ~table_merged["in_both"].isin([f"{self.model_name} new", f"{self.model_name} both"]),
                            "geom_area_New",
                        ] = None

                        table_merged.loc[
                            table_merged["in_both"].isin([f"{self.model_name} old", f"{self.model_name} both"]),
                            "geom_area_Old",
                        ] = table_merged["geometry"].area
                        table_merged.loc[
                            ~table_merged["in_both"].isin([f"{self.model_name} old", f"{self.model_name} both"]),
                            "geom_area_Old",
                        ] = None

                        table_merged["geom_length_New"] = 0
                        table_merged["geom_length_Old"] = 0
                    else:
                        # hier iets om geometrieen te bepalen
                        intersection = gpd.GeoDataFrame(
                            pd.concat(
                                [
                                    table_merged.code,
                                    table_merged["geometry_New"].intersection(table_merged["geometry_Old"]),
                                ],
                                axis=1,
                            )
                        ).rename(columns={0: "geometry_diff"})
                        intersection["origin"] = "intersection"
                        diff_A = gpd.GeoDataFrame(
                            pd.concat(
                                [table_merged.code, table_merged["geometry_New"].difference(table_merged["geometry_Old"])],
                                axis=1,
                            )
                        ).rename(columns={0: "geometry_diff"})
                        diff_A["origin"] = "diff_New"
                        diff_B = gpd.GeoDataFrame(
                            pd.concat(
                                [table_merged.code, table_merged["geometry_New"].difference(table_merged["geometry_Old"])],
                                axis=1,
                            )
                        ).rename(columns={0: "geometry_diff"})
                        diff_B["origin"] = "diff_Old"

                        df_intersections = pd.concat([intersection, diff_A, diff_B])
                        df_intersections = df_intersections[df_intersections["geometry_diff"].notna()]
                        table_merged = table_merged.merge(df_intersections, how="outer", on="code")

                        # table_merged = table_merged.explode(column='geometry_diff', index_parts=True)
                        # table_merged = table_merged[table_merged.geometry_diff.geom_type == 'Polygon']

                        table_merged["geometry_diff"] = table_merged.apply(
                            lambda x: self.get_significant_geometry(x["in_both"], x["geometry_New"], x["geometry_Old"])
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
                        lambda x: self.get_significant_geometry(x["in_both"], x["geometry_New"], x["geometry_Old"]), axis=1
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
                "Count New",
                "Count Old",
                "Count difference",
                "Length New",
                "Length Old",
                "Length difference",
                "Area New",
                "Area Old",
                "Area difference",
            ]
        )
        for i, layer_name in enumerate(table_C):
            self.logger.debug(f"Layer name: {layer_name}")
            count_A = len(
                table_C[layer_name].loc[
                    (table_C[layer_name]["in_both"] == f"{self.model_name} new")
                    | (table_C[layer_name]["in_both"] == f"{self.model_name} both")
                ]
            )
            count_B = len(
                table_C[layer_name].loc[
                    (table_C[layer_name]["in_both"] == f"{self.model_name} old")
                    | (table_C[layer_name]["in_both"] == f"{self.model_name} both")
                ]
            )
            count_diff = count_B - count_A
            length_A = sum(
                table_C[layer_name]
                .loc[
                    (table_C[layer_name]["in_both"] == f"{self.model_name} new")
                    | (table_C[layer_name]["in_both"] == f"{self.model_name} both")
                ]
                .geom_length_New
            )
            length_B = sum(
                table_C[layer_name]
                .loc[
                    (table_C[layer_name]["in_both"] == f"{self.model_name} old")
                    | (table_C[layer_name]["in_both"] == f"{self.model_name} both")
                ]
                .geom_length_Old
            )
            length_diff = length_B - length_A
            area_A = sum(
                table_C[layer_name]
                .loc[
                    (table_C[layer_name]["in_both"] == f"{self.model_name} new")
                    | (table_C[layer_name]["in_both"] == f"{self.model_name} both")
                ]
                .geom_area_New
            )
            area_B = sum(
                table_C[layer_name]
                .loc[
                    (table_C[layer_name]["in_both"] == f"{self.model_name} old")
                    | (table_C[layer_name]["in_both"] == f"{self.model_name} both")
                ]
                .geom_area_Old
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
            self.export_comparison_new(table_C, statistics, filename, overwrite=overwrite)

        statistics.to_csv(r"E:\02.modellen\castricum\01_source_data\vergelijkingsTool\output\statistics.csv", sep=";")
        # table_C.to_csv(r"E:\02.modellen\castricum\01_source_data\vergelijkingsTool\output\TableC.csv", sep = ';')

        return table_C, statistics


# %%
