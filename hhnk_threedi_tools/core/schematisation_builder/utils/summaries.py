import json
import logging
from pathlib import Path

import fiona
import geopandas as gpd
import pandas as pd
from hydamo_validation import __version__
from hydamo_validation.summaries import LayersSummary, ResultSummary
from hydamo_validation.utils import normalize_fiona_schema

OUTPUT_TYPES = ["geopackage", "geojson", "csv"]

class ExtendedResultSummary(ResultSummary):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.prep_result = []
        self.fix_result = []
        self.fix_layers = []

    def to_json(self, results_path, file_name):
        """
        Write result to json

        Parameters
        ----------
        results_path : str or Path
            Directory where results are to be written to

        Returns
        -------
        None.

        """

        result_json = Path(results_path).joinpath(file_name)

        result_dict = {k: v for k, v in self.__dict__.items() if v is not None}
        with open(result_json, "w", encoding="utf-8", newline="\n") as dst:
            json.dump(result_dict, dst, indent=4)


class ExtendedLayersSummary(LayersSummary):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def export(self, results_path, gpkg_name, output_types=OUTPUT_TYPES):
        """
        Export the content of class to results_path

        Parameters
        ----------
        results_path : str or Path
            Directory where results are to be written to
        output_types : List[str], optional
            The types of output files that will be written. Options are
            ["geojson", "csv", "geopackage"]. By default all will be written
        Returns
        -------
        layers : List(str)
            A list of HyDAMO layers that are successfully written

        """

        gdf_dict = {
            k: v for k, v in self.__dict__.items() if isinstance(v, gpd.GeoDataFrame)
        }
        layers = []
        # make directories for output_types
        results_path = Path(results_path)
        for output_type in ["geojson", "csv"]:
            if output_type in output_types:
                result_dir = results_path.joinpath(output_type)
                result_dir.mkdir(parents=True, exist_ok=True)

        # export results to files
        for object_layer, gdf in gdf_dict.items():
            if "rating" not in gdf.columns:
                gdf["rating"] = 10

            if not gdf.empty:
                schema = {
                    "properties": self._get_properties(gdf),
                    "geometry": self.geo_types[object_layer],
                }

                # add date_check
                gdf["date_check"] = self.date_check
                schema["properties"]["date_check"] = "str"

                for output_type in output_types:
                    # set gdf to WGS84 for export to geojson
                    if output_type == "geojson":
                        file_path = results_path.joinpath(
                            output_type, f"{object_layer}.geojson"
                        )
                        gdf_out = gdf.copy()
                        if gdf_out.crs:
                            gdf_out.to_crs("epsg:4326", inplace=True)
                        gdf_out.to_file(file_path, driver="GeoJSON", engine="pyogrio")

                    # drop geometry for writing to csv
                    elif output_type == "csv":
                        file_path = results_path.joinpath(
                            output_type, f"{object_layer}.csv"
                        )
                        df = gdf.drop("geometry", axis=1)
                        df.to_csv(file_path, index=False)

                    # write to geopackage as is
                    elif output_type == "geopackage":
                        file_path = results_path.joinpath(gpkg_name)

                        gdf.to_file(
                            file_path,
                            layer=object_layer,
                            driver="GPKG",
                            engine="pyogrio",
                            layer_options={"OVERWRITE": "YES"},
                        )
                layers += [object_layer]
            else:
                self.logger.warning(f"{object_layer} is empty (!)")
        return layers

    def to_geopackage(self):
        pass

    @classmethod
    def from_geopackage(cls, file_path):
        """
        Initialize FixLayerSummary class from GeoPackage

        Parameters
        ----------
        file_path : path-string
            Path-string to the hydamo GeoPackage

        Returns
        -------
        layers_summary : FixLayerSummary
            FixLayerSummary object initialized with content of GeoPackage

        """
        layers_summary = cls()
        for layer in fiona.listlayers(file_path):
            with fiona.open(file_path, layer=layer) as src:
                schema = normalize_fiona_schema(src.schema)
            gdf = gpd.read_file(file_path, layer=layer)
            layers_summary.set_data(gdf, layer, schema["geometry"])
        return layers_summary


class FixLayersSummary:
    def __init__(self, log_level="DEBUG", date_check=pd.Timestamp.now().isoformat()):
        self.geo_types = {}
        self.date_check = date_check
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(getattr(logging, log_level))

    def _get_properties(self, gdf):
        properties = {"nen3610id": "str"}
        for i in gdf.columns:
            if i == "syntax_oordeel":
                properties[i] = "bool"
            elif "syntax" in i:
                properties[i] = "str"
            elif "validate" in i:
                properties[i] = "bool"
            elif "general" in i:
                if gdf[i].dtype == float:
                    properties[i] = "float"
                else:
                    properties[i] = "str"
            elif i == "rating":
                properties[i] = "int"
            elif i != "geometry":
                properties[i] = "str"
        return properties

    def set_data(self, gdf, layer, geo_type):
        """
        Set a gdf as a property of results

        Parameters
        ----------
        gdf : GeoDataFrame
            GeoDataFrame that will be property of LayersSummary
        layer : str
            The name of the property the GeoDataFrame will be assigned to. This
            is typically a HyDAMO layer (stuw, hydroobject, ...)
        geo_type : str
            Fiona string representation of geometry type (LineString, Polygon, ...)

        Returns
        -------
        None.

        """

        setattr(self, layer, gdf)
        gdf = getattr(self, layer)
        self.geo_types[layer] = geo_type

    def join_gdf(self, gdf, layer):
        """
        Join a GeoDataFrame to an existing property in layer summary.

        Parameters
        ----------
        gdf : GeoDataFrame
            GeoDataFrame to join to layer
        layer : str
            The property-name this gdf should be linked to. This
            is typically a HyDAMO layer (stuw, hydroobject, ...)

        Returns
        -------
        None.

        """

        if hasattr(self, layer):
            results_gdf = getattr(self, layer)
            drop_cols = [i for i in ["geometry", "nen3610id"] if i in gdf.columns]
            if drop_cols:
                gdf.drop(columns=drop_cols, inplace=True)
            setattr(
                self,
                layer,
                results_gdf.join(gdf, how="left", rsuffix="_right"),
            )

    def export(self, results_path, output_types=OUTPUT_TYPES):
        """
        Export the content of class to results_path

        Parameters
        ----------
        results_path : str or Path
            Directory where results are to be written to
        output_types : List[str], optional
            The types of output files that will be written. Options are
            ["geojson", "csv", "geopackage"]. By default all will be written
        Returns
        -------
        layers : List(str)
            A list of HyDAMO layers that are successfully written

        """

        gdf_dict = {k: v for k, v in self.__dict__.items() if isinstance(v, gpd.GeoDataFrame)}
        layers = []
        # make directories for output_types
        results_path = Path(results_path)
        for output_type in ["geojson", "csv"]:
            if output_type in output_types:
                result_dir = results_path.joinpath(output_type)
                result_dir.mkdir(parents=True, exist_ok=True)

        # export results to files
        for object_layer, gdf in gdf_dict.items():
            if "rating" not in gdf.columns:
                gdf["rating"] = 10

            if not gdf.empty:
                schema = {
                    "properties": self._get_properties(gdf),
                    "geometry": self.geo_types[object_layer],
                }

                # add date_check
                gdf["date_check"] = self.date_check
                schema["properties"]["date_check"] = "str"

                # Maybe implement this if the columns need to be altered (taken from HyDAMO.to_geopackage())
                # --------------------------------------------------------
                # if use_schema:
                #     # match fiona layer schema keys with gdf.columns
                #     schema = getattr(self, layer)._get_schema()
                #     schema_cols = list(schema["properties"].keys()) + ["geometry"]
                #     drop_cols = [i for i in gdf.columns if i not in schema_cols]
                #     gdf.drop(columns=drop_cols, inplace=True)

                #     schema["properties"] = {k: v for k, v in schema["properties"].items() if k in gdf.columns}

                #     # write gdf to geopackage, including schema
                #     if gdf.index.name in gdf.columns:
                #         gdf.reset_index(drop=True, inplace=True)
                # --------------------------------------------------------

                for output_type in output_types:
                    # set gdf to WGS84 for export to geojson
                    if output_type == "geojson":
                        file_path = results_path.joinpath(output_type, f"{object_layer}.geojson")
                        gdf_out = gdf.copy()
                        if gdf_out.crs:
                            gdf_out.to_crs("epsg:4326", inplace=True)
                        gdf_out.to_file(file_path, driver="GeoJSON", engine="pyogrio")

                    # drop geometry for writing to csv
                    elif output_type == "csv":
                        file_path = results_path.joinpath(output_type, f"{object_layer}.csv")
                        df = gdf.drop("geometry", axis=1)
                        df.to_csv(file_path, index=False)

                    # write to geopackage as is
                    elif output_type == "geopackage":
                        file_path = results_path.joinpath("fix_overview.gpkg")

                        gdf.to_file(
                            file_path,
                            layer=object_layer,
                            driver="GPKG",
                            engine="pyogrio",
                            layer_options={"OVERWRITE": "YES"},
                        )
                layers += [object_layer]
            else:
                self.logger.warning(f"{object_layer} is empty (!)")
        return layers

    def to_dict(self):
        """Return class-content as dict."""
        return {k: v for k, v in self.__dict__.items() if v is not None}

    @classmethod
    def from_geopackage(cls, file_path):
        """
        Initialize FixLayerSummary class from GeoPackage

        Parameters
        ----------
        file_path : path-string
            Path-string to the hydamo GeoPackage

        Returns
        -------
        layers_summary : FixLayerSummary
            FixLayerSummary object initialized with content of GeoPackage

        """
        layers_summary = cls()
        for layer in fiona.listlayers(file_path):
            with fiona.open(file_path, layer=layer) as src:
                schema = normalize_fiona_schema(src.schema)
            gdf = gpd.read_file(file_path, layer=layer)
            layers_summary.set_data(gdf, layer, schema["geometry"])
        return layers_summary


class FixResultSummary:  ## Logs / states what has been going on to fix the hydamo data
    def __init__(self, date_check=pd.Timestamp.now().isoformat()):
        """Initialize class."""
        self.success = False
        self.module_version = __version__
        self.date_check = date_check
        self.duration = None
        self.status = "Initialization"
        self.dataset_layers = []
        self.result_layers = []
        self.missing_layers = []
        self.error_layers = []
        self.prep_result = []
        self.fix_result = []  ## layer names that have been fixed (that are in the fix_rules objects)
        self.error = []
        self.errors = None
        self.warnings = None

    def _append_to_list(self, message, property):
        """Append a a message to a list."""
        if getattr(self, property) is None:
            setattr(self, property, [])
        getattr(self, property).append(message)

    def to_json(self, results_path):
        """
        Write result to json

        Parameters
        ----------
        results_path : str or Path
            Directory where results are to be written to

        Returns
        -------
        None.

        """

        result_json = Path(results_path).joinpath("fix_result.json")

        result_dict = {k: v for k, v in self.__dict__.items() if v is not None}
        with open(result_json, "w", encoding="utf-8", newline="\n") as dst:
            json.dump(result_dict, dst, indent=4)

    def to_dict(self):
        """Return class-content as dict."""
        return {k: v for k, v in self.__dict__.items() if v is not None}

    def to_all(self, results_path):
        """
        Write result to json and return class-content as dict

        Parameters
        ----------
        results_path : str or Path
            Directory where results are to be written to

        Returns
        -------
        None.

        """
        self.to_json(results_path)
        return self.to_dict()

    def append_warning(self, message):
        """
        Append a warning

        Parameters
        ----------
        message : str
            Warning message to be appended

        Returns
        -------
        None.

        """

        self._append_to_list(message, "warnings")

    def append_error(self, message):
        """
        Append a error

        Parameters
        ----------
        message : str
            Error message to be appended

        Returns
        -------
        None.

        """
        self._append_to_list(message, "errors")
