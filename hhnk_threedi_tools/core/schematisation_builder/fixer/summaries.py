import json
from pathlib import Path

import fiona
import geopandas as gpd
import numpy as np
import pandas as pd
from hydamo_validation import __version__
from hydamo_validation.datamodel import HyDAMO
from hydamo_validation.summaries import LayersSummary, ResultSummary
from hydamo_validation.utils import normalize_fiona_schema, read_geopackage

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

    @property
    def empty(self) -> bool:
        """
        Return True if the instance has *no* GeoDataFrame attributes.
        """
        for value in self.__dict__.values():
            if isinstance(value, gpd.GeoDataFrame):
                return False
        return True

    @property
    def data_layers(self):
        return [key for key, value in vars(self).items() if isinstance(value, gpd.GeoDataFrame) and not value.empty]

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

    def to_json(self):
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


class ExtendedHyDAMO(HyDAMO):
    def __init__(self, hydamo_path: Path = None, results_path: Path = None, rules_objects: list = [], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hydamo_path = hydamo_path
        self.results_path = results_path

        self.post_process_datamodel(rules_objects)

    @property
    def empty(self) -> bool:
        """
        Return True if the instance has *no* GeoDataFrame attributes.
        """
        for value in self.__dict__.values():
            if isinstance(value, gpd.GeoDataFrame):
                return False
        return True

    def post_process_datamodel(self, objects: list) -> None:
        """Post-process DataModel from self.validation_results."""
        self.validation_results: dict[str, gpd.GeoDataFrame] = {}
        self.validation_rules: dict[str, dict] = {}

        validation_results = ExtendedLayersSummary.from_geopackage(self.results_path)
        for hydamo_layer in self.layers:
            self.validation_results[hydamo_layer] = getattr(validation_results, hydamo_layer)
            self.validation_rules[hydamo_layer] = next((obj for obj in objects if obj["object"] == hydamo_layer), {})

        self._set_properties()

    def _set_properties(self):
        self.properties = {}
        if self.hydamo_path:
            layers = fiona.listlayers(self.hydamo_path)
            layers_dict = {}
            for layer in layers:
                with fiona.open(self.hydamo_path) as src:
                    schema = normalize_fiona_schema(src.schema)
                    layers_dict[layer] = schema

            self.properties[self.hydamo_path.name] = layers_dict
        return self.properties

    def _filter_status(self, gdf: gpd.GeoDataFrame, status_object: list[str]):
        if status_object is not None:
            if "statusobject" in gdf.columns:
                gdf = gdf.loc[np.where(gdf["statusobject"].isna() | gdf["statusobject"].isin(status_object))]
        return gdf

    def read_layer(self, layer, result_summary=ExtendedResultSummary(), status_object=None):
        """
        Read a layer from the datamodel.

        Parameters
        ----------
        layer : str
            Name of the layer (case sensitive!)
        result_summary : FixResultSummary
            A hydamo_fix FixResultSummary class where a possible exception
            will be appended to.
        status_object : List[str], optional
            A list of statusobject values used as a filter. The default is None.

        Raises
        ------
        e
            General exception while reading the layer from the geopackage.
        KeyError
            Specific exception; the layer is not part of the geopackage.

        Returns
        -------
        gdf : GeoDataFrame
            GeoDataFrame read from datasets (all columns are converted to lower case)
        schema : TYPE
            Fiona schema read from the layer
        """

        if not self.hydamo_path:
            raise ValueError(f"Path to geopackage unknown. This function only works if a geopackage is available.")

        if layer in self.layers:
            dataset = {k: v for k, v in self.properties.items() if layer in v.keys()}
            file_path = self.hydamo_path
            schema = list(dataset.values())[0][layer]
            try:
                if self.empty:
                    gdf = read_geopackage(file_path, layer=layer)
                else:
                    gdf = getattr(self, layer)
                gdf = self._filter_status(gdf, status_object)
            except Exception as e:
                result_summary.append_warning(
                    (
                        f"Laag {layer} uit bestand {file_path.name} is geen "
                        "GeoPackage die wij kunnen openen. Vervang het bestand en "
                        "probeer opnieuw."
                    )
                )
                raise e

            # we will read all lower case
            schema["properties"] = {k.lower(): v for k, v in schema["properties"].items()}
            gdf.columns = [i.lower() for i in gdf.columns]
        else:
            raise KeyError(f"'{layer}' not in dataset-layers: '{self.layers}'")

        return gdf, schema

    @classmethod
    def from_geopackage(
        cls,
        hydamo_path=None,
        results_path=None,
        rules_objects=None,
        version="2.4",
        ignored_layers=[],
        check_columns=True,
        check_geotype=True,
    ):
        """
        Initialize ExtendedHyDAMO class from GeoPackage

        Parameters
        ----------
        file_path : path-string
            Path-string to the hydamo GeoPackage
        check_columns : bool, optional
            Check if all required columns are present in the GeoDataFrame.
            The default is True.
        check_geotype : bool, optional
            Check if the geometry is of the required type. The default is True.

        Returns
        -------
        hydamo : ExtendedHyDAMO
            ExtendedHyDAMO object initialized with content of GeoPackage

        """
        if not hydamo_path:
            raise ValueError(f"No geopackage path is provided.")

        hydamo = cls(
            hydamo_path=hydamo_path,
            results_path=results_path,
            rules_objects=rules_objects,
            version=version,
            ignored_layers=ignored_layers,
        )
        for layer in fiona.listlayers(hydamo_path):
            if layer in hydamo.layers:
                hydamo_layer: HyDAMO = getattr(hydamo, layer)
                hydamo_layer.set_data(
                    gpd.read_file(hydamo_path, layer=layer),
                    check_columns=check_columns,
                    check_geotype=check_geotype,
                )
        return hydamo
