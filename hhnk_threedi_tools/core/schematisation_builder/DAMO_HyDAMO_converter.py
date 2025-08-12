# %%
import importlib.resources as importlib_resources
import json
import logging
import os
import xml.etree.ElementTree as ET
from functools import cached_property
from pathlib import Path
from typing import Literal, Optional, Tuple, Union

import fiona
import geopandas as gpd
import hhnk_research_tools as hrt
import pandas as pd

WATERSCHAPSCODE = 12  # Hoogheemraadschap Hollands Noorderkwartier
SCHEMA_VERSIONS = {  # Available versions of (hy)damo schematisation that are implemented
    "DAMO": ["2.3", "2.4.1", "2.5"],
    "HyDAMO": ["2.3", "2.4"],
}
CRS = "EPSG:28992"


class DAMO_to_HyDAMO_Converter:
    """
    Convert DAMO to HyDAMO. Also known as 'wasmachine'.

    Functionalities
    ---------------
    - Convert domain values in DAMO to descriptive values in HyDAMO
    - Add NEN3610id column to the layer
    - Correct HyDAMO field types are assigned to the attributes based on the HyDAMO schema

    Parameters
    ----------
    damo_file_path : Path
        Path to the source DAMO geopackage
    hydamo_file_path : Path
        Path to the target HyDAMO geopackage. Converted to hrt.SpatialDatabase after initialization
    layers : list
        List of layer names to convert to HyDAMO
    hydamo_schema_path : Path
        Path to the HyDAMO schema (json file)
    hydamo_version: str
        Version number of the HyDAMO schema (format: 1.1, 3.2.1, 4.0, etc)
    damo_schema_path : Path
        Path to the DAMO schema (xml file)
    damo_version: str
        Version number of the HyDAMO schema (format: 1.1, 3.2.1, 4.0, etc)
    overwrite : bool
        If True, overwrite an existing layer in existing HyDAMO geopackage
    convert_domain_values : bool
        If True, convert the domain values in the DAMO layer to descriptive values in HyDAMO.
        If False, the domain values are not converted.
    logger : hrt.logging.Logger, optional
        Logger to use for logging. If None, a default logger is created.
    convert_domain_values : bool
        If True, convert the domain values in the DAMO layer to descriptive values in HyDAMO.
        If False, the domain values are not converted.
    logger : hrt.logging.Logger, optional
        Logger to use for logging. If None, a default logger is created.

    Sources
    -------
    HyDAMO schema: https://github.com/HetWaterschapshuis/HyDAMOValidatieModule/blob/main/hydamo_validation/schemas/hydamo/HyDAMO_2.3.json
    DAMO_schema: XML file retrieved from Het Waterschapshuis by mail
    """

    def __init__(
        self,
        damo_file_path: os.PathLike,
        hydamo_file_path: Union[Path, hrt.SpatialDatabase],
        layers: list[str] = None,
        hydamo_schema_path: Optional[Path] = None,
        hydamo_version: str = "2.4",
        damo_schema_path: Optional[Path] = None,
        damo_version: str = "2.4.1",
        overwrite: bool = False,
        add_status_object: bool = True,
        convert_domain_values: bool = False,
        logger: Optional[logging.Logger] = None,
    ):
        if logger:
            self.logger = logger
        else:
            self.logger = hrt.logging.get_logger(__name__)

        self.damo_file_path = Path(damo_file_path)
        self.hydamo_file_path = hrt.SpatialDatabase(hydamo_file_path)
        self.layers = layers
        self.overwrite = overwrite
        self.add_status_object = add_status_object
        self.convert_domain_values = convert_domain_values
        self.damo_version = damo_version

        self.hydamo_schema_path = self._get_schema_path(
            schema_path=hydamo_schema_path, schema_basename="HyDAMO", schema_version=hydamo_version
        )

        if convert_domain_values:
            if damo_schema_path is None:
                raise NotImplementedError(
                    "DAMO schema path must be provided to convert domain values. "
                    "It is not included in the repository to avoid unnecessary storage usage. "
                    "Please specify a valid DAMO schema path."
                )

            self.damo_schema_path = self._get_schema_path(
                schema_path=damo_schema_path, schema_basename="DAMO", schema_version=damo_version
            )
            self.domains, self.objects = self._retrieve_damo_domain_mapping()

    @cached_property
    def hydamo_definitions(self) -> dict:
        """Definitions of the hydamo schematisation."""
        with open(self.hydamo_schema_path, "r") as json_file:
            hydamo_schema = json.load(json_file)

        return hydamo_schema.get("definitions", {})

    def _get_schema_path(
        self,
        schema_path,
        schema_basename: Literal["DAMO", "HyDAMO"],
        schema_version: str,
    ) -> Path:
        """Return Path to schematisation settings from package resources when schema_path is not provided."""
        if schema_path is None:
            if schema_version not in SCHEMA_VERSIONS[schema_basename]:
                raise ValueError(
                    f"{schema_basename} version number {schema_version} is not implemented or incorrect. Options are; {SCHEMA_VERSIONS[schema_basename]}"
                )
            if schema_basename == "DAMO":
                package_resource = "hhnk_threedi_tools.resources.schematisation_builder"
                schema_name = f"{schema_basename}_{schema_version.replace('.', '_')}.xml"
            elif schema_basename == "HyDAMO":
                package_resource = "hydamo_validation.schemas.hydamo"
                schema_name = f"HyDAMO_{schema_version}.json"

            schema_path = Path(str(importlib_resources.files(package_resource).joinpath(schema_name)))

        if not schema_path.exists():
            raise FileNotFoundError(f"{schema_path} does not exist.")

        return schema_path

    def _retrieve_damo_domain_mapping(self) -> Tuple[dict, dict]:
        """
        Retrieve the domain mapping from the DAMO schema.

        Returns
        -------
        domains : dict
            Dictionary with the domains and their coded values
        objects : dict
            Dictionary with the objects and their fields
        """
        # Read XML file content
        with open(self.damo_schema_path, "r") as xml_file:
            xml_text = xml_file.read()

        # Parse the XML text
        root = ET.fromstring(xml_text)

        # Initialize the domain dictionary
        domains = {}

        # Find all Domain elements
        domain_elements = root.findall(".//Domain")

        # Extract coded values and create the dictionary structure
        for domain_element in domain_elements:
            domain_name = domain_element.find("DomainName").text.lower()
            coded_values = domain_element.find(".//CodedValues")
            if coded_values is not None:
                coded_value_dict = {}
                for coded_value in coded_values.findall("CodedValue"):
                    name = coded_value.find("Name").text.lower()
                    code = coded_value.find("Code").text
                    if code.isdigit():
                        code = int(code)
                    else:
                        code = code.lower()
                    coded_value_dict[code] = name
                domains[domain_name.lower()] = coded_value_dict

        # Initialize the objects dictionary
        objects = {}

        # Find all DataElement elements
        data_element_elements = root.findall(".//DataElement")

        # Extract Name and create the nested dictionary structure
        for data_element_element in data_element_elements:
            data_name = data_element_element.find(".//Name").text.lower()
            field_dict = {}

            fields_element = data_element_element.find(".//Fields")
            if fields_element is not None:
                field_array_element = fields_element.find(".//FieldArray")
                if field_array_element is not None:
                    for field_element in field_array_element.findall(".//Field"):
                        field_name = field_element.find("Name").text.lower()
                        field_domain = field_element.find("Domain")
                        if field_domain is not None:
                            field_type = field_domain.find("DomainName").text.lower()
                        else:
                            field_type = field_element.find("Type").text.lower()
                            field_type = field_type.replace("esriFieldType", "")
                        field_dict[field_name] = field_type

            objects[data_name.lower()] = field_dict

        return domains, objects

    def convert_layers(self) -> None:
        """
        Open layer by layer and convert the layer to HyDAMO and write to the target HyDAMO geopackage.

        Writes
        ------
        self.hydamo_file_path.path : Path
            GPKG file containing HyDAMO layers contained in self.layers
        """
        self.hydamo_file_path.parent.mkdir(parents=True, exist_ok=True)

        if self.layers is None:
            self.layers = fiona.listlayers(self.damo_file_path, engine="pyogrio")

        for layer_name in self.layers:
            layer_name = layer_name.lower()
            self.logger.info(f"Conversion of {layer_name}")
            if not self.overwrite and self.hydamo_file_path.exists():
                if layer_name in self.hydamo_file_path.available_layers():
                    self.logger.info(
                        f"Layer {layer_name} already exists in {self.hydamo_file_path.path}. Skipping conversion."
                    )
                    return

            layer_gdf = gpd.read_file(self.damo_file_path, layer=layer_name, engine="pyogrio")
            layer_gdf = self._convert_attributes(layer_gdf, layer_name)
            layer_gdf = self._add_column_NEN3610id(layer_gdf, layer_name)
            if self.add_status_object:
                layer_gdf = self._add_column_status_object(layer_gdf, layer_name)

            if isinstance(layer_gdf, gpd.GeoDataFrame) and layer_gdf.geometry.name in layer_gdf.columns:
                layer_gdf.to_file(self.hydamo_file_path.path, layer=layer_name, engine="pyogrio")
            else:  # dataframe handling
                gdf_no_geom = gpd.GeoDataFrame(layer_gdf, geometry=[None] * len(layer_gdf), crs=CRS)
                gdf_no_geom.to_file(self.hydamo_file_path.path, layer=layer_name, engine="pyogrio", driver="GPKG")
                if gdf_no_geom.empty:
                    self.logger.warning(f"Layer {layer_name} is empty.")
                else:
                    self.logger.info(f"Layer {layer_name} has no geometry.")

    def _convert_attributes(self, layer_gdf: gpd.GeoDataFrame, layer_name: str) -> gpd.GeoDataFrame:
        """
        Convert the attributes of the layer to HyDAMO.

        Parameters
        ----------
        layer_gdf : gpd.GeoDataFrame
            Layer to convert
        layer_name : str
            Layer name to convert

        Returns
        -------
        layer_gdf : gpd.GeoDataFrame
            Converted layer
        """
        for column_name in layer_gdf.columns:
            if column_name != "geometry":
                lower_column_name = column_name.lower()
                layer_gdf.rename(columns={column_name: lower_column_name}, inplace=True)
                layer_gdf[lower_column_name] = self._convert_column(
                    column=layer_gdf[lower_column_name],
                    column_name=lower_column_name,
                    layer_name=layer_name,
                )

        return layer_gdf

    def _convert_column(self, column: pd.Series, column_name: str, layer_name: str) -> pd.Series:
        """
        Convert the attribute column to HyDAMO.

        Parameters
        ----------
        column : pd.Series
            Attribute column to convert
        column_name : str
            Name of the attribute column (is already lowered)
        layer_name : str
            Name of the layer

        Returns
        -------
        column : pd.Series
            Converted attribute column
        """
        # Get the field type of the attribute from the HyDAMO schema
        field_type = self._get_field_type(column_name, layer_name)
        # Convert the domain values to HyDAMO target values
        if self.convert_domain_values:
            column = self._convert_domain_values(layer_name, column_name, column)
        # Convert the field type to the correct type
        if field_type is not None:
            # Convert to the field type, but safely handle NaN values
            if field_type in [int, float]:
                column = pd.to_numeric(column, errors="coerce").astype(field_type, errors="ignore")
            elif field_type is str:
                column = column.astype(str)
            else:
                self.logger.warning(
                    f"Field type {field_type} for column {column_name} in layer {layer_name} is not supported."
                )

        return column

    def _get_field_type(self, column_name: str, layer_name: str) -> str:
        """
        Retrieve the field type of a specific attribute in a definition.

        Parameters
        ----------
        column_name : str
            The name of the field (e.g., 'nen3610id').
        layer_name : str
            The name of the object (e.g., 'hydroobject').

        Returns
        -------
        field_type : str or None
            The type of the field if found, else None.
        """
        field_types_dict = {
            "string": str,
            "integer": int,
            "number": float,
        }

        try:
            # Check if the layer_name exists in the definitions
            field_type = self.hydamo_definitions[layer_name]["properties"][column_name]["type"]

            field_type = field_types_dict.get(field_type, None)
            if field_type is None:
                self.logger.debug(
                    f"Field type is not find in field_types_dict for field {column_name} in layer {layer_name}"
                )
        except Exception as e:
            self.logger.debug(f"Field {column_name} not found in schema definitions for layer {layer_name}")
            field_type = None
        finally:
            return field_type

    def _convert_domain_values(self, object_name: str, column_name: str, column: pd.Series) -> pd.Series:
        """
        Check if the column_name corresponds to a field in the specified object that is a domain.
        If it is a domain, convert the values of the column using the associated domain.
        Else, return the column as is.

        Called by: self._convert_column

        Parameters
        ----------
        object_name : str
            Name of the object containing the field.
        column_name : str
            Name of the column to check and convert.
        column : pd.Series
            Attribute column to convert.

        Returns
        -------
        pd.Series
            Converted attribute column if it corresponds to a domain, else the original column.
        """
        # Check if the object_name exists and contains the column_name
        object_name = object_name.lower()
        if object_name in self.objects.keys():
            fields = self.objects[object_name]
            if column_name in fields.keys():
                field_domain = fields[column_name]
                # If field_domain corresponds to a domain, perform the conversion
                if field_domain in self.domains.keys():
                    domain = self.domains[field_domain]
                    mapped_column = column.map(domain)
                    self.logger.info(
                        f"Converted domain values of column {column_name} in object {object_name} using domain {field_domain}"
                    )
                    return mapped_column

        # If no domain is found, return the column as is
        return column

    def _add_column_NEN3610id(self, layer_gdf: gpd.GeoDataFrame, layer_name: str):
        """
        Add the NEN3610id column to the layer.
        It is a concatenation of 'NL.WBHCODE.', the WATERSCHAPSCODE, the object name and the value of the column 'code'.
        For example: 'NL.WBHCODE.12.hydroobject.1234'

        Parameters
        ----------
        layer_gdf : gpd.GeoDataFrame
            Layer to add the NEN3610id column to
        layer_name : str
            Name of the layer

        Returns
        -------
        layer_gdf : gpd.GeoDataFrame
            Layer with the NEN3610id column
        """
        if "code" in layer_gdf.columns:
            layer_gdf["NEN3610id"] = layer_gdf["code"].apply(
                lambda x: f"NL.WBHCODE.{WATERSCHAPSCODE}.{layer_name}.{x}"
            )
        return layer_gdf

    def _add_column_status_object(self, layer_gdf: gpd.GeoDataFrame, layer_name: str) -> gpd.GeoDataFrame:
        """
        Add the statusobject column to the layer.
        - if not exists, add it with value 'gerealiseerd'.
        - else, fillna with 'gerealiseerd'.
        - check if existing values are within valid options ['gerealiseerd', 'planvorming'] and logs warning if not.
        - for now, also replace invalid values with 'gerealiseerd'.

        Parameters
        ----------
        layer_gdf : gpd.GeoDataFrame
            Layer to add the statusobject column to
        layer_name : str
            Name of the layer

        Returns
        -------
        layer_gdf : gpd.GeoDataFrame
            Layer with the statusobject column
        """
        if "statusobject" not in layer_gdf.columns:
            layer_gdf["statusobject"] = "gerealiseerd"
        else:
            layer_gdf["statusobject"] = layer_gdf["statusobject"].fillna("gerealiseerd")
            # Check if existing values are within valid options
            valid_options = ["gerealiseerd", "planvorming"]
            invalid_values = layer_gdf["statusobject"].loc[~layer_gdf["statusobject"].isin(valid_options)]
            if not invalid_values.empty:
                self.logger.warning(
                    f"Invalid statusobject values found in layer {layer_name}. For now set to 'gerealiseerd', invalid values: {invalid_values.tolist()}"
                )
                layer_gdf["statusobject"] = layer_gdf["statusobject"].where(
                    layer_gdf["statusobject"].isin(valid_options), "gerealiseerd"
                )

        return layer_gdf

    def run(self) -> None:
        """Caller for the class that starts the conversion.

        Output is written to self.hydamo_file_path.path
        """
        self.convert_layers()
