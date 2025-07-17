import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional, Tuple, Union

import geopandas as gpd
import hhnk_research_tools as hrt
import pandas as pd

import hhnk_threedi_tools as htt

WATERSCHAPSCODE = 12  # Hoogheemraadschap Hollands Noorderkwartier


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

    Sources
    -------
    HyDAMO schema: https://github.com/HetWaterschapshuis/HyDAMOValidatieModule/blob/main/hydamo_validation/schemas/hydamo/HyDAMO_2.3.json
    DAMO_schema: XML file retrieved from Het Waterschapshuis by mail
    """

    def __init__(
        self,
        damo_file_path: Path,
        hydamo_file_path: Union[Path, hrt.SpatialDatabase],
        layers: list,
        hydamo_schema_path: Optional[Path] = None,
        hydamo_version: str = None,
        damo_schema_path: Optional[Path] = None,
        damo_version: str = None,
        overwrite: bool = False,
        logger=None,
    ):
        self.damo_file_path = Path(damo_file_path)
        self.hydamo_file_path = hrt.SpatialDatabase(hydamo_file_path)
        self.layers = layers
        self.overwrite = overwrite

        if logger:
            self.logger = logger
        else:
            self.logger = hrt.logging.get_logger(__name__)

        self.logger.info(f"conversion layers are {self.layers}")

        if hydamo_schema_path is None:
            if "." not in hydamo_version:
                raise ValueError("HyDAMO version number is in incorrect format. Should be: 1.1, 3.2.1, 4.0, etc.")
            if hydamo_version not in ["2.3", "2.4"]:
                raise ValueError(f"HyDAMO version number {hydamo_version} is not implemented or incorrect. Choose another.")
            hydamo_name = f"HyDAMO_{hydamo_version.replace(".","_")}.json"
            self.hydamo_schema_path = hrt.get_pkg_resource_path(
                package_resource=htt.resources.schematisation_builder, name=hydamo_name
            )
        else:
            self.hydamo_schema_path = Path(hydamo_schema_path)
        if not self.hydamo_schema_path.exists():
            raise FileNotFoundError(f"{self.hydamo_schema_path} does not exist.")

        if damo_schema_path is None:
            if "." not in damo_version:
                raise ValueError("DAMO version number is in incorrect format. Should be: 1.1, 3.2.1, 4.0, etc.")
            if damo_version not in ["2.3", "2.4.1", "2.5"]:
                raise ValueError(f"DAMO version number {damo_version} is not implemented or incorrect. Choose another.")
            damo_name = f"DAMO_{damo_version.replace(".","_")}.xml"
            self.damo_schema_path = hrt.get_pkg_resource_path(
                package_resource=htt.resources.schematisation_builder, name=damo_name
            )
        else:
            self.damo_schema_path = Path(damo_schema_path)
        if not self.damo_schema_path.exists():
            raise FileNotFoundError(f"{self.damo_schema_path} does not exist.")

        self.domains, self.objects = self.retrieve_domain_mapping()

        # Read JSON definitions
        with open(self.hydamo_schema_path, "r") as json_file:
            hydamo_schema = json.load(json_file)

        self.definitions = hydamo_schema.get("definitions", {})

    def run(self) -> None:
        """self.convert_layers writes to self.hydamo_file_path.path"""
        self.convert_layers()

    def retrieve_domain_mapping(self) -> Tuple[dict, dict]:
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
        -------
        self.hydamo_file_path.path : Path
            GPKG file containing HyDAMO layers contained in self.layers
        """
        self.hydamo_file_path.parent.mkdir(parents=True, exist_ok=True)

        for layer_name in self.layers:
            self.logger.info(f"Conversion of {layer_name}")
            if not self.overwrite and self.hydamo_file_path.exists():
                if layer_name in self.hydamo_file_path.available_layers():
                    self.logger.info(
                        f"Layer {layer_name} already exists in {self.hydamo_file_path.path}. Skipping conversion."
                    )
                    return

            layer_gdf = gpd.read_file(self.damo_file_path, layer=layer_name, engine="pyogrio")
            layer_gdf = self.convert_attributes(layer_gdf, layer_name)
            layer_gdf = self.add_column_NEN3610id(layer_gdf, layer_name)
            layer_gdf.to_file(self.hydamo_file_path.path, layer=layer_name, engine="pyogrio")

    def convert_attributes(self, layer_gdf: gpd.GeoDataFrame, layer_name: str) -> gpd.GeoDataFrame:
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
        gpd.GeoDataFrame
            Converted layer
        """
        for column_name in layer_gdf.columns:
            column_name = column_name.lower()
            layer_gdf[column_name] = self.convert_column(layer_gdf[column_name], column_name, layer_name)
        return layer_gdf

    def convert_column(self, column: pd.Series, column_name: str, layer_name: str) -> pd.Series:
        """
        Convert the attribute column to HyDAMO.

        Parameters
        ----------
        column : pd.Series
            Attribute column to convert
        column_name : str
            Name of the attribute column
        layer_name : str
            Name of the layer

        Returns
        -------
        column : pd.Series
            Converted attribute column
        """
        # Get the field type of the attribute from the HyDAMO schema
        field_type = self.get_field_type(column_name, layer_name)
        # Convert the domain values to HyDAMO target values
        column = self.convert_domain_values(layer_name, column_name, column)
        # Convert the field type to the correct type
        if field_type is not None and pd.notna(column).all():
            column = column.astype(field_type)  # Convert to the field type
        else:
            self.logger.info(
                f"field_type is None and/or column values are NaN for field {column_name} in layer {layer_name}"
            )

        return column

    def get_field_type(self, column_name: str, layer_name: str) -> str:
        """
        Retrieve the field type of a specific attribute in a definition.

        Parameters
        ----------
        layer_name : str
            The name of the object (e.g., 'hydroobject').
        column_name : str
            The name of the field (e.g., 'nen3610id').

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
            # make sure the layer_name and column_name are lowercase
            layer_name = layer_name.lower()
            column_name = column_name.lower()
            # Check if the layer_name exists in the definitions
            field_type = self.definitions[layer_name]["properties"][column_name]["type"]

            field_type = field_types_dict.get(field_type, None)
            if field_type is None:
                self.logger.info(
                    f"Field type is not find in field_types_dict for field {column_name} in layer {layer_name}"
                )
        except Exception as e:
            self.logger.info(f"Field {column_name} not found in schema definitions for layer {layer_name}")
            field_type = None
        finally:
            return field_type

    def convert_domain_values(self, object_name: str, column_name: str, column: pd.Series) -> pd.Series:
        """
        Check if the column_name corresponds to a field in the specified object that is a domain.
        If it is a domain, convert the values of the column using the associated domain.
        Else, return the column as is.

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

    def add_column_NEN3610id(self, layer_gdf, layer_name):
        """
        Add the NEN3610id column to the layer.
        It is a concatenation of 'NL.WBHCODE.', the WATERSCHAPSCODE, the object name and the value of the column 'code'.
        For example: 'NL.WBHCODE.12.hydroobject.1234'

        Parameters
        ----------
        layer_gdf : geopandas.GeoDataFrame
            Layer to add the NEN3610id column to
        layer_name : str
            Name of the layer

        Returns
        -------
        geopandas.GeoDataFrame
            Layer with the NEN3610id column
        """
        layer_gdf["NEN3610id"] = layer_gdf["code"].apply(
            lambda x: "NL.WBHCODE." + str(WATERSCHAPSCODE) + "." + layer_name + "." + str(x)
        )
        return layer_gdf
