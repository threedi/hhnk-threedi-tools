import json
import xml.etree.ElementTree as ET
from pathlib import Path

import geopandas as gpd
import pandas as pd

WATERSCHAPSCODE = 12  # Hoogheemraadschap Hollands Noorderkwartier


class Converter:
    """
    Convert DAMO to HyDAMO. Also known as 'wasmachine'.

    Functionalities
    ---------------
    - Convert domain values in DAMO to descriptive values in HyDAMO
    - Add NEN3610id column to the layer
    - Correct HyDAMO field types are assigned to the attributes based on the HyDAMO schema

    Parameters
    ----------
    DAMO_path : str
        Path to the source DAMO geopackage
    HyDAMO_path : str
        Path to the target HyDAMO geopackage
    hydamo_schema_path : str
        Path to the HyDAMO schema (json file)
    DAMO_schema_path : str
        Path to the DAMO schema (xml file)
    layers : list
        List of layer names to convert to HyDAMO

    Sources
    ----------
    HyDAMO schema: https://github.com/HetWaterschapshuis/HyDAMOValidatieModule/blob/main/hydamo_validation/schemas/hydamo/HyDAMO_2.3.json
    DAMO_schema: XML file retrieved from Het Waterschapshuis by mail
    """

    def __init__(self, DAMO_path, HyDAMO_path, layers, hydamo_schema_path=None, damo_schema_path=None):
        self.DAMO_path = Path(DAMO_path)
        self.HyDAMO_path = Path(HyDAMO_path)
        self.layers = layers

        if hydamo_schema_path is None:
            self.hydamo_schema_path = Path(__file__).parents[2] / "resources/HyDAMO_2_3/HyDAMO_2_3.json"
        else:
            self.hydamo_schema_path = Path(hydamo_schema_path)

        if damo_schema_path is None:
            self.damo_schema_path = Path(__file__).parents[2] / "resources/DAMO_2_3/DAMO_2_3.xml"
        else:
            self.damo_schema_path = Path(damo_schema_path)

        self.retrieve_domain_mapping()
        self.retrieve_HyDAMO_definitions()

    def run(self):
        self.convert()

    def retrieve_domain_mapping(self):
        """
        Retrieve the domain mapping from the DAMO schema.DA

        Returns
        -------
        """
        # Read XML file content
        with open(self.damo_schema_path, "r") as xml_file:
            xml_text = xml_file.read()

        # Parse the XML text
        root = ET.fromstring(xml_text)

        # Initialize the domain dictionary
        self.domains = {}

        # Find all Domain elements
        domain_elements = root.findall(".//Domain")

        # Extract coded values and create the dictionary structure
        for domain_element in domain_elements:
            domain_name = domain_element.find("DomainName").text
            coded_values = domain_element.find(".//CodedValues")
            if coded_values is not None:
                coded_value_dict = {}
                for coded_value in coded_values.findall("CodedValue"):
                    name = coded_value.find("Name").text
                    code = coded_value.find("Code").text
                    if code.isdigit():
                        code = int(code)
                    coded_value_dict[code] = name
                self.domains[domain_name.lower()] = coded_value_dict

        # Initialize the objects dictionary
        self.objects = {}

        # Find all DataElement elements
        data_element_elements = root.findall(".//DataElement")

        # Extract Name and create the nested dictionary structure
        for data_element_element in data_element_elements:
            data_name = data_element_element.find(".//Name").text
            field_dict = {}

            fields_element = data_element_element.find(".//Fields")
            if fields_element is not None:
                field_array_element = fields_element.find(".//FieldArray")
                if field_array_element is not None:
                    for field_element in field_array_element.findall(".//Field"):
                        field_name = field_element.find("Name").text
                        field_domain = field_element.find("Domain")
                        if field_domain is not None:
                            field_type = field_domain.find("DomainName").text
                        else:
                            field_type = field_element.find("Type").text
                            field_type = field_type.replace("esriFieldType", "")
                        field_dict[field_name] = field_type

            self.objects[data_name] = field_dict

    def retrieve_HyDAMO_definitions(self):
        """
        Reads JSON definitions and assigns them as instance variables.
        """
        with open(self.hydamo_schema_path, "r") as json_file:
            hydamo_schema = json.load(json_file)

        definitions = hydamo_schema.get("definitions", {})

        for key, value in definitions.items():
            attr_name = f"definition_{key}"
            setattr(self, attr_name, value)

    def convert(self):
        """
        Open layer by layer and convert the attributes to HyDAMO.

        Returns
        ----
        """
        for layer_name in self.layers:
            self.convert_layer(layer_name)

    def convert_layer(self, layer_name):
        """
        Convert the layer to HyDAMO and write to the target HyDAMO geopackage.

        Parameters
        ----------
        layer : str
            Name of the layer to convert

        Returns
        ----
        """
        layer_gdf = gpd.read_file(self.DAMO_path, layer=layer_name)
        layer_gdf = self.convert_attributes(layer_gdf, layer_name)
        layer_gdf = self.add_column_NEN3610id(layer_gdf, layer_name)
        layer_gdf.to_file(self.HyDAMO_path, layer=layer_name, driver="GPKG")

    def convert_attributes(self, layer_gdf, layer_name):
        """
        Convert the attributes of the layer to HyDAMO.

        Parameters
        ----------
        layer_gdf : geopandas.GeoDataFrame
            Layer to convert

        Returns
        -------
        geopandas.GeoDataFrame
            Converted layer
        """
        for column_name in layer_gdf.columns:
            column_name = column_name.lower()
            layer_gdf[column_name] = self.convert_column(layer_gdf[column_name], column_name, layer_name)
        return layer_gdf

    def convert_column(self, column, column_name, layer_name):
        """
        Convert the attribute column to HyDAMO.

        Parameters
        ----------
        column : pandas.Series
            Attribute column to convert
        column_name : str
            Name of the attribute column

        Returns
        -------
        pandas.Series
            Converted attribute column
        """
        # Get the field type of the attribute from the HyDAMO schema
        field_type = self.get_field_type(column_name, layer_name)
        # Convert the domain values to HyDAMO target values
        column = self.convert_domain_values(column_name, column)
        # Convert the field type to the correct type
        column = self.convert_field_type(column, field_type)
        return column

    def get_field_type(self, column_name, layer_name):
        """
        Retrieves the field type of a specific attribute in a definition.

        Args:
            layer_name (str): The name of the object (e.g., 'hydroobject').
            column_name (str): The name of the field (e.g., 'nen3610id').

        Returns:
            str: The type of the field if found, else None.
        """
        # Find the object definition
        object_def = getattr(self, f"definition_{layer_name}", {})

        # Navigate to the 'properties' of the object
        properties = object_def.get("properties", {})

        # Retrieve the field type
        field = properties.get(column_name, {})
        return field.get("type", None)

    def convert_domain_values(self, column_name, column):
        """
        Check if column_name is a domain.
        If it is a domain, convert the values of the column to the HyDAMO target values.
        Else, return the column as is.

        Parameters
        ----------
        column : pandas.Series
            Attribute column to convert

        Returns
        -------
        pandas.Series
            Converted attribute column
        """
        if column_name in self.domains:
            domain = self.domains[column_name]
            mapped_column = column.map(domain)
            print(f"Converted domain values of column {column_name} from Integer to Text")
            return mapped_column

        return column

    def convert_field_type(self, column, field_type):
        """
        Convert the field type of the column to the correct type.

        Parameters
        ----------
        column : pandas.Series
            Attribute column to convert
        field_type : str
            Field type to convert to

        Returns
        -------
        pandas.Series
            Converted attribute column
        """
        if field_type == "string":
            column = column.apply(lambda x: str(x) if pd.notnull(x) else None)
        elif field_type == "integer":
            column = column.apply(lambda x: int(x) if pd.notnull(x) else None)
        elif field_type == "number":
            column = column.apply(lambda x: float(x) if pd.notnull(x) else None)
        elif field_type == None:  # column not part of the schema, so we keep it as is
            return column
        else:
            raise ValueError(f"Field type {field_type} not supported")
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
