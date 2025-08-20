import json
import logging
import os
from collections import OrderedDict
from datetime import time
import typing
from pathlib import Path

if __name__ == "__main__":
    import sys
    sys.path.append(Path(__file__).parent.parent.parent.parent.as_posix())

import fiona

from hhnk_threedi_tools.git_model_repo.utils.file_change_detection import FileChangeDetection
from hhnk_threedi_tools.git_model_repo.utils.timer_log import SubTimer

log = logging.getLogger(__name__)


def format_json(
        obj: typing.Any,
        parent_key: str = "",
        depth: int = 0,
):
    """Format a JSON object for improved readability in git diff.

    Parameters
    ----------
    obj : Any
        The JSON object to format.
    parent_key : str, optional
        The key of the parent object (default is "").
    depth : int, optional
        The current depth in the JSON structure (default is 0).

    Returns
    -------
    str
        The formatted JSON string.
    """
    indent = " "  # one space for indentation level

    if not isinstance(obj, (dict, list)) or depth > 2 or parent_key in ["crs"]:
        return json.dumps(obj)

    if isinstance(obj, list):
        formatted_items = [format_json(item, "", depth + 1) for item in obj]
        return (
            "[\n" + ",\n".join(indent * (depth + 1) + item for item in formatted_items) + "\n" + indent * depth + "]"
        )
    elif isinstance(obj, dict):
        formatted_items = [f'"{key}": {format_json(value, key, depth + 1)}' for key, value in obj.items()]
        return (
            "{\n" + ",\n".join(indent * (depth + 1) + item for item in formatted_items) + "\n" + indent * depth + "}"
        )


class GeoPackageDump(object):
    """
    Basic version using Fiona for dumping GeoPackage files.

    Parameters
    ----------
    file_path : str
        Path to the GeoPackage file.
    output_path : str, optional
        Path to the output directory. If None, a default directory is created next to the input file.

    Attributes
    ----------
    file_path : str
        Path to the GeoPackage file.
    output_path : str
        Path to the output directory.
    changed_files : list of str
        List of files that have changed after dumping.

    Methods
    -------
    get_schema_layer(layer_name)
        Get the schema of a specific layer as a dictionary.
    get_schema()
        Get the schema of all layers as an OrderedDict.
    dump_schema()
        Dump the schema to a JSON file.
    dump_layers(reformat=True)
        Dump all layers and features to GeoJSON files.
    dump()
        Dump both the schema and layers.
    """

    def __init__(self, file_path, output_path=None):
        """
        Initialize the GeoPackageDump object.

        Parameters
        ----------
        file_path : str
            Path to the GeoPackage file.
        output_path : str, optional
            Path to the output directory. If None, a default directory is created.
        """
        self.file_path = file_path

        if output_path is None:
            base = os.path.splitext(os.path.basename(file_path))
            output_path = os.path.join(os.path.dirname(file_path), f"{base[0]}_{base[1]}")
        self.output_path = output_path
        os.makedirs(self.output_path, exist_ok=True)

        self.changed_files = []

    def get_schema_layer(self, layer_name):
        """Get the schema of a specific layer in the GeoPackage.

        Parameters
        ----------
        layer_name : str
            Name of the layer.

        Returns
        -------
        dict
            The schema of the layer.
        """

        f = fiona.open(self.file_path, "r", layer=layer_name)
        return f.schema

    def get_schema(self):
        """Get the schema of all layers in the GeoPackage.

        Returns
        -------
        OrderedDict
            Dictionary with layer names as keys and their schemas as values.
        """

        layers = fiona.listlayers(self.file_path)
        schema = OrderedDict()
        for layer_name in layers:
            schema[layer_name] = self.get_schema_layer(layer_name)
        return schema

    def dump_schema(self):
        """Dump the schema of the GeoPackage to a JSON file.

        Returns
        -------
        None
        """
        file_path = os.path.join(self.output_path, "schema.json")
        cd = FileChangeDetection(file_path)

        schema = self.get_schema()
        with file_path.open("w") as fp:
            json.dump(schema, fp, indent=2)

        if cd.has_changed():
            self.changed_files.append(file_path)

    def dump_layers(self, reformat=True):
        """Dump all layers and features of the GeoPackage to GeoJSON files.

        Parameters
        ----------
        reformat : bool, optional
            Whether to reformat the output JSON for readability (default is True).

        Returns
        -------
        None
        """

        layers = fiona.listlayers(self.file_path)

        for layer_name in layers:
            log.info("dump layer %s", layer_name)

            layer = fiona.open(self.file_path, "r", layer=layer_name)
            output_file_path = os.path.join(self.output_path, f"{layer.name}.geojson")

            cd = FileChangeDetection(output_file_path)

            with SubTimer(f"dump layer {layer_name}"):
                # make sure th fid is copied too (fiona does not do this by default)
                schema = layer.schema
                schema["properties"]["fid"] = "int"
                dest_src = fiona.open(
                    output_file_path,
                    "w",
                    driver="GeoJSON",
                    crs=layer.crs,
                    schema=schema,
                    COORDINATE_PRECISION=6,
                    id_field="fid",
                )

                for feature in layer:
                    feature["properties"]["fid"] = feature["id"]
                    dest_src.write(feature)

                dest_src.close()

            # reformat json is experiment to check what is most useful for git diff
            if reformat:
                with SubTimer(f"reformat json {layer_name}"):
                    f = output_file_path.open("r")
                    data = json.load(f)
                    f.close()
                    if len(data.get("features", [])) == 0:
                        output_file_path.unlink()  # remove empty files
                    else:
                        f = output_file_path.open("w")
                        f.write(format_json(data))
                        f.close()

            if cd.has_changed():
                self.changed_files.append(output_file_path)

    def dump(self):
        """Dump the GeoPackage schema and layers to files.

        Returns
        -------
        None
        """
        self.dump_schema()
        self.dump_layers()
