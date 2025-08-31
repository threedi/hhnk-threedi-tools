import json
import logging
import typing
from collections import OrderedDict
from pathlib import Path

import geopandas as gpd
import pyogrio

from hhnk_threedi_tools.git_model_repo.utils.file_change_detection import FileChangeDetection
from hhnk_threedi_tools.git_model_repo.utils.timer_log import SubTimer

logger = logging.getLogger(__name__)


def format_json(
    obj: typing.Any,
    parent_key: str = "",
    depth: int = 0,
) -> str:
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
    indent = " "
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


class GeoPackageDump:
    """Basic version using GeoPandas and Pyogrio for dumping GeoPackage files.

    Attributes
    ----------
    file_path : Path
        Path to the GeoPackage file.
    output_path : Path
        Path to the output directory.
    changed_files : list of Path
        List of files that have changed after dumping.
    """

    def __init__(self, file_path: Path, output_path: typing.Optional[Path] = None) -> None:
        """Initialize the GeoPackageDump object.

        Parameters
        ----------
        file_path : Path
            Path to the GeoPackage file.
        output_path : Path, optional
            Path to the output directory. If None, a default directory is created.
        """
        self.file_path = file_path

        if output_path is None:
            base = file_path.stem
            output_path = file_path.parent / f"{base}_output"
        self.output_path = output_path
        self.output_path.mkdir(exist_ok=True)

        self.changed_files = []

    def get_schema_layer(self, layer_name: str) -> dict:
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
        df = pyogrio.read_info(str(self.file_path), layer=layer_name)
        schema = {
            "geometry": df["geometry_type"],
            "properties": {field["name"]: field["type"] for field in df["fields"]},
        }
        return schema

    def get_schema(self) -> OrderedDict:
        """Get the schema of all layers in the GeoPackage.

        Returns
        -------
        OrderedDict
            Dictionary with layer names as keys and their schemas as values.
        """
        layers = pyogrio.list_layers(str(self.file_path))
        schema = OrderedDict()
        for layer in layers:
            schema[layer["name"]] = self.get_schema_layer(layer["name"])
        return schema

    def dump_schema(self) -> None:
        """Dump the schema of the GeoPackage to a JSON file.

        Returns
        -------
        None
        """
        file_path = self.output_path / "schema.json"
        cd = FileChangeDetection(file_path)

        schema = self.get_schema()
        with file_path.open("w") as fp:
            json.dump(schema, fp, indent=2)

        if cd.has_changed():
            self.changed_files.append(file_path)

    def dump_layers(self, reformat: bool = True) -> None:
        """Dump all layers and features of the GeoPackage to GeoJSON files.

        Parameters
        ----------
        reformat : bool, optional
            Whether to reformat the output JSON for readability (default is True).

        Returns
        -------
        None
        """
        layers = pyogrio.list_layers(str(self.file_path))

        for layer in layers:
            layer_name = layer["name"]
            logger.info("dump layer %s", layer_name)

            with SubTimer(f"dump layer {layer_name}"):
                gdf = gpd.read_file(self.file_path, layer=layer_name)
                # Add fid column for compatibility
                gdf["fid"] = gdf.index.astype(int)
                output_file_path = self.output_path / f"{layer_name}.geojson"
                cd = FileChangeDetection(output_file_path)
                # Set coordinate precision to 6 using GeoPandas to_file options
                gdf.to_file(output_file_path, driver="GeoJSON", driver_options={"COORDINATE_PRECISION": 6})

            if reformat:
                with SubTimer(f"reformat json {layer_name}"):
                    with output_file_path.open("r") as f:
                        data = json.load(f)
                    if len(data.get("features", [])) == 0:
                        output_file_path.unlink()
                    else:
                        with output_file_path.open("w") as f:
                            f.write(format_json(data))

            if cd.has_changed():
                self.changed_files.append(output_file_path)

    def dump(self) -> None:
        """Dump the GeoPackage schema and layers to files.

        Returns
        -------
        None
        """
        self.dump_schema()
        self.dump_layers()
