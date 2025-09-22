import json
import typing
from pathlib import Path

import geopandas as gpd


class GeoPackageRestore:
    """Restore a GeoPackage from schema and GeoJSON files.

    Parameters
    ----------
    gpkg_path : Path
        Path to the folder containing the GeoJSON files and the schema.json file.
    output_file_path : Path, optional
        Path to the output GeoPackage file. If None, the output file will be stored
        in the parent directory of the input directory.

    Attributes
    ----------
    gpkg_path : Path
        Path to the input folder.
    output_file_path : Path
        Path to the output GeoPackage file.

    Methods
    -------
    read_schema()
        Read the schema from the schema.json file.
    restore_layers()
        Restore all layers from GeoJSON files into the GeoPackage.
    restore()
        Restore the GeoPackage from the directory.
    """

    def __init__(self, gpkg_path: Path, output_file_path: typing.Optional[Path] = None) -> None:
        """Initialize the GeoPackageRestore object.

        Parameters
        ----------
        gpkg_path : Path
            Path to the folder containing the GeoJSON files and the schema.json file.
        output_file_path : Path, optional
            Path to the output GeoPackage file. If None, the output file will be stored
            in the parent directory of the input directory.
        """
        self.gpkg_path = Path(gpkg_path)
        if output_file_path is None:
            filename = self.gpkg_path.name.rstrip("_gpkg") + "_restored.gpkg"
            output_file_path = self.gpkg_path.parent.parent / filename
        self.output_file_path = Path(output_file_path)

    def read_schema(self) -> dict:
        """Read the schema from the schema.json file.

        Returns
        -------
        dict
            The schema dictionary loaded from schema.json.
        """
        schema_file = self.gpkg_path / "schema.json"
        with schema_file.open("r") as f:
            return json.load(f)

    def restore_layers(self) -> None:
        """Restore all layers from GeoJSON files into the GeoPackage.

        Returns
        -------
        None
        """
        schema = self.read_schema()
        for layer_name in schema.keys():
            geojson_file = self.gpkg_path / f"{layer_name}.geojson"
            if not geojson_file.exists():
                continue
            gdf = gpd.read_file(geojson_file).astype(schema[layer_name]["properties"])
            # Write to GeoPackage, append if file exists, otherwise create
            gdf.to_file(self.output_file_path, layer=layer_name, driver="GPKG", index=False)

    def restore(self) -> None:
        """Restore the GeoPackage from directory with schema and GeoJSON files.

        Returns
        -------
        None
        """
        self.restore_layers()
