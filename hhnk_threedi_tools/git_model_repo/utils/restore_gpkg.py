import json
from pathlib import Path
import fiona


class GeoPackageRestore(object):
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

    def __init__(self, gpkg_path: Path, output_file_path: Path = None):
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

    def restore_layers(self):
        """Restore all layers from GeoJSON files into the GeoPackage.

        Returns
        -------
        None
        """
        schema = self.read_schema()
        for layer_name, layer_schema in schema.items():
            geojson_file = self.gpkg_path / f"{layer_name}.geojson"
            layer = fiona.open(geojson_file.as_posix(), "r", layer=layer_name)

            # Ensure the fid is copied too (fiona does not do this by default)
            layer_schema["properties"]["fid"] = "int"
            dest_src = fiona.open(
                self.output_file_path.as_posix(),
                "w",
                driver="GPKG",
                crs=layer.crs,
                schema=layer_schema,
                layer=layer_name,
                FID="fid",
                overwrite=True,
            )

            for feature in layer:
                feature["properties"]["fid"] = int(feature["id"])
                dest_src.write(feature)
            dest_src.close()
            layer.close()

    def restore(self):
        """
        Restore the GeoPackage from directory with schema and GeoJSON files.

        Returns
        -------
        None
        """
        self.restore_layers()
