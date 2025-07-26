import json
import os

import fiona


class GeoPackageRestore(object):
    def __init__(self, gpkg_path, output_file_path=None):
        """Restore a GeoPackage from the schema and geojson dump.
        :param gpkg_path: path to the folder containing the geojson files and the schema.json file
        :param output_file_path: path to the output gpkg file. If None, the output file will be stored in the same
                                 directory as the parent of the input directory.
                                 This parameter is especially usefull for testing
        """
        self.gpkg_path = gpkg_path
        if output_file_path is None:
            filename = os.path.basename(gpkg_path.rtrim("_gpkg")) + "_restored.gpkg"
            output_file_path = os.path.join(os.path.dirname(gpkg_path), os.pardir, filename)

        self.output_file_path = output_file_path

    def read_schema(self):
        """Restore a GeoPackage from a backup."""
        return json.load(open(os.path.join(self.gpkg_path, "schema.json")))

    def restore_layers(self):
        schema = self.read_schema()

        for layer_name, layer_schema in schema.items():
            layer = fiona.open(os.path.join(self.gpkg_path, layer_name + ".geojson"), "r", layer=layer_name)

            # make sure th fid is copied too (fiona does not do this by default)
            layer_schema["properties"]["fid"] = "int"
            dest_src = fiona.open(
                self.output_file_path,
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
        """Restore a GeoPackage from directory with schema and geojson files."""
        self.restore_layers()
