import json
import os
from collections import OrderedDict

import fiona

from utils.file_change_detection import FileChangeDetection


def format_json(obj, parent_key='', depth=0):
    """Formatteer een JSON-object met een maximale diepte van 2, voor betere leesbaarheid in git diff."""
    INDENT = ' '  # Twee spaties per indentatieniveau

    if not isinstance(obj, (dict, list)) or depth > 2 or parent_key in ['crs']:
        return json.dumps(obj)

    if isinstance(obj, list):
        formatted_items = [format_json(item, '', depth + 1) for item in obj]
        return '[\n' + ',\n'.join(INDENT * (depth + 1) + item for item in formatted_items) + '\n' + INDENT * depth + ']'

    if isinstance(obj, dict):
        formatted_items = [f'"{key}": {format_json(value, key, depth + 1)}' for key, value in obj.items()]
        return '{\n' + ',\n'.join(INDENT * (depth + 1) + item for item in formatted_items) + '\n' + INDENT * depth + '}'


class GeoPackageDump(object):
    """ basic version using Fiona"""

    def __init__(self, file_path, output_path=None):
        self.file_path = file_path

        if output_path is None:
            base = os.path.splitext(os.path.basename(file_path))
            output_path = os.path.join(
                os.path.dirname(file_path),
                f"{base[0]}_{base[1]}"
            )
        self.output_path = output_path
        os.makedirs(self.output_path, exist_ok=True)

        self.changed_files = []

    def get_schema_layer(self, layer_name):
        """get schema of the geopackage datamodel as dictionary.
        """

        f = fiona.open(self.file_path, 'r', layer=layer_name)
        return f.schema

    def get_schema(self):
        """get schema of the geopackage datamodel as dictionary.
        """

        layers = fiona.listlayers(self.file_path)
        schema = OrderedDict()
        for layer_name in layers:
            schema[layer_name] = self.get_schema_layer(layer_name)
        return schema

    def dump_schema(self):
        """Dump the schema of the geopackage datamodel to a json file.
        """
        file_path = os.path.join(self.output_path, 'schema.json')
        cd = FileChangeDetection(file_path)

        schema = self.get_schema()
        with open(os.path.join(self.output_path, 'schema.json'), 'w') as fp:
            json.dump(schema, fp, indent=2)

        if cd.has_changed():
            self.changed_files.append(file_path)

    def dump_layers(self, reformat=True):
        """Dump the layers and features of the geopackage to a Geojson file.
        """

        layers = fiona.listlayers(self.file_path)

        for layer_name in layers:

            layer = fiona.open(self.file_path, 'r', layer=layer_name)
            output_file_path = os.path.join(self.output_path, f"{layer.name}.geojson")

            cd = FileChangeDetection(output_file_path)

            # make sure th fid is copied too (fiona does not do this by default)
            schema = layer.schema
            schema['properties']['fid'] = 'int'
            dest_src = fiona.open(
                output_file_path,
                'w',
                driver='GeoJSON',
                crs=layer.crs,
                schema=schema,
                COORDINATE_PRECISION=6,
                id_field='fid',
            )

            for feature in layer:
                feature['properties']['fid'] = feature['id']
                dest_src.write(feature)

            dest_src.close()

            # reformat json is experiment to check what is most useful for git diff
            if reformat:
                f = open(output_file_path, 'r')
                data = json.load(f)
                f.close()
                f = open(output_file_path, 'w')
                f.write(format_json(data))
                f.close()

            if cd.has_changed():
                self.changed_files.append(output_file_path)

    def dump(self):
        """Dump the geopackage to a Geojson file.
        """
        self.dump_schema()
        self.dump_layers()
