import os

import fiona
from osgeo import ogr

from hhnk_threedi_tools.git_model_repo.utils.dump_gpkg import GeoPackageDump
from hhnk_threedi_tools.git_model_repo.utils.restore_gpkg import GeoPackageRestore
from tests.tests_git_model_repo.get_local_output_dir_for_development import get_local_development_output_dir

""" 
important for dump and restore (checked in tests):
- layers, layer names and multiple layers in one gpkg
- field and field definitions
- geometry type and crs
- FID
- overwrite
"""

dir = get_local_development_output_dir(clean=True)


class TestDumpAndRestoreGeopackage:
    test_fields_gpkg = os.path.join(os.path.dirname(__file__), "data", "test_geopackage.gpkg")
    test_multilayer_gpgk = os.path.join(os.path.dirname(__file__), "data", "test_geopackage_with_multiple_layers.gpkg")

    test_fields_geojson_dir = os.path.join(os.path.dirname(__file__), "data", "test_fields")
    test_multilayer_geojson_dir = os.path.join(os.path.dirname(__file__), "data", "test_multilayer")

    def test_dump_schema_single_layer(self, tmp_path):
        tmp_path = dir

        dumper = GeoPackageDump(self.test_fields_gpkg, tmp_path)
        dumper.dump_schema()

        assert os.path.exists(os.path.join(tmp_path, "schema.json"))

    def test_dump_schema_multilayer(self, tmp_path):
        tmp_path = dir

        dumper = GeoPackageDump(self.test_multilayer_gpgk, tmp_path)
        dumper.dump_schema()

        assert os.path.exists(os.path.join(tmp_path, "schema.json"))

    def test_dump_layers_single_layer(self, tmp_path):
        tmp_path = dir

        dumper = GeoPackageDump(self.test_fields_gpkg, tmp_path)
        dumper.dump_layers()

        assert os.path.exists(os.path.join(tmp_path, "test_geopackage.geojson"))

    def test_dump_layers_multilayer(self, tmp_path):
        tmp_path = dir

        dumper = GeoPackageDump(self.test_multilayer_gpgk, tmp_path)
        dumper.dump_layers()

        assert os.path.exists(os.path.join(tmp_path, "points.geojson"))
        assert os.path.exists(os.path.join(tmp_path, "lines.geojson"))
        assert os.path.exists(os.path.join(tmp_path, "multi_polygons.geojson"))
        assert os.path.exists(os.path.join(tmp_path, "no_geom.geojson"))

    def test_restore_check_fields(self, tmp_path):
        tmp_path = dir

        tmp_file_path = os.path.join(tmp_path, "test_geopackage_restored.gpkg")
        os.makedirs(tmp_path, exist_ok=True)

        restorer = GeoPackageRestore(self.test_fields_geojson_dir, tmp_file_path)
        restorer.restore_layers()

        assert os.path.exists(tmp_file_path)

        source = ogr.Open(tmp_file_path, 0)
        layers = [l for l in source]
        layer = layers[0]
        # field definitions
        layer_def = layer.GetLayerDefn()

        assert source.GetLayerCount() == 1
        assert layer.GetFeatureCount() == 2
        assert layer.GetName() == "test_geopackage"
        assert layer.GetGeomType() == ogr.wkbPoint
        assert layer.GetLayerDefn().GetFieldCount() == 4

        date_field = layer_def.GetFieldDefn(layer_def.GetFieldIndex("last_modified"))
        assert date_field.GetType() == ogr.OFTDateTime
        float_field = layer_def.GetFieldDefn(layer_def.GetFieldIndex("water_level"))
        assert float_field.GetType() == ogr.OFTReal
        int_field = layer_def.GetFieldDefn(layer_def.GetFieldIndex("id"))
        assert int_field.GetType() == ogr.OFTInteger64
        string_field = layer_def.GetFieldDefn(layer_def.GetFieldIndex("code"))
        assert string_field.GetType() == ogr.OFTString
        assert string_field.GetWidth() == 12

        slayer = fiona.open(self.test_fields_gpkg, "r")
        ref = layer.GetSpatialRef()
        assert ref.ExportToWkt() == slayer.crs_wkt

    def test_restore_check_multilayer(self, tmp_path):
        tmp_path = dir

        tmp_file_path = os.path.join(tmp_path, "test_multi_restored.gpkg")
        os.makedirs(tmp_path, exist_ok=True)

        restorer = GeoPackageRestore(self.test_multilayer_geojson_dir, tmp_file_path)
        restorer.restore_layers()

        assert os.path.exists(tmp_file_path)

        source = ogr.Open(tmp_file_path, 0)
        layers = {l.GetName(): l for l in source}

        assert source.GetLayerCount() == 4
        assert layers["points"].GetFeatureCount() == 4
        assert layers["lines"].GetFeatureCount() == 2
        assert layers["multi_polygons"].GetFeatureCount() == 1
        assert layers["no_geom"].GetFeatureCount() == 2

        assert layers["points"].GetGeomType() == ogr.wkbPoint

        # test FID restore, the key
        features = [f.GetFID() for f in layers["points"]]

        assert features == [1, 4, 9, 10]

    def test_restore_overwrite(self, tmp_path):
        tmp_path = dir

        tmp_file_path = os.path.join(tmp_path, "test_overwrite_restored.gpkg")
        os.makedirs(tmp_path, exist_ok=True)

        # make initial fill
        slayer = fiona.open(self.test_fields_gpkg, "r")

        dest_src = fiona.open(
            tmp_file_path,
            "w",
            driver="GPKG",
            crs=slayer.crs,
            schema={
                "properties": {
                    "id": "int",
                    "fid": "int",
                },
                "geometry": "Point",
            },
            layer="test_exra",
            FID="fid",
            overwrite=True,
        )
        dest_src.close()

        dest_src = fiona.open(
            tmp_file_path,
            "w",
            driver="GPKG",
            crs=slayer.crs,
            schema={
                "properties": {
                    "id": "int",
                    "fid": "int",
                },
                "geometry": "Point",
            },
            layer="test_geopackage",
            FID="fid",
            overwrite=True,
        )

        for i in range(10):
            dest_src.write(
                {
                    "geometry": {"type": "Point", "coordinates": [5.0, 52.0]},
                    "properties": {
                        "id": i,
                        "fid": i,
                    },
                }
            )
        dest_src.close()

        # test initial fill to compare with
        source = ogr.Open(tmp_file_path, 0)
        layer = source.GetLayer("test_geopackage")

        assert source.GetLayerCount() == 2
        assert layer.GetFeatureCount() == 10
        assert layer.GetLayerDefn().GetFieldCount() == 1

        # overwrite with restore
        restorer = GeoPackageRestore(self.test_fields_geojson_dir, tmp_file_path)
        restorer.restore_layers()

        source = ogr.Open(tmp_file_path, 0)
        layer = source.GetLayer("test_geopackage")

        assert source.GetLayerCount() == 2
        assert layer.GetFeatureCount() == 2
        assert layer.GetLayerDefn().GetFieldCount() == 4
