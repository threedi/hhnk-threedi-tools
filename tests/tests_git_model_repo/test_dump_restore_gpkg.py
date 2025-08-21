from pathlib import Path

import fiona
from osgeo import ogr

from hhnk_threedi_tools.git_model_repo.utils.dump_gpkg import GeoPackageDump
from hhnk_threedi_tools.git_model_repo.utils.restore_gpkg import GeoPackageRestore
from tests.tests_git_model_repo.helpers import get_local_development_output_dir

""" 
Note
----
important for dump and restore (checked in tests):
- layers, layer names and multiple layers in one gpkg
- field and field definitions
- geometry type and crs
- FID
- overwrite
"""

dir = get_local_development_output_dir(clean=True)


class TestDumpAndRestoreGeopackage:
    test_fields_gpkg = Path(__file__).parent / "data" / "test_geopackage.gpkg"
    test_multilayer_gpgk = Path(__file__).parent / "data" / "test_geopackage_with_multiple_layers.gpkg"

    test_fields_geojson_dir = Path(__file__).parent / "data" / "test_fields"
    test_multilayer_geojson_dir = Path(__file__).parent / "data" / "test_multilayer"

    def test_dump_schema_single_layer(self, tmp_path):
        tmp_path = Path(dir)

        dumper = GeoPackageDump(self.test_fields_gpkg, tmp_path)
        dumper.dump_schema()

        assert (tmp_path / "schema.json").exists()

    def test_dump_schema_multilayer(self, tmp_path):
        tmp_path = Path(dir)

        dumper = GeoPackageDump(self.test_multilayer_gpgk, tmp_path)
        dumper.dump_schema()

        assert (tmp_path / "schema.json").exists()

    def test_dump_layers_single_layer(self, tmp_path):
        tmp_path = Path(dir)

        dumper = GeoPackageDump(self.test_fields_gpkg, tmp_path)
        dumper.dump_layers()

        assert (tmp_path / "test_geopackage.geojson").exists()

    def test_dump_layers_multilayer(self, tmp_path):
        tmp_path = Path(dir)

        dumper = GeoPackageDump(self.test_multilayer_gpgk, tmp_path)
        dumper.dump_layers()

        assert (tmp_path / "points.geojson").exists()
        assert (tmp_path / "lines.geojson").exists()
        assert (tmp_path / "multi_polygons.geojson").exists()
        assert (tmp_path / "no_geom.geojson").exists()

    def test_restore_check_fields(self, tmp_path):
        tmp_path = Path(dir)

        tmp_file_path = tmp_path / "test_geopackage_restored.gpkg"
        tmp_path.mkdir(parents=True, exist_ok=True)

        restorer = GeoPackageRestore(self.test_fields_geojson_dir, tmp_file_path)
        restorer.restore_layers()

        assert tmp_file_path.exists()

        source = ogr.Open(str(tmp_file_path), 0)
        layers = [l for l in source]
        layer = layers[0]
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

        slayer = fiona.open(str(self.test_fields_gpkg), "r")
        ref = layer.GetSpatialRef()
        assert ref.ExportToWkt() == slayer.crs_wkt

    def test_restore_check_multilayer(self, tmp_path):
        tmp_path = Path(dir)

        tmp_file_path = tmp_path / "test_multi_restored.gpkg"
        tmp_path.mkdir(parents=True, exist_ok=True)

        restorer = GeoPackageRestore(self.test_multilayer_geojson_dir, tmp_file_path)
        restorer.restore_layers()

        assert tmp_file_path.exists()

        source = ogr.Open(str(tmp_file_path), 0)
        layers = {l.GetName(): l for l in source}

        assert source.GetLayerCount() == 4
        assert layers["points"].GetFeatureCount() == 4
        assert layers["lines"].GetFeatureCount() == 2
        assert layers["multi_polygons"].GetFeatureCount() == 1
        assert layers["no_geom"].GetFeatureCount() == 2

        assert layers["points"].GetGeomType() == ogr.wkbPoint

        features = [f.GetFID() for f in layers["points"]]
        assert features == [1, 4, 9, 10]

    def test_restore_overwrite(self, tmp_path):
        tmp_path = Path(dir)

        tmp_file_path = tmp_path / "test_overwrite_restored.gpkg"
        tmp_path.mkdir(parents=True, exist_ok=True)

        slayer = fiona.open(str(self.test_fields_gpkg), "r")

        dest_src = fiona.open(
            str(tmp_file_path),
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
            str(tmp_file_path),
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

        source = ogr.Open(str(tmp_file_path), 0)
        layer = source.GetLayer("test_geopackage")

        assert source.GetLayerCount() == 2
        assert layer.GetFeatureCount() == 10
        assert layer.GetLayerDefn().GetFieldCount() == 1

        restorer = GeoPackageRestore(self.test_fields_geojson_dir, tmp_file_path)
        restorer.restore_layers()

        source = ogr.Open(str(tmp_file_path), 0)
        layer = source.GetLayer("test_geopackage")

        assert source.GetLayerCount() == 2
        assert layer.GetFeatureCount() == 2
        assert layer.GetLayerDefn().GetFieldCount() == 4
