# %%
import importlib
import shutil
import sys
from pathlib import Path

sys.path.insert(0, r"C:\Users\jacostabarragan\AppData\Roaming\3Di\QGIS3\profiles\default\python\plugins")


# %%
def force_3di_qgis_plugin_paths() -> None:
    """
    Force Python to use the 3Di/QGIS plugin packages instead of the packages
    from the normal Pixi/conda environment.

    This is needed because the normal environment may use threedi_schema 0.300.x,
    while the 3Di QGIS plugin uses threedi_schema 0.219.3.
    """

    qgis_plugins_root = (
        Path.home() / "AppData" / "Roaming" / "3Di" / "QGIS3" / "profiles" / "default" / "python" / "plugins"
    )

    threedi_results_deps = qgis_plugins_root / "threedi_results_analysis" / "deps"

    if not qgis_plugins_root.exists():
        raise FileNotFoundError(f"QGIS plugins folder not found: {qgis_plugins_root}")

    if not threedi_results_deps.exists():
        raise FileNotFoundError(f"3Di plugin deps folder not found: {threedi_results_deps}")

    # Put these paths at the beginning of sys.path.
    # Order matters: deps first, plugin root second.
    sys.path.insert(0, str(qgis_plugins_root))
    sys.path.insert(0, str(threedi_results_deps))


force_3di_qgis_plugin_paths()

import threedi_schema
from threedi_schema import ThreediDatabase

# %%
from threedi_schematisation_editor import datamodel as dm
from threedi_schematisation_editor.conversion import ModelDataConverter

print("Using threedi_schema from:")
print(threedi_schema.__file__)
print("threedi_schema version:")
print(threedi_schema.__version__)


# %%
sqlite_path = Path(
    r"\\corp.hhnk.nl\data\Hydrologen_data\Data\personen\jacosta\update_3di_model_test\test_model\02_schematisation\00_basis\bwn_zijpe-west.sqlite"
)

dst_gpkg = sqlite_path.with_suffix(".gpkg")

hdb_path = Path(r"H:\01.basisgegevens\00.HDB\Hydro_database.gpkg")

datachecker_path = Path(
    r"\\corp.hhnk.nl\data\Hydrologen_data\Data\personen\jacosta\update_3di_model_test\test_model\01_source_data\datachecker.gpkg"
)

polder_polygon_path = Path(
    r"\\corp.hhnk.nl\data\Hydrologen_data\Data\personen\jacosta\update_3di_model_test\test_model\01_source_data\polder_polygon.gpkg"
)


# ============================================================
# 4. UPGRADE SQLITE TO THE PLUGIN SUPPORTED VERSION
# ============================================================


def upgrade_sqlite_with_qgis_schema(sqlite_path: Path) -> None:
    """
    Upgrade the SQLite using the threedi_schema version from the 3Di/QGIS plugin.

    Do not use revision='0219' here. In the QGIS plugin environment,
    upgrade() uses the package head version, which is the version we need.
    """

    db = ThreediDatabase(sqlite_path)

    version_before = db.schema.get_version()
    print(f"Schema before upgrade: {version_before}")

    db.schema.upgrade(backup=True)

    version_after = db.schema.get_version()
    print(f"Schema after upgrade: {version_after}")


# ============================================================
# 5. CONVERT SQLITE 219 TO GEOPACKAGE 219
# ============================================================


def sqlite_to_geopackage(sqlite_path: Path, gpkg_path: Path) -> Path:
    """
    Convert upgraded SQLite/Spatialite model to GeoPackage using
    the 3Di schematisation editor converter.
    """

    if gpkg_path.exists():
        gpkg_path.unlink()

    converter = ModelDataConverter(
        src_sqlite=str(sqlite_path),
        dst_gpkg=str(gpkg_path),
    )

    print(
        "Spatialite version used by converter:",
        converter.spatialite_schema_version(str(sqlite_path)),
    )

    converter.set_epsg_from_sqlite()
    converter.create_empty_user_layers(overwrite=True)
    converter.import_all_model_data()
    converter.report_conversion_errors()

    print("GeoPackage created:")
    print(gpkg_path)

    return gpkg_path


# ============================================================
# 6. RUN YOUR IMPERVIOUS SURFACE SCRIPT ON THE GEOPACKAGE
# ============================================================


def run_impervious_surface_script_on_gpkg(
    gpkg_path: Path,
    hdb_path: Path,
    datachecker_path: Path,
    polder_polygon_path: Path,
) -> None:
    """Run your impervious surface logic on the GeoPackage."""

    # Example:
    from hhnk_threedi_tools.gpkg_builder import create_impervious_surface
    #     create_surface_layer,
    #     get_percentage_afvoernorm,
    #     create_surface_map_layer,
    # )

    # Temporary placeholder if you run this from the same file:
    # createa_voronoi_polygons = ...
    # create_surface_layer = ...
    # get_percentage_afvoernorm = ...
    # create_surface_map_layer = ...

    subcatchments = create_impervious_surface.createa_voronoi_polygons(
        model_path_gpkg=gpkg_path,
        datacheker_path=datachecker_path,
        polder_polygon_path=polder_polygon_path,
    )

    surfaces = create_surface_layer(
        subcatchments=subcatchments,
        impervious_out_polygon_gpkg=gpkg_path,
    )

    percentage_by_surface = get_percentage_afvoernorm(
        hdb_path=hdb_path,
        surfaces=surfaces,
    )

    surface_map = create_surface_map_layer(
        model_path_gpkg=gpkg_path,
        surfaces=surfaces,
        percentage_by_surface=percentage_by_surface,
        impervious_out_line_gpkg=gpkg_path,
    )

    # Write updated layers to GeoPackage.
    # These are the GeoPackage user layer names.
    surfaces.to_file(
        gpkg_path,
        layer="impervious_surface",
        driver="GPKG",
    )

    surface_map.to_file(
        gpkg_path,
        layer="impervious_surface_map",
        driver="GPKG",
    )

    print("Impervious layers written to GeoPackage:")
    print(gpkg_path)


# ============================================================
# 7. EXPORT ONLY IMPERVIOUS LAYERS BACK TO SQLITE
# ============================================================


def export_impervious_layers_back_to_sqlite(sqlite_path: Path, gpkg_path: Path) -> None:
    """
    Export only the modified impervious layers from GeoPackage back to SQLite.

    This avoids exporting the whole model back and reduces the risk of
    accidentally changing unrelated layers.
    """

    converter = ModelDataConverter(
        src_sqlite=str(sqlite_path),
        dst_gpkg=str(gpkg_path),
    )

    converter.export_model_data(dm.ImperviousSurface)
    converter.export_model_data(dm.ImperviousSurfaceMap)
    converter.report_conversion_errors()

    print("Impervious layers exported back to SQLite:")
    print(sqlite_path)


# ============================================================
# 8. FULL WORKFLOW
# ============================================================


def run_full_workflow(
    src_sqlite: Path,
    dst_gpkg: Path,
    hdb_path: Path,
    datachecker_path: Path,
    polder_polygon_path: Path,
) -> None:
    """
    Full workflow:

    1. Backup SQLite.
    2. Upgrade SQLite with QGIS/3Di plugin threedi_schema.
    3. Convert SQLite to GeoPackage.
    4. Run impervious surface script on GeoPackage.
    5. Export impervious layers back to SQLite.
    """

    src_sqlite = Path(src_sqlite)
    dst_gpkg = Path(dst_gpkg)

    backup_path = src_sqlite.with_name(src_sqlite.stem + "_backup_before_impervious.sqlite")
    shutil.copy2(src_sqlite, backup_path)
    print("Backup created:")
    print(backup_path)

    upgrade_sqlite_with_qgis_schema(src_sqlite)

    sqlite_to_geopackage(src_sqlite, dst_gpkg)

    run_impervious_surface_script_on_gpkg(
        gpkg_path=dst_gpkg,
        hdb_path=hdb_path,
        datachecker_path=datachecker_path,
        polder_polygon_path=polder_polygon_path,
    )

    export_impervious_layers_back_to_sqlite(
        sqlite_path=src_sqlite,
        gpkg_path=dst_gpkg,
    )

    print("Workflow finished successfully.")


if __name__ == "__main__":
    run_full_workflow(
        src_sqlite=src_sqlite,
        dst_gpkg=dst_gpkg,
        hdb_path=hdb_path,
        datachecker_path=datachecker_path,
        polder_polygon_path=polder_polygon_path,
    )
