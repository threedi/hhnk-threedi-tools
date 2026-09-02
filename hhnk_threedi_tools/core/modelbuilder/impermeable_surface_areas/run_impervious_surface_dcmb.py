# %%
"""
Runner for the Datachecker ModelBuilder workflow.

This script:
1. Initializes the 3Di/QGIS dependencies.
2. Converts the 3Di SQLite to a GeoPackage.
3. Creates impervious_surface and impervious_surface_map layers.
4. Exports those layers back to the original SQLite.

For direct 3Di tooling, use create_impervious_surface.run() instead.
"""

import importlib
import sqlite3
import sys
from pathlib import Path

from core.modelbuilder.impermeable_surface_areas import create_impervious_surface

# Force 3Di/QGIS plugin paths
QGIS_PLUGIN_DIR = Path.home() / "AppData" / "Roaming" / "3Di" / "QGIS3" / "profiles" / "default" / "python" / "plugins"

THREEDI_DEPS = QGIS_PLUGIN_DIR / "threedi_results_analysis" / "deps"

sys.path.insert(0, str(QGIS_PLUGIN_DIR))
sys.path.insert(0, str(THREEDI_DEPS))

importlib.invalidate_caches()

# Initialize QGIS application

from qgis.core import QgsApplication

QGIS_PREFIX_PATH = Path(r"C:\Program Files\3DiModellerInterface 3.34\apps\qgis-ltr")

QgsApplication.setPrefixPath(str(QGIS_PREFIX_PATH), True)

QGIS_APP = QgsApplication.instance()

if QGIS_APP is None:
    QGIS_APP = QgsApplication([], True)
    QGIS_APP.initQgis()

print("QGIS initialized with prefix:")
print(QGIS_PREFIX_PATH)

# Import 3Di/QGIS plugin packages

import threedi_schema

print("threedi_schema loaded from:")
print(threedi_schema.__file__)
print("threedi_schema version:")
print(threedi_schema.__version__)

from threedi_schema import ThreediDatabase
from threedi_schematisation_editor import data_models as dm
from threedi_schematisation_editor.conversion import ModelDataConverter


def clear_existing_impervious_layers_in_sqlite(sqlite_path: Path) -> None:
    """
    Remove existing impervious surface data from the SQLite before exporting
    the new generated layers from the GeoPackage.

    Order matters:
    1. v2_impervious_surface_map
    2. v2_impervious_surface
    """

    print("Clearing existing impervious layers in SQLite...")
    # connect to the SQLite database and execute the delete statements
    with sqlite3.connect(str(sqlite_path)) as conn:
        conn.execute("PRAGMA foreign_keys = OFF;")

        for table_name in [
            "v2_impervious_surface_map",
            "v2_impervious_surface",
        ]:
            # Get the count of records before deletion
            count_before = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]

            print(f"{table_name}: {count_before} records before delete")

            # Execute the delete statement
            conn.execute(f'DELETE FROM "{table_name}";')

            # Get the count of records after deletion
            count_after = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]

            print(f"{table_name}: {count_after} records after delete")

        # Commit the changes to the database
        conn.commit()

    print("Existing impervious layers cleared.")


# SQLite to GeoPackage
def sqlite_to_gpkg_219(sqlite_path: Path) -> Path:
    """
    Create de geopackge from the sqlite to gpkg. It uses the sqlite path as input.
    This step is needed to later on use connection nodes  as input and also to set the
    output as is expected in the geopackge.
    """
    # set the coordinate system
    EPSG_CODE = 28992
    # Set the sqplite a Threedidatabase object.
    db = ThreediDatabase(sqlite_path)

    # Check sqlite version.
    version_before = db.schema.get_version()
    print("Schema before upgrade:", version_before)

    # Control version lower than 219. Convert into interger first.
    if int(version_before) < 219:
        # updatesqlite making a back up and also upgrading he sqlite. Otherwise we get error
        db.schema.upgrade(backup=True, upgrade_spatialite_version=True)

    elif int(version_before) > 219:
        print("sqlite version higher than 219")

    # print upgraded version.
    version_after = db.schema.get_version()
    print("Schema after upgrade:", version_after)

    # copy name and location, change suffix
    gpkg_path = sqlite_path.with_suffix(".gpkg")

    # delete file if exists.
    if gpkg_path.exists():
        gpkg_path.unlink()

    # Set the converter as an object to later on import all layer into the given gpkg
    converter = ModelDataConverter(
        src_sqlite=str(sqlite_path),
        dst_gpkg=str(gpkg_path),
        epsg_code=EPSG_CODE,
    )

    # Transfer the sqlite to gpkg
    converter.create_empty_user_layers(overwrite=True)
    converter.import_all_model_data()
    converter.report_conversion_errors()

    print("GeoPackage created:")
    print(gpkg_path)

    return gpkg_path


# Create impervious layers in GeoPackage
def create_impervious_layers_in_gpkg(
    gpkg_path: Path,
    hdb_path: Path,
    datachecker_path: Path,
    polder_polygon_path: Path,
) -> None:
    """
    Create the impervious_surface and impervious_surface_map layers
    in the GeoPackage.
    """
    print("Creating impervious surface layers in GeoPackage...")
    print("GeoPackage:", gpkg_path)

    # Get connection nodes assigned to FDLA polygons
    nodes_fdla, fdla = create_impervious_surface.get_nodes_within_fdla(
        model_path_gpkg=gpkg_path,
        datacheker_path=datachecker_path,
        polder_polygon_path=polder_polygon_path,
    )

    # Create Voronoi polygons
    voronoi_cells, rows = create_impervious_surface.createa_voronoi_polygons(
        nodes_fdla=nodes_fdla,
        fdla=fdla,
    )

    # Correct orphan Voronoi polygons and create final subcatchments
    subcatchments = create_impervious_surface.correct_voronoi_polygons(
        voronoi_cells=voronoi_cells,
        rows=rows,
    )

    # Transform subcatchments into the format expected by 3Di
    surfaces = create_impervious_surface.create_surface_layer(
        subcatchments=subcatchments,
        impervious_out_polygon_gpkg=gpkg_path,
    )

    # Calculate afvoernorm percentage per surface
    percentage_by_surface = create_impervious_surface.get_percentage_afvoernorm(
        hdb_path=hdb_path,
        surfaces=surfaces,
    )

    # Create surface map lines
    surface_map = create_impervious_surface.create_surface_map_layer(
        model_path_gpkg=gpkg_path,
        surfaces=surfaces,
        percentage_by_surface=percentage_by_surface,
        impervious_out_line_gpkg=gpkg_path,
    )

    # Write impervious_surface and impervious_surface_map to the GeoPackage
    create_impervious_surface.update_model_geopackage(
        model_path_gpkg=gpkg_path,
        surfaces=surfaces,
        surface_map=surface_map,
        output_model_path=gpkg_path,
        sure_update=True,
    )

    print("Impervious layers written to GeoPackage.")


# Export impervious layers back to SQLite
def export_impervious_layers_to_sqlite(sqlite_path: Path, gpkg_path: Path) -> None:
    """
    Export the impervious surface layers from the GeoPackage back to the SQLite database.
    This function uses the ModelDataConverter to perform the export and checks for any conversion errors.
    """
    EPSG_CODE = 28992
    clear_existing_impervious_layers_in_sqlite(sqlite_path)

    converter = ModelDataConverter(
        src_sqlite=str(sqlite_path),
        dst_gpkg=str(gpkg_path),
        epsg_code=EPSG_CODE,
    )

    converter.export_model_data(dm.ImperviousSurface)
    converter.export_model_data(dm.ImperviousSurfaceMap)
    converter.report_conversion_errors()

    if any(converter.conversion_errors.values()):
        raise RuntimeError(
            "Errors occurred while exporting impervious layers back to SQLite. Check the conversion errors above."
        )

    print("Impervious layers exported back to SQLite.")


# Main
def main():
    data_path = Path(__file__).resolve().parents[4].joinpath("data")
    sqlite_path = list((data_path).rglob("*.sqlite"))[0]
    hdb_path = list((data_path).rglob("hdb.gpkg"))[0]
    datachecker_path = list((data_path).rglob("datachecker_output.gpkg"))[0]
    polder_polygon_path = list((data_path).rglob("polder_polygon.shp"))[0]

    gpkg_path = sqlite_to_gpkg_219(sqlite_path)

    create_impervious_layers_in_gpkg(
        gpkg_path=gpkg_path,
        hdb_path=hdb_path,
        datachecker_path=datachecker_path,
        polder_polygon_path=polder_polygon_path,
    )

    export_impervious_layers_to_sqlite(
        sqlite_path=sqlite_path,
        gpkg_path=gpkg_path,
    )

    print("Done:", sqlite_path)


if __name__ == "__main__":
    # This block help to cath any error in the script and print it to the log. Very Helpfull.
    try:
        main()
    except BaseException:
        import traceback

        print("\nERROR IN run_impervious_surface.py")
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)
    finally:
        if QGIS_APP is not None:
            QGIS_APP.exitQgis()
# %%
