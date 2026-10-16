# %%

# styling path comes from RANA:
# \python\plugins\threedi_results_analysis\processing\styles

import logging
import sqlite3
import sys
from pathlib import Path

sys.path.append(r"D:\github\jacostabarragan\hhnk-hydro-core\src")
from hhnk_hydro_core.files_and_folders.utils import check_create_new_file
from threedigrid.admin.exporters.geopackage import GeopackageExporter
from threedigrid_builder import make_gridadmin

logger = logging.getLogger(__name__)


# %%
class BreachModelControl:
    """
    Control and validation of breach inundation models.

    Parameters
        ----------
    schematisation_fp : Path
        Path to schematisation GeoPackage.
    dem_fp : Path
        Path to model DEM.
    output_fp : Path, optional
        Path to computational grid GeoPackage.
    overwrite : bool
        Whether existing output may be overwritten.
    """

    def __init__(
        self,
        schematisation_fp: Path,
        dem_fp: Path,
        output_fp: Path = None,
        overwrite: bool = True,
    ):
        self.schematisation_fp = Path(schematisation_fp)
        self.dem_fp = Path(dem_fp)
        self.gpkg_output: Path

        if output_fp is None:
            basepath = schematisation_fp.parents[1]
            self.gpkg_output = basepath / "computation_grid.gpkg"
        else:
            self.gpkg_output = Path(output_fp)

        self.gridadmin = self.gpkg_output.with_suffix(".h5")
        self.overwrite = overwrite

    @property
    def create(self) -> bool:
        can_create, message = check_create_new_file(path=self.gridadmin, overwrite=self.overwrite)
        if can_create:
            logger.info(message)
            return can_create
        else:
            raise FileExistsError(f"{self.gridadmin} already exists.")

    @staticmethod
    def _create_style_table(con):
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS layer_styles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                f_table_catalog TEXT,
                f_table_schema TEXT,
                f_table_name TEXT,
                f_geometry_column TEXT,
                styleName TEXT,
                styleQML TEXT,
                styleSLD TEXT,
                useAsDefault BOOLEAN,
                description TEXT,
                owner TEXT,
                ui TEXT,
                update_time DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        con.execute(
            """
            INSERT OR IGNORE INTO gpkg_contents (
                table_name,
                data_type,
                identifier,
                description,
                last_change
            )
            VALUES (
                'layer_styles',
                'attributes',
                'layer_styles',
                '',
                CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _save_style(con, layer_name, geometry_column, qml_file):
        con.execute(
            "DELETE FROM layer_styles WHERE f_table_name = ?",
            (layer_name,),
        )

        con.execute(
            """
            INSERT INTO layer_styles (
                f_table_catalog,
                f_table_schema,
                f_table_name,
                f_geometry_column,
                styleName,
                styleQML,
                useAsDefault
            )
            VALUES ('', '', ?, ?, ?, ?, 1)
            """,
            (
                layer_name,
                geometry_column,
                layer_name,
                qml_file.read_text(encoding="utf-8"),
            ),
        )

    def apply_styles(self):
        qml_dir = Path(__file__).parent / "qml_files"
        con = sqlite3.connect(self.gpkg_output)

        try:
            self._create_style_table(con)

            layers = con.execute("SELECT table_name, column_name FROM gpkg_geometry_columns").fetchall()

            for layer_name, geometry_column in layers:
                qml_file = qml_dir / f"{layer_name}.qml"

                if not qml_file.exists():
                    logger.debug(f"No QML found for {layer_name}")
                    continue

                self._save_style(
                    con=con,
                    layer_name=layer_name,
                    geometry_column=geometry_column,
                    qml_file=qml_file,
                )

            con.commit()

        except Exception:
            con.rollback()
            raise

        finally:
            con.close()

    def run_make_gridadmin(self):
        """
        Use threedigrid_builder to convert schematisation to gpkg with all elements.
        threedigrid documentation and parameter names have not been updated and are therefore confusing
        """

        if self.create:
            make_gridadmin(
                sqlite_path=self.schematisation_fp,
                dem_path=self.dem_fp,
                out_path=self.gridadmin,
            )

            logger.info(f"Saved gridadmin to {self.gridadmin}")

            exporter = GeopackageExporter(
                self.gridadmin,
                self.gpkg_output,
            )

            exporter.export()

            self.apply_styles()

            logger.info(f"Saved computational grid to {self.gpkg_output}")
        return self.gpkg_output


# %%
if __name__ == "__main__":
    from hhnk_hydro_core.utils.time import current_time

    # rana_result_admin_fp = FOLDER_TEST.threedi_results.one_d_two_d[0].admin_path
    schematisation_fp = Path(
        r"H:\02.modellen\ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_NO\work in progress\schematisation\ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_NO.gpkg"
    )
    dem_fp = Path(
        r"H:\02.modellen\ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_NO\work in progress\schematisation\rasters\dem_1_met_amstelmeer_2m.tif"
    )

    model_control = BreachModelControl(
        schematisation_fp=schematisation_fp,
        dem_fp=dem_fp,
    )
    model_control.run_make_gridadmin()


# %%
