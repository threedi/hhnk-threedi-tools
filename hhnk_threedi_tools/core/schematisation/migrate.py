# %%
"""
processing tool to function.

copied and edited from ThreeDiToolbox\processing\schematisation_algorithms.py
"""

import os
import shutil
from pathlib import Path

import hhnk_research_tools as hrt
from hhnk_research_tools.folder_file_classes.folder_file_classes import Sqlite
from threedi_schema import ThreediDatabase, errors


def backup_sqlite(filename, clear_folder=False):
    """Make a backup of the sqlite database.
    direct copy of ThreeDiToolbox\\utils\\utils.py to reduce dependencies"""

    backup_folder = hrt.Folder(Path(filename).parents[1] / "_backup")
    backup_folder.create()

    if clear_folder:
        backup_folder.unlink_contents()

    backup_sqlite_path = backup_folder.full_path(f"{hrt.get_uuid()}_{os.path.basename(filename)}")
    shutil.copyfile(str(filename), str(backup_sqlite_path))
    return backup_sqlite_path


class MigrateSchema:
    """Migrate schema to newest version
    filename: path to sqlite"""

    def __init__(self, filename):
        self.filename = filename
        self.schema_raw = Sqlite(filename)

    def get_threedi_database(self, filename):
        try:
            threedi_db = ThreediDatabase(filename)
            threedi_db.check_connection()
            return threedi_db
        except Exception as e:
            raise e

    def get_schema(self, filename):
        self.threedi_db = self.get_threedi_database(filename=filename)
        if not self.threedi_db:
            raise Exception(f"Database does not exist; {filename}")
        schema = self.threedi_db.schema
        return schema

    def backup(self, clear_folder=False) -> Sqlite:
        """create backup"""
        backup_filepath = backup_sqlite(self.schema_raw.base, clear_folder=clear_folder)
        return Sqlite(backup_filepath)

    def overwrite_original(self):
        """overwrite original sqlite"""

        backup2 = self.backup()
        try:
            self.schema_raw.unlink(missing_ok=True)

            # Write migrated sqlite to file
            shutil.copy(self.schema_backup.base, self.schema_raw.base)

            backup2.unlink(missing_ok=True)

        except Exception as e:
            print(f"Backup of sqlite saved in {backup2.base}")
            raise e

    def run(self):
        # Create a backup and work on this one.
        # Editing in the original file causes filelock issues
        self.schema_backup = self.backup(clear_folder=True)

        self.schema = self.get_schema(filename=self.schema_backup.base)

        try:
            self.schema.validate_schema()
            # self.schema.set_spatial_indexes()
        except errors.MigrationMissingError as e:
            # Schema not up to date so we migrate.
            print("Starting migration")
            old_version = self.schema.get_version()
            try:
                retry_count = 0
                self.schema.upgrade(backup=False, upgrade_spatialite_version=True)
            except NotADirectoryError as e:
                """FIXME for some reason it works if you try it twice...
                othwise there is some filelock on a temporary file or it doesnt exist.."""
                retry_count += 1
                if retry_count == 1:
                    self.schema.upgrade(backup=False, upgrade_spatialite_version=True)
                else:
                    raise e

            new_version = self.schema.get_version()

            self.schema.set_spatial_indexes()

            self.overwrite_original()

            print(f"Upgraded database from version {old_version} to {new_version}")

        except errors.UpgradeFailedError:
            print("UpgradeFailedError")


# %%
if __name__ == "__main__":
    from pathlib import Path

    from hhnk_threedi_tools.core.folders import Folders

    for i in range(1, 5):
        TEST_MODEL = Path(__file__).parents[i].absolute() / "tests/data/model_test/"
        folder = Folders(TEST_MODEL)
        if folder.exists():
            break

    filename_orig = folder.full_path("bwn_test_pre_migration.sqlite")
    filename = filename_orig.with_name("test_migration.sqlite")
    try:
        filename.unlink(missing_ok=True)
    except:
        raise

    shutil.copy(filename_orig, filename)
    assert filename.exists()

    self = MigrateSchema(filename)
    self.run()
