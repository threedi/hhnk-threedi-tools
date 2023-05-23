# %%
"""
processing tool to function.

copied and edited from ThreeDiToolbox\processing\schematisation_algorithms.py
"""  

import os
from uuid import uuid4



import shutil
from pathlib import Path

from threedi_schema import ThreediDatabase
from threedi_schema import errors

from hhnk_research_tools.folder_file_classes.folder_file_classes import Sqlite


def backup_sqlite(filename):
    u"""Make a backup of the sqlite database.
    direct copy of ThreeDiToolbox\\utils\\utils.py to reduce dependencies"""

    backup_folder = os.path.join(os.path.dirname(os.path.dirname(filename)), "_backup")
    os.makedirs(backup_folder, exist_ok=True)
    prefix = str(uuid4())[:8]
    backup_sqlite_path = os.path.join(
        backup_folder, f"{prefix}_{os.path.basename(filename)}"
    )
    shutil.copyfile(filename, backup_sqlite_path)
    return backup_sqlite_path


class MigrateSchema():
    """Migrate schema to newest version
    filename: path to sqlite"""
    def __init__(self, filename):
        self.schema_raw = Sqlite(filename)
       

    def get_threedi_database(self, filename):
        try:
            threedi_db = ThreediDatabase(filename)
            threedi_db.check_connection()
            return threedi_db
        except Exception as e:
            raise e


    def backup(self) -> Sqlite:
        """create backup"""
        backup_filepath = backup_sqlite(self.schema_raw.path)
        return Sqlite(backup_filepath)
    

    def overwrite_original(self):
        """overwrite original sqlite"""

        backup2 = self.backup()
        try:
            self.schema_raw.unlink_if_exists()

            #Write migrated sqlite to file
            shutil.copy(self.schema_backup.path, self.schema_raw.path)

            backup2.unlink_if_exists()

        except Exception as e:
            print(f"Backup of sqlite saved in {backup2.path}")
            raise e
        

    def run(self):
            #Create a backup and work on this one.
            #Editing in the original file causes filelock issues
            self.schema_backup = self.backup()

            self.threedi_db = self.get_threedi_database(filename=self.schema_backup.path)
            if not self.threedi_db:
                raise Exception(f"Database does not exist; {self.schema_backup.path}")
            schema = self.threedi_db.schema

            
            try:
                schema.validate_schema()
                # schema.set_spatial_indexes()
            except errors.MigrationMissingError as e:
                #Schema not up to date so we migrate.
                print("Starting migration")
                old_version = schema.get_version()
                try:
                    retry_count=0
                    schema.upgrade(backup=False, upgrade_spatialite_version=True)
                except NotADirectoryError as e:
                    """FIXME for some reason it works if you try it twice... 
                    othwise there is some filelock on a temporary file or it doesnt exist.."""
                    retry_count +=1
                    if retry_count==1:
                        schema.upgrade(backup=False, upgrade_spatialite_version=True)
                    else:
                        raise e
                    
                new_version = schema.get_version()
                    
                schema.set_spatial_indexes()

                self.overwrite_original()

                print(f"Upgraded database from version {old_version} to {new_version}")
                # self.schema_backup.unlink_if_exists()

            except errors.UpgradeFailedError:
                print("UpgradeFailedError")



# %%
if __name__ == "__main__":
    from hhnk_threedi_tools.core.folders import Folders
    from pathlib import Path

    for i in range(1,5):
        TEST_MODEL = Path(__file__).parents[i].absolute() / "tests/data/model_test/"
        folder = Folders(TEST_MODEL)
        if folder.exists:
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