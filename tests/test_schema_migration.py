 # %%
# First-party imports
import shutil

# Local imports
from hhnk_threedi_tools.core.schematisation.migrate import MigrateSchema


import hhnk_research_tools as hrt
from tests.config import FOLDER_TEST, TEMP_DIR

def test_schema_migration():
    """test if an old sqlite can be migrated to the latest schema version"""
    #Make a copy, the migration is done on the same database.
    database_old = FOLDER_TEST.full_path("bwn_test_v216.sqlite")
    database_new = hrt.Folder(TEMP_DIR).full_path(f"migrated_sqlite_{hrt.get_uuid()}.sqlite")

    try:
        database_new.unlink(missing_ok=True)
    except:
        raise

    shutil.copy(database_old.path, database_new.path)

    #filelocks if we run get_schema twice on the same MigrateSchema instance, only in remote pytests.
    migrate_schema = MigrateSchema(database_old) 
    assert migrate_schema.get_schema(filename=database_old).get_version() == 216

    #Migrate to newest version
    migrate_schema = MigrateSchema(database_new)
    migrate_schema.run()

    assert migrate_schema.schema.get_version() == 217
    

# %%
if __name__ == "__main__":
    test_schema_migration()
    