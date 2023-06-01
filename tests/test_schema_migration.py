 # %%
"""
"""
if __name__ == "__main__":
    import set_local_paths  # add local git repos.

# First-party imports
import os
import pathlib
import shutil

# Local imports
from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.core.migrate_schematisation import MigrateSchema


from tests.config import FOLDER_TEST


def test_schema_migration():
    """test if an old sqlite can be migrated to the latest schema version"""

    #Make a copy, the migration is done on the same database.
    filename_orig = FOLDER_TEST.full_path("bwn_test_pre_migration.sqlite")
    filename = filename_orig.with_name("test_migration.sqlite")

    try:
        filename.unlink(missing_ok=True)
    except:
        raise

    shutil.copy(filename_orig, filename)
    assert filename_orig.stat().st_size == 4918272

    #Migrate to newest version
    migrate_schema = MigrateSchema(filename)
    migrate_schema.run()

    assert filename.stat().st_size == 7225344
    




# %%
if __name__ == "__main__":
    test_schema_migration()