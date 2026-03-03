# %%
# First-party imports
import shutil

import hhnk_research_tools as hrt

# Local imports
from hhnk_threedi_tools.core.schematisation.migrate import MigrateSchema
from tests.config import FOLDER_TEST, TEMP_DIR

# %%


def test_schema_migration():
    """Test if an old sqlite can be migrated to the latest schema version"""
    # Make a copy, the migration is done on the same database.
    database_old = FOLDER_TEST.full_path("bwn_test_v216.sqlite")
    database_new = hrt.Folder(TEMP_DIR).full_path(f"migrated_sqlite_{hrt.get_uuid()}.gpkg")

    try:
        database_new.unlink(missing_ok=True)
    except:
        raise

    shutil.copy(database_old.path, database_new.path)

    # filelocks if we run get_schema twice on the same MigrateSchema instance, only in remote pytests.
    migrate_schema = MigrateSchema(database_old.path)
    assert migrate_schema.get_schema(filename=database_old.path).get_version() == 216

    # Migrate to newest version
    migrate_schema = MigrateSchema(database_new)
    migrate_schema.run()

    assert migrate_schema.schema.get_version() == 219


# %%


# TODO sqlite instellingen beiden op True zetten;
# "use_0d_inflow": True
# "use_structure_control": True
# Dan migreren.

from pathlib import Path

import threedi_schema

print(threedi_schema.__version__)
from threedi_schema import ModelSchema, ThreediDatabase

import hhnk_threedi_tools as htt

sqlite_old = rf"{Path(htt.__file__).parents[1]}\tests\data\model_test\02_schematisation\bwn_test_300.sqlite"

# %%
database_new = Path(htt.__file__).parents[1].joinpath(r"tests\data\model_test\02_schematisation\bwn_test_300.gpkg")
threedi_db: ThreediDatabase = ThreediDatabase(path=database_new)
model_schema: ModelSchema = threedi_db.schema
model_schema.upgrade(upgrade_spatialite_version=True, keep_spatialite=True)

# %%
from pathlib import Path

import geopandas as gpd

import hhnk_threedi_tools as htt

database_new = Path(htt.__file__).parents[1].joinpath(r"tests\data\model_test\02_schematisation\bwn_test_300.gpkg")
print(type(gpd.read_file(database_new, layer="model_settings").loc[0, "use_2d_rain"]))

# %%
if __name__ == "__main__":
    test_schema_migration()
