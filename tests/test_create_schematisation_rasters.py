# %%
import shutil

import hhnk_research_tools as hrt

from hhnk_threedi_tools.core import folders_modelbuilder
from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.core.modelbuilder.create_schematisation_rasters import create_schematisation_rasters
from tests.config import FOLDER_TEST, TEMP_DIR

PATH_NEW_FOLDER = TEMP_DIR / f"test_project_{hrt.current_time(date=True)}"
FOLDER_NEW = Folders(PATH_NEW_FOLDER, create=True)
shutil.copytree(FOLDER_TEST.source_data.path, FOLDER_NEW.source_data.path, dirs_exist_ok=True)
shutil.copy(FOLDER_TEST.model.calculation_rasters.polder.path, FOLDER_NEW.model.calculation_rasters.polder.path)
shutil.copy(FOLDER_TEST.model.calculation_rasters.waterdeel.path, FOLDER_NEW.model.calculation_rasters.waterdeel.path)


def test_create_schematisation_rasters():
    """Test creation of model rasters"""
    # %%

    rasters_dir = FOLDER_TEST.model.schema_base.rasters
    source_paths = folders_modelbuilder.SourcePaths(
        dem_path=rasters_dir.dem,
        glg_path=rasters_dir.full_path("storage_glg_hoekje.tif"),
        ggg_path=rasters_dir.full_path("storage_ggg_hoekje.tif"),
        ghg_path=rasters_dir.full_path("storage_ghg_hoekje.tif"),
        infiltration_path=rasters_dir.full_path("infiltration_hoekje.tif"),
        friction_path=rasters_dir.full_path("friction_hoekje.tif"),
        landuse_path=FOLDER_TEST.model.schema_base.rasters.landuse,
        polder_path=FOLDER_TEST.source_data.polder_polygon,
        waterdeel_path=FOLDER_TEST.source_data.modelbuilder.channel_from_profiles,
    )

    folder_mb = folders_modelbuilder.FoldersModelbuilder(folder=FOLDER_NEW, source_paths=source_paths)

    assert folder_mb.dst.dem.exists() is False

    create_schematisation_rasters(folder_mb=folder_mb, pytests=True)

    assert folder_mb.dst.dem.exists() is True
    assert folder_mb.dst.dem.shape == [6962, 7686]
    assert folder_mb.dst.landuse.sum() == 234612304.0


# %%
if __name__ == "__main__":
    test_create_schematisation_rasters()
