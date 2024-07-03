# %%

import hhnk_research_tools as hrt

from hhnk_threedi_tools.core.modelbuilder.create_landuse_polder_clip import create_landuse_polder_clip
from tests.config import FOLDER_TEST, TEMP_DIR, TEST_DIRECTORY


def test_create_landuse_polder_clip():
    """Test if a small landuse tif is created on dem extent from a larger landuse file.
    Note that input files are a a bit random here.
    """
    TEST_RESULT_DIR = TEST_DIRECTORY / r"test_modelbuilder"

    dem = hrt.Raster(TEST_RESULT_DIR / "lu_small.tif")
    landuse_name = "test_small"
    output_dir = TEMP_DIR
    landuse_hhnk = hrt.Raster(TEST_RESULT_DIR / "area_test_labels.tif")

    # Create the tiff
    landuse_tif, created = create_landuse_polder_clip(
        dem=dem, landuse_name=landuse_name, output_dir=output_dir, landuse_hhnk=landuse_hhnk, overwrite=True
    )

    assert landuse_tif.sum() == 13941
    assert created is True

    # Check if it doesnt overwrite
    landuse_tif, created = create_landuse_polder_clip(
        dem=dem, landuse_name=landuse_name, output_dir=output_dir, landuse_hhnk=landuse_hhnk, overwrite=False
    )

    assert created is False


# %%
if __name__ == "__main__":
    test_create_landuse_polder_clip()

# %%
