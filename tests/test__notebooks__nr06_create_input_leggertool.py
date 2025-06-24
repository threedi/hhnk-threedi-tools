# %%
import shutil

import hhnk_research_tools as hrt

from hhnk_threedi_tools.utils.notebooks.nr06_create_input_leggertool import (
    create_input_leggertool,
)
from tests.config import FOLDER_TEST, TEMP_DIR

# %%


def test_create_input_leggertool():
    folder = FOLDER_TEST
    berekening_naam = "BWN bwn_test #7 0d1d_test"
    output_file = TEMP_DIR.joinpath(f"debiet_{berekening_naam}_{hrt.get_uuid()}.gpkg")

    structure_all = create_input_leggertool(
        folder=folder,
        berekening_naam=berekening_naam,
        output_file=output_file,
    )

    assert output_file.exists()
    assert output_file.with_suffix(".qml").exists()

    assert structure_all["q_mean_m3_s"].sum() == -0.027619


if __name__ == "__main__":
    test_create_input_leggertool()
