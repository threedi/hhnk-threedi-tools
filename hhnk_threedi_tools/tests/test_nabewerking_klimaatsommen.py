# %%
# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import set_local_paths  # add local git repos.

if __name__ == "__main__":
    import set_local_paths  # add local git repos.

# First-party imports
import os
import pathlib
import pandas as pd
# Local imports
from hhnk_threedi_tools.core.checks.nabewerking_klimaatsommen import Nabewerking_Klimaatsommen
from hhnk_threedi_tools.core.folders import Folders
from pandas.testing import assert_frame_equal

# TEST_MODEL = str(pathlib.Path(__file__).parent.absolute()) + "/data/model_test/"
TEST_MODEL = r'\\corp.hhnk.nl\data\Hydrologen_data\Data\02.modellen\model_test_v2'


def test_run_nabewerking_klimaatsommen():
    folder = Folders(TEST_MODEL)
    klimaatsommen = Nabewerking_Klimaatsommen(folder=folder)
    output = klimaatsommen.run_nabewerking_klimaatsommen()

    csv_files =  folder.threedi_results.batch['batch_test'].path

    damage_csv =  os.path.join(csv_files, 'raster_damage_info.csv')
    damage_data = pd.read_csv((damage_csv))
    damage_data = damage_data.drop(['Unnamed: 0'], axis=1)
    damage_data.set_index(['file name'], inplace = True)

    depth_csv = os.path.join(csv_files, 'raster_depth_info.csv')
    depth_data = pd.read_csv((depth_csv))
    depth_data = depth_data.drop(['Unnamed: 0'], axis=1)
    depth_data.set_index(['file name'], inplace = True)
    
    frames = [damage_data, depth_data]
    result = pd.concat(frames)
    # assert output == (damage_data)
    assert_frame_equal(output, result)
    # assert output['mean'] ==1
if __name__ == "__main__":
    test_run_nabewerking_klimaatsommen()


# %%
