# %%
import shutil
from pathlib import Path

from hhnk_threedi_tools import Folders

paths = [
    r"H:\02.modellen\bergen_noord_huidig_situatie_JA",
    r"H:\02.modellen\bergen_noord_variant_1_JA",
    r"H:\02.modellen\bergen_noord_variant_2_JA",
    r"H:\02.modellen\bergen_noord_variant_3_JA",
]

destination = Path(r"G:\02_Werkplaatsen\06_HYD\Projecten\HKC26015_TAUW")

for location in paths:
    folder = Folders(location)
    batch_path = Path(folder.threedi_results.batch.path)

    # collect subfolders in the batch folder (as Path objects)
    batch_folders = [p for p in batch_path.iterdir() if p.is_dir()]

    folder_name = folder.name
    base_dest = destination / folder_name
    # base_dest.mkdir(parents=True, exist_ok=True)

    for results_path in batch_folders:
        dest = base_dest / results_path.name
        shutil.copytree(results_path, dest)
