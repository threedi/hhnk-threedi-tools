# %%


from pathlib import Path

import hhnk_research_tools as hrt
import numpy as np

from hhnk_threedi_tools import Folders
from hhnk_threedi_tools.core.checks.schematisation.structure_control import (
    create_sorted_actiontable_queries,
    update_sorted_actiontable,
)

logger = hrt.logging.get_logger(__name__)

try:
    from hhnk_threedi_tools.utils.notebooks.notebook_setup import setup_notebook
except:
    from notebook_setup import setup_notebook  # in case hhnk-threedi-tools is not part of python installation

notebook_data = setup_notebook()

# %% [markdown]
# Sorteer de waarden in de control table in de modelmap met onderstaande cel

# %%
if __name__ == "__main__":
    notebook_data = setup_notebook()

    folder_dir = Path(notebook_data["polder_folder"])  # Bestand voor de legger wordt klaargezet in de folder dir
    # folder_dir = Path(r"E:\02.modellen\VNK_leggertool") # Of een specifieke map

    assert folder_dir.exists()
    folder = Folders(folder_dir)

    queries = create_sorted_actiontable_queries(database=folder.model.schema_base.database)
    update_sorted_actiontable(database=folder.model.schema_base.database, queries=queries)

# %%
