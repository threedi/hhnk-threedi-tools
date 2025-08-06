# %%
try:
    from hhnk_threedi_tools.utils.notebooks.notebook_setup import setup_notebook
except:
    from notebook_setup import setup_notebook  # in case hhnk-threedi-tools is not part of python installation
from hhnk_threedi_tools import Folders

notebook_data = setup_notebook()


# Folders inladen
folder = Folders(notebook_data["polder_folder"])

# Of handmatig;
# folder=Folders(r"E:\02.modellen\Z0215_Purmerend_oostflank")
