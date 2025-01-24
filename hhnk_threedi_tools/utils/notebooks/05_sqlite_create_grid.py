# Open in jupyterlab as a notebook; right click .py -> Open With -> Jupytext Notebook
# %% [markdown]
# ## Create grid nodes and lines from sqlite

# %%
# Add qgis plugin deps to syspath and load notebook_data
try:
    from hhnk_threedi_tools.utils.notebooks.notebook_setup import setup_notebook
except:
    from notebook_setup import setup_notebook  # in case hhnk-threedi-tools is not part of python installation


notebook_data = setup_notebook()


from hhnk_threedi_tools import Folders, SqliteCheck

# %%
folder_dir = notebook_data["polder_folder"]

folder = Folders(folder_dir)
sqlite_test = SqliteCheck(folder)

sqlite_test.create_grid_from_sqlite(
    sqlite_path=folder.model.sqlite_paths[0],
    dem_path=folder.model.rasters.dem.path,
    output_folder=folder.output.sqlite_tests.path,
)
