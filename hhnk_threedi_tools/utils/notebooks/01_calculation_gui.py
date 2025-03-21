# %% [markdown]
# Open in jupyterlab as a notebook; right click .py -> Open With -> Jupytext Notebook
# # Steps:
# 1. Load imports
# 2. Enter variables
# 3. Run start_calculation tab
#

# %%
# Add qgis plugin deps to syspath and load notebook_data
try:
    from hhnk_threedi_tools.utils.notebooks.notebook_setup import setup_notebook
except:
    from notebook_setup import setup_notebook  # in case hhnk-threedi-tools is not part of python installation

notebook_data = setup_notebook()


# ipython imports
from IPython.display import HTML, display

display(HTML("<style>.container {width:90% !important;}</style>"))
# %matplotlib inline


from hhnk_threedi_tools.core.api.calculation_gui_class import StartCalculationGui

self = StartCalculationGui(data=notebook_data)
self.w.model.schema_name_widget.value = self.vars.folder.name  # default name

display(self.tab)

# %%
