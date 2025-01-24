# %% [markdown]
# # Steps:
# 1. Laadt imports
# 2. Vul variabelen in
# 3. Draai start_calculation tab
#

# %%
# Add qgis plugin deps to syspath and load notebook_data
from notebook_setup import setup_notebook

notebook_data = setup_notebook()


# ipython imports
import ipywidgets as widgets
from IPython.display import HTML, display

display(HTML("<style>.container {width:90% !important;}</style>"))
# %matplotlib inline


from hhnk_threedi_tools.core.api.calculation_gui_class import StartCalculationGui

self = StartCalculationGui(data=notebook_data)
self.w.model.schema_name_widget.value = self.vars.folder.name  # default name

display(self.tab)
