# %% [markdown]
# Open in jupyterlab as a notebook; right click .py -> Open With -> Jupytext Notebook
# # 02. Downloaden modelresultaten
# - modelnaam in rasters toevoegen
# - HTTPError: 504 Server Error: Gateway Time-out oplossen
# - HTTPError: 503 Server Error: Service Unavailable for url: https://hhnk.lizard.net/api/v3/tasks/92c6b2b8-b841-4fec-b931-87a746892555/

# %%
# Add qgis plugin deps to syspath and load notebook_data
try:
    from hhnk_threedi_tools.utils.notebooks.notebook_setup import setup_notebook
except:
    from notebook_setup import setup_notebook  # in case hhnk-threedi-tools is not part of python installation


notebook_data = setup_notebook()

from IPython.display import HTML, display

display(HTML("<style>.container {width:90% !important;}</style>"))
# %matplotlib inline

# local imports
from hhnk_threedi_tools.core.api.download_gui_class import DownloadGui

download_tab = DownloadGui(data=notebook_data)
download_tab.w.search.sim_name_widget.value = download_tab.vars.folder.name
download_tab.tab

# %%
# Resume downloads
# api_key = input('API key: ')
api_key = download_tab.children[1].value
# batch_csv_file = '../../07. Poldermodellen/10.tHoekje/03. Model resultaten Hydraulische Toets/2021-08-02 08h34_download_raster_batch.csv'
batch_csv_file = r"C:\Users\wvangerwen\github\hhnk-threedi-tools\hhnk_threedi_tools\tests\data\multiple_polders\poldera\03_3di_resultaten\batch_results\bwn_test #5 (1) klimaatsommen\2021-09-24 13h48_download_raster_batch.csv"

# from threedi_scenario_downloader import downloader as dl  # FIXME Zie #102
from hhnk_threedi_tools.external import downloader as dl

dl.set_api_key(api_key)
dl.resume_download_tasks(batch_csv_file, overwrite=False)

# %% [markdown]
# ## Downloaden van rasters voor hetzelfde model op alle tijdstappen

# %%
import getpass
import os

# from threedi_scenario_downloader import downloader as dl  # FIXME Zie #102
from hhnk_threedi_tools.external import downloader as dl

# Change three discenario downloader
# def new_get_headers():
#     """Setting the headers in the original toolbox is not easy when using this GUI.
#     Therefore we change this function in the toolbox so everything else works."""
#     headers_results = {
#     "username": '{}'.format('w.vanesse'),
#     "password": '{}'.format(''),
#     "Content-Type": "application/json"}
#     return headers_results
# setattr(dl,'get_headers', new_get_headers)

output_folder = "../../07. Poldermodellen/102. Schermerboezem"
uuid = "555aaeca-ec9b-40e5-ae0b-ebb9f41505cf"
resolution = 5

a = []
for h in range(4, 24):
    a.append("2016-01-05T{}:14:16".format(str(h).zfill(2)))

for h in range(0, 24):
    a.append("2016-01-06T{}:14:16".format(str(h).zfill(2)))

for h in range(0, 4):
    a.append("2016-01-07T{}:14:16".format(str(h).zfill(2)))


wdepth_path = []
for index, time in enumerate(a):
    time_path = time.replace("-", "_")
    time_path = time_path.replace(":", "_")

    wdepth_path += [os.path.join(output_folder, "wdepth_" + time_path + ".tif")]
    wdepth_path[index]
    if not os.path.exists(wdepth_path[index]):
        print("Preparing download: {}".format(time_path))
        dl.download_waterdepth_raster(uuid, "EPSG:28992", resolution, time, pathname=wdepth_path[index])
