# %% VOORAF
# Zorg dat in de poldermap onder 01. DAMO en HDB een mapje staat met de naam 'peilgebieden'. 
# In dit mapje mapje moet de shape van de peilgebieden. Deze shape moet 'peilgebieden_Polderclusternaam.shp' heten, 
# dus bijvoorbeeld 'peilgebieden_Eijerland.shp'. In de kolom 'name' moet de naam van de polder staan. 
# Dit laatste is van belang wanneer meerdere polders in één poldercluster gezamelijk zijn doorgerekend. 
# Maak een kopie van je datachecker output: fixeddrainagelevelarea. Dat werkt.


# %%
import sys
# sys.path.append(r'C:/Users/chris.kerklaan/Documents/Github/hhnk-threedi-tools')
sys.path.append('C:\\Users\wvangerwen\github\hhnk-threedi-tools')

from hhnk_threedi_tools import Folders
import hhnk_threedi_tools as htt
import hhnk_research_tools as hrt


# import hhnk_threedi_tools.core.climate_scenarios as hrt_climate
import hhnk_threedi_tools.core.climate_scenarios.maskerkaart as maskerkaart
import hhnk_threedi_tools.core.climate_scenarios.ruimtekaart as ruimtekaart

from hhnk_threedi_tools.core.climate_scenarios.masker_filter_rasters import rasterize_maskerkaart, apply_mask_to_raster, remove_mask_from_raster

import os
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
# %matplotlib inline

import importlib.resources as pkg_resources #Load resource from package


#User input
# --------------------------------------------------
polders_folder = "C:/Users/wvangerwen/Github/hhnk-threedi-tools/hhnk_threedi_tools/tests/data/multiple_polders"
polder = 'poldera' 
# --------------------------------------------------

folder = Folders(os.path.join(polders_folder, polder))

import ipywidgets as widgets

def item_layout(width="95%", grid_area="", **kwargs):
    return widgets.Layout(
        width=width, grid_area=grid_area, **kwargs
    )  # override the default width of the button to 'auto' to let the button grow
    
output_folder_options = folder.threedi_results.batch.content
# output_folder_label = widgets.Label('Selecteer output folder:', layout=item_layout(grid_area='output_folder_label'))
output_folder_box = widgets.Select(
    options=output_folder_options,
    rows=3,
    disabled=False,
    layout=item_layout(grid_area="output_folder_box"),
)

## %%
#Batch folder selection


#Display precipitation zones
polder_shape = gpd.read_file(folder.source_data.polder_polygon.path)
with pkg_resources.path(htt.resources, 'precipitation_zones_hhnk.tif') as p:
    neerslag_list, neerslag_nodata, neerslag_meta = hrt.load_gdal_raster(p.absolute().as_posix(), band_count=3)
with pkg_resources.path(htt.resources, 'precipitation_frequency.xlsx') as p:
    freqs = pd.read_excel(p.absolute().as_posix())

fig, ax = plt.subplots(figsize=(15, 15))
ax.imshow(neerslag_list, extent=neerslag_meta['bounds'])
polder_shape.plot(ax=ax, color='red')

precipitation_zone_box = widgets.Select(
    options=['hevig', 'debilt'],
    rows=2,
    disabled=False,
    value=None,
    layout=item_layout(grid_area="precipitation_zone_box"),
)

print("Selecteer map met batch resultaten")
display(output_folder_box)
print("Selecteer neerslagzone")
display(precipitation_zone_box)


## %%
batch_fd = folder.threedi_results.batch[output_folder_box.value]

#Selectie neerslag scenario
precipitation_zone_box.value='debilt' #TODO remove line after testing.

freqs = freqs[['dl_name', 'freq_{}_jaar'.format(precipitation_zone_box.value)]]

## %% Aanmaken of laden peilgebieden polygon
fixeddrainage = folder.source_data.datachecker.load('fixeddrainagelevelarea')
if not folder.source_data.peilgebieden.peilgebieden.exists:
    fixeddrainage.to_file(folder.source_data.peilgebieden.peilgebieden.path)
    print(f'Peilgebieden shapefile aangemaakt: {folder.source_data.peilgebieden.peilgebieden.name}.shp')
else:
    print(f'Peilgebieden shapefile gevonden: {folder.source_data.peilgebieden.peilgebieden.name}.shp')
## %% Maak masker en ruimtekaart

## Maskerkaart

# #Aanmaken polygon van maskerkaart
# maskerkaart.command(path_piek=batch_fd.downloads.piek_GHG_T1000.path, 
#                     path_blok=batch_fd.downloads.blok_GHG_T1000.path,
#                     path_out=batch_fd.output.maskerkaart.path)

# #Omzetten polygon in raster voor diepteraster
# _, _, depth_meta = hrt.load_gdal_raster(batch_fd.downloads.max_depth_piek_GLG_T10.path, 
#                                             return_array=False)
# mask_depth = rasterize_maskerkaart(input_file=batch_fd.output.maskerkaart.path, 
#                       mask_plas_path=batch_fd.output.mask_diepte_plas.path, 
#                       mask_overlast_path=batch_fd.output.mask_diepte_overlast.path, 
#                       meta=depth_meta)

# #Omzetten polygon in raster voor schaderaster (kan verschillen van diepte met andere resolutie)
# _, _, damage_meta = hrt.load_gdal_raster(batch_fd.downloads.total_damage_piek_GLG_T10.path, 
#                                             return_array=False)
# mask_damage = rasterize_maskerkaart(input_file=batch_fd.output.maskerkaart.path, 
#                       mask_plas_path=batch_fd.output.mask_schade_plas.path, 
#                       mask_overlast_path=batch_fd.output.mask_schade_overlast.path, 
#                       meta=damage_meta)


# %%
## Ruimtekaart
import sys, importlib
importlib.reload(ruimtekaart)

maxdepth_prefix = batch_fd.downloads.full_path('max_depth_')
damage_prefix = batch_fd.downloads.full_path('total_damage_')

# if not batch_fd.output.ruimtekaart.exists:

if True:
    ruimtekaart.command(shapefile_path=folder.source_data.peilgebieden.peilgebieden.path, 
                        output_path=batch_fd.output.ruimtekaart.path, 
                        maxdepth_prefix=maxdepth_prefix, 
                        damage_prefix=damage_prefix,
                        mask_path=batch_fd.output.maskerkaart.path)
    print('Ruimtekaart created')


# %%
