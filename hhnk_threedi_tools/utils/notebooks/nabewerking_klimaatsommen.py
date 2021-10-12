# %% VOORAF
# Zorg dat in de poldermap onder 01. DAMO en HDB een mapje staat met de naam 'peilgebieden'. 
# In dit mapje mapje moet de shape van de peilgebieden. Deze shape moet 'peilgebieden_Polderclusternaam.shp' heten, 
# dus bijvoorbeeld 'peilgebieden_Eijerland.shp'. In de kolom 'name' moet de naam van de polder staan. 
# Dit laatste is van belang wanneer meerdere polders in één poldercluster gezamelijk zijn doorgerekend. 
# Maak een kopie van je datachecker output: fixeddrainagelevelarea. Dat werkt.


# %%
import sys

from numpy.lib.function_base import kaiser
# sys.path.append(r'C:/Users/chris.kerklaan/Documents/Github/hhnk-threedi-tools')
sys.path.append('C:\\Users\wvangerwen\github\hhnk-threedi-tools')

from hhnk_threedi_tools import Folders
import hhnk_threedi_tools as htt
import hhnk_research_tools as hrt


# import hhnk_threedi_tools.core.climate_scenarios as hrt_climate
import hhnk_threedi_tools.core.climate_scenarios.maskerkaart as maskerkaart
import hhnk_threedi_tools.core.climate_scenarios.ruimtekaart as ruimtekaart
from hhnk_threedi_tools.core.climate_scenarios.interpolate_rasters import main_interpolate_rasters
import hhnk_threedi_tools.core.climate_scenarios.schadekaart as schadekaart


from hhnk_threedi_tools.core.climate_scenarios.masker_filter_rasters import rasterize_maskerkaart, apply_mask_to_raster, remove_mask_from_raster

import os
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
# %matplotlib inline

import importlib.resources as pkg_resources #Load resource from package
import ipywidgets as widgets



#User input


# --------------------------------------------------
polders_folder = "C:/Users/wvangerwen/Github/hhnk-threedi-tools/hhnk_threedi_tools/tests/data/multiple_polders"
polder = 'poldera' 
# --------------------------------------------------

folder = Folders(os.path.join(polders_folder, polder))


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

# fig, ax = plt.subplots(figsize=(15, 15))
# ax.imshow(neerslag_list, extent=neerslag_meta['bounds'])
# polder_shape.plot(ax=ax, color='red')

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

df = pd.DataFrame(batch_fd.downloads.names, columns=['dl_name'])
for dl_name in batch_fd.downloads.names:
    df.loc[df['dl_name']==dl_name, 'max_depth'] = getattr(batch_fd.downloads,dl_name).max_depth.path
    df.loc[df['dl_name']==dl_name, 'total_damage'] = getattr(batch_fd.downloads,dl_name).total_damage.path
    

## %%

#Selectie neerslag scenario
precipitation_zone_box.value='debilt' #FIXME remove line after testing.

freqs = freqs[['dl_name', 'freq_{}_jaar'.format(precipitation_zone_box.value)]]
freqs.rename({'freq_debilt_jaar':'freq_jaar'}, axis=1, inplace=True)

df_freqs = df.merge(freqs, on='dl_name')

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


## %%
## Ruimtekaart
# import sys, importlib
# importlib.reload(ruimtekaart)

# maxdepth_prefix = batch_fd.downloads.full_path('max_depth_')
# damage_prefix = batch_fd.downloads.full_path('total_damage_')

# # if not batch_fd.output.ruimtekaart.exists:

# if True:
#     ruimtekaart.command(shapefile_path=folder.source_data.peilgebieden.peilgebieden.path, 
#                         output_path=batch_fd.output.ruimtekaart.path, 
#                         maxdepth_prefix=maxdepth_prefix, 
#                         damage_prefix=damage_prefix,
#                         mask_path=batch_fd.output.maskerkaart.path)
#     print('Ruimtekaart created')

## %% Inundatiediepte rasters
# diepte_rasters = df_freqs['max_depth'].values
# frequenties = df_freqs['freq_jaar'].values

# for T in [10, 100, 1000]:

#     output_file = getattr(batch_fd.output, f'depth_T{T}_totaal')

#     #Voor de gegeven herhalingstijd, interpoleer de rasters.
#     main_interpolate_rasters(T = T,
#                                 output_file =  output_file,
#                                 rasters =  diepte_rasters,
#                                 frequenties =  frequenties,
#                                 output_nodata =  -9999.00,
#                                 dem_path = folder.model.rasters.dem.path,
#                                 min_blocksize =  1000,
#                                 extra_nodata_value= 0)


# # %% Filter inundatiediepte
# for T in [10, 100, 1000]:
#     input_raster = rasterclass.Raster(getattr(batch_fd.output, f'depth_T{T}_totaal').path)

#     # Creeer masker voor plas en overlast
#     for mask_type in ['plas','overlast']:
#         mask = rasterclass.Raster(getattr(batch_fd.output, f'mask_diepte_{mask_type}').path)

#         output_raster = getattr(batch_fd.output, f'depth_T{T}_{mask_type}')

#         if not output_raster.exists:
#             raster_array = input_raster._read_array()
#             mask_array = mask._read_array()


#             raster_array[~(mask_array==1)] = input_raster.nodata #Maskeer
#             hrt.save_raster_array_to_tiff(output_file=output_raster.path, 
#                                             raster_array=raster_array, 
#                                             nodata=input_raster.nodata, 
#                                             metadata=input_raster.metadata)
#             print(f'{output_raster.name} created')

#         else: 
#             print(f'{output_raster.name} already exists')
# raster_array=None
# mask_array=None





# # %% Schadekaart
# import sys, importlib

# dv = 0.04 #discontovoet [%]
# n = 50 #investeringstermijn [jaren]
# schade_rasters = df_freqs['total_damage'].values
# frequencies = df_freqs['freq_jaar'].values
# output_raster = batch_fd.output.cw_schade_totaal

# if not output_raster.exists:
#     schadekaart.main_maak_schadekaart(output_raster=output_raster,
#                     schade_rasters=schade_rasters,
#                     frequencies=frequencies,
#                     output_nodata=0,
#                     dv=dv,
#                     n=n,
#                     min_blocksize=1000)
    
#     print(f'{output_raster.name} created')

# else: 
#     print(f'{output_raster.name} already exists')


# %% Filter schadekaart
input_raster = rasterclass.Raster(batch_fd.output.cw_schade_totaal.path)

# Creeer masker voor plas en overlast
for mask_type in ['plas','overlast']:
    mask = rasterclass.Raster(getattr(batch_fd.output, f'mask_schade_{mask_type}').path)

    output_raster = getattr(batch_fd.output, f'cw_schade_{mask_type}')

    if not output_raster.exists:
        raster_array = input_raster._read_array()
        mask_array = mask._read_array()

        raster_array[~(mask_array==1)] = input_raster.nodata
        hrt.save_raster_array_to_tiff(output_file=output_raster.path, 
                                        raster_array=raster_array, 
                                        nodata=input_raster.nodata, 
                                        metadata=input_raster.metadata)
        print(f'{output_raster.name} created')

    else: 
        print(f'{output_raster.name} already exists')
raster_array=None
mask_array=None




## %% inundatiediepte



# %% Schade per peilgebied
import sys, importlib
import rasterclass
importlib.reload(rasterclass)
source_path = 'C:\\Users\\wvangerwen\\Github\\hhnk-threedi-tools\\hhnk_threedi_tools\\tests\\data\\multiple_polders\\poldera\\03_3di_resultaten\\batch_results\\bwn_test #5 (1) klimaatsommen\\01_downloads\\max_depth_piek_GLG_T1000.tif'


pgb_gdf = gpd.read_file(folder.source_data.peilgebieden.peilgebieden.path)
pgb_gdf.reset_index(drop=False, inplace=True)
schade_gdf = pgb_gdf[['id', 'peil_id', 'code', 'name', 'geometry']].copy()

schade_raster = rasterclass.Raster(batch_fd.output.cw_schade_totaal.path)

#Rasterize regions
if not batch_fd.output.peilgebieden_schade.exists:
    create=True
else:
    create=False
    #Check edit times. To see if raster needs to be updated.
    raster_mtime = batch_fd.output.peilgebieden_schade.pl.stat().st_mtime
    shape_mtime = folder.source_data.peilgebieden.peilgebieden.pl.stat().st_mtime
    
    if shape_mtime>raster_mtime:
        create=True
    
if create:
    hrt.gdf_to_raster(gdf=pgb_gdf, 
                        value_field='index', 
                        raster_out=batch_fd.output.peilgebieden_schade.path,
                        nodata=schade_raster.nodata,
                        metadata=schade_raster.metadata)

    print(f'{batch_fd.output.peilgebieden_schade.name} created')

# %%

labels_raster = rasterclass.Raster(batch_fd.output.peilgebieden_schade.path)
labels_index = pgb_gdf['index'].values

#Bereken totale schade per peilgebied voor de twee gemaskerde schaderasters.
for mask_type, mask_name in zip(['plas','overlast'], ['mv', 'ws']):
    schade_raster = rasterclass.Raster(getattr(batch_fd.output, f'cw_schade_{mask_type}').path)

    #Calculate sum per region
    accum  = schade_raster.sum_labels(labels_raster=labels_raster, labels_index=labels_index, min_blocksize=1024)

    schade_gdf[f'cw_{mask_name}'] = accum

schade_gdf['cw_tot'] = schade_gdf['cw_ws'] + schade_gdf['cw_mv']


# %%


# %%
