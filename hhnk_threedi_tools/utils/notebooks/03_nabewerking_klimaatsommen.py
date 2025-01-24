# Open in jupyterlab as a notebook; right click .py -> Open With -> Jupytext Notebook
# %% [markdown]
# # Nabewerking Klimaatsommen
#
# Zorg dat in de poldermap onder 01. DAMO en HDB een folder staat met de naam 'peilgebieden'.
# In deze folder moet de shape van de peilgebieden. Deze shape moet 'peilgebieden_{Polderclusternaam}.shp' heten,
# dus bijvoorbeeld 'peilgebieden_Eijerland.shp'. In de kolom 'name' moet de naam van de polder staan.
# Dit laatste is van belang wanneer meerdere polders in één poldercluster gezamelijk zijn doorgerekend.
# Maak een kopie van je datachecker output: fixeddrainagelevelarea. Dat werkt.
#

# %%
try:
    from hhnk_threedi_tools.utils.notebooks.notebook_setup import setup_notebook
except:
    from notebook_setup import setup_notebook  # in case hhnk-threedi-tools is not part of python installation


notebook_data = setup_notebook()


import geopandas as gpd
import hhnk_research_tools as hrt
import ipywidgets as widgets
import matplotlib.pyplot as plt
import pandas as pd

import hhnk_threedi_tools as htt

# import hhnk_threedi_tools.core.climate_scenarios as hrt_climate
import hhnk_threedi_tools.core.climate_scenarios.maskerkaart as maskerkaart
import hhnk_threedi_tools.core.climate_scenarios.peilgebieden as peilgebieden
import hhnk_threedi_tools.core.climate_scenarios.ruimtekaart as ruimtekaart
import hhnk_threedi_tools.core.climate_scenarios.schadekaart as schadekaart
from hhnk_threedi_tools import Folders
from hhnk_threedi_tools.core.climate_scenarios.interpolate_rasters import main_interpolate_rasters
from hhnk_threedi_tools.core.climate_scenarios.klimaatsommen_prep import KlimaatsommenPrep
from hhnk_threedi_tools.core.climate_scenarios.maskerkaart_raster import rasterize_maskerkaart
from hhnk_threedi_tools.core.climate_scenarios.schadekaart_peilgebieden import maak_schade_polygon

# Folders inladen
folder = Folders(notebook_data["polder_folder"])

# Of handmatig;
# folder=Folders(r"E:\02.modellen\model_test_v2")

# %% [markdown]
# ## Selectie neerslagzone en dem


# %%
def item_layout(width="95%", grid_area="", **kwargs):
    return widgets.Layout(
        width=width, grid_area=grid_area, **kwargs
    )  # override the default width of the button to 'auto' to let the button grow


output_folder_options = [""] + folder.threedi_results.batch.content

# output_folder_label = widgets.Label('Selecteer output folder:', layout=item_layout(grid_area='output_folder_label'))
output_folder_box = widgets.Select(
    options=output_folder_options,
    rows=len(output_folder_options),
    disabled=False,
    layout=item_layout(grid_area="output_folder_box"),
)


# Set dem path
folder.model.set_modelsplitter_paths()
dem = folder.model.schema_1d2d_ggg.rasters.dem
dem_path_dropdown = widgets.Select(
    options=[
        getattr(folder.model, i).rasters.dem
        for i in folder.model.schema_list
        if getattr(folder.model, i).rasters.dem.exists()
    ],
    disabled=False,
    layout=item_layout(grid_area="dem_path_dropdown"),
)

# Display precipitation zones
polder_shape = folder.source_data.polder_polygon.load()

precip_zones_raster = hrt.get_pkg_resource_path(package_resource=htt.resources, name="precipitation_zones_hhnk.tif")
precip_zones_raster = hrt.Raster(precip_zones_raster)
neerslag_array = precip_zones_raster.get_array(band_count=3)


freqs_xlsx = hrt.get_pkg_resource_path(package_resource=htt.resources, name="precipitation_frequency.xlsx")
freqs = pd.read_excel(freqs_xlsx, engine="openpyxl")

fig, ax = plt.subplots(figsize=(8, 8))
ax.imshow(neerslag_array, extent=precip_zones_raster.metadata.bounds)
polder_shape.plot(ax=ax, color="red")


precipitation_zone_box = widgets.Select(
    options=["hevig", "debilt"],
    rows=2,
    disabled=False,
    value=None,
    layout=item_layout(grid_area="precipitation_zone_box"),
)

print("Selecteer map met batch resultaten")
display(output_folder_box)

print("Selecteer neerslagzone")
display(precipitation_zone_box)
if dem.exists():
    print(f"Geselecteerd DEM bestand: {dem}")
else:
    print("Selecteer DEM")
    display(dem_path_dropdown)

# %% [markdown]
# ## Lokaliseren polder folder

# %%
if output_folder_box.value == "":
    raise Exception("Select batch folder")

batch_fd = folder.threedi_results.batch[output_folder_box.value]

# make sure we have a dem selected
if not dem.exists():
    if dem_path_dropdown.value == "":
        raise ValueError("Select dem")
    else:
        dem = hrt.Raster(dem_path_dropdown.value)

if dem.metadata.pixel_width != 0.5:
    new_dem_path = batch_fd.downloads.full_path(f"{dem.stem}_50cm.tif")
    dem = hrt.Raster(new_dem_path)


df = pd.DataFrame(batch_fd.downloads.names, columns=["dl_name"])
for dl_name in batch_fd.downloads.names:
    df.loc[df["dl_name"] == dl_name, "depth_max"] = getattr(batch_fd.downloads, dl_name).depth_max.base
    df.loc[df["dl_name"] == dl_name, "damage_total"] = getattr(batch_fd.downloads, dl_name).damage_total.base


## %%

freqs = freqs[["dl_name", "freq_{}_jaar".format(precipitation_zone_box.value)]]
freqs.rename(
    {"freq_{}_jaar".format(precipitation_zone_box.value): "freq_jaar"},
    axis=1,
    inplace=True,
)

df_freqs = df.merge(freqs, on="dl_name")

## %% Aanmaken of laden peilgebieden polygon
if not folder.source_data.peilgebieden.peilgebieden.exists():
    fixeddrainage = folder.source_data.datachecker.load("fixeddrainagelevelarea")

    # Als het vastloopt:
    # fixeddrainage.pop("created")
    # fixeddrainage.pop("end")
    # fixeddrainage.pop("start")

    fixeddrainage.to_file(folder.source_data.peilgebieden.peilgebieden.base)
    print(f"Peilgebieden shapefile aangemaakt: {folder.source_data.peilgebieden.peilgebieden.name}")
else:
    print(f"Peilgebieden shapefile gevonden: {folder.source_data.peilgebieden.peilgebieden.name}")

# %% [markdown]
# ## Input klaarzetten

# %%
klimaatsommen_prep = KlimaatsommenPrep(
    folder=folder,
    batch_name=batch_fd.name,
    cfg_file="cfg_lizard.cfg",
    landuse_file=r"\\corp.hhnk.nl\data\Hydrologen_data\Data\01.basisgegevens\rasters\landgebruik\landuse2019_tiles\combined_rasters.vrt",
    # landuse_file = folder.model.schema_base.rasters.landuse,
    min_block_size=2**11,  # 2048, high is faster but requires more RAM.
    use_aggregate=False,
    verify=True,
    old_wlvl=True,  # wvg 2025-01; zet naar True om de oude (lizard) wdepth berekening te gebruiken
)

klimaatsommen_prep.run(gridgpkg=True, wlvl_wdepth=True, create_wdepth=True, dmg=True, overwrite=False)

# %% [markdown]
# ## Maskerkaart aanmaken

# %%
# Aanmaken polygon van maskerkaart
maskerkaart.command(
    path_piek=batch_fd.downloads.piek_ghg_T1000.netcdf.base,
    path_blok=batch_fd.downloads.blok_ghg_T1000.netcdf.base,
    path_out=batch_fd.output.maskerkaart.base,
)

# Omzetten polygon in raster voor diepteraster
mask_depth = rasterize_maskerkaart(
    input_file=batch_fd.output.maskerkaart.base,
    mask_plas_path=batch_fd.output.mask_depth_plas,
    mask_overlast_path=batch_fd.output.mask_depth_overlast,
    meta=batch_fd.downloads.piek_glg_T10.depth_max.metadata,
)

# Omzetten polygon in raster voor schaderaster (kan verschillen van diepte met andere resolutie)
mask_damage = rasterize_maskerkaart(
    input_file=batch_fd.output.maskerkaart.path,
    mask_plas_path=batch_fd.output.mask_damage_plas,
    mask_overlast_path=batch_fd.output.mask_damage_overlast,
    meta=batch_fd.downloads.piek_glg_T10.damage_total.metadata,
)

# %% [markdown]
# ## Peilgebieden rasterizeren

# %%
for raster_type, raster_name in zip(["depth_max", "damage_total"], ["depth", "damage"]):
    peilgebieden.rasterize_peilgebieden(
        input_raster=hrt.Raster(df.iloc[0][raster_type]),
        output_file=getattr(batch_fd.output.temp, f"peilgebieden_{raster_name}"),
        input_peilgebieden=folder.source_data.peilgebieden.peilgebieden,
        output_peilgebieden=batch_fd.output.temp.peilgebieden,
        mask_file=batch_fd.output.maskerkaart,
        overwrite=False,
    )

# %% [markdown]
# ## Ruimtekaart aanmaken

# %%
if not batch_fd.output.ruimtekaart.exists():
    # if True:
    ruimtekaart.create_ruimtekaart(
        pgb_path=batch_fd.output.temp.peilgebieden,
        output_path=batch_fd.output.ruimtekaart.base,
        batch_fd=batch_fd,
    )
    print("Ruimtekaart created")

# %% [markdown]
# ## Interpoleren van diepterasters

# %%
diepte_rasters = df_freqs["depth_max"].values
frequenties = df_freqs["freq_jaar"].values


for T in [10, 100, 1000]:
    output_file = getattr(batch_fd.output, f"depth_T{T}_totaal")

    # Voor de gegeven herhalingstijd, interpoleer de rasters.
    main_interpolate_rasters(
        T=T,
        output_file=output_file,
        rasters=diepte_rasters,
        frequenties=frequenties,
        output_nodata=-9999.00,
        dem_raster=dem,
        extra_nodata_value=0,
    )

# %% [markdown]
# #### Plas en overlast kaarten maken

# %%
for T in [10, 100, 1000]:
    input_raster = getattr(batch_fd.output, f"depth_T{T}_totaal")

    # Creeer masker voor plas en overlast
    for mask_type in ["plas", "overlast"]:
        mask = getattr(batch_fd.output, f"mask_depth_{mask_type}")

        output_raster = getattr(batch_fd.output, f"depth_T{T}_{mask_type}")

        if not output_raster.exists():
            raster_array = input_raster._read_array()
            mask_array = mask._read_array()

            raster_array[~(mask_array == 1)] = input_raster.nodata  # Maskeer
            hrt.save_raster_array_to_tiff(
                output_file=output_raster.path,
                raster_array=raster_array,
                nodata=input_raster.nodata,
                metadata=input_raster.metadata,
            )
            print(f"{output_raster.name} created")

        else:
            print(f"{output_raster.name} already exists")
raster_array = None
mask_array = None

# %% [markdown]
# ## Schadekaart maken

# %%
dv = 0.04  # discontovoet [%]
n = 50  # investeringstermijn [jaren]
schade_rasters = df_freqs["damage_total"].values
frequencies = df_freqs["freq_jaar"].values
output_raster = batch_fd.output.cw_schade_totaal

if not output_raster.exists():
    schadekaart.main_maak_schadekaart(
        output_raster=output_raster,
        schade_rasters=schade_rasters,
        frequencies=frequencies,
        output_nodata=0,
        dv=dv,
        n=n,
    )

    print(f"{output_raster.name} created")
else:
    print(f"{output_raster.name} already exists")

# %% [markdown]
# #### Maak masker voor plas en overlast

# %%
input_raster = hrt.Raster(batch_fd.output.cw_schade_totaal.path)

# Creeer masker voor plas en overlast
for mask_type in ["plas", "overlast"]:
    mask = hrt.Raster(getattr(batch_fd.output, f"mask_damage_{mask_type}").path)

    output_raster = getattr(batch_fd.output, f"cw_schade_{mask_type}")

    if not output_raster.exists():
        raster_array = input_raster._read_array()
        mask_array = mask._read_array()

        raster_array[~(mask_array == 1)] = input_raster.nodata
        hrt.save_raster_array_to_tiff(
            output_file=output_raster,
            raster_array=raster_array,
            nodata=input_raster.nodata,
            metadata=input_raster.metadata,
        )
        print(f"{output_raster.name} created")

    else:
        print(f"{output_raster.name} already exists")
raster_array = None
mask_array = None

# %% [markdown]
# ## Bereken schade per gebied

# %%
schade_gdf = batch_fd.output.temp.peilgebieden.load()
labels_raster = batch_fd.output.temp.peilgebieden_damage
labels_index = schade_gdf["index"].values

output_file = batch_fd.output.schade_peilgebied


# Bereken totale schade per peilgebied voor de twee gemaskerde schaderasters.
for mask_type, mask_name in zip(["plas", "overlast"], ["mv", "ws"]):
    schade_raster = getattr(batch_fd.output, f"cw_schade_{mask_type}")

    # Calculate sum per region
    accum = schade_raster.sum_labels(labels_raster=labels_raster, labels_index=labels_index)

    schade_gdf[f"cw_{mask_name}"] = accum

schade_gdf["cw_tot"] = schade_gdf["cw_ws"] + schade_gdf["cw_mv"]

# schade_gdf = schade_gdf.loc[schade_gdf['cw_tot'] > 0.0]


schade_per_polder = (
    schade_gdf[["name", "cw_tot", "cw_ws", "cw_mv"]].groupby("name").sum().sort_values(by="cw_ws", ascending=False)
)

# Opslaan naar shapefile en csv
schade_gdf.to_file(output_file.base)
schade_per_polder.to_csv(batch_fd.output.schade_polder.base)

# %% [markdown]
# # Schade corrigeren
# Als er schade op treedt op plekken waar dit niet logisch is, kan je een shapefile aanmaken die we vervolgens uit het raster zullen knippen. Maak hiervoor het volgende bestand aan:
# .\01. DAMO HDB en Datachecker\peilgebieden\geen_schade.shp

# %%
raster = hrt.Raster(str(batch_fd.output.cw_schade_plas))

dv = 0.04  # discontovoet [%]
n = 50  # investeringstermijn [jaren]
cw_factor = (1 - (1 - dv) ** n) / dv
pixel_factor = raster.metadata["pixel_width"] ** 2 / 0.25  # niet nodig als resolutie goed staat

# %% [markdown]
# ## Verwijder onrealistische schades
#

# %%
maskerkaart2 = gpd.read_file(str(folder.source_data.peilgebieden.geen_schade))  # load maskerkaart (geen_schade.shp)

maskerkaart_union = maskerkaart2.buffer(0.1).unary_union.buffer(-0.1)  # make one geometry from gdf.

# Rasterize polygon
maskerkaart_union = gpd.GeoDataFrame(geometry=[maskerkaart_union])
# Voeg kolom toe aan gdf, deze waarden worden in het raster gezet.
maskerkaart_union["val"] = 1

mask = hrt.gdf_to_raster(
    maskerkaart_union, value_field="val", raster_out="", nodata=0, metadata=damage_meta, epsg=28992, driver="MEM"
)
mask = mask > 0

# %%
for mask_type, mask_name in zip(["plas", "overlast"], ["mv", "ws"]):
    schade_raster = getattr(batch_fd.output, f"cw_schade_{mask_type}")
    output = batch_fd.output.full_path(f"cw_schade_{mask_type}_correctie.tif")
    array = schade_raster.get_array()
    array[mask] = raster.nodata
    hrt.save_raster_array_to_tiff(output, array, raster.nodata, raster.metadata)

# %%
schade_raster_corr_file = {
    "plas": str(batch_fd.output.cw_schade_plas_corr),
    "overlast": str(batch_fd.output.cw_schade_overlast_corr),
}
schades, schade_per_polder = maak_schade_polygon(
    peilgebieden_file=folder.source_data.peilgebieden.peilgebieden,
    schade_raster_file=schade_raster_corr_file,
    pixel_factor=pixel_factor,
    output_schade_file=str(batch_fd.output.schade_peilgebied_corr),
    output_polder_file=str(batch_fd.output.schade_polder_corr),
)

# %% [markdown]
# ## Interpoleren waterstandrasters

# %%
wlvl_rasters = df_freqs["wlvl_max"].values
frequenties = df_freqs["freq_jaar"].values


for T in [10, 100, 1000]:
    output_file = getattr(batch_fd.output, f"wlvl_T{T}_totaal")

    # Voor de gegeven herhalingstijd, interpoleer de rasters.
    main_interpolate_rasters(
        T=T,
        output_file=output_file,
        rasters=wlvl_rasters,
        frequenties=frequenties,
        output_nodata=-9999.00,
        dem_raster=dem,
        min_value=None,
        extra_nodata_value=0,
    )
