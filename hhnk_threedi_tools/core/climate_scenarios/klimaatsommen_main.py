# %%
import os
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import sys
import importlib.resources as pkg_resources  # Load resource from package
import ipywidgets as widgets

import hhnk_threedi_tools as htt
import hhnk_research_tools as hrt

# import hhnk_threedi_tools.core.climate_scenarios as hrt_climate
import hhnk_threedi_tools.core.climate_scenarios.maskerkaart as maskerkaart
import hhnk_threedi_tools.core.climate_scenarios.ruimtekaart as ruimtekaart
from hhnk_threedi_tools.core.climate_scenarios.interpolate_rasters import (
    main_interpolate_rasters,
)
import hhnk_threedi_tools.core.climate_scenarios.schadekaart as schadekaart
import hhnk_threedi_tools.core.climate_scenarios.peilgebieden as peilgebieden
from hhnk_threedi_tools.core.climate_scenarios.schadekaart_peilgebieden import maak_schade_polygon
from hhnk_threedi_tools.core.climate_scenarios.maskerkaart_raster import rasterize_maskerkaart
from hhnk_threedi_tools.core.climate_scenarios.klimaatsommen_prep import KlimaatsommenPrep

plt.ioff() #turn off inline plots, only show when asked
# Folders inladen
# folder = Folders(notebook_data['polder_folder'])

# Of handmatig;
# folder=Folders(r"E:\02.modellen\model_test_v2")
folder = htt.Folders(r"C:\Users\wiets\Documents\GitHub\hhnk-threedi-tools\tests\data\model_test")

# %%


class KlimaatsommenSettings():
    def __init__(self, caller):
        self.caller = caller
        

    def dem_value(self):
        """Get dem used in ggg model"""
        self.folder.model.set_modelsplitter_paths()
        dem_file = self.folder.model.settings_df.loc["1d2d_ggg", "dem_file"]
        dem = self.folder.model.schema_base.full_path(dem_file)
        return dem


class KlimaatsommenMain():
    def __init__(self, folder:htt.Folders):
        self.folder = folder
        self.widgets = KlimaatsommenWidgets(self.folder)
        self.settings = KlimaatsommenSettings(self,)

class KlimaatsommenWidgets():
    """Widgets die helpen bij inputselectie."""
    def __init__(self, folder):
        self.folder = folder

        #Output folder
        output_folder_options = [""] + self.folder.threedi_results.batch.revisions
        self.output_folder_label = widgets.Label('Selecteer batch folder:', 
                                                 layout=self.item_layout(grid_area='output_folder_label'))
        self.output_folder_box = widgets.Select(
                                    options=output_folder_options,
                                    rows=len(output_folder_options),
                                    disabled=False,
                                    layout=self.item_layout(grid_area="output_folder_box"),
                                )
        
        #Neerslagzone
        self.precipitation_zone_label = widgets.Label('Selecteer neerslagzone:', 
                                                 layout=self.item_layout(grid_area='precipitation_zone_label'))
 
        self.precipitation_zone_box = widgets.Select(
                                    options=["hevig", "debilt"],
                                    rows=2,
                                    disabled=False,
                                    value=None,
                                    layout=self.item_layout(grid_area="precipitation_zone_box"),
                                )
        
        self.dem_label = widgets.Label("DEM:",
                                    layout=self.item_layout(grid_area="dem_label"),)
        self.dem_text = widgets.Text(self.dem_value().base.split(self.folder.parent.as_posix())[-1],
                                     disabled=True,
                                     layout=self.item_layout(grid_area="dem_text"),)
        

        self.precip_figure = widgets.Output(layout=self.item_layout(grid_area="precip_figure"))

        self.fig=self.create_precip_figure()
        with self.precip_figure:
            plt.show(self.fig)
        
    def create_precip_figure(self):
        polder_shape = self.folder.source_data.polder_polygon.load()

        precip_zones_raster = hrt.get_pkg_resource_path(package_resource=htt.resources, 
                            name="precipitation_zones_hhnk.tif")
        precip_zones_raster = hrt.Raster(precip_zones_raster)
        neerslag_array = precip_zones_raster.get_array(band_count=3)

        fig, ax = plt.subplots(figsize=(8, 8))
        ax.imshow(neerslag_array, extent=precip_zones_raster.metadata.bounds)
        polder_shape.plot(ax=ax, color='red')
        return fig
    


    def item_layout(self, width="95%", grid_area="", **kwargs):
        return widgets.Layout(
            width=width, grid_area=grid_area, **kwargs
        )  # override the default width of the button to 'auto' to let the button grow


    def gui(self):
        return widgets.GridBox(
            children=[
                self.precip_figure,
                self.output_folder_label,
                self.output_folder_box,
                self.precipitation_zone_label,
                self.precipitation_zone_box,
                self.dem_label,
                self.dem_text,
            ],
            layout=widgets.Layout(
                width="100%",
                grid_row_gap="200px 200px 200px 200px",
                grid_template_rows="auto auto auto auto",
                grid_template_columns="20% 80%",
                grid_template_areas="""
                    'precip_figure precip_figure'
                    'output_folder_label .'
                    'output_folder_box output_folder_box'
                    'precipitation_zone_label precipitation_zone_box'
                    'dem_label dem_text'
                    """,
            ),
        )
    
    @property
    def batch_folder(self):
        selected_batch = self.output_folder_box.value
        if selected_batch != "":
            return self.folder.threedi_results.batch[selected_batch]
        else:
            raise Exception("Select batch folder")
    @property
    def precipitation_zone(self):
        selected_zone = self.precipitation_zone_box.value
        if selected_zone != "":
            return selected_zone
        else:
            raise Exception("Select neerslagzone") 
    
self = KlimaatsommenMain(folder=folder)
self.widgets.gui()


# %%



freqs_xlsx = hrt.get_pkg_resource_path(package_resource=htt.resources, 
                      name="precipitation_frequency.xlsx")
freqs = pd.read_excel(freqs_xlsx, engine="openpyxl")




# %%


if dem.metadata.pixel_width != 0.5:
    new_dem_path = batch_fd.downloads.full_path(f"{dem.stem}_50cm.tif")
    dem = hrt.Raster(new_dem_path)


df = pd.DataFrame(batch_fd.downloads.names, columns=["dl_name"])
for dl_name in batch_fd.downloads.names:
    df.loc[df["dl_name"] == dl_name, "depth_max"] = getattr(
        batch_fd.downloads, dl_name
    ).depth_max.base
    df.loc[df["dl_name"] == dl_name, "damage_total"] = getattr(
        batch_fd.downloads, dl_name
    ).damage_total.base


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
    fixeddrainage.to_file(folder.source_data.peilgebieden.peilgebieden.base)
    print(
        f"Peilgebieden shapefile aangemaakt: {folder.source_data.peilgebieden.peilgebieden.name}.shp"
    )
else:
    print(
        f"Peilgebieden shapefile gevonden: {folder.source_data.peilgebieden.peilgebieden.name}.shp"
    )