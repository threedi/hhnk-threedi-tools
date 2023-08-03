# %%
import os
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import sys
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



class KlimaatsommenSettings():
    def __init__(self, caller, folder):
        self.caller = caller
        self.folder = folder

        self.freqs = self.load_freqs()

        #Note that this in theory can be not the dem used in the gxg model.
        self.dem = self.folder.model.schema_base.rasters.dem_50cm

    # @property
    # def dem(self):
        """Not used currently, but can be used to get dem used in model. """
        # self.folder.model.set_modelsplitter_paths()
        # dem_file = self.folder.model.settings_df.loc["1d2d_ggg", "dem_file"]
        # dem = self.folder.model.schema_base.full_path(dem_file)
        # return dem

    
    @property
    def batch_fd(self):
        """folder class of batch fd, needs to be selected in widget"""
        selected_batch = self.caller.widgets.output_folder_box.value
        if selected_batch != "":
            return self.folder.threedi_results.batch[selected_batch]
        else:
            raise Exception("Select batch folder")
    @property
    def precipitation_zone(self) -> str:
        """hevig or debilt. Needs to be selected in widget"""
        selected_zone = self.caller.widgets.precipitation_zone_box.value
        if selected_zone != "":
            return selected_zone.split(" ")[0]
        else:
            raise Exception("Select neerslagzone") 

    def load_freqs(self):
        freqs_xlsx = hrt.get_pkg_resource_path(package_resource=htt.resources, 
                            name="precipitation_frequency.xlsx")
        freqs = pd.read_excel(freqs_xlsx, engine="openpyxl")
        return freqs[freqs["dl_name"].notna()]


    def create_df_freqs(self):
        """With selected precip zone match frequencies with scenario"""
        downloads = self.batch_fd.downloads

        df = pd.DataFrame(downloads.names, columns=["dl_name"])
        for dl_name in downloads.names:
            df.loc[df["dl_name"] == dl_name, "depth_max"] = getattr(downloads, dl_name).depth_max.base
            df.loc[df["dl_name"] == dl_name, "damage_total"] = getattr(downloads, dl_name).damage_total.base

        freqs = self.freqs[["dl_name", "freq_{}_jaar".format(self.precipitation_zone)]].copy()
        freqs.rename(
            {"freq_{}_jaar".format(self.precipitation_zone): "freq_jaar"},
            axis=1,
            inplace=True,
        )
        return df.merge(freqs, on="dl_name")

    
    def update_settings_after_selection():
        self.df_freqs = self.create_df_freqs()


class KlimaatsommenWidgets():
    """Widgets die helpen bij inputselectie."""
    def __init__(self, caller):
        self.caller = caller
        self.folder = self.caller.settings.folder

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
                                    options=["hevig (blauw)", "debilt (groen)"],
                                    rows=2,
                                    disabled=False,
                                    value=None,
                                    layout=self.item_layout(grid_area="precipitation_zone_box"),
                                )
        
        self.dem_label = widgets.Label("DEM:",
                                    layout=self.item_layout(grid_area="dem_label"),)

        self.dem_text = widgets.Text(self.caller.settings.dem.view_name_with_parents(4),
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

        fig, ax = plt.subplots(figsize=(6, 6))
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


class KlimaatsommenMain():
    def __init__(self, folder:htt.Folders):
        self.settings = KlimaatsommenSettings(self, folder)
        self.widgets = KlimaatsommenWidgets(self)


self = KlimaatsommenMain(folder=folder)
self.widgets.gui()


# %%

# %%






# %%
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


# %%
