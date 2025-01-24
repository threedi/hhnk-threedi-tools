# %%
#FIXME in ontwikkeling
import os
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import sys
import ipywidgets as widgets
import importlib.resources as resources

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

        self.df_freqs_raw = self.load_freqs()

        #Note that this in theory can be not the dem used in the gxg model.
        self.dem = self.folder.model.schema_base.rasters.dem_50cm

        #Peilgebieden
        self.pgb = self.folder.source_data.peilgebieden.peilgebieden
        #Create from datachecker if not available.
        if not self.pgb.exists():
            fixeddrainage = self.folder.source_data.datachecker.load("fixeddrainagelevelarea")
            fixeddrainage.to_file(self.pgb.base)


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
        selected_batch = self.get_full_path(self.caller.widgets.batch_folder_box.value)
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

        freqs = self.df_freqs_raw[["dl_name", "freq_{}_jaar".format(self.precipitation_zone)]].copy()
        freqs.rename(
            {"freq_{}_jaar".format(self.precipitation_zone): "freq_jaar"},
            axis=1,
            inplace=True,
        )
        return df.merge(freqs, on="dl_name")

    
    def update_settings_after_selection(self, val):
        self.df_freqs = self.create_df_freqs()

    def get_full_path(self, tail):
        fullpath = Path(str(hrt.Folder(self.caller.widgets.folder_path_text.value).full_path(tail)))
        if fullpath.exists():
            return fullpath
        else:
            return tail
        
    @property
    def landuse(self):
        return self.get_full_path(self.caller.widgets.wss_landuse_text.value)


class KlimaatsommenWidgets():
    """Widgets die helpen bij inputselectie."""
    def __init__(self, caller):
        self.caller = caller
        self.folder = self.caller.settings.folder

        #Output folder
        self.folder_path_label = widgets.Label('Geselecteerde folder:', 
                                                 layout=self.item_layout(grid_area='folder_path_label'))
        self.folder_path_text = widgets.Text(self.folder.base,
                                             disabled=True,
                                                 layout=self.item_layout(grid_area='folder_path_text'))
                                    

        batch_folder_options = [""] + [hrt.File(i).view_name_with_parents(2) for i in self.folder.threedi_results.batch.revisions]
        self.batch_folder_label = widgets.HTML('<b>Selecteer batch folder:</b>', 
                                                 layout=self.item_layout(grid_area='batch_folder_label'))
        self.batch_folder_box = widgets.Select(
                                    options=batch_folder_options,
                                    rows=len(batch_folder_options),
                                    disabled=False,
                                    layout=self.item_layout(grid_area="batch_folder_box"),
                                )
        
        #Neerslagzone
        self.precipitation_zone_label = widgets.HTML('<b>Selecteer neerslagzone:</b>', 
                                                 layout=self.item_layout(grid_area='precipitation_zone_label'))
 
        self.precipitation_zone_box = widgets.Select(
                                    options=["hevig (blauw)", "debilt (groen)"],
                                    rows=2,
                                    disabled=True,
                                    value=None,
                                    layout=self.item_layout(grid_area="precipitation_zone_box"),
                                )
        
        self.dem_label = widgets.Label("DEM:",
                                    layout=self.item_layout(grid_area="dem_label"),)

        self.dem_text = widgets.Text(self.caller.settings.dem.view_name_with_parents(3),
                                     disabled=True,
                                     layout=self.item_layout(grid_area="dem_text"),)


        self.pgb_label = widgets.Label("Peilgebieden:",
                                    layout=self.item_layout(grid_area="pgb_label"),)

        self.pgb_text = widgets.Text(self.caller.settings.pgb.view_name_with_parents(2),
                                     disabled=True,
                                     layout=self.item_layout(grid_area="pgb_text"),)

        self.wss_label = widgets.HTML("Waterschadeschatter instellingen",
                                       layout=self.item_layout(grid_area="wss_label"))
        
        self.wss_cfg_label = widgets.Label("Config (default='cfg_lizard.cfg'):",
                                    layout=self.item_layout(grid_area="wss_cfg_label"),)
        cfg_dropdown_options = [i.name for i in resources.files(hrt.waterschadeschatter.resources).glob("*.cfg")]
        self.wss_cfg_dropdown = widgets.Dropdown(value="cfg_lizard.cfg",
                                                 options=cfg_dropdown_options,
                                    layout=self.item_layout(grid_area="wss_cfg_dropdown"))

        self.wss_landuse_label = widgets.Label("Landuse:",
                                    layout=self.item_layout(grid_area="wss_landuse_label"),)
        
        landuse_path = folder.model.schema_base.rasters.landuse.view_name_with_parents(3)
        self.wss_landuse_text = widgets.Text(value=landuse_path,
                                    layout=self.item_layout(grid_area="wss_landuse_text"))


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
                self.folder_path_label,
                self.folder_path_text,
                self.batch_folder_label,
                self.batch_folder_box,
                self.precipitation_zone_label,
                self.precipitation_zone_box,
                self.dem_label,
                self.dem_text,
                self.pgb_label,
                self.pgb_text,
                self.wss_label,
                self.wss_cfg_label,
                self.wss_cfg_dropdown,
                self.wss_landuse_label,
                self.wss_landuse_text,
            ],
            layout=widgets.Layout(
                width="100%",
                grid_row_gap="200px 200px 200px 200px",
                grid_template_rows="auto auto auto auto",
                grid_template_columns="30% 70%",
                grid_template_areas="""
                    'precip_figure precip_figure'
                    'folder_path_label folder_path_text'
                    'batch_folder_label batch_folder_box'
                    'precipitation_zone_label precipitation_zone_box'
                    'dem_label dem_text'
                    'pgb_label pgb_text'
                    'wss_label wss_label'
                    'wss_cfg_label wss_cfg_dropdown'
                    'wss_landuse_label wss_landuse_text'
                    """,
            ),
        )


class KlimaatsommenMain():
    def __init__(self, folder:htt.Folders, testing=False):
        #For pytests we disable a couple loops to speed up the test.
        self.testing = testing

        self.settings = KlimaatsommenSettings(self, folder)
        self.widgets = KlimaatsommenWidgets(self)

        #widget Interaction
        self.widgets.precipitation_zone_box.observe(self.settings.update_settings_after_selection, "value")

        def enable_neerslagzone(val):
            if val["new"] != "":
                self.widgets.precipitation_zone_box.disabled = False
            else:
                self.widgets.precipitation_zone_box.disabled = True
        self.widgets.batch_folder_box.observe(enable_neerslagzone, "value")

    @property
    def batch_fd(self):
        return self.settings.batch_fd

    def step1_maskerkaart(self):
        maskerkaart.command(
            path_piek=self.batch_fd.downloads.piek_ghg_T1000.netcdf.base,
            path_blok=self.batch_fd.downloads.blok_ghg_T1000.netcdf.base,
            path_out=self.batch_fd.output.maskerkaart.base,
        )

        # Omzetten polygon in raster voor diepte en schaderaster
        # (kan verschillen van diepte met andere resolutie)
        for rtype, rname in zip(
            ["depth_max", "damage_total"], 
            ["depth", "damage"]
        ):
            masker = rasterize_maskerkaart(
                input_file=self.batch_fd.output.maskerkaart.base,
                mask_plas_raster=getattr(self.batch_fd.output, f"mask_{rname}_plas"),
                mask_overlast_raster=getattr(self.batch_fd.output, f"mask_{rname}_overlast"),
                meta=getattr(self.batch_fd.downloads.piek_glg_T10, rtype).metadata,
            )
            if self.testing:
                break
    
    def step2_rasterize_pgb(self):
        """Peilgebieden rasterizen"""
        for raster_type, raster_name in zip(
            ["depth_max", "damage_total"], 
            ["depth", "damage"]
        ):
            peilgebieden.rasterize_peilgebieden(
                input_raster=hrt.Raster(df.iloc[0][raster_type]),
                output_file=getattr(self.batch_fd.output.temp, f"peilgebieden_{raster_name}"),
                input_peilgebieden=folder.source_data.peilgebieden.peilgebieden,
                output_peilgebieden=self.batch_fd.output.temp.peilgebieden,
                mask_file=self.batch_fd.output.maskerkaart,
                overwrite=False,
            )

if __name__ == "__main__": 
    self = KlimaatsommenMain(folder=folder)
    display(self.widgets.gui())


    #FIXME Voor testen standaard selecteren
    self.widgets.batch_folder_box.value = self.widgets.batch_folder_box.options[1]
    self.widgets.precipitation_zone_box.value = self.widgets.precipitation_zone_box.options[1]


# %% Aanmaken polygon van maskerkaart




