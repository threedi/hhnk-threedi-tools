# %%
class KlimaatsommenSettings:
    def __init__(self, caller, folder):
        self.caller = caller
        self.folder = folder

        self.df_freqs_raw = self.load_freqs()

        # Note that this in theory can be not the dem used in the gxg model.
        self.dem = self.folder.model.schema_base.rasters.dem_50cm

        # Peilgebieden
        self.pgb = self.folder.source_data.peilgebieden.peilgebieden
        # Create from datachecker if not available.
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
        freqs_xlsx = hrt.get_pkg_resource_path(package_resource=htt.resources, name="precipitation_frequency.xlsx")
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
