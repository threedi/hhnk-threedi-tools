# %%
class KlimaatsommenWidgets:
    """Widgets die helpen bij inputselectie."""

    def __init__(self, caller):
        self.caller = caller
        self.folder = self.caller.settings.folder

        # Output folder
        self.folder_path_label = widgets.Label(
            "Geselecteerde folder:", layout=self.item_layout(grid_area="folder_path_label")
        )
        self.folder_path_text = widgets.Text(
            self.folder.base, disabled=True, layout=self.item_layout(grid_area="folder_path_text")
        )

        batch_folder_options = [""] + [
            hrt.File(i).view_name_with_parents(2) for i in self.folder.threedi_results.batch.revisions
        ]
        self.batch_folder_label = widgets.HTML(
            "<b>Selecteer batch folder:</b>", layout=self.item_layout(grid_area="batch_folder_label")
        )
        self.batch_folder_box = widgets.Select(
            options=batch_folder_options,
            rows=len(batch_folder_options),
            disabled=False,
            layout=self.item_layout(grid_area="batch_folder_box"),
        )

        # Neerslagzone
        self.precipitation_zone_label = widgets.HTML(
            "<b>Selecteer neerslagzone:</b>", layout=self.item_layout(grid_area="precipitation_zone_label")
        )

        self.precipitation_zone_box = widgets.Select(
            options=["hevig (blauw)", "debilt (groen)"],
            rows=2,
            disabled=True,
            value=None,
            layout=self.item_layout(grid_area="precipitation_zone_box"),
        )

        self.dem_label = widgets.Label("DEM:", layout=self.item_layout(grid_area="dem_label"))

        self.dem_text = widgets.Text(
            self.caller.settings.dem.view_name_with_parents(3),
            disabled=True,
            layout=self.item_layout(grid_area="dem_text"),
        )

        self.pgb_label = widgets.Label("Peilgebieden:", layout=self.item_layout(grid_area="pgb_label"))

        self.pgb_text = widgets.Text(
            self.caller.settings.pgb.view_name_with_parents(2),
            disabled=True,
            layout=self.item_layout(grid_area="pgb_text"),
        )

        self.wss_label = widgets.HTML(
            "Waterschadeschatter instellingen", layout=self.item_layout(grid_area="wss_label")
        )

        self.wss_cfg_label = widgets.Label(
            "Config (default='cfg_lizard.cfg'):",
            layout=self.item_layout(grid_area="wss_cfg_label"),
        )
        cfg_dropdown_options = [i.name for i in resources.files(hrt.waterschadeschatter.resources).glob("*.cfg")]
        self.wss_cfg_dropdown = widgets.Dropdown(
            value="cfg_lizard.cfg", options=cfg_dropdown_options, layout=self.item_layout(grid_area="wss_cfg_dropdown")
        )

        self.wss_landuse_label = widgets.Label(
            "Landuse:",
            layout=self.item_layout(grid_area="wss_landuse_label"),
        )

        landuse_path = self.folder.model.schema_base.rasters.landuse.view_name_with_parents(3)
        self.wss_landuse_text = widgets.Text(value=landuse_path, layout=self.item_layout(grid_area="wss_landuse_text"))

        self.precip_figure = widgets.Output(layout=self.item_layout(grid_area="precip_figure"))

        self.fig = self.create_precip_figure()
        with self.precip_figure:
            plt.show(self.fig)

    def create_precip_figure(self):
        polder_shape = self.folder.source_data.polder_polygon.load()

        precip_zones_raster = hrt.get_pkg_resource_path(
            package_resource=htt.resources, name="precipitation_zones_hhnk.tif"
        )
        precip_zones_raster = hrt.Raster(precip_zones_raster)

        da_precip = precip_zones_raster.open_rxr()
        neerslag_array = da_precip.transpose("y", "x", "band").data

        fig, ax = plt.subplots(figsize=(6, 6))
        ax.imshow(neerslag_array / 255, extent=precip_zones_raster.metadata.bounds)
        polder_shape.plot(ax=ax, color="red")

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
