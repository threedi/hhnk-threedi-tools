
import os
import hhnk_research_tools as hrt

from hhnk_threedi_tools.variables.api_settings import (
    RAIN_SCENARIOS,
    GROUNDWATER,
    RAIN_TYPES,
    RAW_DOWNLOADS,
)

# Third-party imports
from threedigrid.admin.gridadmin import GridH5Admin
from threedigrid.admin.gridresultadmin import GridH5ResultAdmin


class ClimateResult(hrt.Folder):
    """Individual result with download and output folder"""

    def __init__(self, base, create):
        super().__init__(base, create=create)

        self.downloads = self.ClimateResultDownloads(self.base)
        self.output = self.ClimateResultOutput(self.base)

        # Files
        self.add_file("blok_grid_path", "/01_downloads/blok_ghg_T1000/results_3di.nc")
        self.add_file("blok_admin_path", "/01_downloads/blok_ghg_T1000/gridadmin.h5")
        self.add_file("piek_grid_path", "/01_downloads/piek_ghg_T1000/results_3di.nc")
        self.add_file("piek_admin_path", "/01_downloads/piek_ghg_T1000/gridadmin.h5")

    @property
    def grid_path(self):
        return self.blok_grid_path

    @property
    def admin_path(self):
        return self.blok_admin_path

    def grid(self, _type):
        if _type == "blok":
            return GridH5ResultAdmin(
                self.blok_admin_path.file_path, self.blok_grid_path.file_path
            )
        return GridH5ResultAdmin(
            self.piek_admin_path.file_path, self.piek_grid_path.file_path
        )

    def admin(self):
        return GridH5Admin(self.blok_admin_path.file_path)

    @property
    def structure(self):
        return f"""  
            {self.space}{self.name}
            {self.space}├── downloads
            {self.space}└── output
                """


    class ClimateResultDownloads(hrt.Folder):
        def __init__(self, base):
            super().__init__(os.path.join(base, "01_downloads"))

            # Files
            self.add_file("download_uuid", "download_uuid.csv")
            self.names = GROUNDWATER  # Initializes names.setter

            # for name in RAW_DOWNLOADS:
            #     setattr(self, name, ThreediResult(self.full_path(name)))

            # Files
            self.add_file("blok_grid_path", "/blok_ghg_T1000/results_3di.nc")
            self.add_file("blok_admin_path", "/blok_ghg_T1000/gridadmin.h5")
            self.add_file("piek_grid_path", "/piek_ghg_T1000/results_3di.nc")
            self.add_file("piek_admin_path", "/piek_ghg_T1000/gridadmin.h5")

            for name in self.names:
                setattr(self, name, self.ClimateResultScenario(self.base, name))

        @property
        def names(self):
            return self._names

        @names.setter
        def names(self, groundwater_types=GROUNDWATER):
            names = []
            for rain_type in RAIN_TYPES:
                for groundwater in groundwater_types:
                    for rain_scenario in RAIN_SCENARIOS:
                        names.append(f"{rain_type}_{groundwater}_{rain_scenario}")
            self._names = names

        @property
        def grid_path(self):
            return self.blok_grid_path

        @property
        def admin_path(self):
            return self.blok_admin_path

        def grid(self, _type):
            if _type == "blok":
                return GridH5ResultAdmin(
                    self.blok_admin_path.file_path, self.blok_grid_path.file_path
                )
            return GridH5ResultAdmin(
                self.piek_admin_path.file_path, self.piek_grid_path.file_path
            )

        def admin(self):
            return GridH5Admin(self.blok_admin_path.file_path)

        def __repr__(self):
            return f"""{self.name} @ {self.path}
                        Folders:\t{self.structure}
                        Files:\t{list(self.files.keys())}
                        Layers:\t{list(self.olayers.keys())}
                        Groups:\t{list(self.names)}
                    """


    class ClimateResultOutput(hrt.Folder):
        def __init__(self, base):
            super().__init__(base + "/02_output_rasters")

            # Folders
            self.temp = ClimateResultOutputTemp(self.base)

            # Files
            self.add_file("maskerkaart", "maskerkaart.shp")
            self.add_file("maskerkaart_diepte_tif", "maskerkaart_diepte.tif", "raster")
            self.add_file("maskerkaart_schade_tif", "maskerkaart_schade.tif", "raster")
            self.add_file("geen_schade_tif", "geen_schade.tif", "raster")
            self.add_file("mask_diepte_plas", "mask_diepte_plas.tif", "raster")
            self.add_file("mask_schade_plas", "mask_schade_plas.tif", "raster")
            self.add_file("mask_diepte_overlast", "mask_diepte_overlast.tif", "raster")
            self.add_file("mask_schade_overlast", "mask_schade_overlast.tif", "raster")
            self.add_file("ruimtekaart", "ruimtekaart.shp")
            self.add_file("schade_peilgebied", "schade_per_peilgebied.shp")
            self.add_file("schade_peilgebied_corr", "schade_per_peilgebied_correctie.shp")
            self.add_file("schade_polder", "schade_per_polder.csv")
            self.add_file("schade_polder_corr", "schade_per_polder_correctie.csv")

            self.set_scenario_files()
            self.create(parents=False)  # create outputfolder if parent exists

        def set_scenario_files(self):
            for type_raster, type_raster_name in zip(
                ["depth", "damage"], ["inundatiediepte", "schade"]
            ):
                for masker, masker_name in zip(
                    ["totaal", "plas", "overlast"], ["", "_plas", "_overlast"]
                ):
                    for return_period in [10, 25, 100, 1000]:
                        self.add_file(
                            objectname=f"{type_raster}_T{return_period}_{masker}",
                            filename=f"{type_raster_name}_T{str(return_period).zfill(4)}{masker_name}.tif",
                            ftype="raster",
                        )

            for masker, masker_name in zip(
                ["totaal", "plas", "overlast"], ["", "_plas", "_overlast"]
            ):
                self.add_file(
                    objectname=f"cw_schade_{masker}",
                    filename=f"cw_schade{masker_name}.tif",
                    ftype="raster",
                )

                self.add_file(
                    objectname=f"cw_schade_{masker}_corr",
                    filename=f"cw_schade{masker_name}_correctie.tif",
                    ftype="raster",
                )

        @property
        def structure(self):
            return f"""  
                {self.space}{self.name}
                {self.space}├── temp
                    """


    class ClimateResultOutputTemp(hrt.Folder):
        def __init__(self, base, create):
            super().__init__(os.path.join(base, "temp", create=create))

            self.add_file("peilgebieden_diepte", "peilgebieden_diepte.tif", "raster")
            self.add_file("peilgebieden_schade", "peilgebieden_schade.tif", "raster")
            self.add_file("peilgebieden", "peilgebieden_clipped.shp")


    class ClimateResultScenario(hrt.Folder):
        """Single scenario with multiple results"""

        def __init__(self, base, name):
            super().__init__(base)

            raster_types = ["max_depth", "total_damage", "wlvl_max"]
            for rastertype in raster_types:
                self.add_file(rastertype, f"{rastertype}_{name}.tif", ftype="raster")
            self.structure_extra = []
            # Netcdf for piek_ghg_t1000 and blok_ghg_t1000 for use in ruimtekaart.
            if name in RAW_DOWNLOADS:
                setattr(self, "netcdf", hrt.ThreediResult(self.full_path(name)))
                self.structure_extra = ["netcdf"]

        def __repr__(self):
            return f"""{self.name} @ {self.path}
                        Folders:\t{self.structure_extra}
                        Files:\t{list(self.files.keys())}
                        Layers:\t{list(self.olayers.keys())}
                    """