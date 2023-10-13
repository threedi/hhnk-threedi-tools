
import os
import hhnk_research_tools as hrt

from hhnk_threedi_tools.variables.api_settings import (
    RAIN_SCENARIOS,
    GROUNDWATER,
    RAIN_TYPES,
)


class ClimateResult(hrt.Folder):
    """Individual batch result with download and output folder"""

    def __init__(self, base, create=False):
        super().__init__(base, create=create)
        self.create_bool = create

        self._downloads = None
        self._output = None


    @property
    def downloads(self):
        if self._downloads is None:
            self._downloads = self.ClimateResultDownloads(self.base, create=self.create_bool)
        return self._downloads
    

    @property
    def output(self):
        if self._output is None:
            self._output = self.ClimateResultDownloads(self.base, create=self.create_bool)
        return self._output
    

    @property
    def structure(self):
        return f"""  
            {self.space}{self.name}
            {self.space}├── downloads
            {self.space}└── output
                """


    class ClimateResultDownloads(hrt.Folder):
        def __init__(self, base, create=False):
            super().__init__(os.path.join(base, "01_downloads"), create=create)

            self.create_bool = create
            # Files
            self._names = None

        @property
        def names(self):
            if self._names is None:
                self._names = self.get_names()
            return self._names

        def get_names(self):
            names = []
            for rain_type in RAIN_TYPES:
                for groundwater in GROUNDWATER:
                    for rain_scenario in RAIN_SCENARIOS:
                        names.append(f"{rain_type}_{groundwater}_{rain_scenario}")

            for name in names:
                setattr(self, name, self.ClimateResultScenario(self.base, name, create=self.create_bool))
            return names


        def __repr__(self):
            return f"""{self.name} @ {self.base}
                        Folders:\t{self.structure}
                        Files:\t{list(self.files.keys())}
                        Groups:\t{list(self.names)}
                    """
        

        class ClimateResultScenario(hrt.Folder):
            """Single scenario with multiple results"""

            def __init__(self, base, name, create=False):
                super().__init__(base, create=create)

                #Add rasters to main downloadfolder
                raster_types = ["depth_max", "damage_total", "wlvl_max"]
                for rastertype in raster_types:
                    self.add_file(rastertype, f"{rastertype}_{name}.tif")
                self.structure_extra = []

                #Add netcdf to subfolders for scenario
                setattr(self, "netcdf", hrt.ThreediResult(self.full_path(name)))
                self.structure_extra = ["netcdf"]

            def __repr__(self):
                return f"""{self.name} @ {self.base}
                            Folders:\t{self.structure_extra}
                            Files:\t{list(self.files.keys())}
                        """

    class ClimateResultOutput(hrt.Folder):
        def __init__(self, base, create):
            super().__init__(os.path.join(base, "02_output_rasters"), create=create)

            # Folders
            self.temp = self.ClimateResultOutputTemp(self.base, create)

            # Files
            self.add_file("maskerkaart", "maskerkaart.shp")
            self.add_file("maskerkaart_diepte_tif", "maskerkaart_diepte.tif")
            self.add_file("maskerkaart_schade_tif", "maskerkaart_schade.tif")
            self.add_file("geen_schade_tif", "geen_schade.tif")
            self.add_file("mask_diepte_plas", "mask_diepte_plas.tif")
            self.add_file("mask_schade_plas", "mask_schade_plas.tif")
            self.add_file("mask_diepte_overlast", "mask_diepte_overlast.tif")
            self.add_file("mask_schade_overlast", "mask_schade_overlast.tif")
            self.add_file("ruimtekaart", "ruimtekaart.shp")
            self.add_file("schade_peilgebied", "schade_per_peilgebied.shp")
            self.add_file("schade_peilgebied_corr", "schade_per_peilgebied_correctie.shp")
            self.add_file("schade_polder", "schade_per_polder.csv")
            self.add_file("schade_polder_corr", "schade_per_polder_correctie.csv")

            self.set_scenario_files()

        def set_scenario_files(self):
            for type_raster, type_raster_name in zip(
                ["wlvl", "depth", "damage"], 
                ["wlvl", "inundatiediepte", "schade"]
            ):
                for masker, masker_name in zip(
                    ["totaal", "plas", "overlast"], 
                    ["", "_plas", "_overlast"]
                ):
                    for return_period in [10, 25, 100, 1000]:
                        self.add_file(
                            objectname=f"{type_raster}_T{return_period}_{masker}",
                            filename=f"{type_raster_name}_T{str(return_period).zfill(4)}{masker_name}.tif",
                        )

            for masker, masker_name in zip(
                ["totaal", "plas", "overlast"], ["", "_plas", "_overlast"]
            ):
                self.add_file(
                    objectname=f"cw_schade_{masker}",
                    filename=f"cw_schade{masker_name}.tif",
                )

                self.add_file(
                    objectname=f"cw_schade_{masker}_corr",
                    filename=f"cw_schade{masker_name}_correctie.tif",
                )

        @property
        def structure(self):
            return f"""  
                {self.space}{self.name}
                {self.space}├── temp
                    """


        class ClimateResultOutputTemp(hrt.Folder):
            def __init__(self, base, create):
                super().__init__(os.path.join(base, "temp"), create=create)

                self.add_file("peilgebieden_diepte", "peilgebieden_diepte.tif")
                self.add_file("peilgebieden_schade", "peilgebieden_schade.tif")
                self.add_file("peilgebieden", "peilgebieden_clipped.shp")


