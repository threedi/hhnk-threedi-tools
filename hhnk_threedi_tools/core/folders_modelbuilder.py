"""Folderstructuur voor de modelbuilder."""
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Union

import hhnk_research_tools as hrt


@dataclass
class SourcePaths:
    """Locaties van inputbestanden voor het maken van de rasters"""

    dem_path: Union[str, Path, hrt.Raster]
    glg_path: Union[str, Path, hrt.Raster]
    ggg_path: Union[str, Path, hrt.Raster]
    ghg_path: Union[str, Path, hrt.Raster]
    polder_path: str
    watervlakken_path: str

    def __post_init__(self):
        """
        dem_path (str): raster source van DEM
        glg_path (str): raster source van glg
        ggg_path (str): raster source van ggg
        ghg_path (str): raster source van ghg
        polder_path (str): polder met bounds van de output
        watervlakken_path (str): shapfile met watervlakken van polder
        """

        self.dem = hrt.Raster(self.dem_path)
        self.glg = hrt.Raster(self.glg_path)
        self.ggg = hrt.Raster(self.ggg_path)
        self.ghg = hrt.Raster(self.ghg_path)
        self.polder = hrt.File(self.polder_path)
        self.watervlakken = hrt.File(self.watervlakken_path)
        self.verify()

    def verify(self):
        """Verifiy check if inputs exist"""
        for f in [self.dem, self.glg, self.ggg, self.ghg, self.polder, self.watervlakken]:
            if not f.exists():
                raise FileNotFoundError(f"{f} not found")
        return True


class FoldersModelbuilder:
    """Folder structuur om de rasters in op te slaan.

    dst_base (str): path to output folder
    source_paths (SourcePaths): bronrasters en shapes
    """

    def __init__(self, dst_path: str, source_paths: SourcePaths):
        self.src = source_paths
        self.dst = self.DestPaths(dst_path)

    class DestPaths(hrt.Folder):
        """Output rasters, heeft temp rasters om de berekening te doen."""

        def __init__(self, base):
            super().__init__(base=os.path.join(base, ""))
            self.tmp = self.TempPaths(base)

            self.dem = self.full_path("dem.tif")
            self.glg = self.full_path("glg.tif")
            self.ggg = self.full_path("ggg.tif")
            self.ghg = self.full_path("ghg.tif")

        class TempPaths(hrt.Folder):
            """temp rasters allemaal met dezelfde extent."""

            def __init__(self, base):
                super().__init__(base=os.path.join(base, "tmp"))

                self.dem = self.full_path("dem.vrt")
                self.glg = self.full_path("glg.vrt")
                self.ggg = self.full_path("ggg.vrt")
                self.ghg = self.full_path("ghg.vrt")
                self.polder = self.full_path("polder.tif")
                self.watervlakken = self.full_path("watervlakken.tif")
