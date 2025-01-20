"""Folderstructuur voor de modelbuilder."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Union

import hhnk_research_tools as hrt

from hhnk_threedi_tools.core.folders import Folders


@dataclass
class SourcePaths:
    """Locaties van inputbestanden voor het maken van de rasters"""

    dem_path: Union[str, Path, hrt.Raster]
    glg_path: Union[str, Path, hrt.Raster]
    ggg_path: Union[str, Path, hrt.Raster]
    ghg_path: Union[str, Path, hrt.Raster]
    infiltration_path: Union[str, Path, hrt.Raster]
    friction_path: Union[str, Path, hrt.Raster]
    landuse_path: Union[str, Path, hrt.Raster]
    polder_path: str
    waterdeel_path: str

    def __post_init__(self):
        """
        dem_path (str): raster source van DEM
        glg_path (str): raster source van glg
        ggg_path (str): raster source van ggg
        ghg_path (str): raster source van ghg
        polder_path (str): polder met bounds van de output
        waterdeel_path (str): shapfile met waterdeel van polder
        """

        self.dem = hrt.Raster(self.dem_path)
        self.glg = hrt.Raster(self.glg_path)
        self.ggg = hrt.Raster(self.ggg_path)
        self.ghg = hrt.Raster(self.ghg_path)
        self.infiltration = hrt.Raster(self.infiltration_path)
        self.friction = hrt.Raster(self.friction_path)
        self.landuse = hrt.Raster(self.landuse_path)
        self.polder = hrt.File(self.polder_path)
        self.waterdeel = hrt.File(self.waterdeel_path)
        self.verify()

    def verify(self):
        """Verify check if inputs exist"""
        filesnotfound = []
        for f in [
            self.dem,
            self.glg,
            self.ggg,
            self.ghg,
            self.infiltration,
            self.friction,
            self.landuse,
            self.polder,
            self.waterdeel,
        ]:
            try:
                if not f.exists():
                    filesnotfound.append(f.base)
            except RuntimeError as e:
                raise Exception(f"{f.base}") from e

        if filesnotfound:
            raise FileNotFoundError(f"{filesnotfound} not found")

        return True


class FoldersModelbuilder:
    """Folder structuur om de rasters in op te slaan.

    source_paths : SourcePaths
        bronrasters en shapes
    dst_base : str
        path to output folder
    """

    def __init__(self, folder: Folders, source_paths: SourcePaths):
        self.src = source_paths
        self.dst = folder.model.calculation_rasters
